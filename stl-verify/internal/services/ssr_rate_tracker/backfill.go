package ssr_rate_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BackfillConfig holds configuration for the Backfiller.
type BackfillConfig struct {
	Concurrency int
	Logger      *slog.Logger
}

// Backfiller scans a historical block range for SSR rate changes by reading
// trace files from the raw-data S3 bucket, one S3 partition at a time, so
// memory stays constant regardless of range size. Block timestamps are read
// from the S3 block file only for blocks that actually contain a match, so
// the common (empty) case costs a single S3 GET per block.
type Backfiller struct {
	config   BackfillConfig
	s3Reader outbound.S3Reader
	repo     outbound.SSRRateEventRepository
	bucket   string
	chainID  int64
}

// NewBackfiller creates a new Backfiller, validating required fields and
// defaulting Concurrency to 1 if unset.
func NewBackfiller(
	config BackfillConfig,
	s3Reader outbound.S3Reader,
	repo outbound.SSRRateEventRepository,
	bucket string,
	chainID int64,
) (*Backfiller, error) {
	if config.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if s3Reader == nil {
		return nil, fmt.Errorf("s3 reader is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("ssr rate event repository is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if chainID <= 0 {
		return nil, fmt.Errorf("chainID must be positive, got %d", chainID)
	}
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	return &Backfiller{
		config:   config,
		s3Reader: s3Reader,
		repo:     repo,
		bucket:   bucket,
		chainID:  chainID,
	}, nil
}

// Run processes all blocks in [fromBlock, toBlock], partition by partition:
// list one partition's trace versions, process its blocks concurrently, move
// on. Writes are idempotent, so a failed run can simply be re-run.
func (b *Backfiller) Run(ctx context.Context, fromBlock, toBlock int64) error {
	if toBlock < fromBlock {
		return fmt.Errorf("toBlock (%d) must be >= fromBlock (%d)", toBlock, fromBlock)
	}

	totalBlocks := toBlock - fromBlock + 1
	logger := b.config.Logger.With("component", "ssr-rate-backfill")
	logger.Info("starting backfill",
		"fromBlock", fromBlock,
		"toBlock", toBlock,
		"totalBlocks", totalBlocks,
		"concurrency", b.config.Concurrency,
	)

	var processed, matched atomic.Int64
	startTime := time.Now()
	progressCtx, stopProgressLogging := context.WithCancel(ctx)
	defer stopProgressLogging()
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				logProgress(logger, startTime, totalBlocks, &processed, &matched)
			}
		}
	}()

	var err error
	firstPartition := (fromBlock / partition.BlockRangeSize) * partition.BlockRangeSize
	for partStart := firstPartition; partStart <= toBlock && err == nil && ctx.Err() == nil; partStart += partition.BlockRangeSize {
		lo := max(partStart, fromBlock)
		hi := min(partStart+partition.BlockRangeSize-1, toBlock)
		err = b.runPartition(ctx, logger, lo, hi, &processed, &matched)
	}

	stopProgressLogging()
	<-progressDone

	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return fmt.Errorf("backfill cancelled: %w", ctx.Err())
	}

	logger.Info("backfill complete",
		"processed", processed.Load(),
		"events", matched.Load(),
		"total", totalBlocks,
		"elapsed", time.Since(startTime).String(),
	)
	return nil
}

func logProgress(
	logger *slog.Logger,
	startTime time.Time,
	totalBlocks int64,
	processed, matched *atomic.Int64,
) {
	p := processed.Load()
	elapsed := time.Since(startTime).Seconds()

	var rate float64
	if elapsed > 0 {
		rate = float64(p) / elapsed
	}

	var eta float64
	remaining := totalBlocks - p
	if rate > 0 {
		eta = float64(remaining) / rate
	}

	logger.Info("backfill progress",
		"processed", p,
		"total", totalBlocks,
		"events", matched.Load(),
		"rate_per_sec", fmt.Sprintf("%.2f", rate),
		"eta_sec", fmt.Sprintf("%.0f", eta),
	)
}

// runPartition processes the blocks in [lo, hi] (one S3 partition or the
// slice of it inside the requested range) concurrently. It fails before
// queueing any work if a block in the range has no trace file in S3.
func (b *Backfiller) runPartition(
	ctx context.Context,
	logger *slog.Logger,
	lo, hi int64,
	processed, matched *atomic.Int64,
) error {
	versions, err := b.scanPartition(ctx, lo)
	if err != nil {
		return fmt.Errorf("scanning S3 versions: %w", err)
	}
	for blockNum := lo; blockNum <= hi; blockNum++ {
		if _, ok := versions[blockNum]; !ok {
			return fmt.Errorf("traces for block %d not found in S3", blockNum)
		}
	}

	g, workerCtx := errgroup.WithContext(ctx)
	g.SetLimit(b.config.Concurrency)

	for blockNum := lo; blockNum <= hi; blockNum++ {
		g.Go(func() error {
			// g.Go queues goroutines; they may start after another
			// goroutine has already failed.
			if workerCtx.Err() != nil {
				return nil
			}

			count, err := b.processBlockFromS3(workerCtx, blockNum, versions[blockNum])
			if err != nil {
				logger.Error("failed to process block", "block", blockNum, "error", err)
				return err
			}

			processed.Add(1)
			if count > 0 {
				matched.Add(int64(count))
				logger.Info("ssr rate changes persisted", "block", blockNum, "events", count)
			}
			return nil
		})
	}

	return g.Wait()
}

// scanPartition lists the S3 trace keys of the partition containing blockNum
// and returns blockNum → highest version for that partition.
func (b *Backfiller) scanPartition(ctx context.Context, blockNum int64) (map[int64]int, error) {
	partStart := (blockNum / partition.BlockRangeSize) * partition.BlockRangeSize
	prefix := fmt.Sprintf("%d-%d/", partStart, partStart+partition.BlockRangeSize-1)

	keys, err := b.s3Reader.ListPrefix(ctx, b.bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing S3 prefix %s: %w", prefix, err)
	}

	versions := make(map[int64]int)
	for _, key := range keys {
		parsed, ok := s3key.Parse(key)
		if !ok || parsed.DataType != s3key.Traces {
			continue
		}
		if existing, seen := versions[parsed.BlockNumber]; !seen || parsed.Version > existing {
			versions[parsed.BlockNumber] = parsed.Version
		}
	}

	return versions, nil
}

// processBlockFromS3 reads the trace file for blockNum from S3, decodes SSR
// rate changes, and persists any matches. Returns the number of events
// persisted.
func (b *Backfiller) processBlockFromS3(ctx context.Context, blockNum int64, version int) (int, error) {
	tracesJSON, err := b.readS3File(ctx, s3key.Build(blockNum, version, s3key.Traces))
	if err != nil {
		return 0, fmt.Errorf("reading traces for block %d: %w", blockNum, err)
	}

	blockTime := func(ctx context.Context) (time.Time, error) {
		return b.readBlockTimestamp(ctx, blockNum, version)
	}

	return processTraces(ctx, b.repo, b.chainID, blockNum, version, tracesJSON, blockTime)
}

// readBlockTimestamp reads the block data from S3 and extracts the timestamp.
func (b *Backfiller) readBlockTimestamp(ctx context.Context, blockNum int64, version int) (time.Time, error) {
	data, err := b.readS3File(ctx, s3key.Build(blockNum, version, s3key.Block))
	if err != nil {
		return time.Time{}, fmt.Errorf("reading block %d: %w", blockNum, err)
	}

	var block struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &block); err != nil {
		return time.Time{}, fmt.Errorf("unmarshalling block %d: %w", blockNum, err)
	}

	ts, err := hexutil.ParseInt64(block.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing block timestamp %q: %w", block.Timestamp, err)
	}

	return time.Unix(ts, 0).UTC(), nil
}

// readS3File streams and fully reads one S3 object (decompressed by the
// reader for .gz keys).
func (b *Backfiller) readS3File(ctx context.Context, key string) ([]byte, error) {
	rc, err := b.s3Reader.StreamFile(ctx, b.bucket, key)
	if err != nil {
		return nil, fmt.Errorf("streaming S3 file %s: %w", key, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading S3 file %s: %w", key, err)
	}
	return data, nil
}
