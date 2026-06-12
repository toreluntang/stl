// Package ssr_rate_tracker detects SSR (Sky Savings Rate) rate changes by
// scanning cached Ethereum execution traces for successful
// file(bytes32("ssr"), uint256) calls on the sUSDS contract, and persists
// them as ssr_rate_event rows.
//
// Flow (live tail):
//  1. Consume block events from SQS via sqsutil.RunLoop.
//  2. For every block, read the cached traces (Redis with S3 fallback).
//  3. Decode file(ssr) calls via DecodeRateChanges.
//  4. Persist matches via SSRRateEventRepository.SaveSSRRateEvents.
//
// Historical blocks are covered by Backfiller (see backfill.go), which scans
// the raw-data S3 bucket directly.
package ssr_rate_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Config holds configuration for the Service.
type Config struct {
	// ChainID is the expected chain ID for incoming block events.
	ChainID int64

	// MaxMessages is the max number of SQS messages per poll. Defaults to 10.
	MaxMessages int

	// PollInterval controls how often SQS is polled. Defaults to 100ms.
	PollInterval time.Duration

	// Logger is the structured logger.
	Logger *slog.Logger
}

func configDefaults() Config {
	return Config{
		MaxMessages:  10,
		PollInterval: 100 * time.Millisecond,
		Logger:       slog.Default(),
	}
}

// Service consumes block events from SQS and scans each block's cached
// traces for SSR rate changes.
type Service struct {
	config      Config
	sqsConsumer outbound.SQSConsumer
	cache       outbound.BlockCacheReader
	repo        outbound.SSRRateEventRepository
	logger      *slog.Logger
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewService creates a new Service.
func NewService(
	config Config,
	sqsConsumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	repo outbound.SSRRateEventRepository,
) (*Service, error) {
	if sqsConsumer == nil {
		return nil, fmt.Errorf("sqs consumer is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("block cache reader is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("ssr rate event repository is required")
	}
	if config.ChainID == 0 {
		return nil, fmt.Errorf("chain ID is required")
	}

	defaults := configDefaults()
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &Service{
		config:      config,
		sqsConsumer: sqsConsumer,
		cache:       cache,
		repo:        repo,
		logger:      config.Logger.With("component", "ssr-rate-tracker"),
	}, nil
}

// Start begins the SQS processing loop. It returns immediately; processing
// continues until Stop is called or ctx is cancelled.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, sqsutil.Config{
			Consumer:     s.sqsConsumer,
			MaxMessages:  s.config.MaxMessages,
			PollInterval: s.config.PollInterval,
			Logger:       s.logger,
			ChainID:      s.config.ChainID,
		}, s.processBlock)
	})

	s.logger.Info("ssr rate tracker started", "chainID", s.config.ChainID)
	return nil
}

// Stop cancels the SQS processing loop and waits for the goroutine to exit.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("ssr rate tracker stopped")
	return nil
}

// processBlock is called for every block event from SQS. A returned error
// NACKs the message so SQS redelivers it.
func (s *Service) processBlock(ctx context.Context, event outbound.BlockEvent) error {
	tracesJSON, err := s.cache.GetTraces(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching traces from cache: %w", err)
	}
	if tracesJSON == nil {
		return fmt.Errorf("traces not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	blockTime := func(context.Context) (time.Time, error) {
		return time.Unix(event.BlockTimestamp, 0).UTC(), nil
	}

	count, err := processTraces(ctx, s.repo, event.ChainID, event.BlockNumber, event.Version, tracesJSON, blockTime)
	if err != nil {
		return fmt.Errorf("block %d: %w", event.BlockNumber, err)
	}
	if count > 0 {
		s.logger.Info("ssr rate changes persisted", "block", event.BlockNumber, "events", count)
	}
	return nil
}

// processTraces decodes one block's traces and persists any SSR rate changes.
// blockTime is resolved lazily — only when at least one change matched — so
// callers without a cheap timestamp at hand (the backfiller) avoid one block
// read per empty block. Returns the number of events persisted.
func processTraces(
	ctx context.Context,
	repo outbound.SSRRateEventRepository,
	chainID int64,
	blockNumber int64,
	blockVersion int,
	tracesJSON json.RawMessage,
	blockTime func(context.Context) (time.Time, error),
) (int, error) {
	var traces []Trace
	if err := json.Unmarshal(tracesJSON, &traces); err != nil {
		return 0, fmt.Errorf("parse traces: %w", err)
	}

	changes, err := DecodeRateChanges(traces)
	if err != nil {
		return 0, fmt.Errorf("decode rate changes: %w", err)
	}
	if len(changes) == 0 {
		return 0, nil
	}

	bt, err := blockTime(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve block time: %w", err)
	}

	events := make([]*entity.SSRRateEvent, 0, len(changes))
	for i, c := range changes {
		event := &entity.SSRRateEvent{
			ChainID:          chainID,
			BlockNumber:      blockNumber,
			BlockVersion:     blockVersion,
			BlockTime:        bt,
			TxHash:           c.TxHash,
			TraceAddress:     traceAddressKey(c.TraceAddress),
			RatePerSecondRay: c.RatePerSecondRay,
		}
		if err := event.Validate(); err != nil {
			return 0, fmt.Errorf("ssr rate event %d: %w", i, err)
		}
		events = append(events, event)
	}

	if err := repo.SaveSSRRateEvents(ctx, events); err != nil {
		return 0, fmt.Errorf("save ssr rate events: %w", err)
	}
	return len(events), nil
}
