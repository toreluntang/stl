package ssr_rate_tracker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/ssr_rate_tracker"
)

const testBucket = "stl-test-raw"

// ---------------------------------------------------------------------------
// Fake S3 reader
// ---------------------------------------------------------------------------

// fakeS3Reader serves in-memory objects keyed by S3 key. StreamFile returns
// the stored bytes as-is (the real adapter decompresses .gz transparently).
type fakeS3Reader struct {
	mu       sync.Mutex
	files    map[string][]byte
	streamed []string
	listErr  error
}

func newFakeS3Reader() *fakeS3Reader {
	return &fakeS3Reader{files: make(map[string][]byte)}
}

func (f *fakeS3Reader) put(key string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[key] = data
}

func (f *fakeS3Reader) ListFiles(_ context.Context, _, prefix string) ([]outbound.S3File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []outbound.S3File
	for key := range f.files {
		if strings.HasPrefix(key, prefix) {
			out = append(out, outbound.S3File{Key: key})
		}
	}
	return out, nil
}

func (f *fakeS3Reader) ListPrefix(_ context.Context, bucket, prefix string) ([]string, error) {
	if bucket != testBucket {
		return nil, fmt.Errorf("unexpected bucket %q", bucket)
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for key := range f.files {
		if strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	return out, nil
}

func (f *fakeS3Reader) StreamFile(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	if bucket != testBucket {
		return nil, fmt.Errorf("unexpected bucket %q", bucket)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.files[key]
	if !ok {
		return nil, fmt.Errorf("no such key %q", key)
	}
	f.streamed = append(f.streamed, key)
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeS3Reader) streamedKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.streamed))
	copy(out, f.streamed)
	return out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// putBlock stores a minimal block file with the given unix timestamp.
func (f *fakeS3Reader) putBlock(t *testing.T, blockNum int64, version int, unixTime int64) {
	t.Helper()
	body, err := json.Marshal(map[string]string{
		"timestamp": fmt.Sprintf("0x%x", unixTime),
	})
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	f.put(s3key.Build(blockNum, version, s3key.Block), body)
}

// putTraces stores a traces file for the block.
func (f *fakeS3Reader) putTraces(t *testing.T, blockNum int64, version int, traces json.RawMessage) {
	t.Helper()
	f.put(s3key.Build(blockNum, version, s3key.Traces), traces)
}

func newBackfiller(t *testing.T, s3 outbound.S3Reader, repo outbound.SSRRateEventRepository) *ssr_rate_tracker.Backfiller {
	t.Helper()
	b, err := ssr_rate_tracker.NewBackfiller(
		ssr_rate_tracker.BackfillConfig{Concurrency: 2, Logger: slog.Default()},
		s3, repo, testBucket, testChainID,
	)
	if err != nil {
		t.Fatalf("NewBackfiller: %v", err)
	}
	return b
}

// ---------------------------------------------------------------------------
// Constructor validation
// ---------------------------------------------------------------------------

func TestNewBackfiller_Validation(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	repo := &fakeSSRRepo{}
	logger := slog.Default()

	tests := []struct {
		name    string
		config  ssr_rate_tracker.BackfillConfig
		s3      outbound.S3Reader
		repo    outbound.SSRRateEventRepository
		bucket  string
		chainID int64
		wantErr string
	}{
		{name: "valid", config: ssr_rate_tracker.BackfillConfig{Logger: logger}, s3: s3, repo: repo, bucket: testBucket, chainID: 1},
		{name: "nil logger", config: ssr_rate_tracker.BackfillConfig{}, s3: s3, repo: repo, bucket: testBucket, chainID: 1, wantErr: "logger"},
		{name: "nil s3 reader", config: ssr_rate_tracker.BackfillConfig{Logger: logger}, repo: repo, bucket: testBucket, chainID: 1, wantErr: "s3 reader"},
		{name: "nil repo", config: ssr_rate_tracker.BackfillConfig{Logger: logger}, s3: s3, bucket: testBucket, chainID: 1, wantErr: "repository"},
		{name: "empty bucket", config: ssr_rate_tracker.BackfillConfig{Logger: logger}, s3: s3, repo: repo, chainID: 1, wantErr: "bucket"},
		{name: "zero chain id", config: ssr_rate_tracker.BackfillConfig{Logger: logger}, s3: s3, repo: repo, bucket: testBucket, wantErr: "chainID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ssr_rate_tracker.NewBackfiller(tt.config, tt.s3, tt.repo, tt.bucket, tt.chainID)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("NewBackfiller() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("NewBackfiller() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func TestBackfiller_Run_PersistsMatches(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, emptyTracesJSON(t))
	s3.putTraces(t, 101, 0, ssrTracesJSON(t))
	s3.putBlock(t, 101, 0, 1730000000)
	s3.putTraces(t, 102, 0, emptyTracesJSON(t))

	repo := &fakeSSRRepo{}
	if err := newBackfiller(t, s3, repo).Run(context.Background(), 100, 102); err != nil {
		t.Fatalf("Run: %v", err)
	}

	saved := repo.savedEvents()
	if len(saved) != 1 {
		t.Fatalf("savedEvents = %d, want 1", len(saved))
	}
	got := saved[0]
	if got.BlockNumber != 101 {
		t.Errorf("BlockNumber = %d, want 101", got.BlockNumber)
	}
	if want := time.Unix(1730000000, 0).UTC(); !got.BlockTime.Equal(want) {
		t.Errorf("BlockTime = %s, want %s", got.BlockTime, want)
	}
	if got.RatePerSecondRay.Cmp(testRate) != 0 {
		t.Errorf("RatePerSecondRay = %s, want %s", got.RatePerSecondRay, testRate)
	}
	if got.TraceAddress != "0,2" {
		t.Errorf("TraceAddress = %q, want %q", got.TraceAddress, "0,2")
	}

	// Lazy block-time reads: only the matching block's block file is read.
	for _, key := range s3.streamedKeys() {
		parsed, ok := s3key.Parse(key)
		if !ok {
			t.Fatalf("unparseable streamed key %q", key)
		}
		if parsed.DataType == s3key.Block && parsed.BlockNumber != 101 {
			t.Errorf("unexpected block file read for block %d", parsed.BlockNumber)
		}
	}
}

func TestBackfiller_Run_UsesHighestVersion(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	// Version 0 has a match, version 1 (reorged replacement) does not:
	// only the highest version must be processed.
	s3.putTraces(t, 100, 0, ssrTracesJSON(t))
	s3.putBlock(t, 100, 0, 1730000000)
	s3.putTraces(t, 100, 1, emptyTracesJSON(t))

	repo := &fakeSSRRepo{}
	if err := newBackfiller(t, s3, repo).Run(context.Background(), 100, 100); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(repo.savedEvents()) != 0 {
		t.Fatalf("savedEvents = %d, want 0 (version 1 has no matches)", len(repo.savedEvents()))
	}
}

func TestBackfiller_Run_MissingTracesFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, emptyTracesJSON(t))
	// Block 101 has no traces file.

	repo := &fakeSSRRepo{}
	err := newBackfiller(t, s3, repo).Run(context.Background(), 100, 101)
	if err == nil || !strings.Contains(err.Error(), "traces for block 101 not found") {
		t.Fatalf("Run = %v, want missing-traces error", err)
	}
}

func TestBackfiller_Run_MissingBlockFileFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, ssrTracesJSON(t))
	// Matching block but no block file → block time unresolvable → hard error.

	repo := &fakeSSRRepo{}
	err := newBackfiller(t, s3, repo).Run(context.Background(), 100, 100)
	if err == nil || !strings.Contains(err.Error(), "resolve block time") {
		t.Fatalf("Run = %v, want block-time error", err)
	}
	if len(repo.savedEvents()) != 0 {
		t.Errorf("savedEvents = %d, want 0", len(repo.savedEvents()))
	}
}

func TestBackfiller_Run_MalformedTracesFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, json.RawMessage(`{not json`))

	repo := &fakeSSRRepo{}
	err := newBackfiller(t, s3, repo).Run(context.Background(), 100, 100)
	if err == nil || !strings.Contains(err.Error(), "parse traces") {
		t.Fatalf("Run = %v, want parse error", err)
	}
}

func TestBackfiller_Run_ListPrefixErrorFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.listErr = fmt.Errorf("s3 unavailable")

	err := newBackfiller(t, s3, &fakeSSRRepo{}).Run(context.Background(), 100, 100)
	if err == nil || !strings.Contains(err.Error(), "scanning S3 versions") {
		t.Fatalf("Run = %v, want list error", err)
	}
}

func TestBackfiller_Run_IgnoresForeignKeys(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, emptyTracesJSON(t))
	// Same partition, but not trace files / not parseable: must be skipped.
	s3.put(s3key.Build(100, 0, s3key.Receipts), []byte(`[]`))
	s3.put("0-99999/garbage.txt", []byte(`x`))

	if err := newBackfiller(t, s3, &fakeSSRRepo{}).Run(context.Background(), 100, 100); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestBackfiller_Run_MalformedBlockFileFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, ssrTracesJSON(t))
	s3.put(s3key.Build(100, 0, s3key.Block), []byte(`{not json`))

	err := newBackfiller(t, s3, &fakeSSRRepo{}).Run(context.Background(), 100, 100)
	if err == nil || !strings.Contains(err.Error(), "unmarshalling block") {
		t.Fatalf("Run = %v, want block unmarshal error", err)
	}
}

func TestBackfiller_Run_BadBlockTimestampFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, ssrTracesJSON(t))
	s3.put(s3key.Build(100, 0, s3key.Block), []byte(`{"timestamp":"not-hex"}`))

	err := newBackfiller(t, s3, &fakeSSRRepo{}).Run(context.Background(), 100, 100)
	if err == nil || !strings.Contains(err.Error(), "parsing block timestamp") {
		t.Fatalf("Run = %v, want timestamp parse error", err)
	}
}

func TestBackfiller_Run_DecodeErrorFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	traces, err := json.Marshal([]ssr_rate_tracker.Trace{{
		Action:          ssr_rate_tracker.TraceAction{To: "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd", Input: "0xzzzz"},
		TransactionHash: testTxHash,
	}})
	if err != nil {
		t.Fatalf("marshal traces: %v", err)
	}
	s3.putTraces(t, 100, 0, traces)

	runErr := newBackfiller(t, s3, &fakeSSRRepo{}).Run(context.Background(), 100, 100)
	if runErr == nil || !strings.Contains(runErr.Error(), "decode rate changes") {
		t.Fatalf("Run = %v, want decode error", runErr)
	}
}

func TestBackfiller_Run_InvalidEventFails(t *testing.T) {
	t.Parallel()

	// A match at block 0 fails entity validation (block number must be
	// positive) and must abort the backfill.
	s3 := newFakeS3Reader()
	s3.putTraces(t, 0, 0, ssrTracesJSON(t))
	s3.putBlock(t, 0, 0, 1730000000)

	repo := &fakeSSRRepo{}
	err := newBackfiller(t, s3, repo).Run(context.Background(), 0, 0)
	if err == nil || !strings.Contains(err.Error(), "block number must be positive") {
		t.Fatalf("Run = %v, want validation error", err)
	}
	if len(repo.savedEvents()) != 0 {
		t.Errorf("savedEvents = %d, want 0", len(repo.savedEvents()))
	}
}

func TestBackfiller_Run_RepoErrorFails(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putTraces(t, 100, 0, ssrTracesJSON(t))
	s3.putBlock(t, 100, 0, 1730000000)

	repo := &fakeSSRRepo{failOn: fmt.Errorf("db down")}
	err := newBackfiller(t, s3, repo).Run(context.Background(), 100, 100)
	if err == nil || !strings.Contains(err.Error(), "save ssr rate events") {
		t.Fatalf("Run = %v, want save error", err)
	}
}

func TestBackfiller_Run_InvalidRange(t *testing.T) {
	t.Parallel()

	repo := &fakeSSRRepo{}
	err := newBackfiller(t, newFakeS3Reader(), repo).Run(context.Background(), 101, 100)
	if err == nil || !strings.Contains(err.Error(), "must be >=") {
		t.Fatalf("Run = %v, want range error", err)
	}
}

// Sanity check: the putBlock helper must produce hex timestamps in the same
// shape as real cached block files.
func TestPutBlockHelper(t *testing.T) {
	t.Parallel()

	s3 := newFakeS3Reader()
	s3.putBlock(t, 1, 0, 1730000000)
	rc, err := s3.StreamFile(context.Background(), testBucket, s3key.Build(1, 0, s3key.Block))
	if err != nil {
		t.Fatalf("StreamFile: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	var block struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &block); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if want := fmt.Sprintf("0x%x", 1730000000); block.Timestamp != want {
		t.Fatalf("timestamp = %q, want %q", block.Timestamp, want)
	}
}
