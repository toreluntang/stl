package ssr_rate_tracker_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/ssr_rate_tracker"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	testChainID = int64(1)
	testTxHash  = "0x1111111111111111111111111111111111111111111111111111111111111111"
)

var testRate, _ = new(big.Int).SetString("1000000001547125957863212448", 10)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeSSRRepo is a controllable in-memory SSRRateEventRepository.
type fakeSSRRepo struct {
	mu     sync.Mutex
	saved  []*entity.SSRRateEvent
	failOn error
}

func (f *fakeSSRRepo) SaveSSRRateEvents(_ context.Context, events []*entity.SSRRateEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOn != nil {
		return f.failOn
	}
	f.saved = append(f.saved, events...)
	return nil
}

func (f *fakeSSRRepo) savedEvents() []*entity.SSRRateEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*entity.SSRRateEvent, len(f.saved))
	copy(out, f.saved)
	return out
}

// fakeSQS serves the given block events once, then returns empty batches.
type fakeSQS struct {
	mu       sync.Mutex
	messages []outbound.SQSMessage
	served   int
	polls    int
	deleted  []string
}

func newFakeSQS(t *testing.T, events ...outbound.BlockEvent) *fakeSQS {
	t.Helper()
	msgs := make([]outbound.SQSMessage, len(events))
	for i, e := range events {
		body, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("marshal block event: %v", err)
		}
		msgs[i] = outbound.SQSMessage{
			MessageID:     fmt.Sprintf("msg-%d", i),
			ReceiptHandle: fmt.Sprintf("handle-%d", i),
			Body:          string(body),
		}
	}
	return &fakeSQS{messages: msgs}
}

func (f *fakeSQS) ReceiveMessages(_ context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++
	if f.served >= len(f.messages) {
		return nil, nil
	}
	end := min(f.served+maxMessages, len(f.messages))
	batch := f.messages[f.served:end]
	f.served = end
	return batch, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, receiptHandle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, receiptHandle)
	return nil
}

func (f *fakeSQS) Close() error { return nil }

func (f *fakeSQS) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleted)
}

func (f *fakeSQS) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.polls
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ssrTracesJSON returns a traces array containing one successful
// file(bytes32("ssr"), uint256) call on sUSDS.
func ssrTracesJSON(t *testing.T) json.RawMessage {
	t.Helper()
	selector := []byte{0x29, 0xae, 0x81, 0x14}
	key := make([]byte, 32)
	copy(key, "ssr")
	value := make([]byte, 32)
	testRate.FillBytes(value)

	input := "0x" + hex.EncodeToString(selector) + hex.EncodeToString(key) + hex.EncodeToString(value)

	traces := []ssr_rate_tracker.Trace{{
		Action: ssr_rate_tracker.TraceAction{
			To:    "0xa3931d71877c0e7a3148cb7eb4463524fec27fbd",
			Input: input,
		},
		TransactionHash: testTxHash,
		TraceAddress:    []int{0, 2},
	}}
	data, err := json.Marshal(traces)
	if err != nil {
		t.Fatalf("marshal traces: %v", err)
	}
	return data
}

// emptyTracesJSON returns a traces array with no SSR matches.
func emptyTracesJSON(t *testing.T) json.RawMessage {
	t.Helper()
	traces := []ssr_rate_tracker.Trace{{
		Action:          ssr_rate_tracker.TraceAction{To: "0x0000000000000000000000000000000000000001", Input: "0x"},
		TransactionHash: testTxHash,
	}}
	data, err := json.Marshal(traces)
	if err != nil {
		t.Fatalf("marshal traces: %v", err)
	}
	return data
}

func cacheWithTraces(traces json.RawMessage) *testutil.MockBlockCache {
	return &testutil.MockBlockCache{
		GetTracesFn: func(_ context.Context, _, _ int64, _ int) (json.RawMessage, error) {
			return traces, nil
		},
	}
}

func testBlockEvent() outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        testChainID,
		BlockNumber:    21000000,
		Version:        0,
		BlockHash:      "0xabc",
		BlockTimestamp: 1730000000,
	}
}

// waitFor polls cond until it returns true or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func startService(t *testing.T, sqs outbound.SQSConsumer, cache outbound.BlockCacheReader, repo outbound.SSRRateEventRepository) *ssr_rate_tracker.Service {
	t.Helper()
	svc, err := ssr_rate_tracker.NewService(
		ssr_rate_tracker.Config{ChainID: testChainID, PollInterval: 5 * time.Millisecond},
		sqs, cache, repo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return svc
}

// ---------------------------------------------------------------------------
// Constructor validation
// ---------------------------------------------------------------------------

func TestNewService_Validation(t *testing.T) {
	t.Parallel()

	sqs := &fakeSQS{}
	cache := testutil.NewMockBlockCache()
	repo := &fakeSSRRepo{}
	validCfg := ssr_rate_tracker.Config{ChainID: testChainID}

	tests := []struct {
		name    string
		config  ssr_rate_tracker.Config
		sqs     outbound.SQSConsumer
		cache   outbound.BlockCacheReader
		repo    outbound.SSRRateEventRepository
		wantErr string
	}{
		{name: "valid", config: validCfg, sqs: sqs, cache: cache, repo: repo},
		{name: "nil sqs consumer", config: validCfg, cache: cache, repo: repo, wantErr: "sqs consumer"},
		{name: "nil cache", config: validCfg, sqs: sqs, repo: repo, wantErr: "block cache reader"},
		{name: "nil repo", config: validCfg, sqs: sqs, cache: cache, wantErr: "repository"},
		{name: "zero chain id", config: ssr_rate_tracker.Config{}, sqs: sqs, cache: cache, repo: repo, wantErr: "chain ID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ssr_rate_tracker.NewService(tt.config, tt.sqs, tt.cache, tt.repo)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("NewService() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("NewService() = nil, want error containing %q", tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Block processing through the public API
// ---------------------------------------------------------------------------

func TestService_PersistsRateChange(t *testing.T) {
	t.Parallel()

	sqs := newFakeSQS(t, testBlockEvent())
	repo := &fakeSSRRepo{}
	startService(t, sqs, cacheWithTraces(ssrTracesJSON(t)), repo)

	waitFor(t, 2*time.Second, func() bool { return len(repo.savedEvents()) == 1 }, "event persisted")
	waitFor(t, 2*time.Second, func() bool { return sqs.deleteCount() == 1 }, "message deleted")

	got := repo.savedEvents()[0]
	if got.ChainID != testChainID {
		t.Errorf("ChainID = %d, want %d", got.ChainID, testChainID)
	}
	if got.BlockNumber != 21000000 {
		t.Errorf("BlockNumber = %d, want 21000000", got.BlockNumber)
	}
	if got.BlockVersion != 0 {
		t.Errorf("BlockVersion = %d, want 0", got.BlockVersion)
	}
	if want := time.Unix(1730000000, 0).UTC(); !got.BlockTime.Equal(want) {
		t.Errorf("BlockTime = %s, want %s", got.BlockTime, want)
	}
	if got.TxHash.Hex() != testTxHash {
		t.Errorf("TxHash = %s, want %s", got.TxHash.Hex(), testTxHash)
	}
	if got.TraceAddress != "0,2" {
		t.Errorf("TraceAddress = %q, want %q", got.TraceAddress, "0,2")
	}
	if got.RatePerSecondRay.Cmp(testRate) != 0 {
		t.Errorf("RatePerSecondRay = %s, want %s", got.RatePerSecondRay, testRate)
	}
}

func TestService_NoMatchPersistsNothing(t *testing.T) {
	t.Parallel()

	sqs := newFakeSQS(t, testBlockEvent())
	repo := &fakeSSRRepo{}
	startService(t, sqs, cacheWithTraces(emptyTracesJSON(t)), repo)

	waitFor(t, 2*time.Second, func() bool { return sqs.deleteCount() == 1 }, "message deleted")
	if len(repo.savedEvents()) != 0 {
		t.Errorf("savedEvents = %d, want 0", len(repo.savedEvents()))
	}
}

func TestService_MissingTracesNacksMessage(t *testing.T) {
	t.Parallel()

	sqs := newFakeSQS(t, testBlockEvent())
	repo := &fakeSSRRepo{}
	startService(t, sqs, cacheWithTraces(nil), repo)

	// Wait until the message has been served and a few more polls happened,
	// then assert it was never deleted (NACK → SQS would redeliver).
	waitFor(t, 2*time.Second, func() bool { return sqs.pollCount() > 3 }, "messages polled")
	if sqs.deleteCount() != 0 {
		t.Errorf("deleteCount = %d, want 0", sqs.deleteCount())
	}
	if len(repo.savedEvents()) != 0 {
		t.Errorf("savedEvents = %d, want 0", len(repo.savedEvents()))
	}
}

func TestService_CacheErrorNacksMessage(t *testing.T) {
	t.Parallel()

	cache := &testutil.MockBlockCache{
		GetTracesFn: func(_ context.Context, _, _ int64, _ int) (json.RawMessage, error) {
			return nil, errors.New("redis down")
		},
	}
	sqs := newFakeSQS(t, testBlockEvent())
	repo := &fakeSSRRepo{}
	startService(t, sqs, cache, repo)

	waitFor(t, 2*time.Second, func() bool { return sqs.pollCount() > 3 }, "messages polled")
	if sqs.deleteCount() != 0 {
		t.Errorf("deleteCount = %d, want 0", sqs.deleteCount())
	}
}

func TestService_MalformedTracesNacksMessage(t *testing.T) {
	t.Parallel()

	sqs := newFakeSQS(t, testBlockEvent())
	repo := &fakeSSRRepo{}
	startService(t, sqs, cacheWithTraces(json.RawMessage(`{not json`)), repo)

	waitFor(t, 2*time.Second, func() bool { return sqs.pollCount() > 3 }, "messages polled")
	if sqs.deleteCount() != 0 {
		t.Errorf("deleteCount = %d, want 0", sqs.deleteCount())
	}
	if len(repo.savedEvents()) != 0 {
		t.Errorf("savedEvents = %d, want 0", len(repo.savedEvents()))
	}
}

func TestService_RepoErrorNacksMessage(t *testing.T) {
	t.Parallel()

	sqs := newFakeSQS(t, testBlockEvent())
	repo := &fakeSSRRepo{failOn: errors.New("db down")}
	startService(t, sqs, cacheWithTraces(ssrTracesJSON(t)), repo)

	waitFor(t, 2*time.Second, func() bool { return sqs.pollCount() > 3 }, "messages polled")
	if sqs.deleteCount() != 0 {
		t.Errorf("deleteCount = %d, want 0", sqs.deleteCount())
	}
}

func TestService_StopWithoutStart(t *testing.T) {
	t.Parallel()

	svc, err := ssr_rate_tracker.NewService(
		ssr_rate_tracker.Config{ChainID: testChainID},
		&fakeSQS{}, testutil.NewMockBlockCache(), &fakeSSRRepo{},
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop without Start: %v", err)
	}
}
