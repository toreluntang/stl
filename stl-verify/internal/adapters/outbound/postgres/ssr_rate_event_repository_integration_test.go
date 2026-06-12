//go:build integration

package postgres

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const ssrSchemaName = "test_ssr_rate_event"

var ssrPool *pgxpool.Pool

func init() {
	registerTestFileSetup(ssrSchemaName, func() {
		ssrPool = testutil.SetupSchemaForMain(sharedDSN, ssrSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, ssrPool, ssrSchemaName)
	})
}

type ssrRow struct {
	ChainID           int64
	BlockNumber       int64
	BlockVersion      int
	BlockTime         time.Time
	TxHash            []byte
	TraceAddress      string
	RatePerSecondRay  string
	ProcessingVersion int
	BuildID           int
}

func newSSRRepo(t *testing.T, buildID buildregistry.BuildID) *SSRRateEventRepository {
	t.Helper()
	txm, err := NewTxManager(ssrPool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	return NewSSRRateEventRepository(ssrPool, txm, nil, buildID)
}

func truncateSSREvents(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := ssrPool.Exec(ctx, `TRUNCATE ssr_rate_event`); err != nil {
		t.Fatalf("truncate ssr_rate_event: %v", err)
	}
}

func querySSRRows(t *testing.T, ctx context.Context) []ssrRow {
	t.Helper()
	rows, err := ssrPool.Query(ctx, `
		SELECT chain_id, block_number, block_version, block_time, tx_hash,
		       trace_address, rate_per_second_ray::text, processing_version, build_id
		FROM ssr_rate_event
		ORDER BY block_number, block_version, tx_hash, trace_address, processing_version
	`)
	if err != nil {
		t.Fatalf("query ssr_rate_event: %v", err)
	}
	defer rows.Close()

	var out []ssrRow
	for rows.Next() {
		var r ssrRow
		if err := rows.Scan(&r.ChainID, &r.BlockNumber, &r.BlockVersion, &r.BlockTime, &r.TxHash,
			&r.TraceAddress, &r.RatePerSecondRay, &r.ProcessingVersion, &r.BuildID); err != nil {
			t.Fatalf("scan ssr row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ssr rows: %v", err)
	}
	return out
}

func testSSREvent(blockNumber int64) *entity.SSRRateEvent {
	rate, _ := new(big.Int).SetString("1000000001547125957863212448", 10)
	return &entity.SSRRateEvent{
		ChainID:          1,
		BlockNumber:      blockNumber,
		BlockVersion:     0,
		BlockTime:        time.Unix(1730000000, 0).UTC(),
		TxHash:           common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		TraceAddress:     "0,2",
		RatePerSecondRay: rate,
	}
}

func TestSSRRateEventRepository_SaveAndReadBack(t *testing.T) {
	ctx := context.Background()
	truncateSSREvents(t, ctx)

	repo := newSSRRepo(t, 7)
	event := testSSREvent(21000000)
	second := testSSREvent(21000050)
	second.TxHash = common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	second.TraceAddress = ""
	second.RatePerSecondRay = big.NewInt(0)

	if err := repo.SaveSSRRateEvents(ctx, []*entity.SSRRateEvent{event, second}); err != nil {
		t.Fatalf("SaveSSRRateEvents: %v", err)
	}

	rows := querySSRRows(t, ctx)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}

	got := rows[0]
	if got.ChainID != 1 || got.BlockNumber != 21000000 || got.BlockVersion != 0 {
		t.Errorf("row identity = %+v", got)
	}
	if !got.BlockTime.Equal(time.Unix(1730000000, 0).UTC()) {
		t.Errorf("BlockTime = %s, want %s", got.BlockTime, time.Unix(1730000000, 0).UTC())
	}
	if !bytes.Equal(got.TxHash, event.TxHash.Bytes()) {
		t.Errorf("TxHash = %x, want %x", got.TxHash, event.TxHash.Bytes())
	}
	if got.TraceAddress != "0,2" {
		t.Errorf("TraceAddress = %q, want %q", got.TraceAddress, "0,2")
	}
	// NUMERIC must round-trip the full 28-digit ray value exactly.
	if got.RatePerSecondRay != "1000000001547125957863212448" {
		t.Errorf("RatePerSecondRay = %s, want 1000000001547125957863212448", got.RatePerSecondRay)
	}
	if got.ProcessingVersion != 0 {
		t.Errorf("ProcessingVersion = %d, want 0", got.ProcessingVersion)
	}
	if got.BuildID != 7 {
		t.Errorf("BuildID = %d, want 7", got.BuildID)
	}

	if rows[1].RatePerSecondRay != "0" {
		t.Errorf("second RatePerSecondRay = %s, want 0", rows[1].RatePerSecondRay)
	}
	if rows[1].TraceAddress != "" {
		t.Errorf("second TraceAddress = %q, want empty", rows[1].TraceAddress)
	}
}

func TestSSRRateEventRepository_SameBuildReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	truncateSSREvents(t, ctx)

	repo := newSSRRepo(t, 7)
	if err := repo.SaveSSRRateEvents(ctx, []*entity.SSRRateEvent{testSSREvent(21000000)}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := repo.SaveSSRRateEvents(ctx, []*entity.SSRRateEvent{testSSREvent(21000000)}); err != nil {
		t.Fatalf("replay save: %v", err)
	}

	rows := querySSRRows(t, ctx)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1 (same-build replay must be idempotent)", len(rows))
	}
	if rows[0].ProcessingVersion != 0 {
		t.Errorf("ProcessingVersion = %d, want 0", rows[0].ProcessingVersion)
	}
}

func TestSSRRateEventRepository_NewBuildAppendsProcessingVersion(t *testing.T) {
	ctx := context.Background()
	truncateSSREvents(t, ctx)

	if err := newSSRRepo(t, 7).SaveSSRRateEvents(ctx, []*entity.SSRRateEvent{testSSREvent(21000000)}); err != nil {
		t.Fatalf("build 7 save: %v", err)
	}
	if err := newSSRRepo(t, 8).SaveSSRRateEvents(ctx, []*entity.SSRRateEvent{testSSREvent(21000000)}); err != nil {
		t.Fatalf("build 8 save: %v", err)
	}

	rows := querySSRRows(t, ctx)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (new build appends a processing version)", len(rows))
	}
	if rows[0].ProcessingVersion != 0 || rows[1].ProcessingVersion != 1 {
		t.Errorf("processing versions = %d, %d, want 0, 1", rows[0].ProcessingVersion, rows[1].ProcessingVersion)
	}
	if rows[0].BuildID != 7 || rows[1].BuildID != 8 {
		t.Errorf("build ids = %d, %d, want 7, 8", rows[0].BuildID, rows[1].BuildID)
	}
}

func TestSSRRateEventRepository_EmptySliceIsNoOp(t *testing.T) {
	ctx := context.Background()
	truncateSSREvents(t, ctx)

	if err := newSSRRepo(t, 7).SaveSSRRateEvents(ctx, nil); err != nil {
		t.Fatalf("SaveSSRRateEvents(nil): %v", err)
	}
	if rows := querySSRRows(t, ctx); len(rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(rows))
	}
}
