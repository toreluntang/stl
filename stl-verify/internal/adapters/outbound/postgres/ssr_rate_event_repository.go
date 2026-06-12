package postgres

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that SSRRateEventRepository implements the port interface.
var _ outbound.SSRRateEventRepository = (*SSRRateEventRepository)(nil)

// SSRRateEventRepository persists SSR rate-change events to Postgres.
type SSRRateEventRepository struct {
	pool    *pgxpool.Pool
	txm     *TxManager
	logger  *slog.Logger
	buildID buildregistry.BuildID
}

// NewSSRRateEventRepository creates a new SSRRateEventRepository.
func NewSSRRateEventRepository(
	pool *pgxpool.Pool,
	txm *TxManager,
	logger *slog.Logger,
	buildID buildregistry.BuildID,
) *SSRRateEventRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &SSRRateEventRepository{
		pool:    pool,
		txm:     txm,
		logger:  logger.With("component", "ssr-rate-event-repo"),
		buildID: buildID,
	}
}

// SaveSSRRateEvents writes all events in a single batched transaction.
// rate_per_second_ray is stored as NUMERIC via big.Int's decimal string
// representation. Same-build replays of the same natural key (chain_id,
// block_number, block_version, tx_hash, trace_address) are silently skipped
// via the processing-version trigger + ON CONFLICT DO NOTHING.
func (r *SSRRateEventRepository) SaveSSRRateEvents(ctx context.Context, events []*entity.SSRRateEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Sort by natural key so the per-row advisory lock in
	// assign_processing_version_ssr_rate_event is acquired in a
	// transaction-stable order across concurrent callers. See ADR-0002 §3.
	slices.SortFunc(events, func(a, b *entity.SSRRateEvent) int {
		return cmp.Or(
			cmp.Compare(a.ChainID, b.ChainID),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			a.TxHash.Cmp(b.TxHash),
			cmp.Compare(a.TraceAddress, b.TraceAddress),
		)
	})

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO ssr_rate_event (chain_id, block_number, block_version, block_time, tx_hash, trace_address, rate_per_second_ray, build_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (chain_id, block_number, block_version, tx_hash, trace_address, processing_version) DO NOTHING
		`

		batch := &pgx.Batch{}
		for _, e := range events {
			batch.Queue(q,
				e.ChainID,
				e.BlockNumber,
				e.BlockVersion,
				e.BlockTime,
				e.TxHash.Bytes(),
				e.TraceAddress,
				e.RatePerSecondRay.String(), // NUMERIC from decimal string representation of big.Int
				int(r.buildID),
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, e := range events {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert ssr rate event %d (block=%d, tx=%s): %w", i, e.BlockNumber, e.TxHash, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Debug("ssr rate events saved", "count", len(events))
		return nil
	})
}
