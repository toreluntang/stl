package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// SSRRateEventRepository defines persistence operations for SSR rate changes.
type SSRRateEventRepository interface {
	// SaveSSRRateEvents persists a batch of SSR rate-change events in a
	// single transaction. Replays of the same natural key by the same
	// build are idempotent.
	SaveSSRRateEvents(ctx context.Context, events []*entity.SSRRateEvent) error
}
