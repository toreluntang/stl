package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// SSRRateEvent is a single SSR (Sky Savings Rate) rate change, decoded from a
// successful file(bytes32("ssr"), uint256) execution trace on the sUSDS
// contract.
type SSRRateEvent struct {
	ChainID      int64
	BlockNumber  int64
	BlockVersion int
	// BlockTime is the timestamp of the block containing the trace.
	BlockTime time.Time
	// TxHash is the transaction containing the file() trace.
	TxHash common.Hash
	// TraceAddress is the comma-joined trace path within the transaction
	// (empty for a top-level call). It disambiguates multiple file(ssr)
	// calls inside one transaction.
	TraceAddress string
	// RatePerSecondRay is the raw per-second rate, ray-scaled (1e27),
	// exactly as filed on-chain. APY conversion is the reader's job.
	RatePerSecondRay *big.Int
}

// Validate checks that the event is well-formed before persistence.
func (e *SSRRateEvent) Validate() error {
	if e.ChainID <= 0 {
		return fmt.Errorf("chain id must be positive")
	}
	if e.BlockNumber <= 0 {
		return fmt.Errorf("block number must be positive")
	}
	if e.BlockVersion < 0 {
		return fmt.Errorf("block version must not be negative")
	}
	if e.BlockTime.IsZero() {
		return fmt.Errorf("block time is required")
	}
	if e.TxHash == (common.Hash{}) {
		return fmt.Errorf("tx hash is required")
	}
	if e.RatePerSecondRay == nil {
		return fmt.Errorf("rate per second is required")
	}
	if e.RatePerSecondRay.Sign() < 0 {
		return fmt.Errorf("rate per second must not be negative")
	}
	return nil
}
