package entity

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// BlockQuerier abstracts block number lookups from an Ethereum node.
type BlockQuerier interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

// DebtQuery represents a single vault to query debt for.
type DebtQuery struct {
	Ilk          [32]byte
	VaultAddress common.Address
}

// DebtResult holds the on-chain debt data for one vault. Exactly one of
// {Rate+Art populated, Reverted=true} is meaningful:
//
//   - Rate+Art populated: success.
//   - Reverted=true: the contract returned `Success: false` for at least
//     one of the per-vault calls (vat.ilks or vat.urns). This is the
//     contract's "no debt this block" signal under AllowFailure: true and
//     the consumer should skip-and-continue without retrying.
//
// Structural failures (ABI decode, unexpected type, missing multicall
// result) fail the whole batch as the outer `error` from
// VatCaller.ReadDebts, never as a per-vault result, so a decode bug stops
// the block instead of writing holes.
type DebtResult struct {
	VaultAddress common.Address
	Rate         *big.Int // cumulative stability fee rate (ray, 1e27)
	Art          *big.Int // normalized debt (wad, 1e18)
	Reverted     bool     // contract-level "no data this block" — skip
}
