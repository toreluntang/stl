package ssr_rate_tracker

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// susdsAddress is the sUSDS contract on Ethereum mainnet — the target of
// SP-BEAM file(bytes32("ssr"), uint256) governance calls.
var susdsAddress = common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")

// fileSelector is the 4-byte function selector of file(bytes32,uint256).
var fileSelector = [4]byte{0x29, 0xae, 0x81, 0x14}

// ssrKey is bytes32("ssr") — the storage key filed for SSR rate changes.
var ssrKey = func() [32]byte {
	var k [32]byte
	copy(k[:], "ssr")
	return k
}()

// Trace is the subset of a parity-style trace_block entry needed to detect
// SSR rate changes. Matches the JSON cached under
// stl:{chainId}:{blockNumber}:{version}:traces.
type Trace struct {
	Action TraceAction `json:"action"`
	// Error is non-empty when this trace frame reverted (e.g. "Reverted").
	Error           string `json:"error"`
	TransactionHash string `json:"transactionHash"`
	// TraceAddress is the position of this frame in the transaction's call
	// tree (empty for the top-level call).
	TraceAddress []int `json:"traceAddress"`
}

// TraceAction holds the call parameters of a trace frame.
type TraceAction struct {
	To    string `json:"to"`
	Input string `json:"input"`
}

// RateChange is a decoded SSR rate change from a single successful trace.
type RateChange struct {
	TxHash       common.Hash
	TraceAddress []int
	// RatePerSecondRay is the raw ray-scaled (1e27) per-second rate.
	RatePerSecondRay *big.Int
}

// DecodeRateChanges scans one block's traces for successful
// file(bytes32("ssr"), uint256) calls on sUSDS and returns the decoded rate
// changes in trace order.
//
// Calldata layout (68 bytes): 4-byte selector ++ 32-byte key ++ 32-byte
// uint256 rate. A trace counts as successful only if neither it nor any of
// its ancestor frames reverted, matching Dune's ethereum.traces success
// semantics.
func DecodeRateChanges(traces []Trace) ([]RateChange, error) {
	failed := indexFailedTraces(traces)

	var changes []RateChange
	for i, tr := range traces {
		if common.HexToAddress(tr.Action.To) != susdsAddress {
			continue
		}
		if tr.Action.Input == "" {
			// Non-call trace types (create, suicide, reward) carry no input.
			continue
		}
		input, err := hexutil.Decode(tr.Action.Input)
		if err != nil {
			return nil, fmt.Errorf("trace %d (tx %s): decode input: %w", i, tr.TransactionHash, err)
		}
		if len(input) < 4 || !bytes.Equal(input[:4], fileSelector[:]) {
			continue
		}
		if len(input) < 36 || !bytes.Equal(input[4:36], ssrKey[:]) {
			continue
		}
		if hasFailedAncestor(failed, tr) {
			continue
		}
		if len(input) < 68 {
			return nil, fmt.Errorf("trace %d (tx %s): file(ssr) calldata is %d bytes, want 68", i, tr.TransactionHash, len(input))
		}

		changes = append(changes, RateChange{
			TxHash:           common.HexToHash(tr.TransactionHash),
			TraceAddress:     tr.TraceAddress,
			RatePerSecondRay: new(big.Int).SetBytes(input[36:68]),
		})
	}

	return changes, nil
}

// indexFailedTraces returns, per transaction hash, the set of trace-address
// keys whose frame reverted.
func indexFailedTraces(traces []Trace) map[string]map[string]struct{} {
	failed := make(map[string]map[string]struct{})
	for _, tr := range traces {
		if tr.Error == "" {
			continue
		}
		set, ok := failed[tr.TransactionHash]
		if !ok {
			set = make(map[string]struct{})
			failed[tr.TransactionHash] = set
		}
		set[traceAddressKey(tr.TraceAddress)] = struct{}{}
	}
	return failed
}

// hasFailedAncestor reports whether the trace itself or any ancestor frame in
// the same transaction reverted.
func hasFailedAncestor(failed map[string]map[string]struct{}, tr Trace) bool {
	set, ok := failed[tr.TransactionHash]
	if !ok {
		return false
	}
	for i := 0; i <= len(tr.TraceAddress); i++ {
		if _, bad := set[traceAddressKey(tr.TraceAddress[:i])]; bad {
			return true
		}
	}
	return false
}

// traceAddressKey renders a trace address as a comma-joined string
// (e.g. [0 2 1] → "0,2,1"; the top-level call → "").
func traceAddressKey(addr []int) string {
	if len(addr) == 0 {
		return ""
	}
	parts := make([]string, len(addr))
	for i, n := range addr {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}
