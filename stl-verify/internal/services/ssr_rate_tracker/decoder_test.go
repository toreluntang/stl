package ssr_rate_tracker

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

const (
	susdsHex = "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"
	txHash1  = "0x1111111111111111111111111111111111111111111111111111111111111111"
	txHash2  = "0x2222222222222222222222222222222222222222222222222222222222222222"
)

// rate5pct is a realistic ray-scaled per-second rate (~5% APY).
var rate5pct, _ = new(big.Int).SetString("1000000001547125957863212448", 10)

// buildCalldata constructs file(bytes32,uint256) calldata as
// 4-byte selector ++ 32-byte key ++ 32-byte uint256 value.
func buildCalldata(t *testing.T, selector [4]byte, key string, value *big.Int) string {
	t.Helper()
	data := make([]byte, 0, 68)
	data = append(data, selector[:]...)

	var keyBytes [32]byte
	copy(keyBytes[:], key)
	data = append(data, keyBytes[:]...)

	valueBytes := make([]byte, 32)
	value.FillBytes(valueBytes)
	data = append(data, valueBytes...)

	return "0x" + hex.EncodeToString(data)
}

// ssrCalldata is the happy-path calldata: file(bytes32("ssr"), rate5pct).
func ssrCalldata(t *testing.T) string {
	t.Helper()
	return buildCalldata(t, fileSelector, "ssr", rate5pct)
}

func TestDecodeRateChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		traces  []Trace
		want    []RateChange
		wantErr string
	}{
		{
			name:   "no traces",
			traces: nil,
			want:   nil,
		},
		{
			name: "top-level match",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)},
				TransactionHash: txHash1,
			}},
			want: []RateChange{{
				TxHash:           common.HexToHash(txHash1),
				RatePerSecondRay: rate5pct,
			}},
		},
		{
			name: "nested match keeps trace address",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)},
				TransactionHash: txHash1,
				TraceAddress:    []int{0, 2, 1},
			}},
			want: []RateChange{{
				TxHash:           common.HexToHash(txHash1),
				TraceAddress:     []int{0, 2, 1},
				RatePerSecondRay: rate5pct,
			}},
		},
		{
			name: "to address is case-insensitive",
			traces: []Trace{{
				Action:          TraceAction{To: strings.ToLower(susdsHex), Input: ssrCalldata(t)},
				TransactionHash: txHash1,
			}},
			want: []RateChange{{
				TxHash:           common.HexToHash(txHash1),
				RatePerSecondRay: rate5pct,
			}},
		},
		{
			name: "wrong target contract",
			traces: []Trace{{
				Action:          TraceAction{To: "0x0000000000000000000000000000000000000001", Input: ssrCalldata(t)},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "selector mismatch",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: buildCalldata(t, [4]byte{0xde, 0xad, 0xbe, 0xef}, "ssr", rate5pct)},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "key mismatch (dsr)",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: buildCalldata(t, fileSelector, "dsr", rate5pct)},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "empty input skipped",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: ""},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "0x input skipped",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: "0x"},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "selector only, no key",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: "0x29ae8114"},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "selector and partial key",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: "0x29ae8114737372"},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "matched key but truncated value errors",
			traces: []Trace{{
				// Full selector + key but value cut to 16 bytes.
				Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)[:len(ssrCalldata(t))-32]},
				TransactionHash: txHash1,
			}},
			wantErr: "calldata is 52 bytes, want 68",
		},
		{
			name: "invalid hex input on target errors",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: "0xzzzz"},
				TransactionHash: txHash1,
			}},
			wantErr: "decode input",
		},
		{
			name: "invalid hex input on other contract is ignored",
			traces: []Trace{{
				Action:          TraceAction{To: "0x0000000000000000000000000000000000000001", Input: "0xzzzz"},
				TransactionHash: txHash1,
			}},
			want: nil,
		},
		{
			name: "reverted trace skipped",
			traces: []Trace{{
				Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)},
				TransactionHash: txHash1,
				Error:           "Reverted",
			}},
			want: nil,
		},
		{
			name: "reverted ancestor skipped",
			traces: []Trace{
				{
					Action:          TraceAction{To: "0x0000000000000000000000000000000000000001", Input: "0x"},
					TransactionHash: txHash1,
					TraceAddress:    []int{0},
					Error:           "Reverted",
				},
				{
					Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)},
					TransactionHash: txHash1,
					TraceAddress:    []int{0, 1},
				},
			},
			want: nil,
		},
		{
			name: "reverted sibling does not suppress",
			traces: []Trace{
				{
					Action:          TraceAction{To: "0x0000000000000000000000000000000000000001", Input: "0x"},
					TransactionHash: txHash1,
					TraceAddress:    []int{0},
					Error:           "Reverted",
				},
				{
					Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)},
					TransactionHash: txHash1,
					TraceAddress:    []int{1},
				},
			},
			want: []RateChange{{
				TxHash:           common.HexToHash(txHash1),
				TraceAddress:     []int{1},
				RatePerSecondRay: rate5pct,
			}},
		},
		{
			name: "reverted frame in other transaction does not suppress",
			traces: []Trace{
				{
					Action:          TraceAction{To: "0x0000000000000000000000000000000000000001", Input: "0x"},
					TransactionHash: txHash2,
					TraceAddress:    []int{0},
					Error:           "Reverted",
				},
				{
					Action:          TraceAction{To: susdsHex, Input: ssrCalldata(t)},
					TransactionHash: txHash1,
					TraceAddress:    []int{0, 1},
				},
			},
			want: []RateChange{{
				TxHash:           common.HexToHash(txHash1),
				TraceAddress:     []int{0, 1},
				RatePerSecondRay: rate5pct,
			}},
		},
		{
			name: "max uint256 value decodes exactly",
			traces: []Trace{{
				Action: TraceAction{
					To:    susdsHex,
					Input: buildCalldata(t, fileSelector, "ssr", new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))),
				},
				TransactionHash: txHash1,
			}},
			want: []RateChange{{
				TxHash:           common.HexToHash(txHash1),
				RatePerSecondRay: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)),
			}},
		},
		{
			name: "multiple matches preserve trace order",
			traces: []Trace{
				{
					Action:          TraceAction{To: susdsHex, Input: buildCalldata(t, fileSelector, "ssr", big.NewInt(1))},
					TransactionHash: txHash1,
					TraceAddress:    []int{0},
				},
				{
					Action:          TraceAction{To: "0x0000000000000000000000000000000000000001", Input: "0x"},
					TransactionHash: txHash1,
					TraceAddress:    []int{1},
				},
				{
					Action:          TraceAction{To: susdsHex, Input: buildCalldata(t, fileSelector, "ssr", big.NewInt(2))},
					TransactionHash: txHash2,
					TraceAddress:    []int{0, 3},
				},
			},
			want: []RateChange{
				{TxHash: common.HexToHash(txHash1), TraceAddress: []int{0}, RatePerSecondRay: big.NewInt(1)},
				{TxHash: common.HexToHash(txHash2), TraceAddress: []int{0, 3}, RatePerSecondRay: big.NewInt(2)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := DecodeRateChanges(tt.traces)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("DecodeRateChanges() error = nil, want containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("DecodeRateChanges() error = %q, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeRateChanges() unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("DecodeRateChanges() returned %d changes, want %d: %+v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i].TxHash != tt.want[i].TxHash {
					t.Errorf("change %d: TxHash = %s, want %s", i, got[i].TxHash, tt.want[i].TxHash)
				}
				if traceAddressKey(got[i].TraceAddress) != traceAddressKey(tt.want[i].TraceAddress) {
					t.Errorf("change %d: TraceAddress = %v, want %v", i, got[i].TraceAddress, tt.want[i].TraceAddress)
				}
				if got[i].RatePerSecondRay.Cmp(tt.want[i].RatePerSecondRay) != 0 {
					t.Errorf("change %d: RatePerSecondRay = %s, want %s", i, got[i].RatePerSecondRay, tt.want[i].RatePerSecondRay)
				}
			}
		})
	}
}

func TestTraceAddressKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		addr []int
		want string
	}{
		{name: "nil", addr: nil, want: ""},
		{name: "empty", addr: []int{}, want: ""},
		{name: "single", addr: []int{0}, want: "0"},
		{name: "nested", addr: []int{0, 2, 11}, want: "0,2,11"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := traceAddressKey(tt.addr); got != tt.want {
				t.Errorf("traceAddressKey(%v) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}
