package blockchain

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var testVatAddress = common.HexToAddress("0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b")

func newTestVatCaller(t *testing.T, mock *testutil.MockMulticaller) *VatCaller {
	t.Helper()
	caller, err := NewVatCaller(mock, testVatAddress)
	if err != nil {
		t.Fatalf("NewVatCaller: %v", err)
	}
	return caller
}

func testDebtQuery() entity.DebtQuery {
	return entity.DebtQuery{
		Ilk:          [32]byte{1, 2, 3},
		VaultAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
	}
}

func packIlksForTest(t *testing.T, rate *big.Int) []byte {
	t.Helper()
	vatABI, err := abis.GetVatABI()
	if err != nil {
		t.Fatalf("GetVatABI: %v", err)
	}
	data, err := vatABI.Methods["ilks"].Outputs.Pack(
		big.NewInt(100), rate, big.NewInt(3), big.NewInt(4), big.NewInt(5))
	if err != nil {
		t.Fatalf("pack ilks outputs: %v", err)
	}
	return data
}

func packUrnsForTest(t *testing.T, art *big.Int) []byte {
	t.Helper()
	vatABI, err := abis.GetVatABI()
	if err != nil {
		t.Fatalf("GetVatABI: %v", err)
	}
	data, err := vatABI.Methods["urns"].Outputs.Pack(big.NewInt(7), art)
	if err != nil {
		t.Fatalf("pack urns outputs: %v", err)
	}
	return data
}

func TestReadDebts_Success(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	rate := big.NewInt(1e9)
	art := big.NewInt(2e9)
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			t.Fatalf("expected 2 calls, got %d", len(calls))
		}
		return []outbound.Result{
			{Success: true, ReturnData: packIlksForTest(t, rate)},
			{Success: true, ReturnData: packUrnsForTest(t, art)},
		}, nil
	}

	results, err := newTestVatCaller(t, mock).ReadDebts(context.Background(), []entity.DebtQuery{testDebtQuery()}, big.NewInt(100))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Reverted {
		t.Error("expected Reverted=false on success")
	}
	if results[0].Rate.Cmp(rate) != 0 {
		t.Errorf("Rate = %s, want %s", results[0].Rate, rate)
	}
	if results[0].Art.Cmp(art) != 0 {
		t.Errorf("Art = %s, want %s", results[0].Art, art)
	}
}

func TestReadDebts_RevertSkipsWithoutError(t *testing.T) {
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: false},
			{Success: true, ReturnData: packUrnsForTest(t, big.NewInt(1))},
		}, nil
	}

	results, err := newTestVatCaller(t, mock).ReadDebts(context.Background(), []entity.DebtQuery{testDebtQuery()}, big.NewInt(100))
	if err != nil {
		t.Fatalf("revert must not fail the batch, got: %v", err)
	}
	if len(results) != 1 || !results[0].Reverted {
		t.Fatalf("expected one Reverted result, got %+v", results)
	}
}

func TestReadDebts_StructuralFailuresFailBatch(t *testing.T) {
	rate := big.NewInt(1e9)

	tests := []struct {
		name    string
		results []outbound.Result
		wantErr string
	}{
		{
			name:    "missing results",
			results: []outbound.Result{},
			wantErr: "missing multicall results",
		},
		{
			name: "undecodable ilks",
			results: []outbound.Result{
				{Success: true, ReturnData: []byte{0x01, 0x02}},
				{Success: true, ReturnData: packUrnsForTest(t, big.NewInt(1))},
			},
			wantErr: "unpack ilks",
		},
		{
			name: "undecodable urns",
			results: []outbound.Result{
				{Success: true, ReturnData: packIlksForTest(t, rate)},
				{Success: true, ReturnData: []byte{0x01, 0x02}},
			},
			wantErr: "unpack urns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := testutil.NewMockMulticaller()
			mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return tt.results, nil
			}

			results, err := newTestVatCaller(t, mock).ReadDebts(context.Background(), []entity.DebtQuery{testDebtQuery()}, big.NewInt(100))
			if err == nil {
				t.Fatalf("expected batch error containing %q, got nil (results=%+v)", tt.wantErr, results)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
			if results != nil {
				t.Errorf("expected nil results on batch failure, got %+v", results)
			}
		})
	}
}
