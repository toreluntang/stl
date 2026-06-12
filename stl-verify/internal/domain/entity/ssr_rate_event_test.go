package entity

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func validSSRRateEvent() *SSRRateEvent {
	rate, _ := new(big.Int).SetString("1000000001547125957863212448", 10)
	return &SSRRateEvent{
		ChainID:          1,
		BlockNumber:      21000000,
		BlockVersion:     0,
		BlockTime:        time.Unix(1730000000, 0).UTC(),
		TxHash:           common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		TraceAddress:     "0,2",
		RatePerSecondRay: rate,
	}
}

func TestSSRRateEvent_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*SSRRateEvent)
		wantErr string
	}{
		{name: "valid", mutate: func(e *SSRRateEvent) {}},
		{name: "valid with empty trace address", mutate: func(e *SSRRateEvent) { e.TraceAddress = "" }},
		{name: "valid with zero rate", mutate: func(e *SSRRateEvent) { e.RatePerSecondRay = big.NewInt(0) }},
		{name: "zero chain id", mutate: func(e *SSRRateEvent) { e.ChainID = 0 }, wantErr: "chain id"},
		{name: "zero block number", mutate: func(e *SSRRateEvent) { e.BlockNumber = 0 }, wantErr: "block number"},
		{name: "negative block version", mutate: func(e *SSRRateEvent) { e.BlockVersion = -1 }, wantErr: "block version"},
		{name: "zero block time", mutate: func(e *SSRRateEvent) { e.BlockTime = time.Time{} }, wantErr: "block time"},
		{name: "zero tx hash", mutate: func(e *SSRRateEvent) { e.TxHash = common.Hash{} }, wantErr: "tx hash"},
		{name: "nil rate", mutate: func(e *SSRRateEvent) { e.RatePerSecondRay = nil }, wantErr: "rate per second is required"},
		{name: "negative rate", mutate: func(e *SSRRateEvent) { e.RatePerSecondRay = big.NewInt(-1) }, wantErr: "must not be negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := validSSRRateEvent()
			tt.mutate(event)

			err := event.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %q, want containing %q", err, tt.wantErr)
			}
		})
	}
}
