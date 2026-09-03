package main

import (
	"strings"
	"testing"
)

func TestParseChainID(t *testing.T) {
	tests := []struct {
		name      string
		chainID   string
		set       bool
		want      int64
		wantError string
	}{
		{name: "mainnet", chainID: "1", set: true, want: 1},
		{name: "avalanche", chainID: "43114", set: true, want: 43114},
		{name: "missing", set: false, wantError: "CHAIN_ID environment variable is required"},
		{name: "invalid", chainID: "not-a-number", set: true, wantError: "parsing CHAIN_ID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set {
				t.Setenv("CHAIN_ID", tt.chainID)
			} else {
				t.Setenv("CHAIN_ID", "")
			}

			got, err := parseChainID()

			if tt.wantError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantError)
				}
				if !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("expected %d, got %d", tt.want, got)
			}
		})
	}
}
