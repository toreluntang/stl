package blockevent

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecode(t *testing.T) {
	validHash := "0x" + strings.Repeat("ab", 32)

	tests := []struct {
		name      string
		body      string
		wantChain int64
		wantHash  string
		wantError string
	}{
		{
			name:      "valid event",
			body:      `{"chainId":1,"blockNumber":100,"version":0,"blockHash":"` + validHash + `"}`,
			wantChain: 1,
			wantHash:  validHash,
		},
		{
			name:      "malformed JSON",
			body:      "not json",
			wantError: "malformed body",
		},
		{
			name:      "empty body",
			body:      "",
			wantError: "malformed body",
		},
		{
			name:      "SNS envelope rejected",
			body:      `{"Type":"Notification","Message":"{\"chainId\":1}","Timestamp":"2024-01-01T00:00:00Z"}`,
			wantError: "SNS notification envelope",
		},
		{
			name:      "zero event rejected",
			body:      `{}`,
			wantError: "missing chainId",
		},
		{
			name:      "explicit zero chain rejected",
			body:      `{"chainId":0,"blockNumber":100,"blockHash":"` + validHash + `"}`,
			wantError: "missing chainId",
		},
		{
			name:      "missing hash rejected",
			body:      `{"chainId":1,"blockNumber":100}`,
			wantError: "missing blockHash",
		},
		{
			name:      "unknown fields tolerated",
			body:      `{"chainId":43114,"blockNumber":7,"blockHash":"0xabc","futureField":true}`,
			wantChain: 43114,
			wantHash:  "0xabc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := Decode([]byte(tt.body))

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
			if event.ChainID != tt.wantChain {
				t.Errorf("ChainID: expected %d, got %d", tt.wantChain, event.ChainID)
			}
			if event.BlockHash != tt.wantHash {
				t.Errorf("BlockHash: expected %q, got %q", tt.wantHash, event.BlockHash)
			}
		})
	}
}

func TestDecode_EnvelopeShape(t *testing.T) {
	eventJSON, err := json.Marshal(map[string]any{"chainId": 1, "blockHash": "0xabc"})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	envelope, err := json.Marshal(map[string]any{
		"Type":      "Notification",
		"Message":   string(eventJSON),
		"Timestamp": "2024-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}

	if _, err := Decode(envelope); err == nil {
		t.Fatal("expected envelope to be rejected, got nil error")
	} else if !strings.Contains(err.Error(), "SNS notification envelope") {
		t.Fatalf("expected SNS envelope error, got %q", err.Error())
	}
}
