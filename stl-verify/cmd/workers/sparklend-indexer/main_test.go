package main

import (
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantChain int64
		wantError string
	}{
		{
			name: "chain ID from env",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "my-bucket",
				"DEPLOY_ENV":      "sentinelstaging",
				"CHAIN_ID":        "1",
			},
			wantChain: 1,
		},
		{
			name: "non-default chain ID from env",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "my-bucket",
				"DEPLOY_ENV":      "sentinelstaging",
				"CHAIN_ID":        "43114",
			},
			wantChain: 43114,
		},
		{
			name: "missing CHAIN_ID",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "my-bucket",
				"DEPLOY_ENV":      "sentinelstaging",
			},
			wantError: "CHAIN_ID environment variable is required",
		},
		{
			name: "invalid CHAIN_ID",
			args: []string{
				"-queue", "https://sqs.us-east-1.amazonaws.com/123/q",
				"-db", "postgres://localhost/db",
				"-redis", "localhost:6379",
			},
			envVars: map[string]string{
				"ALCHEMY_API_KEY": "test-key",
				"S3_BUCKET":       "my-bucket",
				"DEPLOY_ENV":      "sentinelstaging",
				"CHAIN_ID":        "not-a-number",
			},
			wantError: "parsing CHAIN_ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"ALCHEMY_API_KEY", "ALCHEMY_HTTP_URL", "AWS_SQS_QUEUE_URL",
				"DATABASE_URL", "REDIS_ADDR", "REDIS_PASSWORD",
				"CHAIN_ID", "S3_BUCKET", "DEPLOY_ENV", "AWS_SQS_ENDPOINT",
			} {
				if _, has := tt.envVars[key]; !has {
					t.Setenv(key, "")
				}
			}

			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			cfg, err := parseConfig(tt.args)

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
			if cfg.chainID != tt.wantChain {
				t.Errorf("chainID: expected %d, got %d", tt.wantChain, cfg.chainID)
			}
		})
	}
}
