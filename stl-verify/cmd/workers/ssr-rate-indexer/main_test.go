package main

import (
	"strings"
	"testing"
)

// isolateEnv clears every env var parseConfig reads so ambient values from
// the developer/CI environment can't leak into a test case.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AWS_SQS_QUEUE_URL", "DATABASE_URL", "REDIS_ADDR",
		"SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT", "CHAIN_ID",
		"S3_BUCKET", "DEPLOY_ENV",
	} {
		t.Setenv(k, "")
	}
}

func TestParseConfig(t *testing.T) {
	baseEnv := map[string]string{
		"S3_BUCKET":  "raw-data",
		"DEPLOY_ENV": "test",
	}

	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name:    "all flags provided via CLI",
			args:    []string{"-queue", "https://sqs/q", "-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars: baseEnv,
			wantCfg: cliConfig{
				queueURL:          "https://sqs/q",
				redisAddr:         "localhost:6379",
				dbURL:             "postgres://localhost/db",
				s3Bucket:          "raw-data",
				deployEnv:         "test",
				maxMessages:       10,
				waitTime:          20,
				visibilityTimeout: 300,
				chainID:           1,
			},
		},
		{
			name: "everything from env vars",
			args: nil,
			envVars: map[string]string{
				"AWS_SQS_QUEUE_URL":      "https://sqs/env-q",
				"DATABASE_URL":           "postgres://localhost/envdb",
				"REDIS_ADDR":             "redis:6379",
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
				"CHAIN_ID":               "8453",
				"S3_BUCKET":              "raw-data",
				"DEPLOY_ENV":             "staging",
			},
			wantCfg: cliConfig{
				queueURL:          "https://sqs/env-q",
				redisAddr:         "redis:6379",
				dbURL:             "postgres://localhost/envdb",
				s3Bucket:          "raw-data",
				deployEnv:         "staging",
				maxMessages:       10,
				waitTime:          5,
				visibilityTimeout: 60,
				chainID:           8453,
			},
		},
		{
			name:      "missing queue URL",
			args:      []string{"-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars:   baseEnv,
			wantError: "queue URL not provided",
		},
		{
			name:      "missing database URL",
			args:      []string{"-queue", "https://sqs/q", "-redis", "localhost:6379"},
			envVars:   baseEnv,
			wantError: "database URL not provided",
		},
		{
			name:      "missing redis address",
			args:      []string{"-queue", "https://sqs/q", "-db", "postgres://localhost/db"},
			envVars:   baseEnv,
			wantError: "redis address not provided",
		},
		{
			name:      "missing S3 bucket",
			args:      []string{"-queue", "https://sqs/q", "-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars:   map[string]string{"DEPLOY_ENV": "test"},
			wantError: "S3_BUCKET environment variable is required",
		},
		{
			name:      "missing deploy env",
			args:      []string{"-queue", "https://sqs/q", "-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars:   map[string]string{"S3_BUCKET": "raw-data"},
			wantError: "DEPLOY_ENV environment variable is required",
		},
		{
			name: "invalid SQS_WAIT_TIME",
			args: []string{"-queue", "https://sqs/q", "-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars: map[string]string{
				"S3_BUCKET": "raw-data", "DEPLOY_ENV": "test", "SQS_WAIT_TIME": "abc",
			},
			wantError: "parsing SQS_WAIT_TIME",
		},
		{
			name: "invalid SQS_VISIBILITY_TIMEOUT",
			args: []string{"-queue", "https://sqs/q", "-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars: map[string]string{
				"S3_BUCKET": "raw-data", "DEPLOY_ENV": "test", "SQS_VISIBILITY_TIMEOUT": "xyz",
			},
			wantError: "parsing SQS_VISIBILITY_TIMEOUT",
		},
		{
			name: "invalid CHAIN_ID",
			args: []string{"-queue", "https://sqs/q", "-redis", "localhost:6379", "-db", "postgres://localhost/db"},
			envVars: map[string]string{
				"S3_BUCKET": "raw-data", "DEPLOY_ENV": "test", "CHAIN_ID": "not-a-number",
			},
			wantError: "parsing CHAIN_ID",
		},
		{
			name:      "invalid flag",
			args:      []string{"--nonexistent"},
			envVars:   baseEnv,
			wantError: "flag provided but not defined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateEnv(t)
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
			if cfg != tt.wantCfg {
				t.Errorf("config mismatch:\n got  %+v\n want %+v", cfg, tt.wantCfg)
			}
		})
	}
}
