package main

import (
	"strings"
	"testing"
)

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name: "all flags provided via CLI",
			args: []string{"-from", "1000", "-to", "2000", "-bucket", "raw-data", "-db", "postgres://localhost:5432/testdb"},
			wantCfg: cliConfig{
				fromBlock:   1000,
				toBlock:     2000,
				concurrency: 10,
				bucket:      "raw-data",
				dbURL:       "postgres://localhost:5432/testdb",
				chainID:     1,
				awsRegion:   "eu-west-1",
			},
		},
		{
			name: "custom concurrency, chain-id and region",
			args: []string{"-from", "100", "-to", "200", "-bucket", "b", "-db", "postgres://localhost/db", "-concurrency", "50", "-chain-id", "8453", "-aws-region", "us-east-1"},
			wantCfg: cliConfig{
				fromBlock:   100,
				toBlock:     200,
				concurrency: 50,
				bucket:      "b",
				dbURL:       "postgres://localhost/db",
				chainID:     8453,
				awsRegion:   "us-east-1",
			},
		},
		{
			name:    "db from env var",
			args:    []string{"-from", "100", "-to", "200", "-bucket", "b"},
			envVars: map[string]string{"DATABASE_URL": "postgres://localhost:5432/envdb"},
			wantCfg: cliConfig{
				fromBlock:   100,
				toBlock:     200,
				concurrency: 10,
				bucket:      "b",
				dbURL:       "postgres://localhost:5432/envdb",
				chainID:     1,
				awsRegion:   "eu-west-1",
			},
		},
		{
			name: "CLI db flag takes precedence over env var",
			args: []string{"-from", "100", "-to", "200", "-bucket", "b", "-db", "postgres://localhost/cli-db"},
			envVars: map[string]string{
				"DATABASE_URL": "postgres://localhost/env-db",
			},
			wantCfg: cliConfig{
				fromBlock:   100,
				toBlock:     200,
				concurrency: 10,
				bucket:      "b",
				dbURL:       "postgres://localhost/cli-db",
				chainID:     1,
				awsRegion:   "eu-west-1",
			},
		},
		{
			name: "from equals to is valid",
			args: []string{"-from", "100", "-to", "100", "-bucket", "b", "-db", "postgres://localhost/db"},
			wantCfg: cliConfig{
				fromBlock:   100,
				toBlock:     100,
				concurrency: 10,
				bucket:      "b",
				dbURL:       "postgres://localhost/db",
				chainID:     1,
				awsRegion:   "eu-west-1",
			},
		},
		{
			name:      "missing from",
			args:      []string{"-to", "200", "-bucket", "b", "-db", "postgres://localhost/db"},
			wantError: "--from is required",
		},
		{
			name:      "missing to",
			args:      []string{"-from", "100", "-bucket", "b", "-db", "postgres://localhost/db"},
			wantError: "--to is required",
		},
		{
			name:      "to less than from",
			args:      []string{"-from", "200", "-to", "100", "-bucket", "b", "-db", "postgres://localhost/db"},
			wantError: "--to must be >= --from",
		},
		{
			name:      "missing bucket",
			args:      []string{"-from", "100", "-to", "200", "-db", "postgres://localhost/db"},
			wantError: "--bucket is required",
		},
		{
			name:      "missing database URL - no flag no env",
			args:      []string{"-from", "100", "-to", "200", "-bucket", "b"},
			envVars:   map[string]string{"DATABASE_URL": ""},
			wantError: "database URL not provided",
		},
		{
			name:      "invalid flag",
			args:      []string{"--nonexistent"},
			wantError: "flag provided but not defined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Isolate from the ambient environment.
			t.Setenv("DATABASE_URL", "")
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			cfg, err := parseFlags(tt.args)

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
