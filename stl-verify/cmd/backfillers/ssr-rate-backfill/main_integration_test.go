//go:build integration

package main

import (
	"strings"
	"testing"
)

// TestRunIntegration_BadDatabaseURL exercises run() up to the database
// connection. S3 reader construction is lazy (no dial), so an unreachable
// Postgres is the first hard dependency.
func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	err := run([]string{
		"-from", "100",
		"-to", "105",
		"-bucket", "raw-data",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	if !strings.Contains(err.Error(), "connecting to database") {
		t.Errorf("expected database connection error, got: %v", err)
	}
}
