//go:build integration

package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRunIntegration_RedisUnreachable exercises run() through config parsing,
// AWS config loading and SQS consumer construction (both non-dialing) up to
// the Redis ping, the first dependency that actually connects.
func TestRunIntegration_RedisUnreachable(t *testing.T) {
	t.Setenv("S3_BUCKET", "raw-data")
	t.Setenv("DEPLOY_ENV", "test")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := run(ctx, []string{
		"-queue", "https://sqs.eu-west-1.amazonaws.com/000000000000/test-queue",
		"-redis", "localhost:1",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for unreachable Redis")
	}
	if !strings.Contains(err.Error(), "connecting to Redis") {
		t.Errorf("expected Redis connection error, got: %v", err)
	}
}
