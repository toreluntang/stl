// Package main provides a one-shot CLI tool that backfills historical SSR
// (Sky Savings Rate) rate changes by scanning execution-trace files in the
// raw-data S3 bucket for file(bytes32("ssr"), uint256) calls on sUSDS and
// persisting matches to PostgreSQL.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/ssr_rate_tracker"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
	slog.Info("completed successfully")
}

type cliConfig struct {
	fromBlock   int64
	toBlock     int64
	concurrency int
	bucket      string
	dbURL       string
	chainID     int64
	awsRegion   string
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("ssr-rate-backfill", flag.ContinueOnError)
	fromBlock := fs.Int64("from", -1, "Start block number (required)")
	toBlock := fs.Int64("to", -1, "End block number (required)")
	concurrency := fs.Int("concurrency", 10, "Number of concurrent workers")
	bucket := fs.String("bucket", "", "S3 bucket containing raw block data (traces + blocks)")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	chainID := fs.Int64("chain-id", 1, "Ethereum chain ID")
	awsRegion := fs.String("aws-region", "eu-west-1", "AWS region")

	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}

	cfg := cliConfig{
		fromBlock:   *fromBlock,
		toBlock:     *toBlock,
		concurrency: *concurrency,
		bucket:      *bucket,
		dbURL:       *dbURL,
		chainID:     *chainID,
		awsRegion:   *awsRegion,
	}

	if cfg.fromBlock < 0 {
		return cliConfig{}, fmt.Errorf("--from is required")
	}
	if cfg.toBlock < 0 {
		return cliConfig{}, fmt.Errorf("--to is required")
	}
	if cfg.toBlock < cfg.fromBlock {
		return cliConfig{}, fmt.Errorf("--to must be >= --from")
	}
	if cfg.bucket == "" {
		return cliConfig{}, fmt.Errorf("--bucket is required")
	}

	if cfg.dbURL == "" {
		cfg.dbURL = env.Get("DATABASE_URL", "")
	}
	if cfg.dbURL == "" {
		return cliConfig{}, fmt.Errorf("database URL not provided (use --db flag or DATABASE_URL env var)")
	}

	return cfg, nil
}

func run(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)
	go func() {
		select {
		case sig := <-sigChan:
			logger.Info("received signal, shutting down...", "signal", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	// Load AWS config; support LocalStack via AWS_ENDPOINT_URL
	awsOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.awsRegion),
	}
	if endpointURL := os.Getenv("AWS_ENDPOINT_URL"); endpointURL != "" {
		awsOpts = append(awsOpts, config.WithBaseEndpoint(endpointURL))
		logger.Info("using custom AWS endpoint", "url", endpointURL)
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// When a custom endpoint is configured (e.g. LocalStack), path-style addressing
	// is required because virtual-hosted-style URLs won't resolve correctly.
	var s3Reader *s3adapter.Reader
	if os.Getenv("AWS_ENDPOINT_URL") != "" {
		s3Reader = s3adapter.NewReaderWithOptions(awsCfg, logger, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	} else {
		s3Reader = s3adapter.NewReader(awsCfg, logger)
	}
	logger.Info("S3 reader created")

	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating tx manager: %w", err)
	}
	repo := postgres.NewSSRRateEventRepository(pool, txm, logger, buildReg.BuildID())

	backfiller, err := ssr_rate_tracker.NewBackfiller(
		ssr_rate_tracker.BackfillConfig{
			Concurrency: cfg.concurrency,
			Logger:      logger,
		},
		s3Reader,
		repo,
		cfg.bucket,
		cfg.chainID,
	)
	if err != nil {
		return fmt.Errorf("creating backfiller: %w", err)
	}

	logger.Info("starting ssr rate backfill",
		"fromBlock", cfg.fromBlock,
		"toBlock", cfg.toBlock,
		"concurrency", cfg.concurrency,
		"bucket", cfg.bucket,
		"chainID", cfg.chainID,
	)

	return backfiller.Run(ctx, cfg.fromBlock, cfg.toBlock)
}
