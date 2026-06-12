// Package main consumes Ethereum block events from SQS, scans each block's
// cached execution traces for SSR (Sky Savings Rate) rate changes —
// file(bytes32("ssr"), uint256) calls on sUSDS — and persists them to
// Postgres.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cache"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	redisAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	sqsAdapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/services/ssr_rate_tracker"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info.
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("ssr-rate-indexer exited with error", "error", err)
		os.Exit(1)
	}
}

type cliConfig struct {
	queueURL          string
	redisAddr         string
	dbURL             string
	s3Bucket          string
	deployEnv         string
	maxMessages       int
	waitTime          int
	visibilityTimeout int
	chainID           int64
}

func parseConfig(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("ssr-rate-indexer", flag.ContinueOnError)
	queueURL := fs.String("queue", "", "SQS Queue URL")
	redisAddr := fs.String("redis", "", "Redis address")
	dbURL := fs.String("db", "", "PostgreSQL connection URL")
	maxMessages := fs.Int("max", 10, "Max messages per poll")
	waitTime := fs.Int("wait", 20, "Wait time in seconds (long polling)")
	visibilityTimeout := fs.Int("visibility-timeout", 300, "SQS visibility timeout in seconds")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, fmt.Errorf("parse flags: %w", err)
	}

	cfg := cliConfig{
		queueURL:          *queueURL,
		redisAddr:         *redisAddr,
		dbURL:             *dbURL,
		maxMessages:       *maxMessages,
		waitTime:          *waitTime,
		visibilityTimeout: *visibilityTimeout,
	}

	if cfg.queueURL == "" {
		cfg.queueURL = env.Get("AWS_SQS_QUEUE_URL", "")
	}
	if cfg.queueURL == "" {
		return cliConfig{}, fmt.Errorf("queue URL not provided (use -queue flag or AWS_SQS_QUEUE_URL env var)")
	}

	if cfg.dbURL == "" {
		cfg.dbURL = env.Get("DATABASE_URL", "")
	}
	if cfg.dbURL == "" {
		return cliConfig{}, fmt.Errorf("database URL not provided (use -db flag or DATABASE_URL env var)")
	}

	if cfg.redisAddr == "" {
		cfg.redisAddr = env.Get("REDIS_ADDR", "")
	}
	if cfg.redisAddr == "" {
		return cliConfig{}, fmt.Errorf("redis address not provided (use -redis flag or REDIS_ADDR env var)")
	}

	if waitTimeStr := env.Get("SQS_WAIT_TIME", ""); waitTimeStr != "" {
		v, err := strconv.Atoi(waitTimeStr)
		if err != nil {
			return cliConfig{}, fmt.Errorf("parsing SQS_WAIT_TIME %q: %w", waitTimeStr, err)
		}
		cfg.waitTime = v
	}
	if visTimeStr := env.Get("SQS_VISIBILITY_TIMEOUT", ""); visTimeStr != "" {
		v, err := strconv.Atoi(visTimeStr)
		if err != nil {
			return cliConfig{}, fmt.Errorf("parsing SQS_VISIBILITY_TIMEOUT %q: %w", visTimeStr, err)
		}
		cfg.visibilityTimeout = v
	}

	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.ParseInt(chainIDStr, 10, 64)
	if err != nil {
		return cliConfig{}, fmt.Errorf("parsing CHAIN_ID %q: %w", chainIDStr, err)
	}
	cfg.chainID = chainID

	cfg.s3Bucket = env.Get("S3_BUCKET", "")
	if cfg.s3Bucket == "" {
		return cliConfig{}, fmt.Errorf("S3_BUCKET environment variable is required")
	}

	cfg.deployEnv = env.Get("DEPLOY_ENV", "")
	if cfg.deployEnv == "" {
		return cliConfig{}, fmt.Errorf("DEPLOY_ENV environment variable is required")
	}

	return cfg, nil
}

// run is the entry point for the ssr-rate-indexer.
// It is extracted from main() to allow integration testing.
func run(ctx context.Context, args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{
		StaticCredentialsFromEnv: true,
	})
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// SQS
	sqsConsumer, err := sqsAdapter.NewConsumer(awsCfg, sqsAdapter.Config{
		QueueURL:          cfg.queueURL,
		WaitTimeSeconds:   int32(cfg.waitTime),
		VisibilityTimeout: int32(cfg.visibilityTimeout),
		BaseEndpoint:      env.Get("AWS_SQS_ENDPOINT", ""),
	}, logger)
	if err != nil {
		return fmt.Errorf("creating SQS consumer: %w", err)
	}
	defer sqsConsumer.Close()

	// Redis (block cache)
	cacheCfg := redisAdapter.ConfigDefaults()
	cacheCfg.Addr = cfg.redisAddr
	cacheCfg.Password = env.Get("REDIS_PASSWORD", "")
	blockCache, err := redisAdapter.NewBlockCache(cacheCfg, logger)
	if err != nil {
		return fmt.Errorf("creating block cache: %w", err)
	}
	if err := blockCache.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to Redis at %s: %w", cfg.redisAddr, err)
	}
	defer blockCache.Close()
	logger.Info("Redis connected", "addr", cfg.redisAddr)

	// S3 + cache reader with fallback
	s3Opts := []func(*awss3.Options){}
	if s3Endpoint := env.Get("AWS_S3_ENDPOINT", ""); s3Endpoint != "" {
		s3Opts = append(s3Opts, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
			o.UsePathStyle = true
		})
	}
	s3Reader := s3adapter.NewReaderWithOptions(awsCfg, logger, s3Opts...)
	cacheReader, err := cache.NewReaderWithFallback(blockCache, s3Reader, cfg.chainID, cfg.deployEnv, cfg.s3Bucket, logger)
	if err != nil {
		return fmt.Errorf("creating cache reader: %w", err)
	}

	// PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	buildReg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return fmt.Errorf("registering build: %w", err)
	}

	txm, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}
	repo := postgres.NewSSRRateEventRepository(pool, txm, logger, buildReg.BuildID())

	svc, err := ssr_rate_tracker.NewService(
		ssr_rate_tracker.Config{
			ChainID:     cfg.chainID,
			MaxMessages: cfg.maxMessages,
			Logger:      logger,
		},
		sqsConsumer,
		cacheReader,
		repo,
	)
	if err != nil {
		return fmt.Errorf("creating ssr rate tracker: %w", err)
	}

	logger.Info("starting ssr-rate-indexer",
		"commit", buildReg.GitHash(),
		"branch", GitBranch,
		"buildTime", BuildTime,
		"chainID", cfg.chainID,
		"queue", cfg.queueURL,
	)

	return lifecycle.Run(ctx, logger, svc)
}
