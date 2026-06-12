-- SSR (Sky Savings Rate) rate-change events decoded from sUSDS file() traces.
--
-- One row per successful file(bytes32("ssr"), uint256) trace on the sUSDS
-- contract (0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD). Populated by the
-- ssr-rate-indexer worker (live tail) and the ssr-rate-backfill one-shot.
--
-- rate_per_second_ray stores the raw RAY-scaled (1e27) per-second rate as an
-- exact NUMERIC. Conversion to APY ((rate/1e27)^31536000 - 1) belongs to the
-- read layer / consumer and is never persisted here.
--
-- Volume is tiny (a handful of rate changes per year), so this is a plain
-- table, not a hypertable.
--
-- Auditability follows the repo+trigger pattern used by token_total_supply:
--   - Repository supplies build_id at construction.
--   - assign_processing_version_ssr_rate_event BEFORE INSERT trigger assigns
--     processing_version so same-build replays are idempotent and cross-build
--     reprocesses append a new version.

CREATE TABLE IF NOT EXISTS ssr_rate_event (
    chain_id            INT         NOT NULL REFERENCES chain (chain_id),
    block_number        BIGINT      NOT NULL,
    block_version       INT         NOT NULL DEFAULT 0,
    block_time          TIMESTAMPTZ NOT NULL,
    tx_hash             BYTEA       NOT NULL,
    trace_address       TEXT        NOT NULL,
    rate_per_second_ray NUMERIC     NOT NULL,
    processing_version  INT         NOT NULL DEFAULT 0,
    build_id            INT         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, block_number, block_version, tx_hash, trace_address, processing_version)
);

-- No secondary indexes: the table holds a handful of rows per year, so the
-- primary key plus sequential scans cover every access pattern.

CREATE OR REPLACE FUNCTION assign_processing_version_ssr_rate_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('ssr|%s|%s|%s|%s|%s',
            NEW.chain_id,
            NEW.block_number,
            NEW.block_version,
            NEW.tx_hash,
            NEW.trace_address),
        0));

    SELECT processing_version INTO existing_ver
    FROM ssr_rate_event
    WHERE chain_id      = NEW.chain_id
      AND block_number  = NEW.block_number
      AND block_version = NEW.block_version
      AND tx_hash       = NEW.tx_hash
      AND trace_address = NEW.trace_address
      AND build_id      = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM ssr_rate_event
        WHERE chain_id      = NEW.chain_id
          AND block_number  = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash       = NEW.tx_hash
          AND trace_address = NEW.trace_address;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON ssr_rate_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_ssr_rate_event();

GRANT SELECT ON ssr_rate_event TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON ssr_rate_event TO stl_readwrite;

-- Metadata catalogue comments (see 20260609_120000_add_schema_comments.sql).

COMMENT ON TABLE ssr_rate_event IS
  '[Operational] SSR (Sky Savings Rate) rate changes decoded from successful file(bytes32("ssr"), uint256) execution traces on the sUSDS contract. Append-only; a handful of rows per year.';
COMMENT ON COLUMN ssr_rate_event.chain_id IS
  'FK→chain.chain_id. Always 1 (Ethereum mainnet) today; sUSDS governance lives on mainnet.';
COMMENT ON COLUMN ssr_rate_event.block_number IS
  'Block containing the file() trace.';
COMMENT ON COLUMN ssr_rate_event.block_version IS
  'Reorg version of the block (0 = first observation). Matches the block cache versioning.';
COMMENT ON COLUMN ssr_rate_event.block_time IS
  'Timestamp of the block containing the trace. Drives effective-date bucketing in consumers.';
COMMENT ON COLUMN ssr_rate_event.tx_hash IS
  'Transaction hash containing the file() trace (32 bytes).';
COMMENT ON COLUMN ssr_rate_event.trace_address IS
  'Comma-joined trace path within the transaction (empty = top-level call). Disambiguates multiple file(ssr) calls in one transaction.';
COMMENT ON COLUMN ssr_rate_event.rate_per_second_ray IS
  'Raw per-second rate, ray-scaled (1e27). Exact on-chain uint256; APY = (value/1e27)^31536000 - 1 computed by readers.';
COMMENT ON COLUMN ssr_rate_event.processing_version IS
  'Audit. Assigned by trigger; same build re-inserting the same natural key keeps the version (idempotent), a new build appends version+1.';
COMMENT ON COLUMN ssr_rate_event.build_id IS
  'Audit. FK-like reference to the build registry row of the writer binary.';
COMMENT ON COLUMN ssr_rate_event.created_at IS
  'Audit. Wall-clock insert time.';

INSERT INTO migrations (filename)
VALUES ('20260612_130000_create_ssr_rate_event.sql')
ON CONFLICT (filename) DO NOTHING;
