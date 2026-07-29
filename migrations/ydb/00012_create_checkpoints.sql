-- +goose Up
CREATE TABLE IF NOT EXISTS `checkpoints` (
    tenant_id Utf8,
    attempt_id Utf8,
    sequence Uint64,
    checkpoint_id Utf8,
    run_id Utf8,
    blob_key Utf8,
    created_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, attempt_id, sequence)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
