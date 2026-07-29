-- +goose Up
CREATE TABLE IF NOT EXISTS `attempts` (
    tenant_id Utf8,
    attempt_id Utf8,
    run_id Utf8,
    attempt_number Uint32,
    status Utf8,
    worker_id Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, attempt_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
