-- +goose Up
CREATE TABLE IF NOT EXISTS `worker_jobs` (
    tenant_id Utf8,
    run_id Utf8,
    attempt_id Utf8,
    reservation_id Utf8,
    created_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, run_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
