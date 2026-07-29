-- +goose Up
CREATE TABLE IF NOT EXISTS `leases` (
    tenant_id Utf8,
    lease_id Utf8,
    run_id Utf8,
    attempt_id Utf8,
    worker_id Utf8,
    fence_token Uint64,
    acquired_at Timestamp,
    expires_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, lease_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
