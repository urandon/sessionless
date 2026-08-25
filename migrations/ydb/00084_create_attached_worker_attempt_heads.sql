-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_attempt_heads` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    connection_id Utf8,
    attempt_id Utf8,
    run_id Utf8,
    lease_id Utf8,
    lease_generation Uint64,
    fence_token Utf8,
    enrollment_generation Uint64,
    connection_generation Uint64,
    state Utf8,
    lease_expires_at Timestamp,
    cancel_deadline Timestamp,
    updated_at Timestamp,
    revision Uint64,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, worker_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
