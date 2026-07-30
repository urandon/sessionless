-- +goose Up
CREATE TABLE IF NOT EXISTS `lease_expiry_v2` (
    shard_bucket Uint32,
    expires_at Timestamp,
    tenant_id Utf8,
    run_id Utf8,
    lease_id Utf8,
    fence_token Uint64,
    PRIMARY KEY (shard_bucket, expires_at, tenant_id, run_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
