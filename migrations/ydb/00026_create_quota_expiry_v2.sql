-- +goose Up
CREATE TABLE IF NOT EXISTS `quota_expiry_v2` (
    shard_bucket Uint32,
    expires_at Timestamp,
    tenant_id Utf8,
    quota_reservation_id Utf8,
    run_id Utf8,
    subscription_connection_id Utf8,
    PRIMARY KEY (shard_bucket, expires_at, tenant_id, quota_reservation_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
