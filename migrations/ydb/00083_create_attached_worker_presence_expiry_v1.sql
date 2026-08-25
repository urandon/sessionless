-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_presence_expiry_v1` (
    shard_bucket Uint32,
    presence_expires_at Timestamp,
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    connection_id Utf8,
    connection_generation Uint64,
    connection_revision Uint64,
    PRIMARY KEY (shard_bucket, presence_expires_at, tenant_id, owner_user_id, worker_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
