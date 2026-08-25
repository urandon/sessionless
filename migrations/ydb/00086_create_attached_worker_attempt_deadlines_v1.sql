-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_attempt_deadlines_v1` (
    shard_bucket Uint32,
    deadline_at Timestamp,
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    attempt_id Utf8,
    kind Utf8,
    lease_generation Uint64,
    attempt_revision Uint64,
    PRIMARY KEY (
        shard_bucket, deadline_at, tenant_id, owner_user_id,
        worker_id, attempt_id, kind
    )
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
