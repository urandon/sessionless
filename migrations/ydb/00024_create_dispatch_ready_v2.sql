-- +goose Up
CREATE TABLE IF NOT EXISTS `dispatch_ready_v2` (
    shard_bucket Uint32,
    available_at Timestamp,
    tenant_id Utf8,
    dispatch_outbox_id Utf8,
    run_id Utf8,
    attempt_id Utf8,
    PRIMARY KEY (shard_bucket, available_at, tenant_id, dispatch_outbox_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512,
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 256,
    PARTITION_AT_KEYS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
