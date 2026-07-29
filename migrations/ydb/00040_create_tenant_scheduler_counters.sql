-- +goose Up
CREATE TABLE IF NOT EXISTS `tenant_scheduler_counters` (
    tenant_id Utf8,
    queue_depth Uint32,
    active_runs Uint32,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512,
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 256
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
