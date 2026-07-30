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
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
