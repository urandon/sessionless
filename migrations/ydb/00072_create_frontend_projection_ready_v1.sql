-- +goose Up
CREATE TABLE IF NOT EXISTS `frontend_projection_ready_v1` (
    frontend Utf8,
    shard_bucket Uint32,
    created_at Timestamp,
    tenant_id Utf8,
    frontend_projection_id Utf8,
    run_id Utf8,
    PRIMARY KEY (frontend, shard_bucket, created_at, tenant_id, frontend_projection_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
