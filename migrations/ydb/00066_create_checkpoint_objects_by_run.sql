-- +goose Up
CREATE TABLE IF NOT EXISTS `checkpoint_objects_by_run` (
    tenant_id Utf8,
    run_id Utf8,
    checkpoint_id Utf8,
    record JsonDocument,
    PRIMARY KEY (tenant_id, run_id, checkpoint_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
