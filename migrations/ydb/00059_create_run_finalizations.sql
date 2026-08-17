-- +goose Up
CREATE TABLE IF NOT EXISTS `run_finalizations` (
    tenant_id Utf8,
    run_id Utf8,
    terminal_status Utf8,
    content_digest Utf8,
    created_at Timestamp,
    PRIMARY KEY (tenant_id, run_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
