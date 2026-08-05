-- +goose Up
CREATE TABLE IF NOT EXISTS `runs_by_session` (
    tenant_id Utf8,
    session_id Utf8,
    created_at Timestamp,
    run_id Utf8,
    trigger_event_id Utf8,
    status Utf8,
    PRIMARY KEY (tenant_id, session_id, created_at, run_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
