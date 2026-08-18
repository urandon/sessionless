-- +goose Up
CREATE TABLE IF NOT EXISTS `session_deletions` (
    tenant_id Utf8,
    session_id Utf8,
    state Utf8,
    requested_at Timestamp,
    started_at Timestamp,
    completed_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, session_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
