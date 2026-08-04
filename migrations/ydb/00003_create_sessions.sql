-- +goose Up
CREATE TABLE IF NOT EXISTS `sessions` (
    tenant_id Utf8,
    session_id Utf8,
    created_by Utf8,
    status Utf8,
    last_event_sequence Uint64,
    created_at Timestamp,
    updated_at Timestamp,
    archived_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, session_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
