-- +goose Up
CREATE TABLE IF NOT EXISTS `session_events` (
    tenant_id Utf8,
    session_id Utf8,
    sequence Uint64,
    event_id Utf8,
    kind Utf8,
    author_user_id Utf8,
    run_id Utf8,
    idempotency_key Utf8,
    blob_key Utf8,
    created_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, session_id, sequence)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
