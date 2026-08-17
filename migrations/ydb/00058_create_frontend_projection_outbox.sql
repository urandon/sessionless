-- +goose Up
CREATE TABLE IF NOT EXISTS `frontend_projection_outbox` (
    tenant_id Utf8,
    frontend_projection_id Utf8,
    session_id Utf8,
    event_id Utf8,
    event_sequence Uint64,
    binding_id Utf8,
    binding_revision Uint64,
    frontend Utf8,
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, frontend_projection_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
