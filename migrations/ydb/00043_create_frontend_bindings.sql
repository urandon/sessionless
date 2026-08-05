-- +goose Up
CREATE TABLE IF NOT EXISTS `frontend_bindings` (
    tenant_id Utf8,
    binding_id Utf8,
    frontend Utf8,
    external_conversation_id Utf8,
    session_id Utf8,
    revision Uint64,
    created_at Timestamp,
    updated_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, binding_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
