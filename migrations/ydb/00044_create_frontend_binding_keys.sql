-- +goose Up
CREATE TABLE IF NOT EXISTS `frontend_binding_keys` (
    tenant_id Utf8,
    frontend Utf8,
    external_conversation_id Utf8,
    binding_id Utf8,
    PRIMARY KEY (tenant_id, frontend, external_conversation_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
