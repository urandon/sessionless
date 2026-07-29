-- +goose Up
CREATE TABLE IF NOT EXISTS `conversations` (
    tenant_id Utf8,
    conversation_id Utf8,
    frontend Utf8,
    external_id Utf8,
    current_context_epoch Uint64,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, conversation_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
