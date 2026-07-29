-- +goose Up
CREATE TABLE IF NOT EXISTS `context_epochs` (
    tenant_id Utf8,
    conversation_id Utf8,
    context_epoch Uint64,
    requested_by_actor_id Utf8,
    trigger_message_id Utf8,
    idempotency_key Utf8,
    context_blob_key Utf8,
    created_at Timestamp,
    PRIMARY KEY (tenant_id, conversation_id, context_epoch)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
