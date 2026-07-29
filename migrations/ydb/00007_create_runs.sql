-- +goose Up
CREATE TABLE IF NOT EXISTS `runs` (
    tenant_id Utf8,
    run_id Utf8,
    conversation_id Utf8,
    subscription_connection_id Utf8,
    context_epoch Uint64,
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, run_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
