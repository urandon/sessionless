-- +goose Up
CREATE TABLE IF NOT EXISTS `web_upload_intents` (
    tenant_id Utf8,
    upload_id Utf8,
    user_id Utf8,
    session_id Utf8,
    creation_idempotency_key Utf8,
    status Utf8,
    expires_at Timestamp,
    claimed_by_message_idempotency_key Utf8,
    record JsonDocument,
    PRIMARY KEY (tenant_id, upload_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
