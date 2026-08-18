-- +goose Up
CREATE TABLE IF NOT EXISTS `web_upload_intent_creations` (
    tenant_id Utf8,
    user_id Utf8,
    creation_idempotency_key Utf8,
    upload_id Utf8,
    created_at Timestamp,
    PRIMARY KEY (tenant_id, user_id, creation_idempotency_key)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
