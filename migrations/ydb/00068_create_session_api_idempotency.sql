-- +goose Up
CREATE TABLE IF NOT EXISTS `session_api_idempotency` (
    tenant_id Utf8,
    user_id Utf8,
    idempotency_key Utf8,
    session_id Utf8,
    created_at Timestamp,
    PRIMARY KEY (tenant_id, user_id, idempotency_key)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
