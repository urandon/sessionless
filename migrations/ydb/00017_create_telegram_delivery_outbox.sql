-- +goose Up
CREATE TABLE IF NOT EXISTS `telegram_delivery_outbox` (
    tenant_id Utf8,
    telegram_delivery_id Utf8,
    run_id Utf8,
    status Utf8,
    chat_id Int64,
    next_attempt_at Timestamp,
    created_at Timestamp,
    updated_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, telegram_delivery_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
