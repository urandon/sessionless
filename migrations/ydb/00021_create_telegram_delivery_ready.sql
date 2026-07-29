-- +goose Up
CREATE TABLE IF NOT EXISTS `telegram_delivery_ready` (
    tenant_id Utf8,
    available_at Timestamp,
    telegram_delivery_id Utf8,
    run_id Utf8,
    PRIMARY KEY (tenant_id, available_at, telegram_delivery_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
