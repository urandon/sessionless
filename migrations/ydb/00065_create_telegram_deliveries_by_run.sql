-- +goose Up
CREATE TABLE IF NOT EXISTS `telegram_deliveries_by_run` (
    tenant_id Utf8,
    run_id Utf8,
    telegram_delivery_id Utf8,
    PRIMARY KEY (tenant_id, run_id, telegram_delivery_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
