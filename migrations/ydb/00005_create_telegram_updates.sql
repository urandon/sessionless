-- +goose Up
CREATE TABLE IF NOT EXISTS `telegram_updates` (
    tenant_id Utf8,
    source_id Utf8,
    update_id Int64,
    run_id Utf8,
    received_at Timestamp,
    expire_at Timestamp,
    PRIMARY KEY (tenant_id, source_id, update_id)
)
WITH (
    TTL = Interval("PT0S") ON expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
