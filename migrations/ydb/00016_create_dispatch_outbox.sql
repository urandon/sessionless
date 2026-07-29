-- +goose Up
CREATE TABLE IF NOT EXISTS `dispatch_outbox` (
    tenant_id Utf8,
    dispatch_outbox_id Utf8,
    run_id Utf8,
    attempt_id Utf8,
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, dispatch_outbox_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
