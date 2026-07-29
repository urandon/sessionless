-- +goose Up
CREATE TABLE IF NOT EXISTS `run_idempotency` (
    tenant_id Utf8,
    idempotency_key Utf8,
    run_id Utf8,
    created_at Timestamp,
    expire_at Timestamp,
    PRIMARY KEY (tenant_id, idempotency_key)
)
WITH (
    TTL = Interval("PT0S") ON expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
