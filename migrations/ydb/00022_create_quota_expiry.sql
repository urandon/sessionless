-- +goose Up
CREATE TABLE IF NOT EXISTS `quota_expiry` (
    tenant_id Utf8,
    expires_at Timestamp,
    quota_reservation_id Utf8,
    run_id Utf8,
    subscription_connection_id Utf8,
    PRIMARY KEY (tenant_id, expires_at, quota_reservation_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
