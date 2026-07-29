-- +goose Up
CREATE TABLE IF NOT EXISTS `quota_reservations` (
    tenant_id Utf8,
    quota_reservation_id Utf8,
    run_id Utf8,
    subscription_connection_id Utf8,
    status Utf8,
    capacity_units Uint32,
    held_at Timestamp,
    expires_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, quota_reservation_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
