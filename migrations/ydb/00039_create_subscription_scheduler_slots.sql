-- +goose Up
CREATE TABLE IF NOT EXISTS `subscription_scheduler_slots` (
    tenant_id Utf8,
    subscription_connection_id Utf8,
    state Utf8,
    active_run_id Utf8,
    active_reservation_id Utf8,
    blocked_until Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, subscription_connection_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
