-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_action_plans` (
    tenant_id Utf8,
    owner_user_id Utf8,
    plan_id Utf8,
    worker_id Utf8,
    operation_id Utf8,
    state Utf8,
    expires_at Timestamp,
    retention_expire_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, plan_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
