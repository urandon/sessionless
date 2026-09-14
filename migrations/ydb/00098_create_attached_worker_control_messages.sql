-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_control_messages` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    drain_revision Uint64,
    direction Utf8,
    kind Utf8,
    created_at Timestamp,
    retention_expire_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (
        tenant_id, owner_user_id, worker_id, drain_revision, direction
    )
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
