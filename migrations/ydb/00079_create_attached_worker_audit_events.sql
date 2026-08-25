-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_audit_events` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    worker_revision Uint64,
    version Uint32,
    enrollment_id Utf8,
    action Utf8,
    enrollment_generation Uint64,
    connection_generation Uint64,
    occurred_at Timestamp,
    PRIMARY KEY (tenant_id, owner_user_id, worker_id, worker_revision)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
