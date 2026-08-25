-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_enrollments` (
    tenant_id Utf8,
    owner_user_id Utf8,
    enrollment_id Utf8,
    worker_id Utf8,
    display_name Utf8,
    audience Utf8,
    bootstrap_digest Utf8,
    expires_at Timestamp,
    retain_until Timestamp,
    consumed_at Timestamp,
    created_at Timestamp,
    revision Uint64,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, enrollment_id)
)
WITH (
    TTL = Interval("PT0S") ON retain_until,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
