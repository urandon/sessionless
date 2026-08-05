-- +goose Up
CREATE TABLE IF NOT EXISTS `session_activity` (
    tenant_id Utf8,
    user_id Utf8,
    status Utf8,
    activity_bucket Uint32,
    updated_at Timestamp,
    session_id Utf8,
    PRIMARY KEY (tenant_id, user_id, status, activity_bucket, updated_at, session_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
