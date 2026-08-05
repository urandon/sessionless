-- +goose Up
CREATE TABLE IF NOT EXISTS `tenant_memberships` (
    user_bucket Uint32,
    user_id Utf8,
    tenant_id Utf8,
    role Utf8,
    status Utf8,
    security_version Uint64,
    created_at Timestamp,
    updated_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (user_bucket, user_id, tenant_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
