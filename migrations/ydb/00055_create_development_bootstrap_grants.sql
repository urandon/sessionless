-- +goose Up
CREATE TABLE IF NOT EXISTS `development_bootstrap_grants` (
    tenant_id Utf8,
    user_id Utf8,
    role Utf8,
    operator Utf8,
    reason Utf8,
    granted_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, user_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
