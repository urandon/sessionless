-- +goose Up
CREATE TABLE IF NOT EXISTS `session_participants` (
    tenant_id Utf8,
    session_id Utf8,
    user_id Utf8,
    role Utf8,
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, session_id, user_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
