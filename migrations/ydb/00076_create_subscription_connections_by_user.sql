-- +goose Up
CREATE TABLE IF NOT EXISTS `subscription_connections_by_user` (
    tenant_id Utf8,
    user_id Utf8,
    subscription_connection_id Utf8,
    PRIMARY KEY (tenant_id, user_id, subscription_connection_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
