-- +goose Up
CREATE TABLE IF NOT EXISTS `tenants` (
    tenant_id Utf8,
    status Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
