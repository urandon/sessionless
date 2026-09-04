-- +goose Up
CREATE TABLE IF NOT EXISTS `provider_credential_cleanups` (
    tenant_id Utf8,
    owner_user_id Utf8,
    resource_kind Utf8,
    resource_id Utf8,
    credential_generation Uint64,
    secret_ref Utf8,
    created_at Timestamp,
    PRIMARY KEY (tenant_id, owner_user_id, resource_kind, resource_id, credential_generation, secret_ref)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
