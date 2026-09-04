-- +goose Up
CREATE TABLE IF NOT EXISTS `provider_credential_cleanup_ready_v1` (
    shard_bucket Uint32,
    created_at Timestamp,
    tenant_id Utf8,
    owner_user_id Utf8,
    resource_kind Utf8,
    resource_id Utf8,
    credential_generation Uint64,
    secret_ref Utf8,
    PRIMARY KEY (shard_bucket, created_at, tenant_id, owner_user_id, resource_kind, resource_id, credential_generation, secret_ref)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
