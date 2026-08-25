-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_capability_manifests` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    capability_digest Utf8,
    version Uint32,
    enrollment_generation Uint64,
    manifest_revision Uint64,
    protocol_version Uint32,
    identity_key_digest Utf8,
    manifest_payload String,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, worker_id, capability_digest)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
