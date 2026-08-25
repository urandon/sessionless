-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_connections` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    connection_id Utf8,
    activation_challenge_id Utf8,
    enrollment_generation Uint64,
    connection_generation Uint64,
    protocol_version Uint32,
    capability_digest Utf8,
    channel_binding_digest Utf8,
    secret_digest Utf8,
    manifest_revision Uint64,
    manifest_identity_key_digest Utf8,
    manifest_signature String,
    manifest_observed_at Timestamp,
    state Utf8,
    platform_sequence Uint64,
    worker_sequence Uint64,
    platform_ack Uint64,
    worker_ack Uint64,
    connected_at Timestamp,
    last_checkpoint_at Timestamp,
    presence_expires_at Timestamp,
    auth_expires_at Timestamp,
    revision Uint64,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, worker_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
