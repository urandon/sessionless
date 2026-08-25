-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_workers` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    display_name Utf8,
    identity_public_key String,
    enrollment_generation Uint64,
    connection_generation Uint64,
    desired_state Utf8,
    observed_state Utf8,
    revision Uint64,
    created_at Timestamp,
    updated_at Timestamp,
    revoked_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, worker_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
