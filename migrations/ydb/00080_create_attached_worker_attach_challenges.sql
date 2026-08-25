-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_attach_challenges` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    challenge_id Utf8,
    connection_id Utf8,
    purpose Utf8,
    audience Utf8,
    expected_worker_revision Uint64,
    expected_enrollment_generation Uint64,
    expected_connection_generation Uint64,
    target_connection_generation Uint64,
    selected_protocol_version Uint32,
    worker_nonce_digest Utf8,
    platform_nonce_digest Utf8,
    created_at Timestamp,
    expires_at Timestamp,
    retain_until Timestamp,
    consumed_at Timestamp,
    revision Uint64,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, worker_id, challenge_id)
)
WITH (
    TTL = Interval("PT0S") ON retain_until,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
