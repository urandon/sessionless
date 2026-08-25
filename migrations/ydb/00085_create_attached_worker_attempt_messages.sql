-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_attempt_messages` (
    tenant_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    attempt_id Utf8,
    direction Utf8,
    attempt_sequence Uint64,
    connection_generation Uint64,
    envelope_sequence Uint64,
    kind Utf8,
    fingerprint Utf8,
    created_at Timestamp,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (
        tenant_id, owner_user_id, worker_id, attempt_id,
        direction, attempt_sequence
    )
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
