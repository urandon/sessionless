-- +goose Up
CREATE TABLE IF NOT EXISTS `web_sessions` (
    shard_bucket Uint32,
    session_digest Utf8,
    csrf_token_digest Utf8,
    user_id Utf8,
    active_tenant_id Utf8,
    identity_provider Utf8,
    external_subject Utf8,
    membership_security_version Uint64,
    issued_at Timestamp,
    last_seen_at Timestamp,
    idle_expires_at Timestamp,
    absolute_expires_at Timestamp,
    revoked_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (shard_bucket, session_digest)
)
WITH (
    TTL = Interval("PT0S") ON absolute_expires_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
