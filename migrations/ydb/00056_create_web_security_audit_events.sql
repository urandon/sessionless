-- +goose Up
CREATE TABLE IF NOT EXISTS `web_security_audit_events` (
    shard_bucket Uint32,
    occurred_at Timestamp,
    request_id Utf8,
    action Utf8,
    provider Utf8,
    subject_fingerprint Utf8,
    tenant_id Utf8,
    user_id Utf8,
    membership_security_version Uint64,
    reason_code Utf8,
    record JsonDocument,
    expire_at Timestamp,
    PRIMARY KEY (shard_bucket, occurred_at, request_id)
)
WITH (
    TTL = Interval("PT0S") ON expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
