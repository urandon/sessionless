-- +goose Up
CREATE TABLE IF NOT EXISTS `tenant_invitations` (
    tenant_id Utf8,
    invitation_id Utf8,
    secret_digest Utf8,
    role Utf8,
    expires_at Timestamp,
    consumed_at Timestamp,
    created_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, invitation_id)
)
WITH (
    TTL = Interval("PT0S") ON expires_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
