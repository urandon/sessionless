-- +goose Up
CREATE TABLE IF NOT EXISTS `subscription_connections` (
    tenant_id Utf8,
    subscription_connection_id Utf8,
    actor_id Utf8,
    provider Utf8,
    credential_ref Utf8,
    entitlement_state Utf8,
    quota_state Utf8,
    observed_at Timestamp,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, subscription_connection_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
