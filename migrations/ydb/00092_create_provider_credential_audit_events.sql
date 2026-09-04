-- +goose Up
CREATE TABLE IF NOT EXISTS `provider_credential_audit_events` (
    tenant_id Utf8,
    owner_user_id Utf8,
    resource_kind Utf8,
    resource_id Utf8,
    resource_revision Uint64,
    candidate_mutation_id Utf8,
    receipt_id Utf8,
    action Utf8,
    occurred_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, owner_user_id, resource_kind, resource_id, resource_revision)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
