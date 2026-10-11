-- +goose Up
-- Immutable installation transition receipt, not a spending ledger.
-- Resource metadata/tombstones follow registration retention, not Session TTL.
CREATE TABLE IF NOT EXISTS `owned_profile_publication_audit_v1` (
    tenant_id Utf8,
    locator_digest Utf8,
    mutation_id Utf8,
    intent_digest Utf8,
    record String,
    record_digest Utf8,
    revocation_generation Uint64,
    published_at Timestamp,
    PRIMARY KEY (tenant_id, locator_digest, mutation_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
