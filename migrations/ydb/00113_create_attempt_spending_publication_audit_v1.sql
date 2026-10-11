-- +goose Up
-- Immutable per-Attempt publication correlation, NOT a balance/debit ledger.
-- Canonical Session lifecycle integration must retain unknown-effect links.
-- No independent TTL or destructive down migration.
CREATE TABLE IF NOT EXISTS `attempt_spending_publication_audit_v1` (
    tenant_id Utf8,
    attempt_id Utf8,
    component_kind Utf8,
    billing_owner_user_id Utf8,
    mutation_id Utf8,
    intent_digest Utf8,
    record String,
    record_digest Utf8,
    revocation_generation Uint64,
    published_at Timestamp,
    run_id Utf8,
    session_id Utf8,
    INDEX by_session GLOBAL ON (tenant_id, session_id),
    PRIMARY KEY (tenant_id, attempt_id, component_kind, billing_owner_user_id, mutation_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
