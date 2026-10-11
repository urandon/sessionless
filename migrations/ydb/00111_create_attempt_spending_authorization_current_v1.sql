-- +goose Up
-- Scoped attachment, NOT a balance, debit ledger or payer approval.
-- Exact bytes and original expiry; no independent TTL or terminal reducer.
CREATE TABLE IF NOT EXISTS `attempt_spending_authorization_current_v1` (
    tenant_id Utf8,
    attempt_id Utf8,
    component_kind Utf8,
    billing_owner_user_id Utf8,
    record String,
    version Uint64,
    revocation_generation Uint64,
    state Utf8,
    digest Utf8,
    run_id Utf8,
    session_id Utf8,
    owner_user_id Utf8,
    published_at Timestamp,
    expires_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, attempt_id, component_kind, billing_owner_user_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
