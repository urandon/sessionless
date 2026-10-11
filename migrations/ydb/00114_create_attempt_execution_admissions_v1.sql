-- +goose Up
-- Immutable canonical Attempt child, not an authority registry or ledger.
-- No independent TTL: shared Session deletion/hold/reconciliation owns cleanup.
CREATE TABLE IF NOT EXISTS `attempt_execution_admissions_v1` (
    tenant_id Utf8,
    attempt_id Utf8,
    run_id Utf8,
    session_id Utf8,
    owner_user_id Utf8,
    reservation_id Utf8,
    admitted_at Timestamp,
    record String,
    digest Utf8,
    PRIMARY KEY (tenant_id, attempt_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
