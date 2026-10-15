-- +goose Up
-- One owner/model authority namespace, not a per-run quota/effect ledger.
-- Decision rows are immutable; the singleton head retains revocation forever.
CREATE TABLE IF NOT EXISTS `attached_pi_api_policy_v1` (
    tenant_id Utf8,
    owner_user_id Utf8,
    locator_digest Utf8,
    record_key Utf8,
    record String,
    generation Uint64,
    mapping_digest Utf8,
    state Utf8,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, owner_user_id, locator_digest, record_key)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
