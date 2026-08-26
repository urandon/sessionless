-- +goose Up
CREATE TABLE IF NOT EXISTS `attempt_effect_reservations` (
    tenant_id Utf8,
    attempt_id Utf8,
    kind Utf8,
    run_id Utf8,
    lease_id Utf8,
    fence_token Uint64,
    physical_invocation_claim_id Utf8,
    effect_sequence Uint64,
    invocation_authority_digest Utf8,
    reserved_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, attempt_id, kind)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
