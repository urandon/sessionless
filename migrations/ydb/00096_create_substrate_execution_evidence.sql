-- +goose Up
CREATE TABLE IF NOT EXISTS `substrate_execution_evidence` (
    tenant_id Utf8,
    attempt_id Utf8,
    run_id Utf8,
    physical_invocation_claim_id Utf8,
    evidence_digest Utf8,
    recorded_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, attempt_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
