-- +goose Up
-- Bounded subordinate original/current proxy observations of the SAME parent.
-- No quota, ledger, terminal authority or independent retention window.
CREATE TABLE IF NOT EXISTS `attempt_operation_proxies_v1` (
 tenant_id Utf8, attempt_id Utf8, sequence Uint64,
 service_kind Utf8,
 original_record String, original_digest Utf8,
 current_record String, current_digest Utf8,
 policy_body String, attestation_body String,
 PRIMARY KEY (tenant_id,attempt_id,sequence)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
