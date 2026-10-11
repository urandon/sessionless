-- +goose Up
-- Immutable child correlations, not a second quota/balance or scheduler.
CREATE TABLE IF NOT EXISTS `attempt_operations_v1` (
 tenant_id Utf8, attempt_id Utf8, sequence Uint64,
 record String, digest Utf8, operation_digest Utf8, call_id Utf8,
 observation_record String, observation_digest Utf8,
 PRIMARY KEY (tenant_id,attempt_id,sequence)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
