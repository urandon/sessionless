-- +goose Up
CREATE TABLE IF NOT EXISTS `attempt_operation_calls_v1` (
 tenant_id Utf8, attempt_id Utf8, call_id Utf8,
 sequence Uint64, operation_digest Utf8,
 PRIMARY KEY (tenant_id,attempt_id,call_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
