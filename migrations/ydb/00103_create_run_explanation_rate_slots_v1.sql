-- +goose Up
CREATE TABLE IF NOT EXISTS `run_explanation_rate_slots_v1` (
    slot_id Uint32,
    record JsonDocument,
    expire_at Timestamp,
    PRIMARY KEY (slot_id)
)
WITH (
    -- No independent TTL: validate record/expiry equality before logical reuse.
    -- The fixed 4096-slot key space bounds retained metadata without cleanup.
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Additive finite rate metadata: disable the consumer on rollback.
-- Production down migrations are intentionally disabled.
