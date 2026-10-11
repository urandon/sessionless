-- +goose Up
CREATE TABLE IF NOT EXISTS `compute_choice_rate_slots_v1` (
    slot_id Uint32,
    record JsonDocument,
    expire_at Timestamp,
    PRIMARY KEY (slot_id)
)
WITH (
    -- Separate from Run explanation and CHAT-1. Readers/writers validate the
    -- fixed 4096-slot space and exact record/expiry before logical reuse.
    -- No TTL: physical cleanup must not conceal a corrupt expired occupant.
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Additive finite list metadata; disable the consumer on rollback.
-- Production down migrations are intentionally disabled.
