-- +goose Up
-- Immutable canonical boundaries; no independent TTL or serving authority.
CREATE TABLE IF NOT EXISTS `session_material_boundaries_v1` (
    tenant_id Utf8,
    session_id Utf8,
    sequence Uint64,
    version Uint64,
    through_sequence Uint64,
    last_event_id Utf8,
    prefix_digest Utf8,
    -- Strict decoding happens in Go; String preserves the encoded integers.
    record String,
    PRIMARY KEY (tenant_id, session_id, sequence)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
