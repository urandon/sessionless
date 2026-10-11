-- +goose Up
-- Canonical Session-owned facts, not serving/permission authority. All writers
-- must be deployed and old generations drained before positive selection.
-- No independent TTL: retention and legal holds follow canonical history.
CREATE TABLE IF NOT EXISTS `session_material_current_v1` (
    tenant_id Utf8,
    session_id Utf8,
    version Uint64,
    through_sequence Uint64,
    last_event_id Utf8,
    prefix_digest Utf8,
    -- Preserve exact uint64 JSON integers; binary JsonDocument rounds them.
    record String,
    PRIMARY KEY (tenant_id, session_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled. Rollback disables
-- selection consumers, not canonical facts for retained history.
