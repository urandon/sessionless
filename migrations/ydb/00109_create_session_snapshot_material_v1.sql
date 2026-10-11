-- +goose Up
-- Publication requires actual snapshot-object verification and boundary CAS.
-- The table alone supplies no complete coverage; no independent TTL.
CREATE TABLE IF NOT EXISTS `session_snapshot_material_v1` (
    tenant_id Utf8,
    session_id Utf8,
    snapshot_version Uint64,
    version Uint64,
    through_sequence Uint64,
    last_event_id Utf8,
    boundary_digest Utf8,
    -- Same lossless integer contract as current and boundary records.
    record String,
    PRIMARY KEY (tenant_id, session_id, snapshot_version)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
