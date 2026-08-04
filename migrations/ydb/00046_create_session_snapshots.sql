-- +goose Up
CREATE TABLE IF NOT EXISTS `session_snapshots` (
    tenant_id Utf8,
    session_id Utf8,
    version Uint64,
    snapshot_id Utf8,
    through_sequence Uint64,
    blob_key Utf8,
    created_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, session_id, version)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
