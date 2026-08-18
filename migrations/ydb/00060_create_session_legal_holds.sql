-- +goose Up
CREATE TABLE IF NOT EXISTS `session_legal_holds` (
    tenant_id Utf8,
    session_id Utf8,
    state Utf8,
    set_at Timestamp,
    released_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (tenant_id, session_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
