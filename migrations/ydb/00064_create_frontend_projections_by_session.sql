-- +goose Up
CREATE TABLE IF NOT EXISTS `frontend_projections_by_session` (
    tenant_id Utf8,
    session_id Utf8,
    frontend_projection_id Utf8,
    PRIMARY KEY (tenant_id, session_id, frontend_projection_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
