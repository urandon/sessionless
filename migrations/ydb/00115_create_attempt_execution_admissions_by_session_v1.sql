-- +goose Up
-- Exact bounded reverse inventory for canonical Session cleanup.
CREATE TABLE IF NOT EXISTS `attempt_execution_admissions_by_session_v1` (
    tenant_id Utf8,
    session_id Utf8,
    attempt_id Utf8,
    run_id Utf8,
    PRIMARY KEY (tenant_id, session_id, attempt_id)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
