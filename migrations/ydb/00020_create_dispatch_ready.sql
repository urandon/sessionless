-- +goose Up
CREATE TABLE IF NOT EXISTS `dispatch_ready` (
    tenant_id Utf8,
    available_at Timestamp,
    dispatch_outbox_id Utf8,
    run_id Utf8,
    attempt_id Utf8,
    PRIMARY KEY (tenant_id, available_at, dispatch_outbox_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
