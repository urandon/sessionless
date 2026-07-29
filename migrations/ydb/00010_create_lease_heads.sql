-- +goose Up
CREATE TABLE IF NOT EXISTS `lease_heads` (
    tenant_id Utf8,
    run_id Utf8,
    lease_id Utf8,
    attempt_id Utf8,
    worker_id Utf8,
    fence_token Uint64,
    expires_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, run_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
