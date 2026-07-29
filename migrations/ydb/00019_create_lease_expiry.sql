-- +goose Up
CREATE TABLE IF NOT EXISTS `lease_expiry` (
    tenant_id Utf8,
    expires_at Timestamp,
    run_id Utf8,
    lease_id Utf8,
    fence_token Uint64,
    PRIMARY KEY (tenant_id, expires_at, run_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
