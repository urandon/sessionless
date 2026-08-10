-- +goose Up
CREATE TABLE IF NOT EXISTS `frontend_ingress_idempotency` (
    tenant_id Utf8,
    binding_id Utf8,
    idempotency_key Utf8,
    session_id Utf8,
    sequence Uint64,
    event_id Utf8,
    run_id Utf8,
    origin_digest Utf8,
    created_at Timestamp,
    expire_at Timestamp,
    PRIMARY KEY (tenant_id, binding_id, idempotency_key)
)
WITH (
    TTL = Interval("PT0S") ON expire_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
