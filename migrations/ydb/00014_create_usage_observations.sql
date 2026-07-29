-- +goose Up
CREATE TABLE IF NOT EXISTS `usage_observations` (
    tenant_id Utf8,
    subscription_connection_id Utf8,
    observed_at Timestamp,
    usage_observation_id Utf8,
    run_id Utf8,
    attempt_id Utf8,
    source Utf8,
    input_tokens Uint64,
    output_tokens Uint64,
    retention_expire_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, subscription_connection_id, observed_at, usage_observation_id)
)
WITH (
    TTL = Interval("PT0S") ON retention_expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
