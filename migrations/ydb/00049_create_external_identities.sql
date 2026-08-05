-- +goose Up
CREATE TABLE IF NOT EXISTS `external_identities` (
    shard_bucket Uint32,
    provider Utf8,
    subject Utf8,
    user_id Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (shard_bucket, provider, subject)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
