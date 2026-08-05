-- +goose Up
CREATE TABLE IF NOT EXISTS `external_identities_by_user` (
    user_bucket Uint32,
    user_id Utf8,
    provider Utf8,
    subject Utf8,
    created_at Timestamp,
    PRIMARY KEY (user_bucket, user_id, provider, subject)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
