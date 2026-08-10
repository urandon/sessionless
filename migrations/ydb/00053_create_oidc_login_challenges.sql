-- +goose Up
CREATE TABLE IF NOT EXISTS `oidc_login_challenges` (
    shard_bucket Uint32,
    state_digest Utf8,
    browser_binding_digest Utf8,
    pkce_verifier Utf8,
    nonce Utf8,
    redirect_path Utf8,
    created_at Timestamp,
    expires_at Timestamp,
    consumed_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (shard_bucket, state_digest)
)
WITH (
    TTL = Interval("PT0S") ON expires_at,
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
