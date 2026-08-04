-- +goose Up
CREATE TABLE IF NOT EXISTS `actors` (
    tenant_id Utf8,
    actor_id Utf8,
	user_id Utf8,
    frontend Utf8,
    external_id Utf8,
    created_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, actor_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
