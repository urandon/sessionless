-- +goose Up
-- Expand the existing table only. Old rows have NULL delete_after; the original
-- expires_at TTL remains in force until 00131. ADD COLUMN is not assumed safe
-- to replay after ambiguous DDL: stop for exact schema/history inspection.
-- See docs/development.md, Identity V2 retention and cutover prerequisites.
ALTER TABLE `oidc_login_challenges` ADD COLUMN `delete_after` Timestamp;

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
