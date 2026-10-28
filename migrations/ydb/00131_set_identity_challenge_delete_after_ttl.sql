-- +goose Up
-- Apply only with every old auth writer frozen/drained. NULL delete_after rows
-- do not expire under this TTL and require separately implemented bounded
-- inventory/retirement before unfreezing; this migration performs no backfill.
ALTER TABLE `oidc_login_challenges` SET (TTL = Interval("PT0S") ON delete_after);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
