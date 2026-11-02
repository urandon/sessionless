-- +goose Up
-- Preserve the original local capacity association in the SAME expiry index.
-- No new TTL, key, index, backfill or subscription authority is introduced.
ALTER TABLE `quota_expiry_v2`
ADD COLUMN `local_capacity_association` String,
ADD COLUMN `local_capacity_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
