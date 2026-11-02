-- +goose Up
-- Canonical local association bytes and digest on the SAME capacity slot.
-- Both NULL preserve the genuine legacy subscription arm; no backfill/default.
ALTER TABLE `subscription_scheduler_slots`
ADD COLUMN `local_capacity_association` String,
ADD COLUMN `local_capacity_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
