-- +goose Up
-- Bounded observation attached to the original sole claim, not another ledger.
-- Initial slice supports one fixed provider proxy; other service producers
-- remain denied until their concrete enforcement/custody paths exist.
ALTER TABLE `attempt_effect_reservations`
ADD COLUMN `proxy_original_record` String,
ADD COLUMN `proxy_original_digest` Utf8,
ADD COLUMN `proxy_current_record` String,
ADD COLUMN `proxy_current_digest` Utf8,
ADD COLUMN `proxy_policy_body` String,
ADD COLUMN `proxy_attestation_body` String;

-- +goose Down
-- Production down migrations are intentionally disabled.
