-- +goose Up
-- Bounded consent projection on the SAME sole physical Attempt claim.
-- No independent lifecycle, TTL, scheduler or monetary ledger.
ALTER TABLE `attempt_effect_reservations`
ADD COLUMN `web_search_consent_record` String,
ADD COLUMN `web_search_consent_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
