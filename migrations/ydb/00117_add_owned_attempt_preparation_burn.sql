-- +goose Up
-- One immutable preparation burn on the SAME sole parent row. Both nullable
-- columns are absent together or present together and correlated to owned_digest.
-- No second ledger, expiry/TTL, replacement claim or automatic re-preparation.
ALTER TABLE `attempt_effect_reservations`
ADD COLUMN `preparation_started_at` Timestamp,
ADD COLUMN `preparation_parent_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
