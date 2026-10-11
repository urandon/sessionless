-- +goose Up
-- SAME sole outer claim primary key and kind as single-turn effects. Exactly
-- one record representation is permitted by both readers; INSERT only.
-- No TTL: shared canonical Session retention owns the preserved correlation.
ALTER TABLE `attempt_effect_reservations`
ADD COLUMN `owned_record` String,
ADD COLUMN `owned_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
