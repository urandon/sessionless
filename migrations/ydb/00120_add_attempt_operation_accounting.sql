-- +goose Up
-- Non-refundable cumulative attachment to the existing sole physical claim.
ALTER TABLE `attempt_effect_reservations`
ADD COLUMN `operation_accounting_record` String,
ADD COLUMN `operation_accounting_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
