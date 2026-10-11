-- +goose Up
-- Reserved residual projection on the SAME original parent; no new lifecycle.
-- Source-only until the separately authorized concrete cleanup is implemented.
ALTER TABLE `attempt_effect_reservations` ADD COLUMN `operation_residual_record` String;

-- +goose Down
-- Production down migrations are intentionally disabled.
