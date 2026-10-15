-- +goose Up
-- Requires verified v2 coverage and old-reader/writer drain before rollout.
DROP TABLE IF EXISTS `telegram_delivery_ready`;

-- +goose Down
-- No automatic rollback: reconstruct the old schema and index metadata first.
