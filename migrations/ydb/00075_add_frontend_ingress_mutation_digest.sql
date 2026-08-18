-- +goose Up
ALTER TABLE `frontend_ingress_idempotency`
ADD COLUMN `mutation_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
