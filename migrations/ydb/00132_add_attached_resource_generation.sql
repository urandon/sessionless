-- +goose Up
-- Bounded generation denial/currentness metadata on the SAME resource.
-- No credential material, entitlement activation or independent lifecycle.
ALTER TABLE `subscription_connections`
ADD COLUMN `attached_resource_generation_v1` JsonDocument;

-- +goose Down
-- Production down migrations are intentionally disabled.
