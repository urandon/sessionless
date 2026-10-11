-- +goose Up
-- Create-once actual preparation observation on the SAME sole-parent row.
-- Original active bytes remain immutable; current bytes only retire/revoke.
-- No separate ledger, replacement claim, TTL or expiry-based re-preparation.
ALTER TABLE `attempt_effect_reservations`
ADD COLUMN `allocation_original_record` String,
ADD COLUMN `allocation_original_digest` Utf8,
ADD COLUMN `allocation_current_record` String,
ADD COLUMN `allocation_current_digest` Utf8,
ADD COLUMN `allocation_publication_digest` Utf8,
ADD COLUMN `allocation_transition_parent_digest` Utf8;

-- +goose Down
-- Production down migrations are intentionally disabled.
