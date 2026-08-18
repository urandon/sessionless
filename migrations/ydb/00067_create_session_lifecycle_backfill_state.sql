-- +goose Up
CREATE TABLE IF NOT EXISTS `session_lifecycle_backfill_state` (
    backfill_id Utf8,
    completed_at Timestamp,
    PRIMARY KEY (backfill_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
