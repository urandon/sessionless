-- +goose Up
CREATE TABLE IF NOT EXISTS `execution_placement_cutover_state` (
    cutover_id Utf8,
    completed_at Timestamp,
    PRIMARY KEY (cutover_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
