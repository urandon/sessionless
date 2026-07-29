-- +goose Up
ALTER TABLE `dispatch_ready_v2` SET (
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
