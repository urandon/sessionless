-- +goose Up
-- Checked deployment receipt; serving processes never write this table.
CREATE TABLE IF NOT EXISTS `run_explanation_cutover_state_v1` (
    cutover_id Utf8,
    version Uint32,
    writer_version Uint32,
    schema_version Uint32,
    rated_reader_version Uint32,
    writer_commit Utf8,
    drained_inventory_digest Utf8,
    old_writer_count Uint64,
    completed_at Timestamp,
    PRIMARY KEY (cutover_id)
);

-- +goose Down
-- Disable the reader; do not discard deployment evidence in production.
