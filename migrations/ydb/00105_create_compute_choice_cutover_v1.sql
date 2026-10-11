-- +goose Up
-- Private singleton deployment evidence. Only a trusted installer may publish
-- after compatible writers/readers and drained inventory are independently
-- verified. Serving readers never create or repair this receipt. No TTL.
CREATE TABLE IF NOT EXISTS `compute_choice_cutover_v1` (
    cutover_id Utf8,
    version Uint32,
    manifest_digest Utf8,
    writer_revision Uint32,
    schema_revision Uint32,
    enabled Bool,
    writer_commit Utf8,
    drained_inventory_digest Utf8,
    old_writer_count Uint64,
    completed_at Timestamp,
    record JsonDocument,
    PRIMARY KEY (cutover_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
