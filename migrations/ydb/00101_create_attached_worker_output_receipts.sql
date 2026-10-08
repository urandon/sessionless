-- +goose Up
CREATE TABLE IF NOT EXISTS `attached_worker_output_receipts` (
    tenant_id Utf8,
    run_id Utf8,
    owner_user_id Utf8,
    worker_id Utf8,
    attempt_id Utf8,
    lease_generation Uint64,
    canonical_digest Utf8,
    candidate_fingerprint Utf8,
    created_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, run_id, owner_user_id, worker_id, attempt_id, lease_generation)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
