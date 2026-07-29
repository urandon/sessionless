-- +goose Up
CREATE TABLE IF NOT EXISTS `artifact_manifests` (
    tenant_id Utf8,
    artifact_manifest_id Utf8,
    run_id Utf8,
    created_at Timestamp,
    payload JsonDocument,
    PRIMARY KEY (tenant_id, artifact_manifest_id)
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
