-- +goose Up
CREATE TABLE IF NOT EXISTS `artifact_manifests_by_run` (
    tenant_id Utf8,
    run_id Utf8,
    artifact_manifest_id Utf8,
    PRIMARY KEY (tenant_id, run_id, artifact_manifest_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
