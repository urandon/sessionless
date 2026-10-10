-- +goose Up
CREATE TABLE IF NOT EXISTS `run_explanation_heads_v1` (
    tenant_id Utf8,
    run_id Utf8,
    revision Uint64,
    record JsonDocument,
    PRIMARY KEY (tenant_id, run_id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_BY_LOAD = ENABLED
);

-- +goose Down
-- Additive projection: rollback disables consumers and preserves canonical data.
-- Production down migrations are intentionally disabled.
