-- +goose Up
-- Closed prerequisite projection, NOT installation approval or activation.
-- Exact lossless bytes; revocation tombstones have no independent TTL.
CREATE TABLE IF NOT EXISTS `owned_agent_profile_current_v1` (
    tenant_id Utf8,
    owner_user_id Utf8,
    profile Utf8,
    backend_profile_digest Utf8,
    resource_kind Utf8,
    resource_id Utf8,
    model_vendor_id Utf8,
    model_id Utf8,
    input_data_class Utf8,
    record String,
    version Uint64,
    profile_revision Uint64,
    revocation_generation Uint64,
    state Utf8,
    digest Utf8,
    installation_id Utf8,
    installation_revision Uint64,
    published_at Timestamp,
    expires_at Timestamp,
    updated_at Timestamp,
    PRIMARY KEY (tenant_id, owner_user_id, profile, backend_profile_digest, resource_kind, resource_id, model_vendor_id, model_id, input_data_class)
) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED);

-- +goose Down
-- Production down migrations are intentionally disabled.
