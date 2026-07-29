-- +goose Up
CREATE TABLE IF NOT EXISTS `audit_events` (
    tenant_id Utf8,
    occurred_at Timestamp,
    audit_event_id Utf8,
    actor_id Utf8,
    action Utf8,
    subject_kind Utf8,
    subject_id Utf8,
    outcome Utf8,
    metadata JsonDocument,
    expire_at Timestamp,
    PRIMARY KEY (tenant_id, occurred_at, audit_event_id)
)
WITH (
    TTL = Interval("PT0S") ON expire_at
);

-- +goose Down
-- Production down migrations are intentionally disabled. See migrations/ydb/README.md.
