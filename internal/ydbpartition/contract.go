// Package ydbpartition defines the versioned physical-key and table
// partitioning contract used by YDB writers, readers, migrations, and
// operational inspection.
package ydbpartition

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	ContractVersion = "v1"
	BucketCountV1   = uint32(16)
)

type TableClass string

const (
	ClassEntity      TableClass = "tenant_entity"
	ClassOrdered     TableClass = "per_run_ordered"
	ClassAppend      TableClass = "append_heavy"
	ClassGlobalReady TableClass = "global_ready_expiry"
	ClassLegacyReady TableClass = "legacy_ready_expiry"
)

type Policy struct {
	LogicalName      string
	PhysicalTable    string
	PrimaryKey       []string
	Class            TableClass
	LoadPartitioning bool
	Bucketed         bool
	Rationale        string
}

// BucketV1 maps an immutable operational identifier to one of a bounded set of
// ready/expiry shards. The golden-vector tests make this derivation a stable
// writer/reader protocol rather than an implementation detail.
func BucketV1(identifier string) (uint32, error) {
	if identifier == "" {
		return 0, errors.New("partition identifier must not be empty")
	}
	sum := sha256.Sum256([]byte(identifier))
	return binary.BigEndian.Uint32(sum[:4]) % BucketCountV1, nil
}

func BucketsV1() []uint32 {
	result := make([]uint32, BucketCountV1)
	for bucket := range result {
		result[bucket] = uint32(bucket)
	}
	return result
}

func Policies() []Policy {
	return append([]Policy(nil), policies...)
}

var policies = []Policy{
	entity("tenants", []string{"tenant_id"}, "low-volume tenant registry; random tenant IDs and load split are sufficient"),
	entity("actors", []string{"tenant_id", "actor_id"}, "point access under a distributed tenant ID"),
	hot("sessions", []string{"tenant_id", "session_id"}, ClassEntity, "canonical sessions use random IDs and YDB-managed range splitting"),
	hot("session_events", []string{"tenant_id", "session_id", "sequence"}, ClassOrdered, "event sequence is ordered only inside a random session prefix"),
	hot("session_event_idempotency", []string{"tenant_id", "session_id", "idempotency_key"}, ClassEntity, "point idempotency lookup inside a random session prefix"),
	hot("frontend_bindings", []string{"tenant_id", "binding_id"}, ClassEntity, "point access through random binding IDs"),
	hot("frontend_bindings_by_session", []string{"tenant_id", "session_id", "binding_id"}, ClassEntity, "current bindings are enumerated behind a random session prefix"),
	hot("frontend_binding_keys", []string{"tenant_id", "frontend", "external_conversation_id"}, ClassEntity, "bounded frontend identity lookup under an authorized tenant"),
	hot("frontend_ingress_idempotency", []string{"tenant_id", "binding_id", "idempotency_key"}, ClassEntity, "frontend delivery deduplication remains behind a random binding prefix"),
	hot("frontend_projection_outbox", []string{"tenant_id", "frontend_projection_id"}, ClassEntity, "generic projection work uses stable high-entropy event and binding identities"),
	hot("frontend_projections_by_session", []string{"tenant_id", "session_id", "frontend_projection_id"}, ClassEntity, "deletion inventory is bounded behind a random session prefix"),
	hot("frontend_projections_by_run", []string{"tenant_id", "run_id", "frontend", "frontend_projection_id"}, ClassEntity, "frontend-specific wake processing is bounded behind a random run prefix"),
	{
		LogicalName: "frontend_projection_ready_v1", PhysicalTable: "frontend_projection_ready_v1",
		PrimaryKey: []string{"frontend", "shard_bucket", "created_at", "tenant_id", "frontend_projection_id"},
		Class:      ClassGlobalReady, LoadPartitioning: true, Bucketed: true,
		Rationale: "bounded recovery fans out across fixed hash buckets per frontend",
	},
	hot("session_participants", []string{"tenant_id", "session_id", "user_id"}, ClassEntity, "authorization rows remain behind a random session prefix"),
	hot("session_snapshots", []string{"tenant_id", "session_id", "version"}, ClassOrdered, "immutable versions are ordered inside a random session prefix"),
	hot("session_activity", []string{"tenant_id", "user_id", "status", "activity_bucket", "updated_at", "session_id"}, ClassAppend, "fixed 16-way per-user fan-out avoids a global chronological write edge"),
	hot("session_api_idempotency", []string{"tenant_id", "user_id", "idempotency_key"}, ClassEntity, "user-scoped point idempotency for frontend-neutral session creation"),
	hot("session_api_mutations", []string{"tenant_id", "user_id", "idempotency_key", "kind"}, ClassEntity, "user-scoped point idempotency for session mutations"),
	hot("session_displays", []string{"tenant_id", "session_id"}, ClassEntity, "bounded rebuildable list metadata keyed by canonical session"),
	hot("session_legal_holds", []string{"tenant_id", "session_id"}, ClassEntity, "one durable legal-retention decision per random session prefix"),
	hot("session_deletions", []string{"tenant_id", "session_id"}, ClassEntity, "durable deletion fence and completion tombstone use exact tenant/session lookup"),
	hot("external_identities", []string{"shard_bucket", "provider", "subject"}, ClassEntity, "subject hashes provide point lookup without a monotonic provider subject edge"),
	hot("external_identities_by_user", []string{"user_bucket", "user_id", "provider", "subject"}, ClassEntity, "random internal user IDs provide bounded reverse identity lookup"),
	hot("tenant_memberships", []string{"user_bucket", "user_id", "tenant_id"}, ClassEntity, "membership listing is bounded behind a random internal user prefix"),
	hot("tenant_invitations", []string{"tenant_id", "invitation_id"}, ClassEntity, "one-time invitations use random IDs inside an authorized tenant"),
	hot("development_bootstrap_grants", []string{"tenant_id", "user_id"}, ClassEntity, "audited cloud-dev grants are point-addressable under a random tenant"),
	hot("oidc_login_challenges", []string{"shard_bucket", "state_digest"}, ClassEntity, "random state digests distribute pre-authentication point lookups"),
	hot("web_sessions", []string{"shard_bucket", "session_digest"}, ClassEntity, "random opaque-session digests distribute browser authorization lookups"),
	hot("web_security_audit_events", []string{"shard_bucket", "occurred_at", "request_id"}, ClassAppend, "request hashes distribute pre-authentication security events while preserving bounded per-bucket time reads"),
	hot("telegram_updates", []string{"tenant_id", "source_id", "update_id"}, ClassAppend, "Telegram command update sequence is behind a distributed tenant"),
	entity("subscription_connections", []string{"tenant_id", "subscription_connection_id"}, "low-cardinality point access"),
	hot("subscription_connections_by_user", []string{"tenant_id", "user_id", "subscription_connection_id"}, ClassEntity, "owner-scoped compute selection is bounded behind an authorized user prefix"),
	hot("attached_workers", []string{"tenant_id", "owner_user_id", "worker_id"}, ClassEntity, "owner-scoped attached workers use exact random worker IDs and bounded owner-prefix listing"),
	hot("attached_worker_enrollments", []string{"tenant_id", "owner_user_id", "enrollment_id"}, ClassEntity, "single-use enrollment claims are exact owner-scoped point transactions"),
	hot("attached_worker_audit_events", []string{"tenant_id", "owner_user_id", "worker_id", "worker_revision"}, ClassOrdered, "content-free audit revisions are ordered only behind an exact owner and random worker prefix"),
	hot("attached_worker_attach_challenges", []string{"tenant_id", "owner_user_id", "worker_id", "challenge_id"}, ClassEntity, "single-use attach challenges use exact owner and random worker scope"),
	hot("attached_worker_capability_manifests", []string{"tenant_id", "owner_user_id", "worker_id", "capability_digest"}, ClassEntity, "immutable capability evidence is pinned behind an exact owner and worker"),
	hot("attached_worker_connections", []string{"tenant_id", "owner_user_id", "worker_id"}, ClassEntity, "one current connection head serializes generation and coalesced presence for an owner-scoped worker"),
	{
		LogicalName: "attached_worker_presence_expiry", PhysicalTable: "attached_worker_presence_expiry_v1",
		PrimaryKey: []string{"shard_bucket", "presence_expires_at", "tenant_id", "owner_user_id", "worker_id"},
		Class:      ClassGlobalReady, LoadPartitioning: true, Bucketed: true,
		Rationale: "bounded 16-way expiry recovery with owner scope retained in every candidate",
	},
	hot("attached_worker_attempt_heads", []string{"tenant_id", "owner_user_id", "worker_id"}, ClassEntity, "one owner-scoped concurrency-one attempt contention row per attached worker"),
	hot("attached_worker_attempt_messages", []string{"tenant_id", "owner_user_id", "worker_id", "attempt_id", "direction", "attempt_sequence"}, ClassOrdered, "bounded directional replay records are ordered behind one exact worker attempt"),
	{
		LogicalName: "attached_worker_attempt_deadlines", PhysicalTable: "attached_worker_attempt_deadlines_v1",
		PrimaryKey: []string{"shard_bucket", "deadline_at", "tenant_id", "owner_user_id", "worker_id", "attempt_id", "kind"},
		Class:      ClassGlobalReady, LoadPartitioning: true, Bucketed: true,
		Rationale: "bounded 16-way lease and cancellation deadline recovery with exact attempt fences",
	},
	hot("runs", []string{"tenant_id", "run_id"}, ClassEntity, "high-write entity with random run IDs"),
	hot("run_finalizations", []string{"tenant_id", "run_id"}, ClassEntity, "one idempotency fence per terminal run"),
	hot("runs_by_session", []string{"tenant_id", "session_id", "created_at", "run_id"}, ClassOrdered, "run time ordering remains behind a random session prefix"),
	hot("run_idempotency", []string{"tenant_id", "idempotency_key"}, ClassEntity, "ingress point lookup under a distributed tenant"),
	hot("attempts", []string{"tenant_id", "attempt_id"}, ClassEntity, "high-write entity with random attempt IDs"),
	hot("lease_heads", []string{"tenant_id", "run_id"}, ClassEntity, "per-run contention point distributed by random run IDs"),
	hot("leases", []string{"tenant_id", "lease_id"}, ClassEntity, "high-write entity with random lease IDs"),
	hot("checkpoints", []string{"tenant_id", "attempt_id", "sequence"}, ClassOrdered, "monotonic sequence is behind distributed tenant and attempt IDs"),
	hot("quota_reservations", []string{"tenant_id", "quota_reservation_id"}, ClassEntity, "high-write entity with random reservation IDs"),
	hot("usage_observations", []string{"tenant_id", "subscription_connection_id", "observed_at", "usage_observation_id"}, ClassAppend, "time is behind tenant and subscription"),
	entity("artifact_manifests", []string{"tenant_id", "artifact_manifest_id"}, "immutable point access"),
	hot("artifact_manifests_by_run", []string{"tenant_id", "run_id", "artifact_manifest_id"}, ClassEntity, "bounded manifest inventory behind a random run ID"),
	hot("dispatch_outbox", []string{"tenant_id", "dispatch_outbox_id"}, ClassEntity, "high-write entity with random outbox IDs"),
	hot("telegram_delivery_outbox", []string{"tenant_id", "telegram_delivery_id"}, ClassEntity, "high-write entity with random delivery IDs"),
	hot("telegram_deliveries_by_run", []string{"tenant_id", "run_id", "telegram_delivery_id"}, ClassEntity, "bounded delivery cleanup behind a random run ID"),
	hot("checkpoint_objects_by_run", []string{"tenant_id", "run_id", "checkpoint_id"}, ClassEntity, "durable checkpoint object cleanup behind a random run ID"),
	entity("session_lifecycle_backfill_state", []string{"backfill_id"}, "single deployment migration marker; never a serving contention point"),
	entity("execution_placement_cutover_state", []string{"cutover_id"}, "one explicit empty-backlog cutover marker; never a serving contention point"),
	entity("harness_binding_cutover_state", []string{"cutover_id"}, "one explicit empty-backlog harness-binding cutover marker; never a serving contention point"),
	hot("audit_events", []string{"tenant_id", "occurred_at", "audit_event_id"}, ClassAppend, "time is behind a distributed tenant; cloud evidence gates elephant-tenant scale"),
	hot("subscription_scheduler_slots", []string{"tenant_id", "subscription_connection_id"}, ClassEntity, "one contention row per user-owned subscription connection"),
	hot("tenant_scheduler_counters", []string{"tenant_id"}, ClassEntity, "one bounded counter row per tenant"),
	hot("worker_jobs", []string{"tenant_id", "run_id"}, ClassEntity, "point-addressable durable worker materialization descriptor"),
	bucketed("lease_expiry", "lease_expiry_v2", "expires_at", "run_id"),
	bucketed("dispatch_ready", "dispatch_ready_v2", "available_at", "dispatch_outbox_id"),
	bucketed("telegram_delivery_ready", "telegram_delivery_ready_v2", "available_at", "telegram_delivery_id"),
	bucketed("quota_expiry", "quota_expiry_v2", "expires_at", "quota_reservation_id"),
}

func entity(name string, key []string, rationale string) Policy {
	return Policy{
		LogicalName: name, PhysicalTable: name, PrimaryKey: key, Class: ClassEntity,
		LoadPartitioning: false, Rationale: rationale,
	}
}

func ordered(name string, key []string, rationale string) Policy {
	policy := entity(name, key, rationale)
	policy.Class = ClassOrdered
	return policy
}

func hot(name string, key []string, class TableClass, rationale string) Policy {
	return Policy{
		LogicalName: name, PhysicalTable: name, PrimaryKey: key, Class: class,
		LoadPartitioning: true, Rationale: rationale,
	}
}

func bucketed(logical, physical, timeColumn, idColumn string) Policy {
	return Policy{
		LogicalName: logical, PhysicalTable: physical,
		PrimaryKey: []string{"shard_bucket", timeColumn, "tenant_id", idColumn},
		Class:      ClassGlobalReady, LoadPartitioning: true,
		Bucketed:  true,
		Rationale: "bounded 16-way global time traversal; object hash also distributes an elephant tenant",
	}
}
