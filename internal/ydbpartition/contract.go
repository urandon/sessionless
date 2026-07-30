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
	entity("conversations", []string{"tenant_id", "conversation_id"}, "point access under a distributed tenant ID"),
	ordered("context_epochs", []string{"tenant_id", "conversation_id", "context_epoch"}, "monotonic epoch is behind tenant and conversation"),
	hot("telegram_updates", []string{"tenant_id", "source_id", "update_id"}, ClassAppend, "Telegram update sequence is behind a distributed tenant"),
	entity("subscription_connections", []string{"tenant_id", "subscription_connection_id"}, "low-cardinality point access"),
	hot("runs", []string{"tenant_id", "run_id"}, ClassEntity, "high-write entity with random run IDs"),
	hot("run_idempotency", []string{"tenant_id", "idempotency_key"}, ClassEntity, "ingress point lookup under a distributed tenant"),
	hot("attempts", []string{"tenant_id", "attempt_id"}, ClassEntity, "high-write entity with random attempt IDs"),
	hot("lease_heads", []string{"tenant_id", "run_id"}, ClassEntity, "per-run contention point distributed by random run IDs"),
	hot("leases", []string{"tenant_id", "lease_id"}, ClassEntity, "high-write entity with random lease IDs"),
	hot("checkpoints", []string{"tenant_id", "attempt_id", "sequence"}, ClassOrdered, "monotonic sequence is behind distributed tenant and attempt IDs"),
	hot("quota_reservations", []string{"tenant_id", "quota_reservation_id"}, ClassEntity, "high-write entity with random reservation IDs"),
	hot("usage_observations", []string{"tenant_id", "subscription_connection_id", "observed_at", "usage_observation_id"}, ClassAppend, "time is behind tenant and subscription"),
	entity("artifact_manifests", []string{"tenant_id", "artifact_manifest_id"}, "immutable point access"),
	hot("dispatch_outbox", []string{"tenant_id", "dispatch_outbox_id"}, ClassEntity, "high-write entity with random outbox IDs"),
	hot("telegram_delivery_outbox", []string{"tenant_id", "telegram_delivery_id"}, ClassEntity, "high-write entity with random delivery IDs"),
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
