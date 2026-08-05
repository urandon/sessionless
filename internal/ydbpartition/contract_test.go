package ydbpartition

import (
	"fmt"
	"testing"
)

func TestBucketV1GoldenVectors(t *testing.T) {
	vectors := map[string]uint32{
		"run_aaaaaaaaaaaaaaaaaaaaaaaaaa": 15,
		"run_bbbbbbbbbbbbbbbbbbbbbbbbbb": 5,
		"dispatch-tenant-a-0001":         6,
		"lease-elephant-9999":            10,
	}
	for value, want := range vectors {
		got, err := BucketV1(value)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("BucketV1(%q) = %d, want %d", value, got, want)
		}
	}
}

func TestBucketV1DistributesMonotonicAndElephantTraffic(t *testing.T) {
	counts := make([]int, BucketCountV1)
	for i := 0; i < 65536; i++ {
		bucket, err := BucketV1(fmt.Sprintf("elephant-run-%020d", i))
		if err != nil {
			t.Fatal(err)
		}
		counts[bucket]++
	}
	expected := 65536 / int(BucketCountV1)
	for bucket, count := range counts {
		delta := count - expected
		if delta < 0 {
			delta = -delta
		}
		if delta > expected/10 {
			t.Fatalf("bucket %d count = %d, more than 10%% from %d", bucket, count, expected)
		}
	}
}

func TestBucketV1DistributesManyTenantBurstsWithBoundedFanout(t *testing.T) {
	counts := make([]int, BucketCountV1)
	for tenant := 0; tenant < 4096; tenant++ {
		for burst := 0; burst < 16; burst++ {
			bucket, err := BucketV1(fmt.Sprintf(
				"tenant-%06d-dispatch-%020d",
				tenant,
				tenant*16+burst,
			))
			if err != nil {
				t.Fatal(err)
			}
			counts[bucket]++
		}
	}
	expected := 65536 / int(BucketCountV1)
	for bucket, count := range counts {
		delta := count - expected
		if delta < 0 {
			delta = -delta
		}
		if delta > expected/10 {
			t.Fatalf("bucket %d count = %d, more than 10%% from %d", bucket, count, expected)
		}
	}
	if len(BucketsV1()) != int(BucketCountV1) {
		t.Fatalf("global traversal fanout = %d, want %d", len(BucketsV1()), BucketCountV1)
	}
}

func TestPoliciesCoverLogicalTablesOnce(t *testing.T) {
	seen := make(map[string]struct{})
	for _, policy := range Policies() {
		if _, exists := seen[policy.LogicalName]; exists {
			t.Fatalf("duplicate policy for %s", policy.LogicalName)
		}
		seen[policy.LogicalName] = struct{}{}
	}
	if len(seen) != 32 {
		t.Fatalf("logical table policies = %d, want 32", len(seen))
	}
}

func TestCanonicalSessionIndexesRemainBoundedAndTenantScoped(t *testing.T) {
	want := map[string][]string{
		"sessions":         {"tenant_id", "session_id"},
		"session_events":   {"tenant_id", "session_id", "sequence"},
		"session_activity": {"tenant_id", "user_id", "status", "activity_bucket", "updated_at", "session_id"},
		"runs_by_session":  {"tenant_id", "session_id", "created_at", "run_id"},
	}
	for _, policy := range Policies() {
		key, exists := want[policy.LogicalName]
		if !exists {
			continue
		}
		delete(want, policy.LogicalName)
		if fmt.Sprint(policy.PrimaryKey) != fmt.Sprint(key) {
			t.Errorf("%s key = %v, want %v", policy.LogicalName, policy.PrimaryKey, key)
		}
		if !policy.LoadPartitioning {
			t.Errorf("%s must leave load-aware splitting enabled", policy.LogicalName)
		}
	}
	for table := range want {
		t.Errorf("missing canonical partition policy for %s", table)
	}
}

func TestSchedulerPointTablesUseAutomaticLoadBasedGrowth(t *testing.T) {
	want := map[string]bool{
		"subscription_scheduler_slots": false,
		"tenant_scheduler_counters":    false,
	}
	for _, policy := range Policies() {
		if _, exists := want[policy.LogicalName]; !exists {
			continue
		}
		want[policy.LogicalName] = true
		if policy.Bucketed {
			t.Errorf("%s must remain a tenant-first point table, not a global bucketed index", policy.LogicalName)
		}
		if !policy.LoadPartitioning {
			t.Errorf("%s must enable load-based partitioning", policy.LogicalName)
		}
	}
	for table, found := range want {
		if !found {
			t.Errorf("missing partition policy for %s", table)
		}
	}
}
