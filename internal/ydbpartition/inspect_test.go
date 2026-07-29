package ydbpartition

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestInspectPolicyRejectsPartitionDrift(t *testing.T) {
	policy := bucketed("dispatch_ready", "dispatch_ready_v2", "available_at", "dispatch_outbox_id")
	description := &options.Description{
		PrimaryKey: policy.PrimaryKey,
		PartitioningSettings: options.PartitioningSettings{
			PartitioningBySize: options.FeatureEnabled,
			PartitionSizeMb:    policy.PartitionSizeMB,
			PartitioningByLoad: options.FeatureDisabled,
			MinPartitionsCount: policy.MinPartitions,
			MaxPartitionsCount: policy.MaxPartitions,
		},
		Stats: &options.TableStats{Partitions: policy.InitialPartitions},
	}
	result := inspectPolicy(policy, description)
	if result.MatchesContract {
		t.Fatal("disabled load partitioning was accepted")
	}
}

func TestInspectPolicyAcceptsLowVolumeDefaultSettings(t *testing.T) {
	policy := entity("tenants", []string{"tenant_id"}, "test")
	result := inspectPolicy(policy, &options.Description{
		PrimaryKey: policy.PrimaryKey,
		PartitioningSettings: options.PartitioningSettings{
			PartitioningBySize: options.FeatureEnabled,
			PartitionSizeMb:    2000,
			PartitioningByLoad: options.FeatureDisabled,
			MinPartitionsCount: 1,
			MaxPartitionsCount: 50,
		},
	})
	if !result.MatchesContract {
		t.Fatalf("default low-volume settings rejected: %v", result.ContractViolations)
	}
}
