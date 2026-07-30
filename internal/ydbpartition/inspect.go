package ydbpartition

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type TableDescriber interface {
	DescribeTable(
		ctx context.Context,
		path string,
		opts ...options.DescribeTableOption,
	) (*options.Description, error)
}

type Inspection struct {
	ContractVersion string            `json:"contract_version"`
	BucketCount     uint32            `json:"bucket_count"`
	Valid           bool              `json:"valid"`
	Tables          []TableInspection `json:"tables"`
}

type TableInspection struct {
	LogicalName        string     `json:"logical_name"`
	PhysicalTable      string     `json:"physical_table"`
	Class              TableClass `json:"class"`
	ExpectedPrimaryKey []string   `json:"expected_primary_key"`
	ActualPrimaryKey   []string   `json:"actual_primary_key"`
	LoadPartitioning   string     `json:"load_partitioning"`
	SizePartitioning   string     `json:"size_partitioning"`
	PartitionSizeMB    uint64     `json:"partition_size_mb"`
	MinPartitions      uint64     `json:"min_partitions"`
	MaxPartitions      uint64     `json:"max_partitions"`
	ActualPartitions   uint64     `json:"actual_partitions"`
	RowsEstimate       uint64     `json:"rows_estimate"`
	MatchesContract    bool       `json:"matches_contract"`
	ContractViolations []string   `json:"contract_violations,omitempty"`
	Rationale          string     `json:"rationale"`
}

func Inspect(
	ctx context.Context,
	client TableDescriber,
	databasePath string,
) (Inspection, error) {
	if client == nil {
		return Inspection{}, fmt.Errorf("YDB table describer must not be nil")
	}
	databasePath = path.Clean(strings.TrimSpace(databasePath))
	if databasePath == "." || !strings.HasPrefix(databasePath, "/") {
		return Inspection{}, fmt.Errorf("YDB database path must be absolute")
	}
	result := Inspection{
		ContractVersion: ContractVersion,
		BucketCount:     BucketCountV1,
		Valid:           true,
	}
	for _, policy := range Policies() {
		description, err := client.DescribeTable(
			ctx,
			path.Join(databasePath, policy.PhysicalTable),
			options.WithTableStats(),
			options.WithPartitionStats(),
			options.WithShardKeyBounds(),
		)
		if err != nil {
			return Inspection{}, fmt.Errorf("describe %s: %w", policy.PhysicalTable, err)
		}
		item := inspectPolicy(policy, description)
		if !item.MatchesContract {
			result.Valid = false
		}
		result.Tables = append(result.Tables, item)
	}
	return result, nil
}

func inspectPolicy(policy Policy, description *options.Description) TableInspection {
	settings := description.PartitioningSettings
	item := TableInspection{
		LogicalName: policy.LogicalName, PhysicalTable: policy.PhysicalTable,
		Class: policy.Class, ExpectedPrimaryKey: policy.PrimaryKey,
		ActualPrimaryKey: description.PrimaryKey,
		LoadPartitioning: featureName(settings.PartitioningByLoad),
		SizePartitioning: featureName(settings.PartitioningBySize),
		PartitionSizeMB:  settings.PartitionSizeMb,
		MinPartitions:    settings.MinPartitionsCount,
		MaxPartitions:    settings.MaxPartitionsCount,
		MatchesContract:  true, Rationale: policy.Rationale,
	}
	if description.Stats != nil {
		item.ActualPartitions = description.Stats.Partitions
		item.RowsEstimate = description.Stats.RowsEstimate
	}
	if !slices.Equal(description.PrimaryKey, policy.PrimaryKey) {
		item.ContractViolations = append(item.ContractViolations, "primary key differs from the physical contract")
	}
	if policy.LoadPartitioning {
		if settings.PartitioningByLoad != options.FeatureEnabled {
			item.ContractViolations = append(item.ContractViolations, "load partitioning is not enabled")
		}
		if settings.PartitioningBySize != options.FeatureEnabled {
			item.ContractViolations = append(item.ContractViolations, "size partitioning is not enabled")
		}
	}
	item.MatchesContract = len(item.ContractViolations) == 0
	return item
}

func featureName(flag options.FeatureFlag) string {
	switch flag {
	case options.FeatureEnabled:
		return "enabled"
	case options.FeatureDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}
