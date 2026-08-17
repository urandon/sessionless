package sessioncontext

import (
	"context"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const snapshotCatalogPageSize uint64 = 200

type SnapshotCatalog interface {
	SnapshotStore
	ListSessionSnapshots(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, afterVersion uint64, limit uint64) ([]domain.SessionSnapshot, error)
}

type MaintenancePolicy struct {
	IntervalEvents uint64
	MaxEvents      uint64
	MaxBytes       uint64
	MaxVersions    uint64
}

type Maintainer struct {
	store   SnapshotCatalog
	builder *SnapshotBuilder
	policy  MaintenancePolicy
}

func NewMaintainer(
	store SnapshotCatalog,
	builder *SnapshotBuilder,
	policy MaintenancePolicy,
) (*Maintainer, error) {
	if store == nil || builder == nil {
		return nil, fmt.Errorf("snapshot maintainer dependencies must not be nil")
	}
	if policy.IntervalEvents == 0 || policy.MaxEvents == 0 || policy.MaxBytes == 0 || policy.MaxVersions == 0 {
		return nil, domain.ValidationError{Field: "snapshot_policy", Reason: "all limits must be positive"}
	}
	if policy.IntervalEvents > policy.MaxEvents {
		return nil, domain.ValidationError{Field: "snapshot_policy.interval_events", Reason: "must not exceed max_events"}
	}
	return &Maintainer{store: store, builder: builder, policy: policy}, nil
}

// MaybeCreate applies a bounded maintenance policy after successful dispatch.
// Snapshot failure never changes admission correctness; the next invocation
// can retry maintenance while workers continue to use canonical replay.
func (maintainer *Maintainer) MaybeCreate(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	throughSequence uint64,
) (domain.SessionSnapshot, bool, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.SessionSnapshot{}, false, err
	}
	if err := sessionID.Validate(); err != nil {
		return domain.SessionSnapshot{}, false, err
	}
	if throughSequence == 0 {
		return domain.SessionSnapshot{}, false, domain.ValidationError{
			Field: "snapshot_maintenance.through_sequence", Reason: "must be positive",
		}
	}
	if throughSequence > maintainer.policy.MaxEvents || throughSequence < maintainer.policy.IntervalEvents {
		return domain.SessionSnapshot{}, false, nil
	}
	latest, found, err := maintainer.latestSnapshot(ctx, tenantID, sessionID)
	if err != nil {
		return domain.SessionSnapshot{}, false, err
	}
	if found && (latest.ThroughSequence >= throughSequence ||
		throughSequence-latest.ThroughSequence < maintainer.policy.IntervalEvents) {
		return domain.SessionSnapshot{}, false, nil
	}
	version := uint64(1)
	if found {
		version = latest.Version + 1
		if version == 0 {
			return domain.SessionSnapshot{}, false, domain.ValidationError{
				Field: "snapshot_maintenance.version", Reason: "overflowed",
			}
		}
	}
	snapshot, err := maintainer.builder.Create(ctx, SnapshotRequest{
		TenantID: tenantID, SessionID: sessionID, Version: version,
		ThroughSequence: throughSequence,
		MaxEvents:       maintainer.policy.MaxEvents,
		MaxBytes:        maintainer.policy.MaxBytes,
	})
	if err != nil {
		return domain.SessionSnapshot{}, false, err
	}
	return snapshot, true, nil
}

func (maintainer *Maintainer) latestSnapshot(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
) (domain.SessionSnapshot, bool, error) {
	var latest domain.SessionSnapshot
	afterVersion := uint64(0)
	loaded := uint64(0)
	for loaded < maintainer.policy.MaxVersions {
		limit := snapshotCatalogPageSize
		if remaining := maintainer.policy.MaxVersions - loaded; remaining < limit {
			limit = remaining
		}
		page, err := maintainer.store.ListSessionSnapshots(ctx, tenantID, sessionID, afterVersion, limit)
		if err != nil {
			return domain.SessionSnapshot{}, false, err
		}
		if len(page) == 0 {
			return latest, loaded != 0, nil
		}
		for _, snapshot := range page {
			if err := snapshot.Validate(); err != nil {
				return domain.SessionSnapshot{}, false, err
			}
			if snapshot.TenantID != tenantID || snapshot.SessionID != sessionID || snapshot.Version != afterVersion+1 {
				return domain.SessionSnapshot{}, false, domain.ValidationError{
					Field: "snapshot_maintenance.catalog", Reason: "is not a contiguous tenant/session version page",
				}
			}
			latest = snapshot
			afterVersion = snapshot.Version
			loaded++
		}
		if uint64(len(page)) < limit {
			return latest, true, nil
		}
	}
	return domain.SessionSnapshot{}, false, domain.ValidationError{
		Field: "snapshot_maintenance.catalog", Reason: "exceeds the configured version scan limit",
	}
}
