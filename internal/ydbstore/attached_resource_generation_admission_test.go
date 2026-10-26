package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func generationAdmissionBinding(t *testing.T, f *generationStoreFixture) (domain.HarnessBindingV2, domain.ExecutionPlacementV2) {
	t.Helper()
	binding, _, err := domain.ProjectAttachedResourceGenerationV1(f.baseline, 4)
	if err != nil {
		t.Fatal(err)
	}
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: f.auth.OwnerUserID, WorkerID: f.auth.WorkerID,
		CapabilityDigest: f.baseline.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)}
	digest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "admission-run", "admission-attempt", string(digest)
	f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = generationJSON(t, f.head(4, false))
	return binding, placement
}

func checkGenerationAdmission(t *testing.T, f *generationStoreFixture, binding domain.HarnessBindingV2, placement domain.ExecutionPlacementV2) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tx, err := f.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	return f.store.validateAttachedResourceGenerationAdmissionTx(ctx, &stateTx{store: f.store, sqlTx: tx, tenantID: f.auth.TenantID}, f.auth.OwnerUserID, f.auth.SubscriptionConnectionID, placement, binding)
}

func TestAttachedGenerationAdmissionRequiresExactCurrentProjectionInCallerTransaction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *generationStoreFixture, *domain.HarnessBindingV2, *domain.ExecutionPlacementV2)
	}{
		{"exact", nil},
		{"missing head", func(_ *testing.T, f *generationStoreFixture, _ *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = nil
		}},
		{"revoked", func(t *testing.T, f *generationStoreFixture, _ *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = generationJSON(t, f.head(5, true))
		}},
		{"stale generation", func(t *testing.T, f *generationStoreFixture, _ *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = generationJSON(t, f.head(5, false))
		}},
		{"expired evidence", func(_ *testing.T, f *generationStoreFixture, _ *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			f.sql.rows["SELECT CurrentUtcTimestamp()"] = []driver.Value{*f.baseline.Binding.EvidenceExpiresAt}
		}},
		{"foreign owner", func(_ *testing.T, _ *generationStoreFixture, b *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			b.OwnerUserID = "foreign"
		}},
		{"worker", func(_ *testing.T, _ *generationStoreFixture, _ *domain.HarnessBindingV2, p *domain.ExecutionPlacementV2) {
			p.WorkerID = "foreign"
		}},
		{"model", func(_ *testing.T, _ *generationStoreFixture, b *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			b.ModelID = "foreign-model"
		}},
		{"artifact", func(_ *testing.T, _ *generationStoreFixture, b *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			b.Backend.ArtifactDigest = strings.Repeat("e", 64)
		}},
		{"policy", func(_ *testing.T, _ *generationStoreFixture, b *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			b.EffectivePolicyDigest = strings.Repeat("e", 64)
		}},
		{"lost membership", func(_ *testing.T, f *generationStoreFixture, _ *domain.HarnessBindingV2, _ *domain.ExecutionPlacementV2) {
			f.sql.rows[generationSQLKey(`SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`)] = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newGenerationStoreFixture(t)
			binding, placement := generationAdmissionBinding(t, f)
			if tc.mutate != nil {
				tc.mutate(t, f, &binding, &placement)
			}
			err := checkGenerationAdmission(t, f, binding, placement)
			if (err == nil) != (tc.mutate == nil) {
				t.Fatalf("case=%s admission err=%v", tc.name, err)
			}
			if f.sql.begins != 1 || f.sql.commits != 0 || f.sql.writes != 0 {
				t.Fatalf("gate escaped caller transaction or wrote: %+v", f.sql)
			}
		})
	}
}

func TestAttachedGenerationAdmissionCannotFallBackAfterRegistryRemoval(t *testing.T) {
	for _, hasHead := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy absent head", true: "published head"}[hasHead], func(t *testing.T) {
			f := newGenerationStoreFixture(t)
			binding, placement := generationAdmissionBinding(t, f)
			f.store.attachedResourceGenerations = nil
			key := generationSQLKey(attachedGenerationPresenceSQL)
			f.sql.keys[key] = []driver.Value{string(f.auth.TenantID), string(f.auth.SubscriptionConnectionID)}
			f.sql.rows[key] = []driver.Value{nil}
			if hasHead {
				f.sql.rows[key][0] = generationJSON(t, f.head(4, false))
			}
			err := checkGenerationAdmission(t, f, binding, placement)
			if (err != nil) != hasHead {
				t.Fatalf("hasHead=%t err=%v", hasHead, err)
			}
			if f.sql.writes != 0 || f.sql.commits != 0 {
				t.Fatal("presence fence wrote or committed")
			}
		})
	}
}
