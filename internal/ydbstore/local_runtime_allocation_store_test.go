package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

type localRuntimeConnectedStore struct {
	actual      localRuntimeActualFixture
	parent      *ownedAttemptEffectParentFixture
	sql         *allocationSQLFixture
	bridge      *serverlessharness.BoundedOwnedAllocationPreparationV1
	grant       ports.AttemptEffectOwnershipGrantV1
	publication ports.OwnedAllocationPublicationV2
}

// Actual private installer/profile publisher/retained Source, actual parent
// reserve, Store preparation burn, executing-runtime producer and allocation
// publisher. SQL isolation, external funding declarations and the effect-source
// verifier remain SCRIPTED; this is not normal composition or native YDB proof.
func newLocalRuntimeConnectedStore(t *testing.T, sourceParents ...context.Context) localRuntimeConnectedStore {
	t.Helper()
	if len(sourceParents) > 1 {
		t.Fatal("connected fixture accepts at most one original Source parent")
	}
	var sourceParent context.Context
	if len(sourceParents) == 1 {
		sourceParent = sourceParents[0]
	}
	canonical := currentWorkerAuthorityFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: canonical.Run.TenantID, OwnerUserID: canonical.Job.CredentialOwnerUserID,
		SessionID: canonical.Run.SessionID, RunID: canonical.Run.ID, AttemptID: canonical.Attempt.ID}
	// Narrow the synthetic operator's declared setup budget BEFORE installation
	// so the existing one-minute worker envelope contains a longer active phase.
	actual := newLocalRuntimeActualFixture(t, true, true, localRuntimeActualPrebuild{scope: scope, setupDuration: 10 * time.Second, sourceParent: sourceParent})
	inputs := &ownedFinalSources{profile: actual.profile, spending: actual.execution.Spending}
	parent := newOwnedAttemptEffectParentFixture(t, ownedFinalPrebuild{at: actual.at, sources: inputs})
	if !reflect.DeepEqual(parent.base.reader.profile, actual.profile) || !reflect.DeepEqual(parent.source, actual.execution) {
		t.Fatal("canonical graph changed the genuine original installed profile/spending/source")
	}
	for i := range actual.execution.Spending {
		if !reflect.DeepEqual(parent.base.reader.spending[i], actual.execution.Spending[i]) {
			t.Fatalf("pre-seal graph rewrote original component %d authorization", i)
		}
	}
	fixture := &allocationSQLFixture{parent: parent.sql}
	db := sql.OpenDB(allocationConnector{fixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	parent.store.db = db
	// The connector consumes the same canonical setup accounting reserved by
	// the original burn, never a later fixture-inserted accounting record.
	installOperationIssuer(t, parent)
	reserved := parent.reserve(t, parent.request)
	if reserved.Grant == nil || parent.sql.inserts != 1 {
		t.Fatal("actual Store parent reservation did not create its sole original grant")
	}
	// The producer uses the SAME issuer as Store reserve/burn verification; no
	// test signer, fake prepared observation or boolean burn callback is installed.
	bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(parent.issuer.issuer, parent.parent.Authority.HarnessBinding.Backend,
		parent.parent.Authority.SubstrateBinding, actual.source, allocationBurnAdapter{parent.store})
	if err != nil {
		t.Fatalf("genuine local runtime producer composition: %v", err)
	}
	t.Cleanup(func() { _ = bridge.Close() })
	parent.store.ownedAllocationPublicationVerifier = bridge
	publication, err := bridge.PrepareAllocationPublication(actual.ctx, reserved.Grant.Clone())
	if err != nil {
		t.Fatalf("real Store burn followed by actual executing-runtime observation: %v", err)
	}
	if publication.Allocation.LocalRuntime == nil || parent.sql.burnUpdates != 1 || fixture.writes != 0 {
		t.Fatalf("expected genuine local observation after exactly one Store burn: local=%t burns=%d allocationWrites=%d", publication.Allocation.LocalRuntime != nil, parent.sql.burnUpdates, fixture.writes)
	}
	return localRuntimeConnectedStore{actual, parent, fixture, bridge, reserved.Grant.Clone(), publication}
}

func (f localRuntimeConnectedStore) advance(t *testing.T, at time.Time) {
	t.Helper()
	if at.Before(time.Now()) {
		t.Fatal("connected fixture cannot move its sole clock backwards")
	}
	time.Sleep(at.Sub(time.Now())) // explicit virtual boundary, never readiness
	synctest.Wait()
	f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
}

func TestLocalRuntimeConnectedStoreRegistrationActiveAndHistory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeConnectedStore(t)
		first, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
		if err != nil || first.Replayed || first.Original.LocalRuntime == nil || first.Original.ObservedImageDigest != "" || f.sql.writes != 1 {
			t.Fatalf("actual local Store registration: err=%v replay=%t writes=%d", err, first.Replayed, f.sql.writes)
		}
		deadline, err := domain.LocalAllocationActiveDeadlineV2(f.parent.parent.Authority, f.parent.parent.Reservation, f.parent.source)
		if err != nil || !first.Original.Window.ExpiresAt.Equal(deadline) || !first.Current.Window.ExpiresAt.Equal(deadline) || !deadline.After(f.publication.PreparedInvocationExpiresAt) || !first.Original.PreparedInvocationExpiresAt.Equal(f.publication.PreparedInvocationExpiresAt) {
			t.Fatalf("Store did not separate original setup and active windows: %v", err)
		}
		intent, err := f.publication.IntentDigest()
		registeredIntent, re := ports.OwnedAllocationRegistrationIntentDigestV2(first.Original)
		if err != nil || re != nil || intent != registeredIntent {
			t.Fatal("Store did not preserve exact genuine producer intent")
		}
		original, _ := domain.EncodeRegisteredAllocationV2(first.Original)
		row := append([]driver.Value(nil), f.sql.row...)
		current, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true)
		if err != nil || current.Digest != first.Digest {
			t.Fatalf("connected current read after actual registration: %v", err)
		}
		if err := f.bridge.EnterOwnedLocalAllocation(f.actual.ctx, f.publication); err != nil {
			t.Fatalf("original entry before setup expiry: %v", err)
		}
		f.advance(t, f.publication.PreparedInvocationExpiresAt.Add(time.Microsecond))
		current, err = f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true)
		if err != nil || current.Digest != first.Digest || f.bridge.VerifyEnteredLocalAllocation(current.Current, f.parent.graph.at) != nil {
			t.Fatalf("registered local allocation truncated at setup expiry: %v", err)
		}
		if f.bridge.VerifyOwnedAllocationPublicationCurrent(f.publication, f.parent.graph.at) == nil {
			t.Error("established active allocation renewed first-publication setup permission")
		}
		if _, err := f.bridge.PrepareAllocationPublication(f.actual.ctx, f.grant.Clone()); err == nil || f.parent.sql.burnUpdates != 1 {
			t.Error("original grant replay triggered a second real Store burn/preparation")
		}
		if err := f.bridge.VerifyEnteredLocalAllocation(current.Current, f.parent.graph.at); err != nil {
			t.Fatalf("denied duplicate closed the entered original allocation: %v", err)
		}
		replayed, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication.Clone())
		if err != nil || !replayed.Replayed || replayed.Digest != first.Digest || f.sql.writes != 1 || !reflect.DeepEqual(f.sql.row, row) {
			t.Fatalf("same-intent publication recovery renewed original state: %v", err)
		}
		if err := f.bridge.Close(); err != nil {
			t.Fatal(err)
		}
		if f.bridge.VerifyEnteredLocalAllocation(first.Current, f.parent.graph.at) == nil {
			t.Error("closed producer authenticated live entered allocation")
		}
		history, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
		historyBytes, he := domain.EncodeRegisteredAllocationV2(history.Original)
		if err != nil || he != nil || !bytes.Equal(historyBytes, original) {
			t.Fatalf("closing actual producer changed historical canonical record: %v", err)
		}
		f.advance(t, deadline)
		if denied, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true); err == nil || !reflect.DeepEqual(denied, ports.OwnedAllocationRegistrationReceiptV2{}) {
			t.Error("expired actual local allocation remained current")
		}
		history, err = f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
		historyBytes, he = domain.EncodeRegisteredAllocationV2(history.Original)
		if err != nil || he != nil || !bytes.Equal(historyBytes, original) || f.sql.writes != 1 || f.parent.sql.burnUpdates != 1 {
			t.Fatalf("active expiry changed history or repeated burn/write: %v", err)
		}
	})
}

func TestLocalRuntimeConnectedStoreLateOrClosedFirstPublicationWritesNothing(t *testing.T) {
	for _, action := range []string{"late", "closed", "cancelled"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalRuntimeConnectedStore(t)
				parentRow := append([]driver.Value(nil), f.parent.sql.parentRow...)
				burnRow := append([]driver.Value(nil), f.parent.sql.burnRow...)
				switch action {
				case "late":
					f.advance(t, f.publication.PreparedInvocationExpiresAt)
				case "closed":
					if err := f.bridge.Close(); err != nil {
						t.Fatal(err)
					}
				case "cancelled":
					f.actual.cancel()
					synctest.Wait()
				}
				if f.bridge.VerifyOwnedAllocationPublication(f.publication, f.parent.graph.at) != nil || f.bridge.VerifyOwnedAllocationPublicationCurrent(f.publication, f.parent.graph.at) == nil {
					t.Fatal("control must retain genuine historical proof but no fresh producer authority")
				}
				denied, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
				if err == nil || !reflect.DeepEqual(denied, ports.OwnedAllocationRegistrationReceiptV2{}) || f.sql.row != nil || f.sql.writes != 0 || f.parent.sql.burnUpdates != 1 || !reflect.DeepEqual(parentRow, f.parent.sql.parentRow) || !reflect.DeepEqual(burnRow, f.parent.sql.burnRow) {
					t.Fatalf("%s first publication wrote/replaced canonical state: err=%v writes=%d burns=%d", action, err, f.sql.writes, f.parent.sql.burnUpdates)
				}
			})
		})
	}
}

func TestLocalRuntimeConnectedStoreLostAckRecoversSameGenuineIntent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeConnectedStore(t)
		before, err := f.publication.IntentBytes()
		if err != nil {
			t.Fatal(err)
		}
		f.parent.graph.commitModes = []string{"ambiguous", ""}
		receipt, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
		if err != nil || !receipt.Replayed || receipt.Original.LocalRuntime == nil || f.sql.writes != 1 || f.parent.sql.burnUpdates != 1 {
			t.Fatalf("committed local allocation lost-ACK recovery: err=%v replay=%t writes=%d burns=%d", err, receipt.Replayed, f.sql.writes, f.parent.sql.burnUpdates)
		}
		after, err := f.publication.IntentBytes()
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("lost acknowledgement changed original actual producer intent")
		}
		row := append([]driver.Value(nil), f.sql.row...)
		replayed, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication.Clone())
		if err != nil || !replayed.Replayed || replayed.Digest != receipt.Digest || !reflect.DeepEqual(replayed.Original, receipt.Original) || !reflect.DeepEqual(row, f.sql.row) || f.sql.writes != 1 || f.parent.sql.burnUpdates != 1 {
			t.Fatalf("same receipt recovery changed original local allocation: %v", err)
		}
	})
}

func TestLocalRuntimeConnectedStoreRetirementPreservesOriginalHistory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeConnectedStore(t)
		first, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
		if err != nil {
			t.Fatal(err)
		}
		original, err := domain.EncodeRegisteredAllocationV2(first.Original)
		if err != nil {
			t.Fatal(err)
		}
		parentRow := append([]driver.Value(nil), f.parent.sql.parentRow...)
		burnRow := append([]driver.Value(nil), f.parent.sql.burnRow...)
		retired, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, first.Digest, domain.RegisteredPreparationRetiredV1)
		if err != nil || retired.Replayed || retired.Current.State != domain.RegisteredPreparationRetiredV1 || retired.Current.RevocationGeneration != 2 || f.sql.writes != 2 {
			t.Fatalf("actual local allocation retirement: err=%v writes=%d", err, f.sql.writes)
		}
		if current, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true); err == nil || !reflect.DeepEqual(current, ports.OwnedAllocationRegistrationReceiptV2{}) {
			t.Error("retired local allocation remained current")
		}
		history, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
		raw, encodeErr := domain.EncodeRegisteredAllocationV2(history.Original)
		if err != nil || encodeErr != nil || history.Digest != retired.Digest || history.Current.State != domain.RegisteredPreparationRetiredV1 || !bytes.Equal(raw, original) {
			t.Fatalf("retirement replaced original local runtime history: %v", err)
		}
		row := append([]driver.Value(nil), f.sql.row...)
		replay, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, first.Digest, domain.RegisteredPreparationRetiredV1)
		if err != nil || !replay.Replayed || replay.Digest != retired.Digest || !reflect.DeepEqual(f.sql.row, row) || f.sql.writes != 2 {
			t.Fatalf("retirement replay changed canonical head: %v", err)
		}
		historicalReplay, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication.Clone())
		if err != nil || !historicalReplay.Replayed || historicalReplay.Digest != retired.Digest || historicalReplay.Current.State != domain.RegisteredPreparationRetiredV1 || !reflect.DeepEqual(historicalReplay.Original, first.Original) {
			t.Errorf("original publication replay did not retain historical retired head: %v", err)
		}
		if !reflect.DeepEqual(f.sql.row, row) || !reflect.DeepEqual(f.parent.sql.parentRow, parentRow) || !reflect.DeepEqual(f.parent.sql.burnRow, burnRow) || f.parent.sql.burnUpdates != 1 || f.sql.writes != 2 {
			t.Fatal("retirement/replay modified original parent or repeated preparation burn")
		}
	})
}

func TestLocalRuntimeConnectedStoreCurrentMismatchDeniesWithoutRepair(t *testing.T) {
	for _, mismatch := range []string{"digest", "runtime_identity", "profile_revision"} {
		t.Run(mismatch, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalRuntimeConnectedStore(t)
				first, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
				if err != nil {
					t.Fatal(err)
				}
				// Deliberately corrupt only the persisted current row. The genuine
				// installer, original receipt, source and producer remain unchanged.
				changed := first.Current.Clone()
				switch mismatch {
				case "digest":
					f.sql.row[9] = strings.Repeat("a", 64)
				case "runtime_identity":
					changed.LocalRuntime.HostIdentityDigest = strings.Repeat("b", 64)
				case "profile_revision":
					changed.Profile.RevocationGeneration++
				}
				if mismatch != "digest" {
					// JSON remains valid, but bypassing the codec simulates a storage
					// corruption, never a valid replacement producer publication.
					raw, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					f.sql.row[8] = string(raw)
					if digest, err := changed.Digest(); err == nil {
						f.sql.row[9] = string(digest)
					}
				}
				before := append([]driver.Value(nil), f.sql.row...)
				for _, requireCurrent := range []bool{true, false} {
					got, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, requireCurrent)
					if err == nil || !reflect.DeepEqual(got, ports.OwnedAllocationRegistrationReceiptV2{}) {
						t.Fatalf("mismatched local %s yielded current=%t record", mismatch, requireCurrent)
					}
				}
				if !reflect.DeepEqual(before, f.sql.row) || f.sql.writes != 1 || f.parent.sql.burnUpdates != 1 {
					t.Fatal("mismatched current read repaired state or repeated original burn")
				}
			})
		})
	}
}

func TestLocalRuntimeConnectedStoreRevokedCurrentProfileDenies(t *testing.T) {
	for _, registered := range []bool{false, true} {
		name := "before_first_publication"
		if registered {
			name = "after_registration"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalRuntimeConnectedStore(t)
				var first ports.OwnedAllocationRegistrationReceiptV2
				if registered {
					var err error
					first, err = f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
					if err != nil {
						t.Fatal(err)
					}
				}
				originalIntent, err := f.publication.IntentBytes()
				if err != nil {
					t.Fatal(err)
				}
				row := append([]driver.Value(nil), f.sql.row...)
				writes := f.sql.writes
				// This is the scripted SQL CURRENT head, not the genuine installer's
				// immutable configuration, original accepted receipt, source or MAC.
				f.parent.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
				f.parent.base.reader.profile.RevocationGeneration++
				f.parent.base.reader.sync(t)
				f.advance(t, f.actual.at.Add(time.Microsecond))
				if err := f.bridge.VerifyOwnedAllocationPublicationCurrent(f.publication, f.parent.graph.at); err != nil {
					t.Fatalf("retained actual producer control unexpectedly lost its original proof: %v", err)
				}
				if got, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true); err == nil || !reflect.DeepEqual(got, ports.OwnedAllocationRegistrationReceiptV2{}) {
					t.Error("revoked canonical local profile yielded current allocation")
				}
				if registered {
					history, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
					if err != nil || history.Digest != first.Digest || !reflect.DeepEqual(history.Original, first.Original) {
						t.Fatalf("scripted current profile revocation rewrote original history: %v", err)
					}
				} else if got, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication); err == nil || !reflect.DeepEqual(got, ports.OwnedAllocationRegistrationReceiptV2{}) {
					t.Error("genuine runtime proof bypassed current installed-profile revocation on first publication")
				}
				afterIntent, err := f.publication.IntentBytes()
				if err != nil || !bytes.Equal(originalIntent, afterIntent) || !reflect.DeepEqual(row, f.sql.row) || f.sql.writes != writes || f.parent.sql.burnUpdates != 1 {
					t.Fatal("current profile denial altered actual producer intent, allocation history or original Store burn")
				}
			})
		})
	}
}
