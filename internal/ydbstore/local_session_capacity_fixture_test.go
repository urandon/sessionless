package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// Retained physical DATA for the old explicitly seeded running fixture. This
// is neither admission nor Source/funding/entitlement authority. Populate its
// exact original hold before any factory/Manager can consume this storage.
func declareLocalSessionCapacityRows(t *testing.T, parent *ownedAttemptEffectParentFixture) {
	t.Helper()
	var run domain.Run
	var quota domain.QuotaReservation
	if json.Unmarshal([]byte(parent.graph.rows["runs"][0].(string)), &run) != nil || json.Unmarshal([]byte(parent.graph.rows["quota_reservations"][0].(string)), &quota) != nil || quota.ValidateForRun(run) != nil || run.LocalCapacityAssociation == nil || quota.Status != domain.ReservationHeld {
		t.Fatal("local retained capacity requires exact original running hold")
	}
	physical, err := encodeCapacityStorageV1(run.TenantID, run.SubscriptionConnectionID, run.LocalCapacityAssociation)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(quota.ID))
	if err != nil {
		t.Fatal(err)
	}
	parent.graph.rows["quota_expiry_v2"] = []driver.Value{int64(bucket), quota.ExpiresAt, string(quota.TenantID), string(quota.ID), string(quota.RunID), physical.key, physical.association.String, physical.digest.String}
	parent.graph.slot = []driver.Value{string(domain.SchedulerPressured), string(run.ID), string(quota.ID), time.Unix(0, 0).UTC(), quota.HeldAt, physical.association.String, physical.digest.String}
	parent.graph.counters = []driver.Value{int64(0), int64(1)}
}

func TestLocalSessionFixtureRetainsPhysicalLocalHold(t *testing.T) {
	seed := NewLocalTerminalPublicationSessionConnectedSeed(t, "http://127.0.0.1:35421/v1", &localEntryBlobs{})
	seed.InstallManagerReadSQL(t)
	store, err := New(seed.DB, seed.Options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	loaded, found, err := store.LoadWorkerJob(ctx, seed.Scope.TenantID, seed.Scope.RunID)
	if err != nil || !found || loaded.Reservation.Status != domain.ReservationHeld {
		t.Fatalf("original local held projection load: found=%t err=%v", found, err)
	}
}

func TestLocalSessionFixtureRejectsMissingForeignMixedCapacity(t *testing.T) {
	for _, fault := range []string{"expiry missing", "expiry foreign", "expiry partial NULL", "quota mixed", "slot missing", "slot foreign", "slot partial NULL"} {
		t.Run(fault, func(t *testing.T) {
			seed := NewLocalTerminalPublicationSessionConnectedSeed(t, "http://127.0.0.1:35421/v1", &localEntryBlobs{})
			seed.InstallManagerReadSQL(t)
			store, e := New(seed.DB, seed.Options)
			if e != nil {
				t.Fatal(e)
			}
			loaded := seed.WorkerPreflightState(t)
			foreign := *loaded.Run.LocalCapacityAssociation
			foreign.OwnerUserID = "foreign-owner"
			physical, e := encodeCapacityStorageV1(loaded.Run.TenantID, "", &foreign)
			if e != nil {
				t.Fatal(e)
			}
			switch fault {
			case "expiry missing":
				seed.parent.graph.rows["quota_expiry_v2"] = nil
			case "expiry foreign":
				row := seed.parent.graph.rows["quota_expiry_v2"]
				row[5], row[6], row[7] = physical.key, physical.association.String, physical.digest.String
			case "expiry partial NULL":
				seed.parent.graph.rows["quota_expiry_v2"][7] = nil
			case "quota mixed":
				mixed := loaded.Reservation.Clone()
				mixed.SubscriptionConnectionID = "legacy-subscription"
				ratedFixturePut(t, seed.parent.graph.ratedSQLFixture, "quota_reservations", mixed)
			case "slot missing":
				seed.parent.graph.slot = nil
			case "slot foreign":
				seed.parent.graph.slot[5], seed.parent.graph.slot[6] = physical.association.String, physical.digest.String
			case "slot partial NULL":
				seed.parent.graph.slot[6] = nil
			}
			before := cloneLocalInstallationRows(seed.parent.graph.rows)
			slot := append([]driver.Value(nil), seed.parent.graph.slot...)
			counters := append([]driver.Value(nil), seed.parent.graph.counters...)
			writes := seed.parent.graph.writes
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			if !strings.HasPrefix(fault, "slot") {
				_, _, e = store.LoadWorkerJob(ctx, seed.Scope.TenantID, seed.Scope.RunID)
			} else {
				e = store.Transact(ctx, seed.Scope.TenantID, func(state ports.StateTx) error {
					return releaseLocalCapacitySlotTx(ctx, state.(*stateTx), loaded.Run, loaded.Reservation, seed.parent.graph.at)
				})
			}
			if e == nil || seed.parent.graph.writes != writes || !reflect.DeepEqual(before, seed.parent.graph.rows) || !reflect.DeepEqual(slot, seed.parent.graph.slot) || !reflect.DeepEqual(counters, seed.parent.graph.counters) {
				t.Fatal("invalid physical local arm accepted or repaired", e)
			}
		})
	}
}

func TestLocalSessionFixtureTerminalSlotWriteSelectedArm(t *testing.T) {
	seed := NewLocalSessionConnectedSeed(t, "http://127.0.0.1:35421/v1")
	run := ratedFixtureRecord[domain.Run](t, seed.parent.graph.ratedSQLFixture, "runs")
	physical, e := encodeCapacityStorageV1(run.TenantID, "", run.LocalCapacityAssociation)
	if e != nil {
		t.Fatal(e)
	}
	statement := `UPSERT INTO subscription_scheduler_slots (tenant_id, subscription_connection_id, state, active_run_id, active_reservation_id, blocked_until, updated_at, local_capacity_association, local_capacity_digest) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	for _, fault := range []string{"none", "foreign owner", "partial NULL", "both NULL", "wrong physical key"} {
		t.Run(fault, func(t *testing.T) {
			values := []driver.Value{string(run.TenantID), physical.key, string(domain.SchedulerReauthRequired), "", "", time.Unix(0, 0).UTC(), run.UpdatedAt, physical.association.String, physical.digest.String}
			switch fault {
			case "foreign owner":
				foreign := *run.LocalCapacityAssociation
				foreign.OwnerUserID = "foreign-owner"
				p, e := encodeCapacityStorageV1(run.TenantID, "", &foreign)
				if e != nil {
					t.Fatal(e)
				}
				values[1], values[7], values[8] = p.key, p.association.String, p.digest.String
			case "partial NULL":
				values[8] = nil
			case "both NULL":
				values[7], values[8] = nil, nil
			case "wrong physical key":
				values[1] = "legacy-subscription"
			}
			args := make([]driver.NamedValue, len(values))
			for i, v := range values {
				args[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
			}
			state := LocalTerminalCompletionSnapshot{}
			counters := []driver.Value{int64(0), int64(1)}
			err := applyTerminalCompletionWrite(&state, seed.parent.graph.rows, &counters, terminalCompletionLocalV2, statement, args)
			if (fault == "none" && err != nil) || (fault != "none" && (err == nil || state.Mutations != 0)) {
				t.Fatal("terminal local selected arm write changed", fault, err)
			}
		})
	}
}

// This validates the existing scripted running fixture's DATA projection, not
// normal canonical admission, a claim, funding or a new execution grant.
func TestLocalSessionFixturePreservesExplicitLocalCapacity(t *testing.T) {
	for _, auth := range []domain.ConfiguredInferenceAuthV1{domain.ConfiguredInferenceAuthNoneV1, domain.ConfiguredInferenceAuthBearerV1} {
		t.Run(string(auth), func(t *testing.T) {
			seed := newLocalSessionConnectedSeedWithAuth(t, "http://127.0.0.1:35421/v1", false, LocalCompactionConnectedLimits{}, auth)
			var run domain.Run
			var job domain.WorkerJob
			var quota domain.QuotaReservation
			var outbox domain.DispatchOutbox
			var attempt domain.Attempt
			for table, target := range map[string]any{"runs": &run, "worker_jobs": &job, "quota_reservations": &quota, "dispatch_outbox": &outbox, "attempts": &attempt} {
				rows := seed.parent.graph.rows[table]
				if len(rows) != 1 || json.Unmarshal([]byte(rows[0].(string)), target) != nil {
					t.Fatalf("scripted local fixture missing %s", table)
				}
			}
			if run.SubscriptionConnectionID != "" || quota.SubscriptionConnectionID != "" || run.LocalCapacityAssociation == nil || job.LocalCapacityAssociation == nil || quota.LocalCapacityAssociation == nil || outbox.LocalCapacityAssociation == nil ||
				*run.LocalCapacityAssociation != *job.LocalCapacityAssociation || *run.LocalCapacityAssociation != *quota.LocalCapacityAssociation || *run.LocalCapacityAssociation != *outbox.LocalCapacityAssociation ||
				run.LocalCapacityAssociation.ValidateForScope(seed.Scope.TenantID, seed.Scope.OwnerUserID, seed.parent.source.Profile.Locator.ResourceID) != nil || job.ValidateForRun(run) != nil || quota.ValidateForRun(run) != nil || outbox.ValidateForAttempt(run, attempt) != nil {
				t.Fatal("scripted local fixture mixed or lost original local identity")
			}
			missing := job.Clone()
			missing.LocalCapacityAssociation = nil
			if missing.ValidateForRun(run) == nil {
				t.Fatal("fixture compatibility weakened missing projection denial")
			}
			mixed := run.Clone()
			mixed.SubscriptionConnectionID = "legacy-subscription"
			if mixed.Validate() == nil {
				t.Fatal("fixture compatibility weakened mixed-arm denial")
			}
		})
	}
}
