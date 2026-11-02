package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// Extends the existing transaction model only with exact physical quota rows
// and their counters. Original Source/profile/funding and the production
// admission/cancellation graphs are not replaced by callbacks or approvals.
type localCancellationSQL struct {
	*ownedFinalScript
	expiry []driver.Value
	lease  []driver.Value
}

func (f *localCancellationSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localCancellationSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := f.ownedFinalScript.BeginTx(ctx, options)
	return localCancellationTx{inner: inner, owner: f, expiry: append([]driver.Value(nil), f.expiry...)}, err
}

type localCancellationTx struct {
	inner  driver.Tx
	owner  *localCancellationSQL
	expiry []driver.Value
}

func (tx localCancellationTx) Commit() error {
	rollback := len(tx.owner.commitModes) != 0 && tx.owner.commitModes[0] == "rollback"
	err := tx.inner.Commit()
	if rollback {
		tx.owner.expiry = tx.expiry
	}
	return err
}
func (tx localCancellationTx) Rollback() error {
	tx.owner.expiry = tx.expiry
	return tx.inner.Rollback()
}
func (f *localCancellationSQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	row := func(values []driver.Value) (driver.Rows, error) {
		f.recordQuery(statement, args)
		return &explanationFixtureRows{values: values}, nil
	}
	switch statement {
	case quotaReservationStorageSQLV1:
		var r domain.QuotaReservation
		stored := f.rows["quota_reservations"]
		if len(stored) == 0 {
			return row(nil)
		}
		if len(args) != 2 || json.Unmarshal([]byte(stored[0].(string)), &r) != nil || args[0].Value != string(r.TenantID) || args[1].Value != string(r.ID) {
			return nil, errors.New("wrong canonical quota key")
		}
		return row([]driver.Value{stored[0], string(r.ID), string(r.TenantID), string(r.RunID), string(r.SubscriptionConnectionID), string(r.Status), int64(r.CapacityUnits), r.HeldAt, r.ExpiresAt, r.ExpiresAt.Add(fixtureRetention)})
	case quotaExpiryStorageSQLV1:
		if len(f.expiry) != 0 && (len(args) != 4 || args[0].Value != f.expiry[0] || args[1].Value != f.expiry[1] || args[2].Value != f.expiry[2] || args[3].Value != f.expiry[3]) {
			return row(nil)
		}
		return row(f.expiry)
	case `SELECT SUBSTRING(CAST(lease_id AS String),0,161) FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`:
		return row(f.lease)
	}
	return f.ownedFinalScript.QueryContext(ctx, statement, args)
}

const fixtureRetention = time.Hour

func (f *localCancellationSQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	result, err := f.ownedFinalScript.ExecContext(ctx, statement, args)
	if err != nil {
		return result, err
	}
	flat := strings.Join(strings.Fields(statement), " ")
	switch {
	case strings.HasPrefix(flat, "UPSERT INTO attempts "):
		f.rows["attempts"] = []driver.Value{args[9].Value}
	case strings.HasPrefix(flat, "UPSERT INTO dispatch_outbox "):
		f.rows["dispatch_outbox"] = []driver.Value{args[8].Value}
	case strings.HasPrefix(flat, "UPSERT INTO quota_expiry_v2 "):
		f.expiry = make([]driver.Value, len(args))
		for i := range args {
			f.expiry[i] = args[i].Value
		}
	case strings.HasPrefix(flat, "DELETE FROM quota_expiry_v2 "):
		f.expiry = nil
	case strings.HasPrefix(flat, "INSERT INTO tenant_scheduler_counters "):
		f.counters = []driver.Value{args[1].Value, args[2].Value}
	case strings.HasPrefix(flat, "UPDATE tenant_scheduler_counters SET queue_depth = queue_depth + "):
		f.counters[0] = f.counters[0].(int64) + args[0].Value.(int64)
	case strings.HasPrefix(flat, "UPDATE tenant_scheduler_counters SET queue_depth=$1,active_runs=$2,updated_at=$3 WHERE tenant_id=$4"):
		f.counters = []driver.Value{args[0].Value, args[1].Value}
	}
	return result, nil
}

func localCancellationFixture(t *testing.T) (*Store, *localCancellationSQL, *originalLocalRecoveryFixture, ports.DispatchAdmissionRequest, ports.ManagedRunCancellationRequestV1) {
	t.Helper()
	original := newOriginalLocalRecoveryFixture(t, 4)
	p, err := original.store.RecoverLocalOwnedExecutionPackage(t.Context(), original.source, original.receipt, original.scope)
	if err != nil {
		t.Fatal(err)
	}
	s := p.Source.Scope
	a := &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: s.TenantID, OwnerUserID: s.OwnerUserID, ResourceID: p.Source.Profile.Locator.ResourceID}
	origin := &domain.FrontendEventOrigin{BindingID: "binding-local-cancel", BindingRevision: 1, Frontend: domain.FrontendWeb, ExternalConversationID: string(s.SessionID), ExternalEventID: "event-local-cancel"}
	state := ports.WorkerJobState{
		Run:         domain.Run{ID: s.RunID, TenantID: s.TenantID, SessionID: s.SessionID, TriggerEventID: "trigger-local-cancel", LocalCapacityAssociation: a, Status: domain.RunCreated, IdempotencyKey: "intent-local-cancel", CreatedAt: p.Source.IssuedAt, UpdatedAt: p.Source.IssuedAt},
		Attempt:     domain.Attempt{ID: s.AttemptID, TenantID: s.TenantID, RunID: s.RunID, Number: 1, Status: domain.AttemptCreated, CreatedAt: p.Source.IssuedAt, UpdatedAt: p.Source.IssuedAt},
		Job:         domain.WorkerJob{TenantID: s.TenantID, RunID: s.RunID, SessionID: s.SessionID, AttemptID: s.AttemptID, CredentialOwnerUserID: s.OwnerUserID, InputManifestID: "manifest-local-cancel", ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, Origin: origin},
		Reservation: domain.QuotaReservation{ID: "reservation-local-cancel"},
	}
	store, script, admission, _ := newOwnedFinalScript(t, ownedFinalPrebuild{at: p.Source.IssuedAt, state: &state, sources: &ownedFinalSources{profile: original.receipt.Profile, spending: p.Source.Spending}})
	// All authority pointers are the actual already-installed Source; the SQL
	// peer supplies DATA only. No fabricated subscription row is available.
	store.ownedProfilePublicationVerifier, store.attemptSpendingPublicationVerifier, store.ownedAttemptEffectSourceVerifier = original.source, original.source, original.source
	manifest := domain.ArtifactManifest{ID: state.Job.InputManifestID, TenantID: s.TenantID, RunID: s.RunID, CreatedAt: p.Source.IssuedAt}
	ratedFixturePut(t, script.ratedSQLFixture, "artifact_manifests", manifest)
	f := &localCancellationSQL{ownedFinalScript: script}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	admission.HoldUntil = p.Source.IssuedAt.Add(30 * time.Second)
	return store, f, original, admission, ports.ManagedRunCancellationRequestV1{TenantID: s.TenantID, UserID: s.OwnerUserID, SessionID: s.SessionID, RunID: s.RunID, DispatchID: admission.OutboxID}
}

func TestLocalRunCancellationBeforeAdmissionDoesNotCreateOrReleaseHold(t *testing.T) {
	store, f, original, _, request := localCancellationFixture(t)
	_ = original.source.Close() // Current funding is irrelevant to known cleanup.
	got, err := store.RequestManagedRunCancellationV1(t.Context(), request)
	if err != nil || got.Status != domain.RunCancelled || !got.NoHold || got.Replayed {
		logLocalCancellationSQLTail(t, f)
		t.Fatalf("unadmitted cancellation: got=%+v err=%v", got, err)
	}
	if len(f.rows["quota_reservations"]) != 0 || len(f.rows["worker_jobs"]) != 0 || len(f.slot) != 0 || len(f.counters) != 0 || len(f.expiry) != 0 {
		t.Fatal("unadmitted cancellation fabricated capacity or Job")
	}
	writes := f.writes
	got, err = store.RequestManagedRunCancellationV1(t.Context(), request)
	if err != nil || !got.Replayed || got.Status != domain.RunCancelled || f.writes != writes {
		t.Fatalf("cancel replay mutated state: got=%+v writes=%d/%d err=%v", got, f.writes, writes, err)
	}
	for _, statement := range f.statements {
		if strings.Contains(statement, "subscription_connections") {
			t.Fatal("local cancellation queried subscription authority")
		}
	}
}

func TestLocalRunCancellationReleasesActualAdmittedHoldAfterSourceRetirement(t *testing.T) {
	store, f, original, admission, request := localCancellationFixture(t)
	got, err := store.AdmitDispatch(t.Context(), admission)
	if err != nil || !got.Admitted {
		logLocalCancellationSQLTail(t, f)
		t.Fatalf("actual trusted local admission: got=%+v err=%v", got, err)
	}
	bucket, bucketErr := ydbpartition.BucketV1(string(admission.ReservationID))
	if bucketErr != nil || len(f.expiry) != 8 || f.expiry[0] != int64(bucket) {
		t.Fatal("actual admission did not preserve original physical expiry key")
	}
	_ = original.source.Close()
	before := f.writes
	cancelled, err := store.RequestManagedRunCancellationV1(t.Context(), request)
	if err != nil || cancelled.Status != domain.RunCancelled || cancelled.NoHold || cancelled.Replayed {
		t.Fatalf("admitted cancellation: got=%+v err=%v", cancelled, err)
	}
	var reservation domain.QuotaReservation
	if json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &reservation) != nil || reservation.Status != domain.ReservationReleased || len(f.expiry) != 0 || f.slot[1] != "" || f.slot[2] != "" || f.slot[0] != string(domain.SchedulerReauthRequired) || f.counters[0] != int64(0) || f.counters[1] != int64(0) {
		t.Fatal("cancellation lost original release, exact expiry or counters")
	}
	if f.writes == before {
		t.Fatal("test did not commit actual cancellation writes")
	}
	writes := f.writes
	cancelled, err = store.RequestManagedRunCancellationV1(t.Context(), request)
	if err != nil || !cancelled.Replayed || f.writes != writes {
		t.Fatalf("terminal replay changed hold: writes=%d/%d err=%v", f.writes, writes, err)
	}
	for _, statement := range f.statements {
		if strings.Contains(statement, "subscription_connections") {
			t.Fatal("local graph queried a subscription")
		}
	}
}

func logLocalCancellationSQLTail(t *testing.T, f *localCancellationSQL) {
	t.Helper()
	start := max(0, len(f.statements)-5)
	for _, statement := range f.statements[start:] {
		fields := strings.Fields(statement)
		for i, field := range fields {
			if (field == "FROM" || field == "INTO" || field == "UPDATE") && i+1 < len(fields) {
				t.Logf("controlled SQL tail: verb=%s table=%s", fields[0], fields[i+1])
				break
			}
		}
	}
	t.Logf("controlled SQL totals: statements=%d writes=%d", len(f.statements), f.writes)
}

func TestLocalRunCancellationForeignPrincipalDoesNotTouchCapacity(t *testing.T) {
	for _, field := range []string{"user", "tenant", "session", "dispatch"} {
		t.Run(field, func(t *testing.T) {
			store, f, _, _, r := localCancellationFixture(t)
			switch field {
			case "user":
				r.UserID = "foreign-user"
			case "tenant":
				r.TenantID = "foreign-tenant"
			case "session":
				r.SessionID = "foreign-session"
			case "dispatch":
				r.DispatchID = "foreign-dispatch"
			}
			writes := f.writes
			got, err := store.RequestManagedRunCancellationV1(t.Context(), r)
			if !errors.Is(err, domain.ErrMembershipDenied) || got != (ports.ManagedRunCancellationResultV1{}) || f.writes != writes {
				t.Fatalf("foreign selector touched state: err=%v writes=%d/%d", err, f.writes, writes)
			}
		})
	}
}

func TestLocalRunCancellationRetainsPossiblyActiveHold(t *testing.T) {
	for _, status := range []domain.RunStatus{domain.RunQueued, domain.RunRunning} {
		t.Run(string(status), func(t *testing.T) {
			store, f, original, admission, request := localCancellationFixture(t)
			if got, err := store.AdmitDispatch(t.Context(), admission); err != nil || !got.Admitted {
				t.Fatalf("actual admission: %+v %v", got, err)
			}
			f.lease = []driver.Value{"lease-possibly-active"} // No stopped-effect witness, even if lease expiry elapsed.
			var run domain.Run
			if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil {
				t.Fatal("read admitted Run fixture")
			}
			if status == domain.RunRunning {
				if err := run.Transition(status, f.at); err != nil {
					t.Fatal(err)
				}
				ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
			}
			_ = original.source.Close()
			before := append([]driver.Value(nil), f.expiry...)
			got, err := store.RequestManagedRunCancellationV1(t.Context(), request)
			if err != nil || got.Status != status || got.NoHold {
				t.Fatalf("possibly active cancellation: got=%+v err=%v", got, err)
			}
			var reservation domain.QuotaReservation
			if json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &reservation) != nil || reservation.Status != domain.ReservationHeld || len(f.expiry) != len(before) || f.slot[1] != string(run.ID) || f.counters[0] != int64(1) {
				t.Fatal("possibly active effect lost its original hold")
			}
			for i := range before {
				if f.expiry[i] != before[i] {
					t.Fatal("possibly active effect changed its expiry")
				}
			}
			if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || run.CancellationRequestedAt == nil || run.Status != status {
				t.Fatal("canonical cancellation intent missing")
			}
		})
	}
}

func TestLocalRunCancellationForeignSlotRollsBackReservationRelease(t *testing.T) {
	store, f, _, admission, request := localCancellationFixture(t)
	if got, err := store.AdmitDispatch(t.Context(), admission); err != nil || !got.Admitted {
		t.Fatalf("actual admission: %+v %v", got, err)
	}
	f.slot[1], f.slot[2] = "foreign-run", "foreign-reservation"
	beforeQuota := f.rows["quota_reservations"][0]
	beforeExpiry := append([]driver.Value(nil), f.expiry...)
	got, err := store.RequestManagedRunCancellationV1(t.Context(), request)
	if !errors.Is(err, domain.ErrMembershipDenied) || got != (ports.ManagedRunCancellationResultV1{}) || f.rows["quota_reservations"][0] != beforeQuota || f.slot[1] != "foreign-run" || f.slot[2] != "foreign-reservation" || f.counters[0] != int64(1) {
		t.Fatal("foreign slot failure committed speculative release/counter changes")
	}
	if len(f.expiry) != len(beforeExpiry) {
		t.Fatal("failed cancellation erased original expiry")
	}
	for i := range beforeExpiry {
		if f.expiry[i] != beforeExpiry[i] {
			t.Fatal("failed cancellation replaced original expiry")
		}
	}
}

func TestLocalRunCancellationFreshStoreReplayDoesNotReleaseTwice(t *testing.T) {
	store, f, original, admission, request := localCancellationFixture(t)
	if got, err := store.AdmitDispatch(t.Context(), admission); err != nil || !got.Admitted {
		t.Fatalf("actual admission: %+v %v", got, err)
	}
	_ = original.source.Close()
	// Lose every ACK within the existing three-callback logical call. The
	// first cancellation commits; subsequent callbacks only read its outcome.
	// No confirmed final transaction means the public call returns no candidate.
	f.commitModes = []string{"ambiguous", "ambiguous", "ambiguous"}
	first, firstErr := store.RequestManagedRunCancellationV1(t.Context(), request)
	if firstErr == nil || first != (ports.ManagedRunCancellationResultV1{}) {
		t.Fatalf("ambiguous commit leaked candidate: got=%+v err=%v", first, firstErr)
	}
	fresh, err := New(store.db, Options{OperationalRetention: fixtureRetention})
	if err != nil {
		t.Fatal(err)
	}
	writes := f.writes
	got, err := fresh.RequestManagedRunCancellationV1(t.Context(), request)
	if err != nil || !got.Replayed || got.Status != domain.RunCancelled || f.writes != writes || f.counters[0] != int64(0) || len(f.expiry) != 0 {
		t.Fatalf("fresh Store lost-ACK replay: got=%+v writes=%d/%d err=%v", got, f.writes, writes, err)
	}
}

func TestLocalRunCancellationRequiresCurrentCanonicalPrincipal(t *testing.T) {
	for _, fault := range []string{"membership revoked", "participant removed", "session deleting"} {
		t.Run(fault, func(t *testing.T) {
			store, f, _, admission, request := localCancellationFixture(t)
			if got, err := store.AdmitDispatch(t.Context(), admission); err != nil || !got.Admitted {
				t.Fatalf("actual admission: %+v %v", got, err)
			}
			switch fault {
			case "membership revoked":
				var m domain.TenantMembership
				if err := json.Unmarshal([]byte(f.membership[0].(string)), &m); err != nil {
					t.Fatal(err)
				}
				m.Status, m.SecurityVersion = domain.TenantMembershipRevoked, m.SecurityVersion+1
				raw, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				f.membership[0], f.membership[2], f.membership[3] = string(raw), string(m.Status), int64(m.SecurityVersion)
			case "participant removed":
				var p domain.SessionParticipant
				if err := json.Unmarshal([]byte(f.participant[0].(string)), &p); err != nil {
					t.Fatal(err)
				}
				p.Status = domain.SessionParticipantRemoved
				raw, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				f.participant[0], f.participant[2] = string(raw), string(p.Status)
			case "session deleting":
				f.participant[3] = int64(1)
			}
			beforeQuota, beforeWrites := f.rows["quota_reservations"][0], f.writes
			got, err := store.RequestManagedRunCancellationV1(t.Context(), request)
			if !errors.Is(err, domain.ErrMembershipDenied) || got != (ports.ManagedRunCancellationResultV1{}) ||
				f.writes != beforeWrites || f.rows["quota_reservations"][0] != beforeQuota || f.counters[0] != int64(1) || len(f.expiry) == 0 {
				t.Fatalf("withdrawn canonical principal changed retained capacity: got=%+v err=%v writes=%d/%d", got, err, f.writes, beforeWrites)
			}
		})
	}
}

func TestLocalRunCancellationInvalidContextDoesNotStartTransaction(t *testing.T) {
	store, f, _, _, request := localCancellationFixture(t)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, ctx := range []context.Context{nil, cancelled} {
		before := len(f.statements)
		got, err := store.RequestManagedRunCancellationV1(ctx, request)
		if !errors.Is(err, domain.ErrMembershipDenied) || got != (ports.ManagedRunCancellationResultV1{}) || len(f.statements) != before {
			t.Fatalf("invalid context reached transaction: got=%+v err=%v statements=%d/%d", got, err, len(f.statements), before)
		}
	}
}
