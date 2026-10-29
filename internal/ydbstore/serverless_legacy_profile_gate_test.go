package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// The legacy reader uses its actual SQL spelling, including the common bounded
// Run point read. Only these six canonical worker-state point reads are
// supplied; any downstream principal/resource/lease query fails the test.
type legacyProfileQueryFixture struct {
	*ownedFinalScript
	job                 ports.WorkerJobState
	quotaRow, expiryRow []driver.Value
}

var legacyProfileWorkerReads = map[string]string{
	"SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM runs WHERE tenant_id = $1 AND run_id = $2": "runs",
	"SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2":                                 "worker_jobs",
	"SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2":                            "attempts",
	completionSQL(quotaReservationStorageSQLV1):                                                        "quota_reservations",
	completionSQL(quotaExpiryStorageSQLV1):                                                             "quota_expiry_v2",
	"SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2":            "artifact_manifests",
}

func (f *legacyProfileQueryFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }

func (f *legacyProfileQueryFixture) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.recordQuery(statement, args)
	values, matched, err := legacyProfileWorkerReadRow(statement, args, f.job, f.rows, f.quotaRow, f.expiryRow)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, errors.New("unexpected downstream SQL after legacy profile gate")
	}
	return &explanationFixtureRows{values: values}, nil
}

// The initial legacy physical row is declared before consumption. Subsequent
// writes retain their actual SQL postimage instead of regenerating scalars
// from a possibly changed JSON payload on each read.
func legacyProfileQuotaRow(t *testing.T, job ports.WorkerJobState, retained time.Time) []driver.Value {
	t.Helper()
	r := job.Reservation
	if r.LocalCapacityAssociation != nil || job.Run.LocalCapacityAssociation != nil || r.SubscriptionConnectionID == "" || r.ValidateForRun(job.Run) != nil || retained.Before(r.ExpiresAt) {
		t.Fatal("genuine retained legacy quota required")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return []driver.Value{string(raw), string(r.ID), string(r.TenantID), string(r.RunID), string(r.SubscriptionConnectionID), string(r.Status), int64(r.CapacityUnits), r.HeldAt, r.ExpiresAt, retained}
}

// Closed SQL and complete point-key inventory shared by the two legacy
// consumers. Absence belongs only to the exact optional legacy expiry key;
// a present row still exposes both nullable local columns to production.
type legacyProfileExpiryPoint struct{ key, row []driver.Value }

func legacyProfileWorkerReadRow(statement string, args []driver.NamedValue, job ports.WorkerJobState, rows map[string][]driver.Value, quota, expiry []driver.Value, candidate ...legacyProfileExpiryPoint) ([]driver.Value, bool, error) {
	table, matched := legacyProfileWorkerReads[completionSQL(statement)]
	if !matched {
		return nil, false, nil
	}
	want := []driver.Value{string(job.Run.TenantID), string(job.Run.ID)}
	selectedExpiry := expiry
	switch table {
	case "attempts":
		want[1] = string(job.Attempt.ID)
	case "artifact_manifests":
		want[1] = string(job.InputManifest.ID)
	case "quota_reservations":
		want[1] = string(job.Reservation.ID)
	case "quota_expiry_v2":
		bucket, err := ydbpartition.BucketV1(string(job.Reservation.ID))
		if err != nil || len(quota) != 10 {
			return nil, true, errors.New("legacy expiry requires retained ten-column quota")
		}
		expires, ok := quota[8].(time.Time)
		if !ok {
			return nil, true, errors.New("legacy quota physical expiry type")
		}
		want = []driver.Value{int64(bucket), expires, string(job.Run.TenantID), string(job.Reservation.ID)}
		if len(candidate) > 1 {
			return nil, true, errors.New("legacy fixture permits one declared candidate expiry point")
		}
		for _, point := range candidate {
			if len(point.key) != 4 || point.key[0] != int64(bucket) || point.key[2] != string(job.Run.TenantID) || point.key[3] != string(job.Reservation.ID) {
				return nil, true, errors.New("legacy candidate expiry must retain original physical scope")
			}
			at, ok := point.key[1].(time.Time)
			if !ok || at.IsZero() {
				return nil, true, errors.New("legacy candidate expiry timestamp")
			}
			if reflect.DeepEqual(localInstallationNamedValues(args), point.key) {
				want = point.key
				selectedExpiry = point.row
			}
		}
	}
	if !reflect.DeepEqual(localInstallationNamedValues(args), want) {
		return nil, true, errors.New("legacy worker read requires exact original point key")
	}
	value := rows[table]
	if table == "quota_reservations" {
		if len(quota) != 10 {
			return nil, true, errors.New("legacy quota projection must have ten columns")
		}
		value = quota
	}
	if table == "quota_expiry_v2" {
		if len(selectedExpiry) != 0 && len(selectedExpiry) != 8 {
			return nil, true, errors.New("legacy expiry projection must have eight columns")
		}
		value = selectedExpiry
	}
	value = append([]driver.Value(nil), value...)
	limits := map[int]int{}
	if table == "quota_reservations" {
		limits = map[int]int{0: 32769, 1: 161, 2: 161, 3: 161, 4: 161, 5: 17}
	}
	if table == "quota_expiry_v2" {
		limits = map[int]int{2: 161, 3: 161, 4: 161, 5: 161, 6: 32769, 7: 65}
	}
	for i, limit := range limits {
		if len(value) > i {
			if text, ok := value[i].(string); ok && len(text) > limit {
				value[i] = text[:limit]
			}
		}
	}
	return value, true, nil
}

func ownedEffectRecordForLegacyGate(t *testing.T, at time.Time) (attemptEffectRecordV1, *serverlessharness.CapabilityIssuer) {
	t.Helper()
	r := newOwnedReaderFixtureAt(t, at.UTC(), "-legacy-effect-gate")
	scope := domain.AttemptExecutionSourceScopeV1{
		TenantID: r.scope.tenant, SessionID: r.scope.session, OwnerUserID: r.scope.owner,
		RunID: r.scope.run, AttemptID: r.scope.attempt,
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, r.profile, r.spending, at)
	if err != nil {
		t.Fatalf("build structurally valid owned invocation package: %v", err)
	}
	deadline := at.Add(60 * time.Second)
	authority := domain.ServerlessInvocationAuthorityV1{
		Version:        domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: pkg.Binding.Clone(), ExecutionPlacementV2: pkg.Placement,
		SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost.Clone(),
		Lease: domain.Lease{
			ID: "legacy-gate-lease", TenantID: scope.TenantID, RunID: scope.RunID,
			AttemptID: scope.AttemptID, WorkerID: "legacy-gate-worker", FenceToken: 1,
			AcquiredAt: at, ExpiresAt: deadline,
		},
		ContextManifestDigest: strings.Repeat("b", 64), InputManifestDigest: strings.Repeat("c", 64),
		InvocationDeadline: deadline,
	}
	if err := authority.Validate(); err != nil {
		t.Fatalf("owned invocation authority must be structurally valid: %v", err)
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "original-claim", nil, at)
	if err != nil {
		t.Fatalf("build persisted owned attempt-effect reservation: %v", err)
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return at }, strings.NewReader(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	return attemptEffectRecordV1{Authority: authority, Reservation: reservation}, issuer
}

func TestReserveAttemptEffectRejectsPersistedOwnedProfileWithoutGrant(t *testing.T) {
	at := time.Date(2026, 10, 11, 3, 0, 0, 1000, time.UTC)
	record, issuer := ownedEffectRecordForLegacyGate(t, at)
	for _, claim := range []string{"original-claim", "competing-claim"} {
		t.Run(claim, func(t *testing.T) {
			f := &effectCommitFixture{record: record, issuer: issuer}
			db := sql.OpenDB(f)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			store, err := New(db, Options{AttemptEffectGrantIssuer: f})
			if err != nil {
				t.Fatal(err)
			}
			store.attachedWorkerNow = func(context.Context, rowQuery) (time.Time, error) { return at, nil }
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			reservation := record.Reservation
			request := ports.ReserveAttemptEffectRequestV1{
				TenantID: reservation.TenantID, RunID: reservation.RunID, AttemptID: reservation.AttemptID,
				LeaseID: reservation.LeaseID, FenceToken: reservation.FenceToken,
				PhysicalInvocationClaimID: claim,
			}
			result, err := store.ReserveAttemptEffect(ctx, request)
			if err == nil {
				t.Fatalf("claim=%q unexpectedly accepted persisted owned profile: result=%+v", claim, result)
			}
			if !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
				t.Errorf("claim=%q denial leaked a nonzero result: %+v", claim, result)
			}
			if f.grantCalls != 0 || f.premature != 0 {
				t.Errorf("claim=%q minted a grant on denial: grantCalls=%d premature=%d", claim, f.grantCalls, f.premature)
			}
			if f.commits != 0 || f.begins != 1 {
				t.Errorf("claim=%q transaction boundary begins=%d commits=%d want 1/0", claim, f.begins, f.commits)
			}
		})
	}
}

func TestCurrentServerlessInvocationAuthorityRejectsOwnedProfileBeforePrerequisiteReads(t *testing.T) {
	store, script, _, _ := newOwnedFinalScript(t)
	graph := script.managedAdmissionGraphFixture
	state := currentWorkerAuthorityFixture(t)
	state.Run.Status = domain.RunRunning
	state.Attempt.Status, state.Attempt.WorkerID = domain.AttemptRunning, "worker-test"

	scope := domain.AttemptExecutionSourceScopeV1{
		TenantID: script.reader.scope.tenant, SessionID: script.reader.scope.session,
		OwnerUserID: script.reader.scope.owner, RunID: script.reader.scope.run, AttemptID: script.reader.scope.attempt,
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, script.reader.profile, script.reader.spending, graph.at)
	if err != nil {
		t.Fatalf("build coherent owned execution package: %v", err)
	}
	state.Job.ExecutionPlacementV2 = pkg.Placement
	state.Job.HarnessBinding = pkg.Binding.Clone()
	substrate, cost := pkg.Substrate, pkg.Cost.Clone()
	state.Job.SubstrateBinding, state.Job.AdmissionCostCeiling = &substrate, &cost
	ownedSource, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatalf("encode canonical owned execution source: %v", err)
	}
	state.Job.OwnedExecutionSourceRecord = string(ownedSource)
	state.Job.CreatedAt = graph.at
	state.Run.CreatedAt, state.Run.UpdatedAt = graph.at, graph.at
	state.Attempt.CreatedAt, state.Attempt.UpdatedAt = graph.at, graph.at
	state.Reservation.HeldAt, state.Reservation.UpdatedAt = graph.at, graph.at
	state.InputManifest.CreatedAt = graph.at
	if err := state.Job.ValidateForRun(state.Run); err != nil {
		t.Fatalf("owned worker job shape: %v", err)
	}
	if err := validateLoadedWorkerJob(state); err != nil {
		t.Fatalf("complete owned worker-state fixture: %v", err)
	}
	for table, value := range map[string]any{
		"runs": state.Run, "attempts": state.Attempt, "worker_jobs": state.Job,
		"quota_reservations": state.Reservation, "artifact_manifests": state.InputManifest,
	} {
		ratedFixturePut(t, graph.ratedSQLFixture, table, value)
	}
	db := sql.OpenDB(&legacyProfileQueryFixture{ownedFinalScript: script, job: state, quotaRow: legacyProfileQuotaRow(t, state, state.Reservation.ExpiresAt.Add(store.operationalRetention))})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	statementStart := len(graph.statements)
	persistedWritesBefore, pendingWritesBefore := graph.persistedWrites, graph.pendingWrites

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	sqlTx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlTx.Rollback() })
	tx := &stateTx{store: store, sqlTx: sqlTx, tenantID: state.Run.TenantID}
	request := ports.ReserveAttemptEffectRequestV1{
		TenantID: state.Run.TenantID, RunID: state.Run.ID, AttemptID: state.Attempt.ID,
		LeaseID: "owned-profile-lease", FenceToken: 1, PhysicalInvocationClaimID: "owned-profile-claim",
	}
	authority, authorized, err := currentServerlessInvocationAuthorityTx(ctx, tx, request, graph.at)
	if err != nil || authorized || !reflect.DeepEqual(authority, domain.ServerlessInvocationAuthorityV1{}) {
		t.Fatalf("owned create-path authorized=%t zeroAuthority=%t err=%v want false/true/nil; SQL=%q", authorized, reflect.DeepEqual(authority, domain.ServerlessInvocationAuthorityV1{}), err, graph.statements[statementStart:])
	}
	statements := graph.statements[statementStart:]
	if len(statements) != len(legacyProfileWorkerReads) {
		t.Errorf("owned create-path SQL reads=%d want exactly six canonical worker-state reads; statements=%q", len(statements), statements)
	}
	for _, statement := range statements {
		if _, ok := legacyProfileWorkerReads[strings.Join(strings.Fields(statement), " ")]; !ok {
			t.Errorf("unexpected owned create-path SQL read before denial: %s", statement)
		}
		if strings.Contains(statement, "session_principal") || strings.Contains(statement, "provider_resource") || strings.Contains(statement, "lease_heads") || strings.Contains(statement, "leases") {
			t.Errorf("owned create-path performed prerequisite SQL after profile gate: %s", statement)
		}
	}
	if graph.persistedWrites != persistedWritesBefore || graph.pendingWrites != pendingWritesBefore {
		t.Errorf("owned create-path denial wrote state: persisted=%d pending=%d before=%d/%d", graph.persistedWrites, graph.pendingWrites, persistedWritesBefore, pendingWritesBefore)
	}
}

func TestLegacyProfileWorkerCapacityProjection(t *testing.T) {
	for _, fault := range []string{"legacy expiry absent", "legacy expiry present", "quota status", "quota run", "quota subscription", "quota expiry", "retention before expiry", "expiry run", "expiry subscription", "expiry association only", "expiry digest only", "local expiry collision"} {
		t.Run(fault, func(t *testing.T) {
			store, script, _, _ := newOwnedFinalScript(t)
			job := currentWorkerAuthorityFixture(t)
			quota := legacyProfileQuotaRow(t, job, job.Reservation.ExpiresAt.Add(time.Hour))
			bucket, err := ydbpartition.BucketV1(string(job.Reservation.ID))
			if err != nil {
				t.Fatal(err)
			}
			expiry := []driver.Value{int64(bucket), job.Reservation.ExpiresAt, string(job.Run.TenantID), string(job.Reservation.ID), string(job.Run.ID), string(job.Run.SubscriptionConnectionID), nil, nil}
			switch fault {
			case "legacy expiry absent":
				expiry = nil
			case "quota status":
				quota[5] = string(domain.ReservationCommitted)
			case "quota run":
				quota[3] = "foreign-run"
			case "quota subscription":
				quota[4] = "foreign-subscription"
			case "quota expiry":
				quota[8] = job.Reservation.ExpiresAt.Add(time.Second)
			case "retention before expiry":
				quota[9] = job.Reservation.ExpiresAt.Add(-time.Second)
			case "expiry run":
				expiry[4] = "foreign-run"
			case "expiry subscription":
				expiry[5] = "foreign-subscription"
			case "expiry association only":
				expiry[6] = ""
			case "expiry digest only":
				expiry[7] = ""
			case "local expiry collision":
				local := &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: job.Run.TenantID, OwnerUserID: job.Job.CredentialOwnerUserID, ResourceID: "local-resource"}
				physical, err := encodeCapacityStorageV1(job.Run.TenantID, "", local)
				if err != nil {
					t.Fatal(err)
				}
				expiry[5], expiry[6], expiry[7] = physical.key, physical.association.String, physical.digest.String
			}
			f := &legacyProfileQueryFixture{ownedFinalScript: script, job: job, quotaRow: quota, expiryRow: expiry}
			db := sql.OpenDB(f)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			store.db = db
			beforeQuota, beforeExpiry := append([]driver.Value(nil), quota...), append([]driver.Value(nil), expiry...)
			writes := script.persistedWrites
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			var got domain.QuotaReservation
			var found bool
			err = store.Transact(ctx, job.Run.TenantID, func(state ports.StateTx) error {
				var err error
				got, found, err = readWorkerQuotaReservationStorageV1(ctx, state.(*stateTx), job.Run, job.Reservation.ID)
				return err
			})
			valid := fault == "legacy expiry absent" || fault == "legacy expiry present"
			if valid {
				if err != nil || !found || !reflect.DeepEqual(got, job.Reservation) {
					t.Fatalf("retained legacy projection found=%t err=%v got=%+v", found, err, got)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || found || !reflect.DeepEqual(got, domain.QuotaReservation{}) {
				t.Fatalf("invalid retained capacity was accepted: found=%t err=%v", found, err)
			}
			if !reflect.DeepEqual(f.quotaRow, beforeQuota) || !reflect.DeepEqual(f.expiryRow, beforeExpiry) || script.persistedWrites != writes || script.pendingWrites != 0 {
				t.Fatal("capacity read repaired rows or wrote authority")
			}
		})
	}
}

func TestLegacyProfileWorkerReadInventoryIsExact(t *testing.T) {
	job := currentWorkerAuthorityFixture(t)
	quota := legacyProfileQuotaRow(t, job, job.Reservation.ExpiresAt.Add(time.Hour))
	for _, tc := range []struct {
		name, sql string
		args      []driver.Value
		matched   bool
	}{
		{"foreign quota key", quotaReservationStorageSQLV1, []driver.Value{string(job.Run.TenantID), "foreign-reservation"}, true},
		{"quota arity", quotaReservationStorageSQLV1, []driver.Value{string(job.Run.TenantID)}, true},
		{"expiry arity", quotaExpiryStorageSQLV1, []driver.Value{int64(0), job.Reservation.ExpiresAt, string(job.Run.TenantID)}, true},
		{"obsolete quota payload", `SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, []driver.Value{string(job.Run.TenantID), string(job.Reservation.ID)}, false},
		{"downstream lease", `SELECT payload FROM leases WHERE tenant_id=$1 AND lease_id=$2`, []driver.Value{string(job.Run.TenantID), "lease-foreign"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := make([]driver.NamedValue, len(tc.args))
			for i, value := range tc.args {
				args[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
			}
			row, matched, err := legacyProfileWorkerReadRow(tc.sql, args, job, nil, quota, nil)
			if row != nil || matched != tc.matched || matched && err == nil {
				t.Fatalf("closed inventory matched=%t err=%v row=%v", matched, err, row)
			}
		})
	}
}

func TestReserveAttemptEffectKeepsExpiredSingleTurnReconcileOnly(t *testing.T) {
	originalAt := time.Unix(20, 0).UTC()
	reconcileAt := originalAt.Add(2 * time.Hour)
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("tenant-legacy", "user-legacy", "run-legacy", "attempt-legacy", "subscription-legacy", originalAt)
	if err != nil {
		t.Fatal(err)
	}
	deadline := originalAt.Add(time.Minute)
	authority := domain.ServerlessInvocationAuthorityV1{
		Version:        domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: managed.HarnessBinding, ExecutionPlacementV2: managed.ExecutionPlacementV2,
		SubstrateBinding: managed.SubstrateBinding, AdmissionCostCeiling: managed.AdmissionCostCeiling,
		Lease: domain.Lease{
			ID: "legacy-reconcile-lease", TenantID: "tenant-legacy", RunID: "run-legacy", AttemptID: "attempt-legacy",
			WorkerID: "legacy-worker", FenceToken: 1, AcquiredAt: originalAt, ExpiresAt: deadline,
		},
		ContextManifestDigest: strings.Repeat("b", 64), InputManifestDigest: strings.Repeat("c", 64),
		InvocationDeadline: deadline,
	}
	if err := authority.Validate(); err != nil {
		t.Fatalf("legacy structural authority: %v", err)
	}
	if err := authority.ValidateAt(reconcileAt); err == nil {
		t.Fatal("control must represent structurally valid but expired authority")
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "original-claim", nil, originalAt)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return reconcileAt }, strings.NewReader(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	f := &effectCommitFixture{record: attemptEffectRecordV1{Authority: authority, Reservation: reservation}, issuer: issuer}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{AttemptEffectGrantIssuer: f})
	if err != nil {
		t.Fatal(err)
	}
	store.attachedWorkerNow = func(context.Context, rowQuery) (time.Time, error) { return reconcileAt, nil }
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, err := store.ReserveAttemptEffect(ctx, ports.ReserveAttemptEffectRequestV1{
		TenantID: reservation.TenantID, RunID: reservation.RunID, AttemptID: reservation.AttemptID,
		LeaseID: reservation.LeaseID, FenceToken: reservation.FenceToken, PhysicalInvocationClaimID: "competing-claim",
	})
	if err != nil || result.Status != ports.AttemptEffectReconcileOnlyV1 || result.Grant != nil || result.ObservationGrant == nil {
		t.Fatalf("expired single-turn historical reconciliation result=%+v err=%v", result, err)
	}
	if err := issuer.VerifyObservationGrant(*result.ObservationGrant); err != nil {
		t.Fatalf("historical observation grant must remain valid after execution deadline: %v", err)
	}
	if f.commits != 1 || f.grantCalls != 1 || f.premature != 0 {
		t.Errorf("historical reconcile boundary commits=%d grantCalls=%d premature=%d want 1/1/0", f.commits, f.grantCalls, f.premature)
	}
}
