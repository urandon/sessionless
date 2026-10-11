package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// The legacy reader uses its original SQL spelling, not the bounded owned
// executor's whitelist. Only these five canonical worker-state point reads are
// supplied; any downstream principal/resource/lease query fails the test.
type legacyProfileQueryFixture struct{ *ownedFinalScript }

var legacyProfileWorkerReads = map[string]string{
	"SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2":                         "runs",
	"SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2":                      "worker_jobs",
	"SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2":                 "attempts",
	"SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2": "quota_reservations",
	"SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2": "artifact_manifests",
}

func (f *legacyProfileQueryFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }

func (f *legacyProfileQueryFixture) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.recordQuery(statement, args)
	table, ok := legacyProfileWorkerReads[strings.Join(strings.Fields(statement), " ")]
	if !ok {
		return nil, errors.New("unexpected downstream SQL after legacy profile gate")
	}
	return &explanationFixtureRows{values: append([]driver.Value(nil), f.rows[table]...)}, nil
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
	db := sql.OpenDB(&legacyProfileQueryFixture{script})
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
		t.Errorf("owned create-path SQL reads=%d want exactly five canonical worker-state reads; statements=%q", len(statements), statements)
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
