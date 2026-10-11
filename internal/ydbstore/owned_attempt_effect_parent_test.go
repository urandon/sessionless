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
)

type ownedAttemptEffectParentFixture struct {
	store    *Store
	base     *ownedFinalScript
	graph    *managedAdmissionGraphFixture
	request  ports.ReserveAttemptEffectRequestV1
	source   domain.AttemptExecutionSourceV1
	parent   domain.OwnedAttemptEffectParentV1
	verifier *ownedAttemptEffectSourceVerifierFixture
	issuer   *ownedAttemptEffectGrantFixture
	sql      *ownedAttemptEffectParentSQLFixture
	at       time.Time
}

type ownedAttemptEffectSourceVerifierFixture struct {
	expected domain.AttemptExecutionSourceV1
	calls    int
	reject   bool
}

func (f *ownedAttemptEffectSourceVerifierFixture) VerifyOwnedAttemptEffectSource(source domain.AttemptExecutionSourceV1) error {
	f.calls++
	if f.reject || !reflect.DeepEqual(source, f.expected) {
		return errors.New("test source verifier denied synthetic fixture source")
	}
	return nil
}

type ownedAttemptEffectGrantFixture struct {
	issuer           *serverlessharness.CapabilityIssuer
	sql              *ownedAttemptEffectParentSQLFixture
	calls            int
	premature        int
	activeSeen       int
	fail             bool
	verifyCalls      int
	rejectVerifyCall int
}

func (f *ownedAttemptEffectGrantFixture) VerifyGrant(grant ports.AttemptEffectOwnershipGrantV1) error {
	f.verifyCalls++
	if f.rejectVerifyCall != 0 && f.verifyCalls == f.rejectVerifyCall {
		return errors.New("test preparation verifier rejected grant")
	}
	return f.issuer.VerifyGrant(grant)
}

func (f *ownedAttemptEffectGrantFixture) MintAttemptEffectOwnershipGrant(a domain.ServerlessInvocationAuthorityV1, r domain.AttemptEffectReservationV1, expires time.Time) (ports.AttemptEffectOwnershipGrantV1, error) {
	f.calls++
	if f.sql.activeTransactions != 0 || f.sql.graph.commits == 0 {
		f.premature++
	}
	f.activeSeen = max(f.activeSeen, f.sql.activeTransactions)
	if f.fail {
		return ports.AttemptEffectOwnershipGrantV1{}, errors.New("test ownership grant issuer failed")
	}
	return f.issuer.MintAttemptEffectOwnershipGrant(a, r, expires)
}

func (f *ownedAttemptEffectGrantFixture) MintAttemptEffectObservationGrant(a domain.ServerlessInvocationAuthorityV1, r domain.AttemptEffectReservationV1, at, expires time.Time) (ports.AttemptEffectObservationGrantV1, error) {
	f.calls++
	if f.sql.activeTransactions != 0 || f.sql.graph.commits == 0 {
		f.premature++
	}
	f.activeSeen = max(f.activeSeen, f.sql.activeTransactions)
	return f.issuer.MintAttemptEffectObservationGrant(a, r, at, expires)
}

// This adapter extends the real ownedFinalScript/managed graph transaction
// path with only the bounded effect-parent and current lease point reads.
// Its row state is test-owned and does not model native YDB isolation.
type ownedAttemptEffectParentSQLFixture struct {
	base               *ownedFinalScript
	graph              *managedAdmissionGraphFixture
	parentRow          []driver.Value
	pendingParentRow   []driver.Value
	burnRow            []driver.Value
	pendingBurnRow     []driver.Value
	leaseRow           []driver.Value
	activeTransactions int
	inserts            int
	burnReads          int
	burnUpdates        int
	parentReads        int
	leaseReads         int
	insertError        error
}

type ownedAttemptEffectParentConnector struct {
	fixture *ownedAttemptEffectParentSQLFixture
}
type ownedAttemptEffectParentDriver struct {
	fixture *ownedAttemptEffectParentSQLFixture
}
type ownedAttemptEffectParentConn struct {
	fixture *ownedAttemptEffectParentSQLFixture
}
type ownedAttemptEffectParentTx struct {
	fixture *ownedAttemptEffectParentSQLFixture
	inner   driver.Tx
}

func (c ownedAttemptEffectParentConnector) Connect(context.Context) (driver.Conn, error) {
	return ownedAttemptEffectParentConn{fixture: c.fixture}, nil
}
func (c ownedAttemptEffectParentConnector) Driver() driver.Driver {
	return ownedAttemptEffectParentDriver{fixture: c.fixture}
}
func (d ownedAttemptEffectParentDriver) Open(string) (driver.Conn, error) {
	return ownedAttemptEffectParentConn{fixture: d.fixture}, nil
}
func (c ownedAttemptEffectParentConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("owned attempt-effect fixture forbids prepared statements")
}
func (ownedAttemptEffectParentConn) Close() error { return nil }
func (ownedAttemptEffectParentConn) Begin() (driver.Tx, error) {
	return nil, errors.New("owned attempt-effect fixture requires serializable BeginTx")
}
func (c ownedAttemptEffectParentConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := c.fixture.base.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.fixture.activeTransactions++
	return ownedAttemptEffectParentTx{fixture: c.fixture, inner: inner}, nil
}

func (c ownedAttemptEffectParentConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	switch statement {
	case ownedAttemptEffectParentSQL:
		c.fixture.parentReads++
		c.fixture.base.recordQuery(statement, args)
		return &explanationFixtureRows{values: append([]driver.Value(nil), c.fixture.parentRow...)}, nil
	case ownedAttemptPreparationBurnSQL:
		c.fixture.burnReads++
		c.fixture.base.recordQuery(statement, args)
		return &explanationFixtureRows{values: append(append([]driver.Value(nil), c.fixture.burnRow...), uint64(0))}, nil
	case ownedAttemptEffectLeaseSQL:
		c.fixture.leaseReads++
		c.fixture.base.recordQuery(statement, args)
		return &explanationFixtureRows{values: append([]driver.Value(nil), c.fixture.leaseRow...)}, nil
	default:
		return c.fixture.base.QueryContext(ctx, statement, args)
	}
}

func (c ownedAttemptEffectParentConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement == ownedAttemptPreparationBurnUpdateSQL {
		if len(args) != 5 || c.fixture.parentRow == nil || args[1].Value != c.fixture.parentRow[14] {
			return nil, errors.New("owned preparation burn UPDATE did not bind the exact persisted parent")
		}
		result, err := c.fixture.base.ExecContext(ctx, statement, args)
		if err != nil {
			return nil, err
		}
		c.fixture.burnUpdates++
		c.fixture.pendingBurnRow = []driver.Value{uint64(1), uint64(1), args[0].Value, args[1].Value}
		return result, nil
	}
	if strings.Contains(statement, "INSERT INTO attempt_effect_reservations") {
		if c.fixture.insertError != nil {
			return nil, c.fixture.insertError
		}
		if len(args) != 12 {
			return nil, errors.New("owned attempt-effect INSERT expected twelve args")
		}
		result, err := c.fixture.base.ExecContext(ctx, statement, args)
		if err != nil {
			return nil, err
		}
		c.fixture.inserts++
		c.fixture.pendingParentRow = []driver.Value{
			args[0].Value, args[1].Value, args[2].Value, args[3].Value, args[4].Value,
			args[5].Value, args[6].Value, args[7].Value, args[8].Value, args[9].Value,
			uint64(0), uint64(1), uint64(1), args[10].Value, args[11].Value,
		}
		return result, nil
	}
	return c.fixture.base.ExecContext(ctx, statement, args)
}

func (tx ownedAttemptEffectParentTx) Commit() error {
	mode := ""
	if len(tx.fixture.graph.commitModes) != 0 {
		mode = tx.fixture.graph.commitModes[0]
	}
	err := tx.inner.Commit()
	if mode != "rollback" && tx.fixture.pendingParentRow != nil {
		tx.fixture.parentRow = append([]driver.Value(nil), tx.fixture.pendingParentRow...)
	}
	if mode != "rollback" && tx.fixture.pendingBurnRow != nil {
		tx.fixture.burnRow = append([]driver.Value(nil), tx.fixture.pendingBurnRow...)
	}
	tx.fixture.pendingParentRow = nil
	tx.fixture.pendingBurnRow = nil
	tx.fixture.activeTransactions--
	return err
}

func (tx ownedAttemptEffectParentTx) Rollback() error {
	tx.fixture.pendingParentRow = nil
	tx.fixture.pendingBurnRow = nil
	tx.fixture.activeTransactions--
	return tx.inner.Rollback()
}

func newOwnedAttemptEffectParentFixture(t *testing.T) *ownedAttemptEffectParentFixture {
	t.Helper()
	store, base, _, source := newOwnedFinalScript(t)
	graph := base.managedAdmissionGraphFixture
	at := graph.at.UTC()
	state := currentWorkerAuthorityFixture(t)
	if state.Run.ID != source.Scope.RunID || state.Attempt.ID != source.Scope.AttemptID || state.Run.SessionID != source.Scope.SessionID || state.Job.CredentialOwnerUserID != source.Scope.OwnerUserID {
		t.Fatalf("owned fixture scope differs from canonical worker rows: state=%+v source=%+v", state.Run, source.Scope)
	}
	state.Run.Status, state.Run.UpdatedAt = domain.RunRunning, at
	state.Attempt.Status, state.Attempt.WorkerID, state.Attempt.UpdatedAt = domain.AttemptRunning, "worker-test", at
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(source.Scope, base.reader.profile, base.reader.spending, at)
	if err != nil {
		t.Fatalf("build owned execution package for worker row: %v", err)
	}
	state.Job.ExecutionPlacementV2, state.Job.HarnessBinding = pkg.Placement, pkg.Binding.Clone()
	substrate, cost := pkg.Substrate, pkg.Cost.Clone()
	state.Job.SubstrateBinding, state.Job.AdmissionCostCeiling = &substrate, &cost
	ownedSource, err := domain.EncodeAttemptExecutionSourceV1(source)
	if err != nil {
		t.Fatal(err)
	}
	state.Job.OwnedExecutionSourceRecord, state.Job.CreatedAt = string(ownedSource), at
	state.Reservation.Status, state.Reservation.HeldAt, state.Reservation.UpdatedAt = domain.ReservationHeld, at, at
	state.Reservation.ExpiresAt = at.Add(time.Hour)
	state.InputManifest.CreatedAt = at
	if err := validateLoadedWorkerJob(state); err != nil {
		t.Fatalf("complete owned running worker-state fixture: %v", err)
	}
	for table, value := range map[string]any{
		"runs": state.Run, "attempts": state.Attempt, "worker_jobs": state.Job,
		"quota_reservations": state.Reservation, "artifact_manifests": state.InputManifest,
	} {
		ratedFixturePut(t, graph.ratedSQLFixture, table, value)
	}
	final, err := domain.BuildAttemptExecutionAdmissionV1(source, state.Reservation.ID, at)
	if err != nil {
		t.Fatalf("build exact persisted Final for owned effect fixture: %v", err)
	}
	finalRaw, err := domain.EncodeAttemptExecutionAdmissionV1(final)
	if err != nil {
		t.Fatal(err)
	}
	finalDigest, err := final.Digest()
	if err != nil {
		t.Fatal(err)
	}
	base.final = []driver.Value{string(finalRaw), finalDigest, string(source.Scope.RunID), string(source.Scope.SessionID), string(source.Scope.OwnerUserID), string(state.Reservation.ID), at}
	request := ports.ReserveAttemptEffectRequestV1{
		TenantID: state.Run.TenantID, RunID: state.Run.ID, AttemptID: state.Attempt.ID,
		LeaseID: "owned-effect-lease", FenceToken: 1, PhysicalInvocationClaimID: "owned-effect-claim",
	}
	lease := domain.Lease{ID: request.LeaseID, TenantID: state.Run.TenantID, RunID: state.Run.ID, AttemptID: state.Attempt.ID, WorkerID: state.Attempt.WorkerID, FenceToken: request.FenceToken, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)}
	leaseRaw, err := marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	leaseValues := []driver.Value{string(lease.TenantID), string(lease.RunID), string(lease.ID), string(lease.AttemptID), lease.WorkerID, lease.FenceToken, lease.ExpiresAt, at,
		string(lease.TenantID), string(lease.ID), string(lease.RunID), string(lease.AttemptID), lease.WorkerID, lease.FenceToken, lease.AcquiredAt, lease.ExpiresAt, leaseRaw}
	sqlFixture := &ownedAttemptEffectParentSQLFixture{base: base, graph: graph, leaseRow: leaseValues,
		burnRow: []driver.Value{uint64(0), uint64(0), time.Unix(0, 0).UTC(), ""}}
	contextDigest, inputDigest, err := domain.ServerlessWorkerJobDigestsV1(state.Job, state.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	deadline := lease.AcquiredAt.Add(state.Job.SubstrateBinding.Limits.InvocationTimeout)
	for _, limit := range []time.Time{lease.ExpiresAt, state.Reservation.ExpiresAt, source.ExpiresAt,
		lease.AcquiredAt.Add(state.Job.HarnessBinding.AgentLoopPolicy.Limits.MaxActiveDuration), lease.AcquiredAt.Add(state.Job.Limits.MaxRuntime)} {
		if limit.Before(deadline) {
			deadline = limit
		}
	}
	authority := domain.ServerlessInvocationAuthorityV1{Version: domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: state.Job.HarnessBinding.Clone(), ExecutionPlacementV2: state.Job.ExecutionPlacementV2,
		SubstrateBinding: *state.Job.SubstrateBinding, AdmissionCostCeiling: state.Job.AdmissionCostCeiling.Clone(),
		Lease: lease, ContextManifestDigest: contextDigest, InputManifestDigest: inputDigest, InvocationDeadline: deadline.UTC()}
	parent, err := domain.BuildOwnedAttemptEffectParentV1(final, authority, request.PhysicalInvocationClaimID, request.UpstreamIdempotencyKeyDigest, at)
	if err != nil {
		t.Fatalf("build exact original owned effect parent for persisted-row controls: %v", err)
	}
	db := sql.OpenDB(ownedAttemptEffectParentConnector{fixture: sqlFixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	verifier := &ownedAttemptEffectSourceVerifierFixture{expected: source}
	probe := &ownedAttemptEffectGrantFixture{sql: sqlFixture}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return graph.at.UTC() }, strings.NewReader(strings.Repeat("o", 32)))
	if err != nil {
		t.Fatal(err)
	}
	probe.issuer = issuer
	store, err = New(db, Options{AttemptEffectGrantIssuer: probe, OwnedAttemptEffectSourceVerifier: verifier, OwnedPreparationGrantVerifier: probe})
	if err != nil {
		t.Fatal(err)
	}
	return &ownedAttemptEffectParentFixture{store: store, base: base, graph: graph, request: request, source: source, parent: parent, verifier: verifier, issuer: probe, sql: sqlFixture, at: at}
}

func (f *ownedAttemptEffectParentFixture) reserve(t *testing.T, request ports.ReserveAttemptEffectRequestV1) ports.ReserveAttemptEffectResultV1 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, err := f.store.reserveBoundedOwnedAttemptEffect(ctx, request)
	if err != nil {
		t.Fatalf("reserve owned attempt effect: %v", err)
	}
	return result
}

func (f *ownedAttemptEffectParentFixture) reserveResult(t *testing.T, request ports.ReserveAttemptEffectRequestV1) (ports.ReserveAttemptEffectResultV1, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return f.store.reserveBoundedOwnedAttemptEffect(ctx, request)
}

func (f *ownedAttemptEffectParentFixture) deny(t *testing.T, request ports.ReserveAttemptEffectRequestV1) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, err := f.store.reserveBoundedOwnedAttemptEffect(ctx, request)
	if err == nil || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
		t.Fatalf("owned attempt-effect denial leaked result: result=%+v err=%v", result, err)
	}
}

func (f *ownedAttemptEffectParentFixture) setCurrentAuthority(t *testing.T, state ports.WorkerJobState) {
	t.Helper()
	for table, value := range map[string]any{
		"runs": state.Run, "attempts": state.Attempt, "worker_jobs": state.Job,
		"quota_reservations": state.Reservation, "artifact_manifests": state.InputManifest,
	} {
		ratedFixturePut(t, f.graph.ratedSQLFixture, table, value)
	}
}

func (f *ownedAttemptEffectParentFixture) renewLease(t *testing.T, expiresAt, updatedAt time.Time) {
	t.Helper()
	var lease domain.Lease
	if err := decodeCurrentExecutionRecord(f.sql.leaseRow[16].(string), &lease, map[string]bool{
		"id": true, "tenant_id": true, "run_id": true, "attempt_id": true, "worker_id": true,
		"fence_token": true, "acquired_at": true, "expires_at": true,
	}); err != nil {
		t.Fatalf("decode coherent lease renewal fixture: %v", err)
	}
	lease.ExpiresAt = expiresAt
	raw, err := marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	f.sql.leaseRow[6], f.sql.leaseRow[7] = expiresAt, updatedAt
	f.sql.leaseRow[15], f.sql.leaseRow[16] = expiresAt, raw
}

func TestBoundedOwnedAttemptEffectPersistsReplaysAndReconcilesOriginalClaim(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Status != ports.AttemptEffectOwnedV1 || created.Grant == nil || created.ObservationGrant != nil || created.Reservation.PhysicalInvocationClaimID != f.request.PhysicalInvocationClaimID {
		t.Fatalf("initial owned attempt-effect result=%+v", created)
	}
	if f.sql.parentRow == nil || f.sql.inserts != 1 || f.issuer.calls != 1 || f.issuer.premature != 0 || f.sql.activeTransactions != 0 {
		t.Fatalf("post-commit boundary parent=%t inserts=%d grants=%d premature=%d active=%d", f.sql.parentRow != nil, f.sql.inserts, f.issuer.calls, f.issuer.premature, f.sql.activeTransactions)
	}
	parentRaw := f.sql.parentRow[13].(string)
	storedDigest := f.sql.parentRow[14].(string)
	storedRow := append([]driver.Value(nil), f.sql.parentRow...)
	stored, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(parentRaw))
	if err != nil || storedDigest == "" || !stored.Reservation.ReservedAt.Equal(f.at) || stored.PreparationExpiresAt.After(f.at.Add(stored.Authority.AdmissionCostCeiling.MaxPreEffectDurationPerDelivery)) {
		t.Fatalf("persisted exact original parent=%+v digest=%q err=%v", stored, storedDigest, err)
	}
	f.graph.at = f.at.Add(time.Second)
	replayed := f.reserve(t, f.request)
	if replayed.Status != ports.AttemptEffectReplayedV1 || replayed.Grant == nil || replayed.ObservationGrant != nil || replayed.Grant.GrantExpiresAt != stored.PreparationExpiresAt {
		t.Fatalf("same-claim original-window replay=%+v want replay with original preparation deadline %s", replayed, stored.PreparationExpiresAt)
	}
	if f.sql.inserts != 1 || f.issuer.calls != 2 || f.issuer.premature != 0 {
		t.Errorf("same-claim replay rewrote or speculatively minted: inserts=%d grants=%d premature=%d", f.sql.inserts, f.issuer.calls, f.issuer.premature)
	}
	f.graph.at = stored.PreparationExpiresAt.Add(time.Second)
	expired := f.reserve(t, f.request)
	if expired.Status != ports.AttemptEffectReconcileOnlyV1 || expired.Grant != nil || expired.ObservationGrant == nil || !reflect.DeepEqual(expired.Reservation, stored.Reservation) {
		t.Fatalf("same-claim after original preparation deadline must be observation-only: result=%+v", expired)
	}
	if f.sql.inserts != 1 || !reflect.DeepEqual(f.sql.parentRow, storedRow) || f.issuer.calls != 3 || f.issuer.premature != 0 {
		t.Fatalf("expired-preparation observation rewrote original parent or minted early: inserts=%d rowPreserved=%t grants=%d premature=%d", f.sql.inserts, reflect.DeepEqual(f.sql.parentRow, storedRow), f.issuer.calls, f.issuer.premature)
	}
	loser := f.request
	loser.PhysicalInvocationClaimID = "different-physical-claim"
	observed := f.reserve(t, loser)
	if observed.Status != ports.AttemptEffectReconcileOnlyV1 || observed.Grant != nil || observed.ObservationGrant == nil {
		t.Fatalf("competing-claim historical observation=%+v", observed)
	}
	if f.sql.inserts != 1 || !reflect.DeepEqual(f.sql.parentRow, storedRow) || f.issuer.calls != 4 || f.issuer.premature != 0 {
		t.Errorf("competing-claim path rewrote or speculatively minted: inserts=%d grants=%d premature=%d", f.sql.inserts, f.issuer.calls, f.issuer.premature)
	}
}

func TestBoundedOwnedAttemptEffectDeniesSameClaimReplayAfterCurrentRevocation(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil || f.sql.parentRow == nil {
		t.Fatalf("create owned parent before revocation: %+v", created)
	}
	storedRow := append([]driver.Value(nil), f.sql.parentRow...)
	storedInserts, grants := f.sql.inserts, f.issuer.calls
	f.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
	f.base.reader.profile.RevocationGeneration++
	f.base.reader.sync(t)
	f.graph.at = f.at.Add(time.Second)
	f.deny(t, f.request)
	if f.sql.inserts != storedInserts || !reflect.DeepEqual(f.sql.parentRow, storedRow) || f.issuer.calls != grants || f.sql.activeTransactions != 0 {
		t.Fatalf("revoked-current replay changed durable parent or leaked a grant: inserts=%d/%d parentPreserved=%t grants=%d/%d active=%d", f.sql.inserts, storedInserts, reflect.DeepEqual(f.sql.parentRow, storedRow), f.issuer.calls, grants, f.sql.activeTransactions)
	}
}

func TestBoundedOwnedAttemptEffectDeniesFutureRunStartOnColdAndReplay(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "cold"
		if replay {
			name = "same_claim_replay"
		}
		t.Run(name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			if replay {
				created := f.reserve(t, f.request)
				if created.Status != ports.AttemptEffectOwnedV1 || created.Grant == nil || f.sql.parentRow == nil {
					t.Fatalf("create owned parent before replay time mutation: %+v", created)
				}
			}
			storedParent := append([]driver.Value(nil), f.sql.parentRow...)
			insertsBefore, grantsBefore := f.sql.inserts, f.issuer.calls
			run := ratedFixtureRecord[domain.Run](t, f.graph.ratedSQLFixture, "runs")
			future := f.graph.at.Add(time.Microsecond)
			run.StartedAt = &future
			if err := run.Validate(); err != nil {
				t.Fatalf("future StartedAt should remain a structurally valid Run: %v", err)
			}
			if run.UpdatedAt.After(f.graph.at) {
				t.Fatalf("fixture updated_at must not explain the time denial: updated=%s clock=%s", run.UpdatedAt, f.graph.at)
			}
			ratedFixturePut(t, f.graph.ratedSQLFixture, "runs", run)
			f.deny(t, f.request)
			if f.sql.inserts != insertsBefore || f.issuer.calls != grantsBefore || f.sql.activeTransactions != 0 || !reflect.DeepEqual(f.sql.parentRow, storedParent) {
				t.Fatalf("future StartedAt leaked durable/grant state: replay=%t inserts=%d/%d grants=%d/%d active=%d parentPreserved=%t", replay, f.sql.inserts, insertsBefore, f.issuer.calls, grantsBefore, f.sql.activeTransactions, reflect.DeepEqual(f.sql.parentRow, storedParent))
			}
		})
	}
}

func TestBoundedOwnedAttemptEffectLeaseRenewalPreservesOriginalAuthorityAndDeadline(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Status != ports.AttemptEffectOwnedV1 || created.Grant == nil || f.sql.parentRow == nil {
		t.Fatalf("create owned parent before lease renewal: %+v", created)
	}
	storedRow := append([]driver.Value(nil), f.sql.parentRow...)
	stored, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(storedRow[13].(string)))
	if err != nil {
		t.Fatal(err)
	}
	f.graph.at = f.at.Add(time.Second)
	f.renewLease(t, f.at.Add(2*time.Hour), f.graph.at)
	replayed := f.reserve(t, f.request)
	var gotAuthorityDigest domain.ServerlessInvocationAuthorityDigestV1
	var gotAuthorityErr error
	if replayed.Grant != nil {
		gotAuthorityDigest, gotAuthorityErr = replayed.Grant.Authority.Digest()
	}
	wantAuthorityDigest, wantAuthorityErr := stored.Authority.Digest()
	if replayed.Status != ports.AttemptEffectReplayedV1 || replayed.Grant == nil || replayed.ObservationGrant != nil || !replayed.Grant.GrantExpiresAt.Equal(stored.PreparationExpiresAt) || gotAuthorityErr != nil || wantAuthorityErr != nil || gotAuthorityDigest != wantAuthorityDigest {
		t.Fatalf("coherently renewed lease changed original authority/preparation deadline: result=%+v originalDeadline=%s", replayed, stored.PreparationExpiresAt)
	}
	if f.sql.inserts != 1 || !reflect.DeepEqual(f.sql.parentRow, storedRow) || f.issuer.calls != 2 || f.issuer.premature != 0 {
		t.Fatalf("lease renewal replay rewrote parent or minted speculatively: inserts=%d preserved=%t grants=%d premature=%d", f.sql.inserts, reflect.DeepEqual(f.sql.parentRow, storedRow), f.issuer.calls, f.issuer.premature)
	}
}

func TestBoundedOwnedAttemptEffectDeniesCoherentLeaseShorteningOnReplay(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil || f.sql.parentRow == nil {
		t.Fatalf("create owned parent before lease shortening: %+v", created)
	}
	storedRow := append([]driver.Value(nil), f.sql.parentRow...)
	storedInserts, grants := f.sql.inserts, f.issuer.calls
	f.graph.at = f.at.Add(time.Second)
	f.renewLease(t, f.at.Add(30*time.Minute), f.graph.at)
	f.deny(t, f.request)
	if f.sql.inserts != storedInserts || !reflect.DeepEqual(f.sql.parentRow, storedRow) || f.issuer.calls != grants || f.sql.activeTransactions != 0 {
		t.Fatalf("shortened current lease changed parent or leaked a grant: inserts=%d/%d parentPreserved=%t grants=%d/%d active=%d", f.sql.inserts, storedInserts, reflect.DeepEqual(f.sql.parentRow, storedRow), f.issuer.calls, grants, f.sql.activeTransactions)
	}
}

func TestBoundedOwnedAttemptEffectDeniesMissingAuthorityAndCurrentRights(t *testing.T) {
	for _, name := range []string{"nil_verifier_before_sql", "missing_final", "verifier_denial", "current_rights", "revoked_current_source", "expired_held_quota", "changed_lease_worker", "malformed_parent_scalar", "legacy_owned_collision", "dual_record_union"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			request := f.request
			switch name {
			case "nil_verifier_before_sql":
				f.store.ownedAttemptEffectSourceVerifier = nil
			case "missing_final":
				f.base.final = nil
			case "verifier_denial":
				f.verifier.reject = true
			case "current_rights":
				var participant domain.SessionParticipant
				if err := json.Unmarshal([]byte(f.base.participant[0].(string)), &participant); err != nil {
					t.Fatalf("decode participant fixture: %v", err)
				}
				participant.Status, participant.UpdatedAt = domain.SessionParticipantRemoved, f.at
				raw, err := json.Marshal(participant)
				if err != nil {
					t.Fatal(err)
				}
				f.base.participant[0], f.base.participant[2] = string(raw), string(participant.Status)
			case "revoked_current_source":
				f.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
				f.base.reader.profile.RevocationGeneration++
				f.base.reader.sync(t)
			case "expired_held_quota":
				state := currentWorkerAuthorityFixture(t)
				state.Run.Status, state.Attempt.Status, state.Attempt.WorkerID = domain.RunRunning, domain.AttemptRunning, "worker-test"
				state.Reservation.Status, state.Reservation.HeldAt, state.Reservation.UpdatedAt = domain.ReservationHeld, f.at, f.at
				state.Reservation.ExpiresAt = f.at.Add(time.Second)
				state.Job.OwnedExecutionSourceRecord = string(mustEncodedAttemptSource(t, f.source))
				state.Job.ExecutionPlacementV2 = f.parent.Authority.ExecutionPlacementV2
				state.Job.HarnessBinding = f.parent.Authority.HarnessBinding.Clone()
				substrate, cost := f.parent.Authority.SubstrateBinding, f.parent.Authority.AdmissionCostCeiling.Clone()
				state.Job.SubstrateBinding, state.Job.AdmissionCostCeiling = &substrate, &cost
				state.Job.CreatedAt = f.at
				state.InputManifest.CreatedAt = f.at
				f.setCurrentAuthority(t, state)
				f.graph.at = f.at.Add(2 * time.Second)
			case "changed_lease_worker":
				f.sql.leaseRow[4] = "worker-other"
			case "malformed_parent_scalar":
				f.sql.parentRow = parentRowFromDomain(t, f.parent)
				f.sql.parentRow[0] = "tenant-other"
			case "legacy_owned_collision":
				f.sql.parentRow = parentRowFromDomain(t, f.parent)
				f.sql.parentRow[10], f.sql.parentRow[11], f.sql.parentRow[12] = uint64(1), uint64(1), uint64(1)
			case "dual_record_union":
				f.sql.parentRow = parentRowFromDomain(t, f.parent)
				f.sql.parentRow[10], f.sql.parentRow[11], f.sql.parentRow[12] = uint64(0), uint64(1), uint64(0)
				request.PhysicalInvocationClaimID = "different-claim"
			}
			callsBefore := f.issuer.calls
			writesBefore := f.graph.writes
			f.deny(t, request)
			if f.issuer.calls != callsBefore || f.graph.writes != writesBefore || f.sql.inserts != 0 || f.sql.activeTransactions != 0 {
				t.Errorf("denied case=%s grants=%d/%d writes=%d/%d inserts=%d active=%d", name, f.issuer.calls, callsBefore, f.graph.writes, writesBefore, f.sql.inserts, f.sql.activeTransactions)
			}
			if name == "nil_verifier_before_sql" && (len(f.graph.statements) != 0 || f.sql.parentReads != 0) {
				t.Errorf("nil verifier should deny before SQL: statements=%d parentReads=%d", len(f.graph.statements), f.sql.parentReads)
			}
		})
	}
}

func TestBoundedOwnedAttemptEffectRejectsMalformedPersistedParentUnions(t *testing.T) {
	for _, name := range []string{"legacy_only", "missing_owned_digest", "legacy_and_owned_collision", "wrong_attempt_scalar"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			f.sql.parentRow = parentRowFromDomain(t, f.parent)
			request := f.request
			request.PhysicalInvocationClaimID = "different-claim"
			switch name {
			case "legacy_only":
				f.sql.parentRow[10], f.sql.parentRow[11], f.sql.parentRow[12] = uint64(1), uint64(0), uint64(0)
			case "missing_owned_digest":
				f.sql.parentRow[10], f.sql.parentRow[11], f.sql.parentRow[12] = uint64(0), uint64(1), uint64(0)
			case "legacy_and_owned_collision":
				f.sql.parentRow[10], f.sql.parentRow[11], f.sql.parentRow[12] = uint64(1), uint64(1), uint64(1)
			case "wrong_attempt_scalar":
				f.sql.parentRow[1] = "attempt-other"
			}
			f.deny(t, request)
			if f.issuer.calls != 0 || f.graph.writes != 0 || f.sql.inserts != 0 {
				t.Errorf("malformed persisted union minted/wrote: grants=%d writes=%d inserts=%d", f.issuer.calls, f.graph.writes, f.sql.inserts)
			}
		})
	}
}

func TestBoundedOwnedAttemptEffectRollbackAndLostAcknowledgementDoNotLeakSpeculativeGrants(t *testing.T) {
	for _, mode := range []string{"rollback_then_commit", "committed_lost_ack"} {
		t.Run(mode, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			if mode == "rollback_then_commit" {
				f.graph.commitModes = []string{"rollback", ""}
			} else {
				f.graph.commitModes = []string{"ambiguous", ""}
			}
			result := f.reserve(t, f.request)
			wantStatus := ports.AttemptEffectOwnedV1
			if mode == "committed_lost_ack" {
				wantStatus = ports.AttemptEffectReplayedV1
			}
			if result.Status != wantStatus {
				t.Fatalf("retry-resolved result=%+v", result)
			}
			if result.Grant == nil || result.ObservationGrant != nil || f.sql.activeTransactions != 0 || f.issuer.calls != 1 || f.issuer.premature != 0 {
				t.Fatalf("retry speculative-grant fence result=%+v active=%d grants=%d premature=%d", result, f.sql.activeTransactions, f.issuer.calls, f.issuer.premature)
			}
			wantInserts := 1
			if mode == "rollback_then_commit" {
				wantInserts = 2 // First callback attempted an INSERT that its rollback discarded.
			}
			if f.graph.commits != 2 || f.sql.parentRow == nil || f.sql.inserts != wantInserts {
				t.Fatalf("retry transaction state commits=%d parent=%t inserts=%d want=%d", f.graph.commits, f.sql.parentRow != nil, f.sql.inserts, wantInserts)
			}
			stored, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(f.sql.parentRow[13].(string)))
			if err != nil || !reflect.DeepEqual(result.Reservation, stored.Reservation) || !result.Grant.GrantExpiresAt.Equal(stored.PreparationExpiresAt) {
				t.Fatalf("retry result changed original parent/deadline: reservation=%+v stored=%+v expiry=%s original=%s err=%v", result.Reservation, stored.Reservation, result.Grant.GrantExpiresAt, stored.PreparationExpiresAt, err)
			}
		})
	}
}

func TestBoundedOwnedAttemptEffectUnresolvedRetriesReturnZeroWithoutGrants(t *testing.T) {
	for _, test := range []struct {
		name    string
		mode    string
		inserts int
		parent  bool
	}{
		{name: "three_rollbacks", mode: "rollback", inserts: 3},
		{name: "three_ambiguous_commits", mode: "ambiguous", inserts: 1, parent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			f.graph.commitModes = []string{test.mode, test.mode, test.mode}
			result, err := f.reserveResult(t, f.request)
			if err == nil || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
				t.Fatalf("unresolved transaction result must be fully zero: result=%+v err=%v", result, err)
			}
			if f.graph.commits != 3 || f.sql.inserts != test.inserts || (f.sql.parentRow != nil) != test.parent || f.sql.activeTransactions != 0 || f.issuer.calls != 0 || f.issuer.premature != 0 {
				t.Fatalf("unresolved transaction leaked grants/state: commits=%d inserts=%d parent=%t active=%d grants=%d premature=%d", f.graph.commits, f.sql.inserts, f.sql.parentRow != nil, f.sql.activeTransactions, f.issuer.calls, f.issuer.premature)
			}
		})
	}
}

func TestBoundedOwnedAttemptEffectPostCommitIssuerFailureReturnsZero(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	f.issuer.fail = true
	result, err := f.reserveResult(t, f.request)
	if err == nil || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
		t.Fatalf("issuer failure must return a zero result after durable commit: result=%+v err=%v", result, err)
	}
	if f.graph.commits != 1 || f.sql.inserts != 1 || f.sql.parentRow == nil || f.sql.activeTransactions != 0 || f.issuer.calls != 1 || f.issuer.premature != 0 {
		t.Fatalf("issuer failure crossed wrong boundary: commits=%d inserts=%d parent=%t active=%d grants=%d premature=%d", f.graph.commits, f.sql.inserts, f.sql.parentRow != nil, f.sql.activeTransactions, f.issuer.calls, f.issuer.premature)
	}
}

func TestBoundedOwnedAttemptEffectRejectsNonProducerWorkerJSONShapes(t *testing.T) {
	for _, name := range []string{"nested_duplicate", "nested_unknown", "nested_case_fold", "nested_omitted", "nested_null"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			raw := f.graph.rows["worker_jobs"][0].(string)
			mutated, ok := mutateOwnedWorkerJobNestedJSON(raw, name)
			if !ok || mutated == raw {
				t.Fatalf("could not construct owned worker JSON mutation %q", name)
			}
			f.graph.rows["worker_jobs"] = []driver.Value{mutated}
			f.deny(t, f.request)
			if f.issuer.calls != 0 || f.graph.writes != 0 || f.sql.inserts != 0 {
				t.Errorf("nonproducer worker JSON minted/wrote: grants=%d writes=%d inserts=%d", f.issuer.calls, f.graph.writes, f.sql.inserts)
			}
		})
	}
}

func TestBoundedOwnedAttemptEffectAcceptsReorderedCanonicalWorkerJSON(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	raw := f.graph.rows["worker_jobs"][0].(string)
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	reordered, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(reordered) == raw {
		t.Fatal("test fixture did not reorder canonical worker JSON")
	}
	f.graph.rows["worker_jobs"] = []driver.Value{string(reordered)}
	result := f.reserve(t, f.request)
	if result.Status != ports.AttemptEffectOwnedV1 || result.Grant == nil || f.sql.inserts != 1 || f.issuer.calls != 1 {
		t.Fatalf("reordered producer-shaped worker JSON result=%+v inserts=%d grants=%d", result, f.sql.inserts, f.issuer.calls)
	}
}

func parentRowFromDomain(t *testing.T, parent domain.OwnedAttemptEffectParentV1) []driver.Value {
	t.Helper()
	raw, err := domain.EncodeOwnedAttemptEffectParentV1(parent)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := parent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	authorityDigest, err := parent.Authority.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := parent.Reservation
	return []driver.Value{string(r.TenantID), string(r.AttemptID), string(r.Kind), string(r.RunID), string(r.LeaseID), r.FenceToken,
		r.PhysicalInvocationClaimID, r.EffectSequence, string(authorityDigest), r.ReservedAt, uint64(0), uint64(1), uint64(1), string(raw), digest}
}

func mustEncodedAttemptSource(t *testing.T, source domain.AttemptExecutionSourceV1) []byte {
	t.Helper()
	raw, err := domain.EncodeAttemptExecutionSourceV1(source)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mutateOwnedWorkerJobNestedJSON(raw, mutation string) (string, bool) {
	const objectKey = `"harness_binding"` + `:`
	keyStart := strings.Index(raw, objectKey)
	if keyStart < 0 {
		return raw, false
	}
	objectStart := strings.Index(raw[keyStart+len(objectKey):], "{") + keyStart + len(objectKey)
	if objectStart < keyStart+len(objectKey) {
		return raw, false
	}
	objectEnd := jsonObjectEnd(raw, objectStart)
	if objectEnd < 0 {
		return raw, false
	}
	object := raw[objectStart : objectEnd+1]
	var changed string
	switch mutation {
	case "nested_duplicate":
		changed = duplicateJSONMember(object, "tenant_id")
	case "nested_unknown":
		changed = object[:len(object)-1] + `,"test_unknown_nested_field":true}`
	case "nested_case_fold":
		changed = strings.Replace(object, `"tenant_id"`, `"Tenant_Id"`, 1)
	case "nested_omitted":
		changed = removeJSONMember(object, "tenant_id")
	case "nested_null":
		changed = replaceJSONMemberObjectWithNull(object, "backend")
	default:
		return raw, false
	}
	if changed == object {
		return raw, false
	}
	return raw[:objectStart] + changed + raw[objectEnd+1:], true
}

func jsonObjectEnd(raw string, start int) int {
	depth := 0
	inString, escaped := false, false
	for i := start; i < len(raw); i++ {
		c := raw[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		if c == '"' {
			inString = true
			continue
		}
		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func duplicateJSONMember(object, key string) string {
	marker := `"` + key + `":`
	start := strings.Index(object, marker)
	if start < 0 {
		return object
	}
	valueStart := start + len(marker)
	valueEnd := strings.IndexByte(object[valueStart:], ',')
	if valueEnd < 0 {
		valueEnd = strings.IndexByte(object[valueStart:], '}')
	}
	if valueEnd < 0 {
		return object
	}
	valueEnd += valueStart
	member := object[start:valueEnd]
	return object[:valueEnd] + "," + member + object[valueEnd:]
}

func removeJSONMember(object, key string) string {
	marker := `"` + key + `":`
	start := strings.Index(object, marker)
	if start < 0 {
		return object
	}
	valueStart := start + len(marker)
	valueEnd := strings.IndexByte(object[valueStart:], ',')
	if valueEnd < 0 {
		valueEnd = strings.IndexByte(object[valueStart:], '}')
	}
	if valueEnd < 0 {
		return object
	}
	valueEnd += valueStart
	if start > 0 && object[start-1] == ',' {
		return object[:start-1] + object[valueEnd:]
	}
	if object[valueEnd] == ',' {
		return object[:start] + object[valueEnd+1:]
	}
	return object[:start] + object[valueEnd+1:]
}

func replaceJSONMemberObjectWithNull(object, key string) string {
	marker := `"` + key + `":`
	start := strings.Index(object, marker)
	if start < 0 {
		return object
	}
	valueStart := start + len(marker)
	for valueStart < len(object) && (object[valueStart] == ' ' || object[valueStart] == '\n' || object[valueStart] == '\t') {
		valueStart++
	}
	if valueStart >= len(object) || object[valueStart] != '{' {
		return object
	}
	valueEnd := jsonObjectEnd(object, valueStart)
	if valueEnd < 0 {
		return object
	}
	return object[:valueStart] + "null" + object[valueEnd+1:]
}
