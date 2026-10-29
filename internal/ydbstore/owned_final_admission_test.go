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
)

// This is a scripted composition test of the shared callback, not proof of
// source approval, native YDB isolation, or serving enablement. Current-source
// rows below are structurally valid synthetic controls only.
type ownedFinalScript struct {
	*managedAdmissionGraphFixture
	reader                           *ownedReaderFixture
	seedJob                          domain.WorkerJob
	seedReservation                  domain.QuotaReservation
	final, index                     []driver.Value
	pendingFinal, pendingIndex       []driver.Value
	session, membership, participant []driver.Value
}

func (f *ownedFinalScript) Connect(ctx context.Context) (driver.Conn, error) { return f, nil }
func (f *ownedFinalScript) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := f.managedAdmissionGraphFixture.BeginTx(ctx, options)
	f.pendingFinal, f.pendingIndex = nil, nil
	return ownedFinalScriptTx{f: f, inner: inner}, err
}

type ownedFinalScriptTx struct {
	f     *ownedFinalScript
	inner driver.Tx
}

func (tx ownedFinalScriptTx) Commit() error {
	mode := ""
	if len(tx.f.commitModes) > 0 {
		mode = tx.f.commitModes[0]
	}
	err := tx.inner.Commit()
	if mode != "rollback" {
		if tx.f.pendingFinal != nil {
			tx.f.final = append([]driver.Value(nil), tx.f.pendingFinal...)
		}
		if tx.f.pendingIndex != nil {
			tx.f.index = append([]driver.Value(nil), tx.f.pendingIndex...)
		}
	}
	tx.f.pendingFinal, tx.f.pendingIndex = nil, nil
	return err
}
func (tx ownedFinalScriptTx) Rollback() error {
	tx.f.pendingFinal, tx.f.pendingIndex = nil, nil
	return tx.inner.Rollback()
}

func (f *ownedFinalScript) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	flat := func(values ...driver.Value) driver.Rows { return &explanationFixtureRows{values: values} }
	switch statement {
	case ownedProfileCurrentSQL:
		// The operation-price path point-reads the SAME head as the joined
		// admission snapshot. Reuse its original exact-key reader, not a new
		// fabricated profile or a permissive unknown-query fallback.
		f.recordQuery(statement, args)
		return f.reader.driver.QueryContext(ctx, statement, args)
	case ownedAdmissionSourcesJoinSQL:
		f.recordQuery(statement, args)
		values := append([]driver.Value(nil), f.reader.driver.profileRow...)
		for i := range f.reader.driver.spendingRows {
			values = append(values, f.reader.driver.spendingRows[i]...)
		}
		return flat(values...), nil
	case ownedAdmissionPrincipalSQL:
		f.recordQuery(statement, args)
		return flat(append(append(append([]driver.Value{}, f.session...), f.membership...), f.participant...)...), nil
	case ownedAdmissionSQL:
		f.recordQuery(statement, args)
		return flat(f.final...), nil
	case "SELECT run_id FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 AND attempt_id=$3":
		f.recordQuery(statement, args)
		return flat(f.index...), nil
	default:
		return f.managedAdmissionGraphFixture.QueryContext(ctx, statement, args)
	}
}
func (f *ownedFinalScript) recordQuery(statement string, args []driver.NamedValue) {
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
}
func (f *ownedFinalScript) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(statement), "INSERT INTO worker_jobs") {
		result, err := f.managedAdmissionGraphFixture.ExecContext(ctx, statement, args)
		if err == nil && len(args) == 7 {
			f.rows["worker_jobs"] = []driver.Value{args[6].Value}
		}
		return result, err
	}
	if strings.Contains(statement, "INSERT INTO attempt_execution_admissions_v1") {
		result, err := f.managedAdmissionGraphFixture.ExecContext(ctx, statement, args)
		if err == nil {
			f.pendingFinal = []driver.Value{args[7].Value, args[8].Value, args[2].Value, args[3].Value, args[4].Value, args[5].Value, args[6].Value}
		}
		return result, err
	}
	if strings.Contains(statement, "INSERT INTO attempt_execution_admissions_by_session_v1") {
		result, err := f.managedAdmissionGraphFixture.ExecContext(ctx, statement, args)
		if err == nil {
			f.pendingIndex = []driver.Value{args[3].Value}
		}
		return result, err
	}
	return f.managedAdmissionGraphFixture.ExecContext(ctx, statement, args)
}

// Optional setup precedes every profile/source/admission seal. Existing
// scripted tests retain their original clock and profile without this option.
type ownedFinalPrebuild struct {
	at      time.Time
	profile func(*domain.OwnedAgentProfileCurrentV1)
	sources *ownedFinalSources
	state   *ports.WorkerJobState // canonical DATA declared before any source/admission seal
}

// Already sealed inputs must not pass through the synthetic rebinding path.
type ownedFinalSources struct {
	profile  domain.OwnedAgentProfileCurrentV1
	spending [4]domain.AttemptSpendingAuthorizationCurrentV1
}

func newOwnedFinalScript(t *testing.T, prebuild ...ownedFinalPrebuild) (*Store, *ownedFinalScript, ports.DispatchAdmissionRequest, domain.AttemptExecutionSourceV1) {
	t.Helper()
	if len(prebuild) > 1 {
		t.Fatal("owned final fixture accepts at most one prebuild option")
	}
	store, graph, request := managedGraphFixture(t)
	if len(prebuild) == 1 && prebuild[0].state != nil {
		s := *prebuild[0].state
		var outbox domain.DispatchOutbox
		if json.Unmarshal([]byte(graph.rows["dispatch_outbox"][0].(string)), &outbox) != nil {
			t.Fatal("pre-intake dispatch template")
		}
		run, attempt := s.Run.Clone(), s.Attempt
		run.Status, attempt.Status, attempt.WorkerID = domain.RunCreated, domain.AttemptCreated, ""
		if run.LocalCapacityAssociation != nil {
			outbox.LocalCapacityAssociation = run.Clone().LocalCapacityAssociation
			outbox.ContextWindow = s.Job.Clone().ContextWindow
			outbox.Origin = s.Job.Clone().Origin
			outbox.DeliveryChat, outbox.ReplyToMessageID = s.Job.DeliveryChat, s.Job.ReplyToMessageID
		}
		outbox.ID, outbox.TenantID, outbox.RunID, outbox.AttemptID = domain.DispatchOutboxID("dispatch-"+string(run.ID)), run.TenantID, run.ID, attempt.ID
		outbox.InputManifestID, outbox.ContextSnapshot, outbox.CredentialOwnerUserID = s.Job.InputManifestID, s.Job.ContextSnapshot, s.Job.CredentialOwnerUserID
		outbox.CreatedAt, outbox.UpdatedAt = run.CreatedAt, run.CreatedAt
		for table, value := range map[string]any{"runs": run, "attempts": attempt, "dispatch_outbox": outbox} {
			ratedFixturePut(t, graph.ratedSQLFixture, table, value)
		}
		graph.rows["run_idempotency"] = []driver.Value{string(run.ID)}
		request.TenantID, request.RunID, request.AttemptID, request.OutboxID, request.ReservationID = run.TenantID, run.ID, attempt.ID, outbox.ID, s.Reservation.ID
	}
	if len(prebuild) == 1 && !prebuild[0].at.IsZero() {
		graph.at = prebuild[0].at.UTC().Truncate(time.Microsecond)
		request.Now, request.HoldUntil = graph.at, graph.at.Add(time.Hour)
	}
	r := newOwnedReaderFixtureAt(t, graph.at.UTC(), "-owned-final-script")
	supplied := len(prebuild) == 1 && prebuild[0].sources != nil
	if supplied {
		if prebuild[0].profile != nil {
			t.Fatal("sealed sources cannot use a profile mutation callback")
		}
		r.profile = prebuild[0].sources.profile.Clone()
		for i, v := range prebuild[0].sources.spending {
			r.spending[i] = v.Clone()
		}
	}
	var run domain.Run
	if err := json.Unmarshal([]byte(graph.rows["runs"][0].(string)), &run); err != nil {
		t.Fatal(err)
	}
	var attempt domain.Attempt
	if err := json.Unmarshal([]byte(graph.rows["attempts"][0].(string)), &attempt); err != nil {
		t.Fatal(err)
	}
	var outbox domain.DispatchOutbox
	if err := json.Unmarshal([]byte(graph.rows["dispatch_outbox"][0].(string)), &outbox); err != nil {
		t.Fatal(err)
	}
	// Rebind only the synthetic fixture identifiers; this is not a publication
	// receipt or evidence that any installation/funding source is trusted.
	r.scope.tenant, r.scope.owner, r.scope.run, r.scope.session, r.scope.attempt = run.TenantID, outbox.CredentialOwnerUserID, run.ID, run.SessionID, attempt.ID
	if supplied {
		if r.profile.Locator.TenantID != r.scope.tenant || r.profile.Locator.OwnerUserID != r.scope.owner {
			t.Fatal("sealed profile must already match canonical graph scope")
		}
	} else {
		r.profile.Locator.TenantID, r.profile.Locator.OwnerUserID = run.TenantID, outbox.CredentialOwnerUserID
		r.profile.Resource.OwnerUserID = outbox.CredentialOwnerUserID
		r.profile.CostCeiling.MCPToolCost.TenantID, r.profile.CostCeiling.MCPToolCost.OwnerUserID = run.TenantID, outbox.CredentialOwnerUserID
		r.profile.CostCeiling.WebSearchCost.TenantID, r.profile.CostCeiling.WebSearchCost.OwnerUserID = run.TenantID, outbox.CredentialOwnerUserID
		sealedCostDigest, err := r.profile.CostCeiling.Digest()
		if err != nil {
			t.Fatal(err)
		}
		r.profile.Substrate.AdmissionCostCeilingDigest = sealedCostDigest
		if len(prebuild) == 1 && prebuild[0].profile != nil {
			prebuild[0].profile(&r.profile)
		}
	}
	digest, err := r.profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r.ref = domain.OwnedAgentProfileReferenceV1{Locator: r.profile.Locator, RevocationGeneration: r.profile.RevocationGeneration, Digest: digest}
	for i := range r.spending {
		v := &r.spending[i]
		if supplied {
			if v.Locator.TenantID != run.TenantID || v.Locator.AttemptID != attempt.ID || v.RunID != run.ID || v.SessionID != run.SessionID || v.OwnerUserID != outbox.CredentialOwnerUserID || v.SourceProfile != r.ref {
				t.Fatal("sealed spending must already match canonical graph/profile scope")
			}
		} else {
			v.Locator.TenantID, v.Locator.AttemptID = run.TenantID, attempt.ID
			v.RunID, v.SessionID, v.OwnerUserID = run.ID, run.SessionID, outbox.CredentialOwnerUserID
			v.SourceProfile = r.ref
			bindReaderSpendingSource(t, v)
		}
		r.refs[i] = ownedSpendingReference{locator: v.Locator, generation: v.RevocationGeneration}
		r.refs[i].digest, err = v.Digest()
		if err != nil {
			t.Fatal(err)
		}
	}
	r.sync(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: run.TenantID, SessionID: run.SessionID, OwnerUserID: outbox.CredentialOwnerUserID, RunID: run.ID, AttemptID: attempt.ID}
	if err := r.profile.ValidateAt(graph.at); err != nil {
		t.Fatalf("synthetic profile shape after identifier rebinding: %v", err)
	}
	for i := range r.spending {
		if err := r.spending[i].ValidateAgainstProfile(r.profile, graph.at); err != nil {
			t.Fatalf("synthetic component %d after identifier rebinding: %v", i, err)
		}
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, r.profile, r.spending, graph.at)
	if err != nil {
		t.Fatalf("synthetic current fixture package: %v", err)
	}
	source, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	outbox.ExecutionPlacementV2, outbox.HarnessBinding = pkg.Placement, pkg.Binding.Clone()
	substrate, cost := pkg.Substrate, pkg.Cost.Clone()
	outbox.SubstrateBinding, outbox.AdmissionCostCeiling = &substrate, &cost
	outbox.OwnedExecutionSourceRecord = string(source)
	if err := outbox.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("synthetic outbox: %v", err)
	}
	ratedFixturePut(t, graph.ratedSQLFixture, "dispatch_outbox", outbox)
	graph.rows["dispatch_outbox"] = graph.ratedSQLFixture.rows["dispatch_outbox"]
	graph.rows["quota_reservations"], graph.rows["worker_jobs"] = nil, nil
	// The exact principal row is independently scripted from canonical records.
	seed := currentWorkerAuthorityFixture(t)
	if len(prebuild) == 1 && prebuild[0].state != nil {
		seed = *prebuild[0].state
	}
	script := &ownedFinalScript{managedAdmissionGraphFixture: graph, reader: r, seedJob: seed.Job, seedReservation: seed.Reservation}
	sess := domain.Session{ID: run.SessionID, TenantID: run.TenantID, CreatedBy: outbox.CredentialOwnerUserID, Status: domain.SessionActive, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt}
	member := domain.TenantMembership{TenantID: run.TenantID, UserID: outbox.CredentialOwnerUserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt}
	part := domain.SessionParticipant{TenantID: run.TenantID, SessionID: run.SessionID, UserID: outbox.CredentialOwnerUserID, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt}
	for _, x := range []struct {
		value  any
		target *[]driver.Value
	}{{sess, &script.session}, {member, &script.membership}, {part, &script.participant}} {
		raw, e := json.Marshal(x.value)
		if e != nil {
			t.Fatal(e)
		}
		*x.target = append(*x.target, string(raw))
	}
	script.session = append(script.session, string(sess.CreatedBy), string(sess.Status), int64(sess.LastEventSequence), int64(0))
	script.membership = append(script.membership, string(member.Role), string(member.Status), int64(member.SecurityVersion))
	script.participant = append(script.participant, string(part.Role), string(part.Status), int64(0))
	db := sql.OpenDB(script)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	store.db = db
	request.Now = graph.at
	return store, script, request, pkg.Source
}

func TestScriptedOwnedAdmissionSharedCallbackColdAndReplay(t *testing.T) {
	store, f, request, source := newOwnedFinalScript(t)
	// A non-nil verifier is the existing private-path guard only. These
	// synthetic source rows do not assert verifier-issued publication.
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	if err != nil || !result.Admitted || result.Code != "admitted" {
		t.Fatalf("shared owned callback cold result=%+v err=%v", result, err)
	}
	if len(f.statements) > 32 {
		t.Fatalf("owned cold statements=%d >32", len(f.statements))
	}
	if f.final == nil || f.index == nil || f.final[6] != f.at || f.final[2] != string(source.Scope.RunID) || f.final[3] != string(source.Scope.SessionID) || f.final[4] != string(source.Scope.OwnerUserID) || f.final[5] != string(request.ReservationID) {
		t.Fatalf("final row does not bind original source/reservation/DB time (row present=%t index present=%t)", f.final != nil, f.index != nil)
	}
	if f.persistedWrites == 0 {
		t.Fatal("scripted callback did not commit canonical graph writes")
	}
	var admitted domain.AttemptExecutionAdmissionV1
	var job domain.WorkerJob
	var reservation domain.QuotaReservation
	var run domain.Run
	var outbox domain.DispatchOutbox
	_ = json.Unmarshal([]byte(f.final[0].(string)), &admitted)
	_ = json.Unmarshal([]byte(f.rows["worker_jobs"][0].(string)), &job)
	_ = json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &reservation)
	_ = json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run)
	_ = json.Unmarshal([]byte(f.rows["dispatch_outbox"][0].(string)), &outbox)
	if !ownedAdmissionMatches(&admitted, source, outbox.OwnedExecutionSourceRecord, reservation, job) {
		t.Fatalf("scripted committed final does not match persisted canonical job/reservation (job_valid=%t reservation_valid=%t held=%t IDs=%t times=%t source=%t)", job.ValidateForRun(run) == nil, reservation.ValidateForRun(run) == nil, reservation.Status == domain.ReservationHeld, reservation.ID == request.ReservationID && job.ReservationID == request.ReservationID, job.CreatedAt.Equal(admitted.AdmittedAt) && reservation.HeldAt.Equal(admitted.AdmittedAt), job.OwnedExecutionSourceRecord == outbox.OwnedExecutionSourceRecord && reflect.DeepEqual(admitted.Source, source))
	}
	writes := f.writes
	original := append([]driver.Value(nil), f.final...)
	index := append([]driver.Value(nil), f.index...)
	replay, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	if err != nil || !replay.Admitted || replay.Code != "already_admitted" || f.writes != writes || !reflect.DeepEqual(f.final, original) || !reflect.DeepEqual(f.index, index) {
		t.Fatalf("exact replay=%+v err=%v writes=%d/%d", replay, err, f.writes, writes)
	}
}

func TestScriptedOwnedAdmissionLostAckRecoversCommittedFinalWithoutWrites(t *testing.T) {
	store, f, request, _ := newOwnedFinalScript(t)
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	f.commitModes = []string{"ambiguous"}
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	if err != nil || !result.Admitted || result.Code != "already_admitted" || f.commits != 2 || f.final == nil || f.index == nil {
		t.Fatalf("committed lost-ACK recovery result=%+v err=%v commits=%d final=%t index=%t", result, err, f.commits, f.final != nil, f.index != nil)
	}
	writes := f.writes
	originalFinal, originalIndex := append([]driver.Value(nil), f.final...), append([]driver.Value(nil), f.index...)
	result, err = store.admitBoundedOwnedDispatch(t.Context(), request)
	if err != nil || !result.Admitted || result.Code != "already_admitted" || f.commits != 3 || f.writes != writes || !reflect.DeepEqual(f.final, originalFinal) || !reflect.DeepEqual(f.index, originalIndex) {
		t.Fatalf("committed lost-ACK exact replay result=%+v err=%v commits=%d writes=%d/%d", result, err, f.commits, f.writes, writes)
	}
	if f.writes != f.persistedWrites || f.persistedWrites == 0 {
		t.Fatalf("lost-ACK recovery write accounting differs: attempted=%d persisted=%d", f.writes, f.persistedWrites)
	}
}

func TestScriptedOwnedAdmissionExistingJobWithoutFinalDenies(t *testing.T) {
	store, f, request, _ := newOwnedFinalScript(t)
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	var run domain.Run
	var job domain.WorkerJob
	var reservation domain.QuotaReservation
	if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run); err != nil {
		t.Fatal(err)
	}
	job, reservation = f.seedJob, f.seedReservation
	var outbox domain.DispatchOutbox
	if err := json.Unmarshal([]byte(f.rows["dispatch_outbox"][0].(string)), &outbox); err != nil {
		t.Fatal(err)
	}
	job.ExecutionPlacementV2, job.HarnessBinding = outbox.ExecutionPlacementV2, outbox.HarnessBinding
	job.SubstrateBinding, job.AdmissionCostCeiling = outbox.SubstrateBinding, outbox.AdmissionCostCeiling
	job.OwnedExecutionSourceRecord = outbox.OwnedExecutionSourceRecord
	job.CreatedAt = f.at
	reservation.HeldAt, reservation.UpdatedAt, reservation.ExpiresAt = f.at, f.at, request.HoldUntil
	ratedFixturePut(t, f.ratedSQLFixture, "worker_jobs", job)
	ratedFixturePut(t, f.ratedSQLFixture, "quota_reservations", reservation)
	f.rows["worker_jobs"], f.rows["quota_reservations"] = f.ratedSQLFixture.rows["worker_jobs"], f.ratedSQLFixture.rows["quota_reservations"]
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
	if err == nil || !reflect.DeepEqual(result, want) || f.final != nil || f.persistedWrites != 0 {
		t.Fatalf("missing-final existing-job result=%+v err=%v final=%t persisted=%d attempted=%d", result, err, f.final != nil, f.persistedWrites, f.writes)
	}
}

func TestScriptedOwnedAdmissionOrphanFinalDenies(t *testing.T) {
	store, f, request, source := newOwnedFinalScript(t)
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	a, err := domain.BuildAttemptExecutionAdmissionV1(source, request.ReservationID, f.at)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := domain.EncodeAttemptExecutionAdmissionV1(a)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.final = []driver.Value{string(raw), digest, string(source.Scope.RunID), string(source.Scope.SessionID), string(source.Scope.OwnerUserID), string(request.ReservationID), f.at}
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
	if err == nil || !reflect.DeepEqual(result, want) || f.persistedWrites != 0 {
		t.Fatalf("orphan final result=%+v err=%v persisted=%d attempted=%d", result, err, f.persistedWrites, f.writes)
	}
}

func TestScriptedOwnedAdmissionRevokedCurrentDeniesReplay(t *testing.T) {
	store, f, request, _ := newOwnedFinalScript(t)
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	if result, err := store.admitBoundedOwnedDispatch(t.Context(), request); err != nil || !result.Admitted {
		t.Fatalf("initial synthetic admission=%+v err=%v", result, err)
	}
	writes := f.writes
	f.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
	f.reader.profile.RevocationGeneration++
	f.reader.sync(t)
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
	if err == nil || !reflect.DeepEqual(result, want) || f.writes != writes {
		t.Fatalf("revoked current replay result=%+v err=%v writes=%d/%d", result, err, f.writes, writes)
	}
}

func TestScriptedOwnedAdmissionRollbackRetryDiscardsFinalRows(t *testing.T) {
	store, f, request, _ := newOwnedFinalScript(t)
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	f.commitModes = []string{"rollback", ""}
	f.afterCommit = func() {
		f.afterCommit = nil
		var outbox domain.DispatchOutbox
		if err := json.Unmarshal([]byte(f.rows["dispatch_outbox"][0].(string)), &outbox); err != nil {
			t.Errorf("decode outbox in test retry hook: %v", err)
			return
		}
		outbox.Status = domain.DispatchCancelled
		ratedFixturePut(t, f.ratedSQLFixture, "dispatch_outbox", outbox)
		f.rows["dispatch_outbox"] = f.ratedSQLFixture.rows["dispatch_outbox"]
	}
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	if err != nil || result.Admitted || result.Code != "dispatch_not_pending" || f.commits != 2 || f.final != nil || f.index != nil || f.persistedWrites != 0 {
		t.Fatalf("rollback retry result=%+v err=%v commits=%d final=%v index=%v persisted=%d", result, err, f.commits, f.final, f.index, f.persistedWrites)
	}
}

func TestScriptedOwnedAdmissionLateErrorRollsBackFinalAndCanonicalWrites(t *testing.T) {
	store, f, request, _ := newOwnedFinalScript(t)
	store.ownedProfilePublicationVerifier = &fixtureProfileVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	store.attemptSpendingPublicationVerifier = &fixtureSpendingVerifier{key: []byte("test-only"), allowed: map[string]bool{}}
	f.rowError = errors.New("scripted bounded snapshot failure after writes")
	result, err := store.admitBoundedOwnedDispatch(t.Context(), request)
	want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
	if err == nil || !reflect.DeepEqual(result, want) || f.final != nil || f.index != nil || f.persistedWrites != 0 || f.writes == 0 {
		t.Fatalf("late error result=%+v err=%v final=%v index=%v persisted=%d attempted=%d", result, err, f.final, f.index, f.persistedWrites, f.writes)
	}
}
