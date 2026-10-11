package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Native storage/admission composition with EXPLICIT synthetic installed-source
// and payer verifiers. This exercises no owned driver, model/tool operation,
// allocation, proxy or serving enablement. Public owned admission stays closed.
type nativeOwnedFinalGraph struct {
	nativeManagedAdmissionGraph
	packageV1       domain.OwnedAgentExecutionPackageV1
	profileVerifier *fixtureProfileVerifier
	spendVerifier   *fixtureSpendingVerifier
	profileReceipt  ports.OwnedProfilePublicationReceiptV1
	spendReceipts   [4]ports.AttemptSpendingPublicationReceiptV1
}

func nativeOwnedFinalDBTime(t *testing.T, g nativeManagedAdmissionGraph) time.Time {
	t.Helper()
	var at time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil || at.IsZero() || at.Nanosecond()%1000 != 0 {
		t.Fatalf("native authoritative time=%v err=%v", at, err)
	}
	return at.UTC()
}

func newNativeOwnedFinalGraph(t *testing.T) nativeOwnedFinalGraph {
	t.Helper()
	return nativeOwnedFinalGraphFrom(t, newNativeManagedAdmissionGraph(t), false)
}

func nativeOwnedFinalGraphFrom(t *testing.T, g nativeManagedAdmissionGraph, preservePreviousHead bool) nativeOwnedFinalGraph {
	t.Helper()
	f, store := g.f, g.f.store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, table := range []string{"attempt_execution_admissions_by_session_v1", "attempt_execution_admissions_v1", "attempt_spending_publication_audit_v1", "owned_profile_publication_audit_v1", "attempt_spending_authorization_current_v1", "owned_agent_profile_current_v1", "tenant_memberships"} {
			if _, err := store.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", g.run.TenantID); err != nil {
				t.Errorf("cleanup exact test-owned %s: %v", table, err)
			}
		}
	})
	result := nativeOwnedFinalGraph{nativeManagedAdmissionGraph: g,
		profileVerifier: &fixtureProfileVerifier{key: []byte("native-final-fixture-profile-only"), allowed: make(map[string]bool)},
		spendVerifier:   &fixtureSpendingVerifier{key: []byte("native-final-fixture-payer-only"), allowed: make(map[string]bool)},
	}
	store.ownedProfilePublicationVerifier = result.profileVerifier
	store.attemptSpendingPublicationVerifier = result.spendVerifier
	// The process-local intent TTL is canonically capped at ten seconds. Sample
	// DB time immediately before EACH public writer; do not reuse a stale setup
	// timestamp or widen that cap. The actual source/evidence windows last an hour.
	at := nativeOwnedFinalDBTime(t, g)
	profileGrant := result.profileVerifier.install(t, ports.OwnedProfilePublicationGrantV1{Profile: f.profile.Clone(), IssuedAt: at, ExpiresAt: at.Add(10 * time.Second)})
	profileReceipt, err := store.PublishOwnedProfile(g.ctx, profileGrant)
	if err != nil || profileReceipt.Replayed {
		t.Fatalf("actual profile publication err=%v replay=%v", err, profileReceipt.Replayed)
	}
	result.profileReceipt = profileReceipt
	f.profile = profileReceipt.Profile.Clone()
	f.ref = domain.OwnedAgentProfileReferenceV1{Locator: f.profile.Locator, RevocationGeneration: f.profile.RevocationGeneration, Digest: profileReceipt.Digest}
	for i := range f.spending {
		value := f.spending[i].Clone()
		value.SourceProfile = f.ref
		value.Window.PublishedAt, value.UpdatedAt = f.profile.Window.PublishedAt, f.profile.Window.PublishedAt
		bindReaderSpendingSource(t, &value)
		at = nativeOwnedFinalDBTime(t, g)
		grant := result.spendVerifier.install(t, ports.AttemptSpendingPublicationGrantV1{Authorization: value, IssuedAt: at, ExpiresAt: at.Add(10 * time.Second)})
		receipt, err := store.PublishAttemptSpending(g.ctx, grant)
		if err != nil || receipt.Replayed {
			t.Fatalf("actual component publication=%s err=%v replay=%v", value.Locator.ComponentKind, err, receipt.Replayed)
		}
		result.spendReceipts[i] = receipt
		f.spending[i] = receipt.Authorization.Clone()
		f.refs[i] = ownedSpendingReference{locator: receipt.Authorization.Locator, generation: receipt.Authorization.RevocationGeneration, digest: receipt.Digest}
	}
	at = nativeOwnedFinalDBTime(t, g)
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(domain.AttemptExecutionSourceScopeV1{TenantID: g.run.TenantID, SessionID: g.run.SessionID, OwnerUserID: f.scope.owner, RunID: g.run.ID, AttemptID: g.attempt.ID}, f.profile, f.spending, at)
	if err != nil {
		t.Fatalf("build original package from actual published receipts: %v", err)
	}
	source, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	result.packageV1 = pkg
	result.outbox.ExecutionPlacementV2 = pkg.Placement
	result.outbox.HarnessBinding = pkg.Binding.Clone()
	substrate, cost := pkg.Substrate, pkg.Cost.Clone()
	result.outbox.SubstrateBinding, result.outbox.AdmissionCostCeiling = &substrate, &cost
	result.outbox.OwnedExecutionSourceRecord = string(source)
	result.outbox.UpdatedAt = at
	if err := result.outbox.ValidateForAttempt(result.run, result.attempt); err != nil {
		t.Fatalf("owned outbox original source validation: %v", err)
	}
	membership := domain.TenantMembership{TenantID: g.run.TenantID, UserID: f.scope.owner, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: g.run.CreatedAt, UpdatedAt: at}
	if err := store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		if err := putMembershipTx(g.ctx, state.(*stateTx).sqlTx, membership); err != nil {
			return err
		}
		if preservePreviousHead {
			// Test-only installation of the exact constructor's validated source
			// at the already seeded outbox PK. A historical earlier denied head
			// is legitimate on load; do NOT reverse-select it after the ordinary
			// PutDispatchOutbox writer has selected the current Attempt.
			raw, err := marshal(result.outbox)
			if err != nil {
				return err
			}
			_, err = state.(*stateTx).sqlTx.ExecContext(g.ctx, `UPDATE dispatch_outbox SET payload=CAST($1 AS JsonDocument),updated_at=$2 WHERE tenant_id=$3 AND dispatch_outbox_id=$4`, raw, result.outbox.UpdatedAt, result.outbox.TenantID, result.outbox.ID)
			return err
		}
		return state.PutDispatchOutbox(g.ctx, result.outbox)
	}); err != nil {
		t.Fatalf("canonical owned source outbox/membership seed: %v", err)
	}
	// Ordinary PutDispatchOutbox selects the current Attempt. The historical
	// variant retains its canonical prior denial. Neither erases the head to
	// claim an artificial colder authority graph.
	for _, table := range nativeManagedAdmissionEffectTables {
		want := uint64(0)
		if table == "run_explanation_heads_v1" {
			want = 1
		}
		result.requireCount(t, table, want)
	}
	result.requireCount(t, "attempt_execution_admissions_v1", 0)
	result.requireCount(t, "attempt_execution_admissions_by_session_v1", 0)
	result.requireCount(t, "owned_profile_publication_audit_v1", 1)
	result.requireCount(t, "attempt_spending_publication_audit_v1", 4)
	result.requireAdmissionPreconditions(t)
	return result
}

// Keep backend syntax/scanner diagnostics separate from production's deliberate
// content-free authorization denial. Both exact production SELECTs and their
// validators run in ONE read-only serializable snapshot; no record is printed.
func (g nativeOwnedFinalGraph) requireAdmissionPreconditions(t *testing.T) {
	t.Helper()
	var diagnostic error
	err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		diagnostic = nil // a retry must not retain speculative diagnostic state
		fail := func(err error) error {
			diagnostic = err
			return err
		}
		scope := g.packageV1.Source.Scope
		bucket, err := webBucket(string(scope.OwnerUserID))
		if err != nil {
			return fail(fmt.Errorf("principal bucket: %w", err))
		}
		var texts [9]string
		var numbers [4]uint64
		principal := []any{&texts[0], &texts[1], &texts[2], &numbers[0], &numbers[1], &texts[3], &texts[4], &texts[5], &numbers[2], &texts[6], &texts[7], &texts[8], &numbers[3]}
		if err := nativeOwnedRawPrecondition(ctx, q, "principal13", ownedAdmissionPrincipalSQL,
			[]any{scope.TenantID, scope.SessionID, scope.OwnerUserID, bucket}, principal); err != nil {
			return fail(err)
		}
		l := g.packageV1.Source.Profile.Locator
		args := []any{l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass}
		for _, spending := range g.packageV1.Source.Spending {
			l := spending.Locator
			args = append(args, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID)
		}
		var profileText [4]string
		var profileNumbers [4]uint64
		var profileTimes [3]time.Time
		destinations := []any{&profileText[0], &profileNumbers[0], &profileNumbers[1], &profileNumbers[2], &profileText[1], &profileText[2], &profileText[3], &profileNumbers[3], &profileTimes[0], &profileTimes[1], &profileTimes[2]}
		var spendTexts [4][6]string
		var spendNumbers [4][2]uint64
		var spendTimes [4][3]time.Time
		for i := range spendTexts {
			s, n, at := &spendTexts[i], &spendNumbers[i], &spendTimes[i]
			destinations = append(destinations, &s[0], &n[0], &n[1], &s[1], &s[2], &s[3], &s[4], &s[5], &at[0], &at[1], &at[2])
		}
		if err := nativeOwnedRawPrecondition(ctx, q, "sources55", ownedAdmissionSourcesJoinSQL, args, destinations); err != nil {
			return fail(err)
		}
		if err := readOwnedAdmissionPrincipal(ctx, q, scope); err != nil {
			return fail(fmt.Errorf("principal validator: %w", err))
		}
		if _, err := readOwnedAdmissionSources(ctx, q, g.packageV1.Source); err != nil {
			return fail(fmt.Errorf("sources validator: %w", err))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read-only admission precondition: diagnostic=%v transaction=%v", diagnostic, err)
	}
}

func nativeOwnedRawPrecondition(ctx context.Context, q *ownedAuthorityQuery, label, statement string, args, destinations []any) error {
	rows, err := q.QueryContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("%s query: %w", label, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("%s next: %w", label, err)
		}
		return fmt.Errorf("%s row missing", label)
	}
	returned, scanErr := scanOwnedCurrentRow(rows, destinations...)
	if err := q.budget.materialized(q.callback, returned); err != nil {
		return fmt.Errorf("%s bytes: %w", label, err)
	}
	if scanErr != nil {
		return fmt.Errorf("%s scan: %w", label, scanErr)
	}
	if rows.Next() {
		return fmt.Errorf("%s duplicate row", label)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s tail: %w", label, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("%s close: %w", label, err)
	}
	return nil
}

func (g nativeOwnedFinalGraph) measuredOwnedAdmission() (ports.DispatchAdmissionResult, *ownedAuthorityBudget, error) {
	result := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	var budget *ownedAuthorityBudget
	err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
		budget = q.budget
		return g.f.store.admitBoundedDispatchCallback(ctx, q, g.request, true, &result)
	})
	if err != nil {
		result = ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	}
	return result, budget, err
}

func (g nativeOwnedFinalGraph) finalAdmission(t *testing.T) (domain.AttemptExecutionAdmissionV1, string) {
	t.Helper()
	var raw, digest string
	var runID domain.RunID
	var sessionID domain.SessionID
	var owner domain.UserID
	var reservationID domain.QuotaReservationID
	var at time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT record,digest,run_id,session_id,owner_user_id,reservation_id,admitted_at FROM attempt_execution_admissions_v1 WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID).Scan(&raw, &digest, &runID, &sessionID, &owner, &reservationID, &at); err != nil {
		t.Fatalf("read final create-once admission: %v", err)
	}
	admission, err := domain.DecodeAttemptExecutionAdmissionV1([]byte(raw))
	actualDigest, digestErr := admission.Digest()
	original, sourceErr := domain.EncodeAttemptExecutionSourceV1(admission.Source)
	if err != nil || digestErr != nil || sourceErr != nil || actualDigest != digest || runID != g.run.ID || sessionID != g.run.SessionID || owner != g.f.scope.owner || reservationID != g.request.ReservationID || !admission.AdmittedAt.Equal(at) || !reflect.DeepEqual(admission.Source, g.packageV1.Source) || string(original) != g.outbox.OwnedExecutionSourceRecord || admission.ValidateForExecution(g.packageV1.Placement, g.packageV1.Binding, g.packageV1.Substrate, g.packageV1.Cost) != nil {
		t.Fatalf("final admission failed exact original/scalar/digest join: decode=%v digest=%v source=%v admission=%+v", err, digestErr, sourceErr, admission)
	}
	var indexedRun domain.RunID
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT run_id FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 AND attempt_id=$3`, g.run.TenantID, g.run.SessionID, g.attempt.ID).Scan(&indexedRun); err != nil || indexedRun != g.run.ID {
		t.Errorf("final reverse index run=%s want=%s err=%v", indexedRun, g.run.ID, err)
	}
	job, found, err := readJSON[domain.WorkerJob](g.ctx, g.f.store.db, `SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || job.HarnessBinding.Profile != domain.HarnessExecutionOwnedAgentV2 || job.OwnedExecutionSourceRecord != g.outbox.OwnedExecutionSourceRecord || !job.CreatedAt.Equal(at) || !reflect.DeepEqual(job.HarnessBinding, g.packageV1.Binding) {
		t.Errorf("final admission/Job original package differs: found=%v err=%v job=%+v", found, err, job)
	}
	reservation, found, err := readJSON[domain.QuotaReservation](g.ctx, g.f.store.db, `SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, g.run.TenantID, g.request.ReservationID)
	if err != nil || !found || !reservation.HeldAt.Equal(at) || reservation.ID != reservationID {
		t.Errorf("final admission/actual held quota time differs: found=%v err=%v reservation=%+v", found, err, reservation)
	}
	g.requireCount(t, "attempt_execution_admissions_v1", 1)
	g.requireCount(t, "attempt_execution_admissions_by_session_v1", 1)
	return admission, raw
}

func (g nativeOwnedFinalGraph) requireOriginalCurrentSources(t *testing.T) {
	t.Helper()
	var actual ownedCurrentSnapshot
	err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		actual, err = readOwnedAdmissionSources(ctx, q, g.packageV1.Source)
		return err
	})
	if err != nil || !reflect.DeepEqual(actual.profile, g.profileReceipt.Profile) {
		t.Fatalf("actual published current profile changed during admission: err=%v", err)
	}
	for i := range actual.spending {
		if !reflect.DeepEqual(actual.spending[i], g.spendReceipts[i].Authorization) {
			t.Errorf("actual published original component=%d changed during admission", i)
		}
	}
	g.requireCount(t, "owned_profile_publication_audit_v1", 1)
	g.requireCount(t, "attempt_spending_publication_audit_v1", 4)
}

func requireNativeOwnedFinalBudget(t *testing.T, budget *ownedAuthorityBudget, phase string) {
	t.Helper()
	if budget == nil || budget.callbacks < 1 || budget.callbacks > ownedAuthorityMaxCallbacks || budget.callbackStatements > ownedAuthorityMaxStatements || budget.statements > ownedAuthorityMaxTotalStatements || budget.bytes == 0 || budget.bytes > ownedAuthorityMaxBytes {
		t.Fatalf("%s actual callback exceeded finite SQL/material budget: %+v", phase, budget)
	}
	t.Logf("%s callbacks=%d statements=%d writes=%d materialized_bytes=%d", phase, budget.callbacks, budget.statements, budget.writes, budget.bytes)
}

func TestOwnedFinalAdmissionYDBCompleteAndExactReplay(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
	if err != nil {
		t.Fatalf("actual private owned final admission: %v", err)
	}
	g.requireCommittedGraph(t, result)
	g.requireOriginalCurrentSources(t)
	admission, originalFinal := g.finalAdmission(t)
	before := g.persistedGraph(t)
	replay, budget, err := g.measuredOwnedAdmission()
	if err != nil || !replay.Admitted || replay.Code != "already_admitted" || replay.Delivery != ports.DispatchDeliveryManagedQueue {
		t.Fatalf("actual shared callback exact replay err=%v result=%+v", err, replay)
	}
	requireNativeOwnedFinalBudget(t, budget, "exact-owned-replay")
	if budget.writes != 0 {
		t.Errorf("exact replay dispatched SQL writes=%d want=0", budget.writes)
	}
	afterAdmission, afterFinal := g.finalAdmission(t)
	if after := g.persistedGraph(t); !reflect.DeepEqual(before, after) || originalFinal != afterFinal || !reflect.DeepEqual(admission, afterAdmission) {
		t.Error("exact replay rewrote original final admission or canonical graph")
	}
	g.requireOriginalCurrentSources(t)
	// Even after this private fixture admission succeeds, PUBLIC admission is
	// categorically closed. Stored queue bytes cannot activate a serving profile.
	public, err := g.f.store.AdmitDispatch(g.ctx, g.request)
	want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(public, want) {
		t.Errorf("public owned serving gate changed: err=%v result=%+v", err, public)
	}
}

func TestOwnedFinalAdmissionYDBPreviousDeniedWholeGraphBudget(t *testing.T) {
	managed, previous, originalSeed := newNativeManagedPreviousDeniedGraph(t)
	g := nativeOwnedFinalGraphFrom(t, managed, true)
	seed, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || seed.SelectedAttemptID != previous.ID || seed.Admission == nil || seed.Admission.Outcome != runexplanation.Denied || !reflect.DeepEqual(seed, originalSeed) {
		t.Fatalf("previous denied source head found=%v err=%v head=%+v", found, err, seed)
	}
	result, budget, err := g.measuredOwnedAdmission()
	if err != nil {
		t.Fatalf("complete worst-case owned admission err=%v budget=%+v", err, budget)
	}
	g.requireCommittedGraph(t, result)
	requireNativeOwnedFinalBudget(t, budget, "previous-denied-owned-whole-graph-16-snapshots")
	admission, _ := g.finalAdmission(t)
	g.requireOriginalCurrentSources(t)
	queued, found, err := readJSON[domain.Run](g.ctx, g.f.store.db, `SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !queued.UpdatedAt.Equal(admission.AdmittedAt) {
		t.Fatalf("committed DB admission clock join: found=%v err=%v", found, err)
	}
	expected, changed, err := runexplanation.Invalidate(seed, true, false)
	if err != nil || !changed {
		t.Fatalf("expected invalidation changed=%v err=%v", changed, err)
	}
	expected, changed, err = runexplanation.Select(&expected, queued, g.attempt, g.outbox.ExecutionPlacementV2, admission.AdmittedAt)
	if err != nil || !changed {
		t.Fatalf("expected selection changed=%v err=%v", changed, err)
	}
	expected, changed, err = runexplanation.RecordAdmission(expected, queued, g.attempt, "admitted", admission.AdmittedAt)
	if err != nil || !changed {
		t.Fatalf("expected admission changed=%v err=%v", changed, err)
	}
	actual, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(actual, expected) {
		t.Errorf("delayed projection changed ordered reducer revisions/fingerprint: found=%v err=%v got=%+v want=%+v", found, err, actual, expected)
	}
}

func TestOwnedFinalAdmissionYDBLateFailureRollsBackFinalAndGraph(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	priorHead, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found {
		t.Fatal("missing canonical initial explanation", err)
	}
	var raw string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CAST(record AS String) FROM session_snapshots WHERE tenant_id=$1 AND session_id=$2 AND version=$3`, g.run.TenantID, g.run.SessionID, uint64(15)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	oversized := strings.TrimSuffix(raw, "}") + `,"test_owned_padding":"` + strings.Repeat("x", admittedSnapshotMetadataBytes) + `"}`
	if !json.Valid([]byte(oversized)) {
		t.Fatal("late oversize fixture must be valid JsonDocument")
	}
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE session_snapshots SET record=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND session_id=$3 AND version=$4`, oversized, g.run.TenantID, g.run.SessionID, uint64(15)); err != nil {
		t.Fatal(err)
	}
	result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
	want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
		t.Errorf("late source overflow escaped final/graph result: err=%v result=%+v", err, result)
	}
	// Snapshot selection happens after create-once final/index INSERTs and
	// queued Run/quota/BOTH quota-index writes. Derived head publication is
	// delayed, but every pure transition must disappear with this rollback.
	requireNativeManagedAdmissionRollback(t, g.nativeManagedAdmissionGraph, &priorHead)
	g.requireCount(t, "attempt_execution_admissions_v1", 0)
	g.requireCount(t, "attempt_execution_admissions_by_session_v1", 0)
	g.requireOriginalCurrentSources(t)
}

func (g nativeOwnedFinalGraph) revokeProfile(t *testing.T) {
	t.Helper()
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	profile := g.profileReceipt.Profile.Clone()
	profile.State, profile.RevocationGeneration = domain.OwnedAgentCurrentRevokedV1, 2
	profile.MutationID = "native-final-profile-revoke"
	grant := g.profileVerifier.install(t, ports.OwnedProfilePublicationGrantV1{Profile: profile, ExpectedGeneration: 1, ExpectedDigest: g.profileReceipt.Digest, IssuedAt: at, ExpiresAt: at.Add(10 * time.Second)})
	receipt, err := g.f.store.PublishOwnedProfile(g.ctx, grant)
	if err != nil || receipt.Replayed || receipt.Profile.State != domain.OwnedAgentCurrentRevokedV1 || receipt.Profile.RevocationGeneration != 2 {
		t.Fatalf("actual coherent current profile revoke: err=%v receipt=%+v", err, receipt)
	}
}

func (g nativeOwnedFinalGraph) revokeMCPSpending(t *testing.T) {
	t.Helper()
	for _, receipt := range g.spendReceipts {
		if receipt.Authorization.Locator.ComponentKind != domain.AttemptSpendingMCPToolV1 {
			continue
		}
		at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
		original := ports.AttemptSpendingPublicationGrantV1{Authorization: receipt.Authorization.Clone()}
		grant := spendingRevoke(t, g.spendVerifier, original, receipt, at)
		revoked, err := g.f.store.PublishAttemptSpending(g.ctx, grant)
		if err != nil || revoked.Replayed || revoked.Authorization.State != domain.OwnedAgentCurrentRevokedV1 || revoked.Authorization.RevocationGeneration != 2 {
			t.Fatalf("actual coherent current MCP spending revoke: err=%v receipt=%+v", err, revoked)
		}
		return
	}
	t.Fatal("fixture lacks an actual published MCP component")
}

func (g nativeOwnedFinalGraph) removeParticipant(t *testing.T) {
	t.Helper()
	participant, found, err := readJSON[domain.SessionParticipant](g.ctx, g.f.store.db, `SELECT record FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, g.run.TenantID, g.run.SessionID, g.f.scope.owner)
	if err != nil || !found {
		t.Fatalf("load current participant: found=%v err=%v", found, err)
	}
	participant.Status = domain.SessionParticipantRemoved
	participant.UpdatedAt = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if err := participant.Validate(); err != nil {
		t.Fatalf("removed participant must be coherent domain state: %v", err)
	}
	raw, err := marshal(participant)
	if err != nil {
		t.Fatal(err)
	}
	// Test-owned lifecycle setup keeps ALL selected scalar facts coherent with
	// the canonical record. A JSON/scalar mismatch must not mask principal denial.
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE session_participants SET status=$1,updated_at=$2,record=CAST($3 AS JsonDocument) WHERE tenant_id=$4 AND session_id=$5 AND user_id=$6`, participant.Status, participant.UpdatedAt, raw, participant.TenantID, participant.SessionID, participant.UserID); err != nil {
		t.Fatal(err)
	}
}

func (g nativeOwnedFinalGraph) suspendMembership(t *testing.T) {
	t.Helper()
	bucket, err := webBucket(string(g.f.scope.owner))
	if err != nil {
		t.Fatal(err)
	}
	membership, found, err := readJSON[domain.TenantMembership](g.ctx, g.f.store.db, `SELECT record FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, bucket, g.f.scope.owner, g.run.TenantID)
	if err != nil || !found {
		t.Fatalf("load current membership: found=%v err=%v", found, err)
	}
	membership.Status = domain.TenantMembershipSuspended
	membership.SecurityVersion++
	membership.UpdatedAt = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if err := g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		return putMembershipTx(g.ctx, state.(*stateTx).sqlTx, membership)
	}); err != nil {
		t.Fatalf("coherent canonical membership suspension: %v", err)
	}
}

func (g nativeOwnedFinalGraph) sourceRecords(t *testing.T) []string {
	t.Helper()
	var profile string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT record FROM owned_agent_profile_current_v1 WHERE tenant_id=$1`, g.run.TenantID).Scan(&profile); err != nil {
		t.Fatal(err)
	}
	result := []string{profile}
	rows, err := g.f.store.db.QueryContext(g.ctx, `SELECT record FROM attempt_spending_authorization_current_v1 WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY component_kind LIMIT $3`, g.run.TenantID, g.attempt.ID, uint64(5))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		result = append(result, raw)
	}
	if err := rows.Err(); err != nil || len(result) != 5 {
		t.Fatalf("exact five current sources count=%d err=%v", len(result), err)
	}
	return result
}

func TestOwnedFinalAdmissionYDBCurrentRevocationAndPrincipalDenial(t *testing.T) {
	for _, phase := range []string{"fresh", "replay"} {
		for _, source := range []string{"profile_revoked", "mcp_spending_revoked", "participant_removed", "membership_suspended"} {
			t.Run(phase+"/"+source, func(t *testing.T) {
				g := newNativeOwnedFinalGraph(t)
				priorHead, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
				if err != nil || !found {
					t.Fatal("missing seeded head", err)
				}
				var graphBefore []string
				var finalBefore string
				if phase == "replay" {
					result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
					if err != nil {
						t.Fatalf("establish original final before revocation: %v", err)
					}
					g.requireCommittedGraph(t, result)
					_, finalBefore = g.finalAdmission(t)
					graphBefore = g.persistedGraph(t)
				}
				switch source {
				case "profile_revoked":
					g.revokeProfile(t)
				case "mcp_spending_revoked":
					g.revokeMCPSpending(t)
				case "participant_removed":
					g.removeParticipant(t)
				case "membership_suspended":
					g.suspendMembership(t)
				}
				sourcesBefore := g.sourceRecords(t)
				result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
				want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
					t.Errorf("phase=%s current=%s authorized stale owned source: err=%v result=%+v", phase, source, err, result)
				}
				if after := g.sourceRecords(t); !reflect.DeepEqual(sourcesBefore, after) {
					t.Error("denied admission rewrote current source records")
				}
				if phase == "fresh" {
					requireNativeManagedAdmissionRollback(t, g.nativeManagedAdmissionGraph, &priorHead)
					g.requireCount(t, "attempt_execution_admissions_v1", 0)
					g.requireCount(t, "attempt_execution_admissions_by_session_v1", 0)
				} else {
					_, afterFinal := g.finalAdmission(t)
					if after := g.persistedGraph(t); !reflect.DeepEqual(graphBefore, after) || finalBefore != afterFinal {
						t.Error("denied replay resealed history or changed canonical scheduler graph")
					}
					for _, table := range nativeManagedAdmissionEffectTables {
						g.requireCount(t, table, 1)
					}
				}
			})
		}
	}
}
