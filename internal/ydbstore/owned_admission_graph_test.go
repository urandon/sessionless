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
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Actual shared admission callback, not a replacement scheduler or native
// isolation proof. Explicit scripted table inventory observes complete SQL.
type managedAdmissionGraphFixture struct {
	*canonicalBudgetFixture
	slot, counters     []driver.Value
	entitlement, quota driver.Value
	oldAttempt         []driver.Value
}

func (f *managedAdmissionGraphFixture) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := f.canonicalBudgetFixture.BeginTx(ctx, options)
	rows := make(map[string][]driver.Value, len(f.rows))
	for table, row := range f.rows {
		rows[table] = append([]driver.Value(nil), row...)
	}
	return managedGraphTx{f: f, inner: inner, rows: rows, slot: append([]driver.Value(nil), f.slot...), counters: append([]driver.Value(nil), f.counters...)}, err
}

type managedGraphTx struct {
	f              *managedAdmissionGraphFixture
	inner          driver.Tx
	rows           map[string][]driver.Value
	slot, counters []driver.Value
}

func (tx managedGraphTx) restore() {
	tx.f.rows = tx.rows
	tx.f.slot = tx.slot
	tx.f.counters = tx.counters
}
func (tx managedGraphTx) Commit() error {
	mode := ""
	if len(tx.f.commitModes) > 0 {
		mode = tx.f.commitModes[0]
	}
	after := tx.f.afterCommit
	tx.f.afterCommit = nil
	err := tx.inner.Commit()
	tx.f.afterCommit = after
	if mode == "rollback" {
		tx.restore()
	}
	if after != nil {
		after()
	}
	return err
}
func (tx managedGraphTx) Rollback() error { tx.restore(); return tx.inner.Rollback() }

func (f *managedAdmissionGraphFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *managedAdmissionGraphFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	var row []driver.Value
	switch {
	case strings.Contains(statement, "FROM subscription_connections"):
		row = []driver.Value{f.entitlement, f.quota}
	case strings.Contains(statement, "FROM subscription_scheduler_slots"):
		row = f.slot
	case strings.Contains(statement, "FROM tenant_scheduler_counters"):
		row = f.counters
	case strings.Join(strings.Fields(statement), " ") == "SELECT run_id FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2":
		row = f.rows["run_idempotency"]
	default:
		if strings.Contains(statement, "FROM attempts") && len(args) > 1 && args[1].Value == "attempt-previous" {
			row = f.oldAttempt
			break
		}
		for original := range ownedCanonicalJSONReads {
			if strings.Join(strings.Fields(statement), " ") == original {
				row = f.rows[strings.Fields(original)[3]]
				f.statements = append(f.statements, statement)
				f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
				return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
			}
		}
		return f.canonicalBudgetFixture.QueryContext(ctx, statement, args)
	}
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		f.deadlineMissing = true
	}
	return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
}
func (f *managedAdmissionGraphFixture) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	fields := strings.Fields(statement)
	if len(fields) < 2 {
		return nil, errors.New("invalid fixture write")
	}
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		f.deadlineMissing = true
	}
	f.writes++
	f.pendingWrites++
	if fields[0] == "UPSERT" {
		table := fields[2]
		payloadArgument := map[string]int{"runs": 8, "quota_reservations": 9, "worker_jobs": 6, "run_explanation_heads_v1": 3}[table]
		if _, ok := map[string]bool{"runs": true, "quota_reservations": true, "worker_jobs": true, "run_explanation_heads_v1": true}[table]; ok {
			f.rows[table] = []driver.Value{args[payloadArgument].Value}
		}
		if table == "subscription_scheduler_slots" {
			f.slot = []driver.Value{args[2].Value, args[3].Value, args[4].Value, args[5].Value, args[6].Value}
		}
	}
	return driver.RowsAffected(1), nil
}

func managedGraphFixture(t *testing.T) (*Store, *managedAdmissionGraphFixture, ports.DispatchAdmissionRequest) {
	t.Helper()
	store, base, s := canonicalFixture(t)
	f := &managedAdmissionGraphFixture{canonicalBudgetFixture: base, entitlement: string(domain.EntitlementActive), quota: string(domain.ProviderQuotaAvailable)}
	s.Run.Status = domain.RunCreated
	s.Attempt.Status, s.Attempt.WorkerID = domain.AttemptCreated, ""
	s.Job.Limits.MaxContextEvents, s.Job.Limits.MaxToolEvents, s.Job.Limits.MaxToolEventBytes = 100, 20, 1<<18
	ratedFixturePut(t, base.ratedSQLFixture, "runs", s.Run)
	ratedFixturePut(t, base.ratedSQLFixture, "attempts", s.Attempt)
	f.rows["quota_reservations"], f.rows["worker_jobs"] = nil, nil
	outbox := domain.DispatchOutbox{
		ID: "dispatch-test", TenantID: s.Run.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID,
		InputManifestID: s.Job.InputManifestID, ContextSnapshot: s.Job.ContextSnapshot,
		ContextWindow:         &domain.SessionContextWindow{ThroughSequence: 1},
		CredentialOwnerUserID: s.Job.CredentialOwnerUserID, ExecutionPlacementV2: s.Job.ExecutionPlacementV2,
		HarnessBinding: s.Job.HarnessBinding, SubstrateBinding: s.Job.SubstrateBinding, AdmissionCostCeiling: s.Job.AdmissionCostCeiling,
		DeliveryChat: s.Job.DeliveryChat, ReplyToMessageID: s.Job.ReplyToMessageID,
		Status: domain.DispatchPending, IdempotencyKey: "dispatch-key", CreatedAt: f.at, UpdatedAt: f.at,
	}
	if err := outbox.ValidateForAttempt(s.Run, s.Attempt); err != nil {
		t.Fatal(err)
	}
	ratedFixturePut(t, base.ratedSQLFixture, "dispatch_outbox", outbox)
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	store.operationalRetention = time.Hour
	return store, f, ports.DispatchAdmissionRequest{TenantID: s.Run.TenantID, OutboxID: outbox.ID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, ReservationID: s.Job.ReservationID, Now: f.at, HoldUntil: f.at.Add(time.Hour), Limits: s.Job.Limits, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
}

func TestBoundedManagedAdmissionCompleteColdGraph(t *testing.T) {
	store, f, request := managedGraphFixture(t)
	var originalRun domain.Run
	var originalAttempt domain.Attempt
	var originalOutbox domain.DispatchOutbox
	for _, item := range []struct {
		table  string
		target any
	}{{"runs", &originalRun}, {"attempts", &originalAttempt}, {"dispatch_outbox", &originalOutbox}} {
		if err := json.Unmarshal([]byte(f.rows[item.table][0].(string)), item.target); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.admitBoundedManagedDispatch(t.Context(), request)
	if err != nil || !result.Admitted || result.Code != "admitted" || result.Delivery != ports.DispatchDeliveryManagedQueue || f.commits != 1 || f.deadlineMissing {
		t.Fatalf("full cold graph err=%v result=%+v statements=%d commits=%d writes=%d deadlineMissing=%v", err, result, len(f.statements), f.commits, f.writes, f.deadlineMissing)
	}
	t.Logf("complete cold managed graph: SQL=%d writes=%d", len(f.statements), f.writes)
	if len(f.statements) > 32 {
		t.Errorf("complete graph SQL=%d >32", len(f.statements))
	}
	for _, table := range []string{"runs", "worker_jobs", "quota_reservations", "run_explanation_heads_v1"} {
		if len(f.rows[table]) != 1 {
			t.Errorf("missing final %s", table)
		}
	}
	var run domain.Run
	if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run); err != nil || run.Status != domain.RunQueued {
		t.Fatalf("canonical run=%+v err=%v", run, err)
	}
	originalRun.Status = domain.RunQueued
	expected, _, err := runexplanation.Select(nil, originalRun, originalAttempt, originalOutbox.ExecutionPlacementV2, f.at)
	if err != nil {
		t.Fatal(err)
	}
	expected, _, err = runexplanation.RecordAdmission(expected, originalRun, originalAttempt, "admitted", f.at)
	if err != nil {
		t.Fatal(err)
	}
	var gotHead runexplanation.HeadV1
	if err := json.Unmarshal([]byte(f.rows["run_explanation_heads_v1"][0].(string)), &gotHead); err != nil || !reflect.DeepEqual(gotHead, expected) {
		t.Fatalf("combined head changed ordered reducers: err=%v got=%+v want=%+v", err, gotHead, expected)
	}
	var job domain.WorkerJob
	if err := json.Unmarshal([]byte(f.rows["worker_jobs"][0].(string)), &job); err != nil || job.ValidateForRun(run) != nil || !job.CreatedAt.Equal(f.at) {
		t.Fatalf("canonical job=%+v err=%v", job, err)
	}
	before := f.writes
	replay, err := store.admitBoundedManagedDispatch(t.Context(), request)
	if err != nil || !replay.Admitted || replay.Code != "already_admitted" || f.writes != before {
		t.Fatalf("replay=%+v err=%v writes=%d want=%d", replay, err, f.writes, before)
	}
}

func TestBoundedManagedAdmissionInvalidScalarAndLateFailureDoNotEscapeResult(t *testing.T) {
	for _, name := range []string{"unknown_entitlement", "unknown_quota", "null_quota", "counter_overflow", "counter_float", "reservation_collision", "snapshot_stream_failure", "expired_hold", "commit_failure", "outbox_physical_key", "run_physical_key", "cancelled_run"} {
		t.Run(name, func(t *testing.T) {
			store, f, request := managedGraphFixture(t)
			switch name {
			case "unknown_entitlement":
				f.entitlement = "invented"
			case "unknown_quota":
				f.quota = "invented"
			case "null_quota":
				f.quota = nil
			case "counter_float":
				f.counters = []driver.Value{float64(0), int64(0)}
			case "reservation_collision":
				s := currentWorkerAuthorityFixture(t)
				ratedFixturePut(t, f.ratedSQLFixture, "quota_reservations", s.Reservation)
			case "outbox_physical_key":
				var outbox domain.DispatchOutbox
				if err := json.Unmarshal([]byte(f.rows["dispatch_outbox"][0].(string)), &outbox); err != nil {
					t.Fatal(err)
				}
				outbox.ID = "wrong-outbox-key"
				ratedFixturePut(t, f.ratedSQLFixture, "dispatch_outbox", outbox)
			case "run_physical_key", "cancelled_run":
				var run domain.Run
				if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run); err != nil {
					t.Fatal(err)
				}
				if name == "run_physical_key" {
					run.ID = "wrong-run-key"
				} else {
					at := f.at
					run.CancellationRequestedAt = &at
				}
				ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
			case "counter_overflow":
				f.counters = []driver.Value{int64(1) << 32, int64(0)}
			case "snapshot_stream_failure":
				f.rowError = errors.New("test-owned stream failure after writes")
			case "expired_hold":
				request.Now = f.at.Add(-2 * time.Hour)
				request.HoldUntil = f.at.Add(-time.Hour)
			case "commit_failure":
				f.commitModes = []string{"unknown"}
			}
			result, err := store.admitBoundedManagedDispatch(t.Context(), request)
			want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
				t.Fatalf("case=%s err=%v speculative result=%+v", name, err, result)
			}
			if name != "commit_failure" && f.persistedWrites != 0 {
				t.Fatalf("denial committed writes=%d", f.persistedWrites)
			}
		})
	}
}

func TestOwnedMetadataCacheInvalidationAndCallbackLifetime(t *testing.T) {
	store, f, s := canonicalFixture(t)
	err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		tx := q.canonicalState(store, s.Run.TenantID)
		first, found, err := tx.GetRun(ctx, s.Run.ID)
		if err != nil || !found {
			return errors.New("missing initial run")
		}
		_, _, err = tx.GetRun(ctx, s.Run.ID)
		if err != nil || q.budget.statements != 2 {
			return errors.New("same snapshot reread did not reuse")
		}
		first.Status = domain.RunQueued
		ratedFixturePut(t, f.ratedSQLFixture, "runs", first)
		if _, err := q.ExecContext(ctx, `UPDATE runs SET status=$1 WHERE tenant_id=$2 AND run_id=$3`, first.Status, first.TenantID, first.ID); err == nil {
			return errors.New("fixture intentionally rejects UPDATE")
		}
		got, _, err := tx.GetRun(ctx, s.Run.ID)
		if err != nil || got.Status != domain.RunQueued || q.budget.statements != 4 {
			return errors.New("write did not invalidate before failed execution")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestManagedAdmissionRetryClearsSpeculativeDecision(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "bounded"}[bounded], func(t *testing.T) {
			store, f, request := managedGraphFixture(t)
			f.commitModes = []string{"rollback", ""}
			f.afterCommit = func() {
				if f.commits != 1 {
					return
				}
				var outbox domain.DispatchOutbox
				if err := json.Unmarshal([]byte(f.rows["dispatch_outbox"][0].(string)), &outbox); err != nil {
					t.Fatal(err)
				}
				outbox.Status = domain.DispatchCancelled
				ratedFixturePut(t, f.ratedSQLFixture, "dispatch_outbox", outbox)
			}
			call := store.AdmitDispatch
			if bounded {
				call = store.admitBoundedManagedDispatch
			}
			result, err := call(t.Context(), request)
			if err != nil || result.Admitted || result.Code != "dispatch_not_pending" || f.commits != 2 || f.persistedWrites != 0 {
				t.Fatalf("bounded=%v retry escaped discarded decision err=%v result=%+v commits=%d persisted=%d", bounded, err, result, f.commits, f.persistedWrites)
			}
		})
	}
}

func TestBoundedManagedAdmissionThreeCompleteCallbacksUseFreshCache(t *testing.T) {
	store, f, request := managedGraphFixture(t)
	f.commitModes = []string{"rollback", "rollback", ""}
	result, err := store.admitBoundedManagedDispatch(t.Context(), request)
	if err != nil || !result.Admitted || f.commits != 3 || len(f.statements) != 84 || f.persistedWrites != 13 {
		t.Fatalf("complete retries err=%v result=%+v commits=%d SQL=%d persisted=%d", err, result, f.commits, len(f.statements), f.persistedWrites)
	}
	queries := 0
	for _, statement := range f.statements {
		if strings.Contains(statement, "FROM dispatch_outbox") {
			queries++
		}
	}
	if queries != 3 {
		t.Fatalf("cache escaped callbacks: outbox reads=%d", queries)
	}
}

func previousDeniedManagedGraphFixture(t *testing.T) (*Store, *managedAdmissionGraphFixture, ports.DispatchAdmissionRequest, runexplanation.HeadV1) {
	t.Helper()
	store, f, request := managedGraphFixture(t)
	var run domain.Run
	var attempt domain.Attempt
	var outbox domain.DispatchOutbox
	for _, item := range []struct {
		table  string
		target any
	}{{"runs", &run}, {"attempts", &attempt}, {"dispatch_outbox", &outbox}} {
		if err := json.Unmarshal([]byte(f.rows[item.table][0].(string)), item.target); err != nil {
			t.Fatal(err)
		}
	}
	previous := attempt
	previous.ID = "attempt-previous"
	raw, err := json.Marshal(previous)
	if err != nil {
		t.Fatal(err)
	}
	f.oldAttempt = []driver.Value{string(raw)}
	attempt.Number = 2
	ratedFixturePut(t, f.ratedSQLFixture, "attempts", attempt)
	run.Status = domain.RunQuotaBlocked
	ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
	head, _, err := runexplanation.Select(nil, run, previous, outbox.ExecutionPlacementV2, f.at)
	if err != nil {
		t.Fatal(err)
	}
	head, _, err = runexplanation.RecordAdmission(head, run, previous, "subscription_slot_busy", f.at)
	if err != nil {
		t.Fatal(err)
	}
	ratedFixturePut(t, f.ratedSQLFixture, "run_explanation_heads_v1", head)
	return store, f, request, head
}

func TestBoundedManagedAdmissionPreviousDeniedExplanationFits(t *testing.T) {
	store, f, request, head := previousDeniedManagedGraphFixture(t)
	var run domain.Run
	var attempt domain.Attempt
	var outbox domain.DispatchOutbox
	for _, item := range []struct {
		table  string
		target any
	}{{"runs", &run}, {"attempts", &attempt}, {"dispatch_outbox", &outbox}} {
		if err := json.Unmarshal([]byte(f.rows[item.table][0].(string)), item.target); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.admitBoundedManagedDispatch(t.Context(), request)
	if err != nil || !result.Admitted || len(f.statements) > 32 {
		t.Fatalf("previous denied head err=%v result=%+v SQL=%d", err, result, len(f.statements))
	}
	t.Logf("complete cold previous-denied managed graph: SQL=%d writes=%d", len(f.statements), f.writes)
	var got runexplanation.HeadV1
	expected, _, err := runexplanation.Invalidate(head, true, false)
	if err != nil {
		t.Fatal(err)
	}
	run.Status = domain.RunQueued
	expected, _, err = runexplanation.Select(&expected, run, attempt, outbox.ExecutionPlacementV2, f.at)
	if err != nil {
		t.Fatal(err)
	}
	expected, _, err = runexplanation.RecordAdmission(expected, run, attempt, "admitted", f.at)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(f.rows["run_explanation_heads_v1"][0].(string)), &got); err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatalf("ordered invalidation/selection/admission: err=%v head=%+v want=%+v", err, got, expected)
	}
}

func TestBoundedManagedAdmissionPreviousAttemptCorrelation(t *testing.T) {
	for _, name := range []string{"wrong_id", "wrong_tenant", "wrong_run", "missing", "future_updated", "future_finished", "invalid_status"} {
		t.Run(name, func(t *testing.T) {
			store, f, request, _ := previousDeniedManagedGraphFixture(t)
			var run domain.Run
			var previous domain.Attempt
			if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(f.oldAttempt[0].(string)), &previous); err != nil {
				t.Fatal(err)
			}
			owningRun := run
			switch name {
			case "wrong_id":
				previous.ID = "coherent-wrong-previous"
			case "wrong_tenant":
				previous.TenantID = "coherent-wrong-tenant"
				owningRun.TenantID = previous.TenantID
			case "wrong_run":
				previous.RunID = "coherent-wrong-run"
				owningRun.ID = previous.RunID
			case "future_updated":
				previous.UpdatedAt = f.at.Add(time.Second)
			case "future_finished":
				at := f.at.Add(time.Second)
				previous.Status, previous.FinishedAt = domain.AttemptSucceeded, &at
			case "invalid_status":
				previous.Status = "invented"
			}
			if name != "missing" && name != "invalid_status" {
				if err := previous.ValidateForRun(owningRun); err != nil {
					t.Fatalf("coherent negative must be separately domain-valid: %v", err)
				}
			}
			raw, err := json.Marshal(previous)
			if err != nil {
				t.Fatal(err)
			}
			f.oldAttempt = []driver.Value{string(raw)}
			if name == "missing" {
				f.oldAttempt = nil
			}
			beforeRun, beforeHead := append([]driver.Value(nil), f.rows["runs"]...), append([]driver.Value(nil), f.rows["run_explanation_heads_v1"]...)
			result, err := store.admitBoundedManagedDispatch(t.Context(), request)
			want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) || f.persistedWrites != 0 || f.writes == 0 || !reflect.DeepEqual(f.rows["runs"], beforeRun) || !reflect.DeepEqual(f.rows["run_explanation_heads_v1"], beforeHead) {
				t.Fatalf("previous correlation case=%s err=%v result=%+v writes=%d persisted=%d", name, err, result, f.writes, f.persistedWrites)
			}
		})
	}
}

func TestBoundedManagedAdmissionManifestCorrelation(t *testing.T) {
	for _, name := range []string{"wrong_id", "wrong_tenant", "future_created", "invalid_artifact"} {
		t.Run(name, func(t *testing.T) {
			store, f, request := managedGraphFixture(t)
			var run domain.Run
			var manifest domain.ArtifactManifest
			if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(f.rows["artifact_manifests"][0].(string)), &manifest); err != nil {
				t.Fatal(err)
			}
			owningRun := run
			switch name {
			case "wrong_id":
				manifest.ID = "coherent-wrong-manifest"
			case "wrong_tenant":
				manifest.TenantID = "coherent-wrong-tenant"
				owningRun.TenantID = manifest.TenantID
				manifest.Artifacts = nil
			case "future_created":
				manifest.CreatedAt = f.at.Add(time.Second)
			case "invalid_artifact":
				manifest.Artifacts = []domain.Artifact{{}}
			}
			if name != "invalid_artifact" {
				if err := manifest.ValidateForRun(owningRun); err != nil {
					t.Fatalf("coherent manifest must be separately domain-valid: %v", err)
				}
			}
			ratedFixturePut(t, f.ratedSQLFixture, "artifact_manifests", manifest)
			before := append([]driver.Value(nil), f.rows["runs"]...)
			result, err := store.admitBoundedManagedDispatch(t.Context(), request)
			want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) || f.persistedWrites != 0 || f.writes == 0 || !reflect.DeepEqual(f.rows["runs"], before) {
				t.Fatalf("manifest correlation case=%s err=%v result=%+v writes=%d persisted=%d", name, err, result, f.writes, f.persistedWrites)
			}
		})
	}
}

func TestBoundedManagedAdmissionHistoricalDatabaseTime(t *testing.T) {
	t.Run("outbox_future", func(t *testing.T) {
		store, f, request := managedGraphFixture(t)
		var outbox domain.DispatchOutbox
		var run domain.Run
		var attempt domain.Attempt
		for _, item := range []struct {
			table  string
			target any
		}{{"dispatch_outbox", &outbox}, {"runs", &run}, {"attempts", &attempt}} {
			if err := json.Unmarshal([]byte(f.rows[item.table][0].(string)), item.target); err != nil {
				t.Fatal(err)
			}
		}
		outbox.UpdatedAt = f.at.Add(time.Second)
		if err := outbox.ValidateForAttempt(run, attempt); err != nil {
			t.Fatalf("coherent future outbox: %v", err)
		}
		ratedFixturePut(t, f.ratedSQLFixture, "dispatch_outbox", outbox)
		result, err := store.admitBoundedManagedDispatch(t.Context(), request)
		if !errors.Is(err, domain.ErrMembershipDenied) || result.Admitted || f.persistedWrites != 0 || f.writes != 0 {
			t.Fatalf("future outbox err=%v result=%+v writes=%d persisted=%d", err, result, f.writes, f.persistedWrites)
		}
	})
	for _, name := range []string{"job_future", "reservation_held_future", "reservation_updated_future"} {
		t.Run(name, func(t *testing.T) {
			store, f, request := managedGraphFixture(t)
			if result, err := store.admitBoundedManagedDispatch(t.Context(), request); err != nil || !result.Admitted {
				t.Fatalf("seed legitimate hold: %v %+v", err, result)
			}
			var run domain.Run
			var job domain.WorkerJob
			var reservation domain.QuotaReservation
			for _, item := range []struct {
				table  string
				target any
			}{{"runs", &run}, {"worker_jobs", &job}, {"quota_reservations", &reservation}} {
				if err := json.Unmarshal([]byte(f.rows[item.table][0].(string)), item.target); err != nil {
					t.Fatal(err)
				}
			}
			if name == "job_future" {
				job.CreatedAt = f.at.Add(time.Second)
				if err := job.ValidateForRun(run); err != nil {
					t.Fatalf("coherent future job: %v", err)
				}
				ratedFixturePut(t, f.ratedSQLFixture, "worker_jobs", job)
			} else {
				reservation.UpdatedAt = f.at.Add(time.Second)
				if name == "reservation_held_future" {
					reservation.HeldAt = reservation.UpdatedAt
				}
				if err := reservation.ValidateForRun(run); err != nil {
					t.Fatalf("coherent future reservation: %v", err)
				}
				ratedFixturePut(t, f.ratedSQLFixture, "quota_reservations", reservation)
			}
			before := f.writes
			result, err := store.admitBoundedManagedDispatch(t.Context(), request)
			want := ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) || f.writes != before {
				t.Fatalf("future replay case=%s err=%v result=%+v writes=%d before=%d", name, err, result, f.writes, before)
			}
		})
	}
}
