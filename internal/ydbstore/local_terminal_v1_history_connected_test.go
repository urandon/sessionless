package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/deterministicharness"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// This is a database/sql storage model, not native SQL/isolation evidence or a
// second executor. Grants, execution evidence, canonical drafts and the V1 seal
// are produced ONLY by their original production owners. The existing exact
// canonical mutation whitelist is shared with the local completion fixture.
type managedV1HistorySQL struct {
	*canonicalBudgetFixture
	principal                *computeChoiceSourceFixture
	job                      ports.WorkerJobState
	lease                    domain.Lease
	state                    LocalTerminalCompletionSnapshot
	effect                   string
	counters                 []driver.Value
	headReads, evidenceReads int
	historyOnly              bool
	historicalTTLReads       int
	lastSQL                  string
}

func (f *managedV1HistorySQL) Connect(context.Context) (driver.Conn, error) { return f, nil }

// Raw row ownership is transactional too: a failed canonical callback cannot
// leave a marker, evidence, head or terminal Run available to history readers.
func (f *managedV1HistorySQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("managed history fixture requires serializable transaction")
	}
	inner, err := f.canonicalBudgetFixture.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	before := make(map[string][]driver.Value, len(f.rows))
	for k, v := range f.rows {
		before[k] = append([]driver.Value(nil), v...)
	}
	state := f.state
	state.Events = append([]domain.SessionEvent(nil), state.Events...)
	state.material = cloneManagedHistoryRows(state.material)
	state.eventIndex = cloneManagedHistoryRows(state.eventIndex)
	state.eventRecords = make(map[string]string, len(f.state.eventRecords))
	for k, v := range f.state.eventRecords {
		state.eventRecords[k] = v
	}
	return &managedV1HistoryTx{Tx: inner, f: f, rows: before, state: state, effect: f.effect, counters: append([]driver.Value(nil), f.counters...)}, nil
}

func cloneManagedHistoryRows(v map[string][]driver.Value) map[string][]driver.Value {
	out := make(map[string][]driver.Value, len(v))
	for k, r := range v {
		out[k] = append([]driver.Value(nil), r...)
	}
	return out
}

type managedV1HistoryTx struct {
	driver.Tx
	f        *managedV1HistorySQL
	rows     map[string][]driver.Value
	state    LocalTerminalCompletionSnapshot
	effect   string
	counters []driver.Value
	done     bool
}

func (tx *managedV1HistoryTx) restore() {
	tx.f.rows, tx.f.state, tx.f.effect, tx.f.counters = tx.rows, tx.state, tx.effect, tx.counters
}
func (tx *managedV1HistoryTx) Commit() error {
	err := tx.Tx.Commit()
	if err != nil {
		tx.restore()
	}
	tx.done = true
	return err
}
func (tx *managedV1HistoryTx) Rollback() error {
	if !tx.done {
		tx.restore()
		tx.done = true
	}
	return tx.Tx.Rollback()
}

const managedHistoryEffectInsert = `INSERT INTO attempt_effect_reservations
 (tenant_id,attempt_id,kind,run_id,lease_id,fence_token,physical_invocation_claim_id,
 effect_sequence,invocation_authority_digest,reserved_at,record)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CAST($11 AS JsonDocument))`

func (f *managedV1HistorySQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	f.lastSQL = completionSQL(statement)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if completionSQL(statement) == completionSQL(managedHistoryEffectInsert) {
		if len(args) != 11 || f.effect != "" {
			return nil, errors.New("duplicate or malformed original effect INSERT")
		}
		raw, ok := args[10].Value.(string)
		var record attemptEffectRecordV1
		if !ok || json.Unmarshal([]byte(raw), &record) != nil || record.validate() != nil ||
			record.Reservation.TenantID != f.job.Run.TenantID || record.Reservation.RunID != f.job.Run.ID || record.Reservation.AttemptID != f.job.Attempt.ID {
			return nil, errors.New("invalid actual effect record")
		}
		f.effect = raw
		return driver.RowsAffected(1), nil
	}
	if err := applyTerminalCompletionWrite(&f.state, f.rows, &f.counters, terminalCompletionManagedV1, statement, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (f *managedV1HistorySQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.lastSQL = completionSQL(statement)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	row := func(v ...driver.Value) (driver.Rows, error) { return &explanationFixtureRows{values: v}, nil }
	empty := func() (driver.Rows, error) { return &explanationFixtureRows{}, nil }
	flat := completionSQL(statement)
	// A deny-only tripwire: history may not revive old operational state.
	// This does not add a permitted SQL shape to the model below.
	if f.historyOnly {
		for _, table := range []string{"attempts", "worker_jobs", "lease_heads", "leases"} {
			if strings.Contains(flat, " FROM "+table+" ") || strings.HasSuffix(flat, " FROM "+table) {
				f.historicalTTLReads++
				return nil, errors.New("historical reader attempted removed TTL execution state")
			}
		}
	}
	// The original principal fixture checks exact SQL AND exact key values.
	if _, ok := f.principal.driver.expected[statement]; ok {
		return f.principal.driver.QueryContext(ctx, statement, args)
	}
	for original := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(original)
		if flat != completionSQL(original) && flat != completionSQL(bounded) {
			continue
		}
		if len(args) != 2 || args[0].Value != string(f.job.Run.TenantID) {
			return nil, errors.New("foreign canonical read")
		}
		table := strings.Fields(original)[3]
		key := map[string]string{
			"runs": string(f.job.Run.ID), "attempts": string(f.job.Attempt.ID),
			"worker_jobs": string(f.job.Run.ID), "quota_reservations": string(f.job.Reservation.ID),
			"artifact_manifests": string(f.job.InputManifest.ID), "sessions": string(f.job.Run.SessionID),
			"session_deletions": string(f.job.Run.SessionID), "session_legal_holds": string(f.job.Run.SessionID),
		}[table]
		if table != "artifact_manifests" && (key == "" || args[1].Value != key) {
			return nil, errors.New("unexpected canonical key in managed proof")
		}
		if table == "sessions" {
			raw, err := json.Marshal(f.state.Session)
			if err != nil {
				return nil, err
			}
			return row(string(raw))
		}
		if table == "artifact_manifests" && args[1].Value == string(f.state.Manifest.ID) && f.state.Manifest.ID != "" {
			raw, err := json.Marshal(f.state.Manifest)
			if err != nil {
				return nil, err
			}
			return row(string(raw))
		}
		if table == "artifact_manifests" && args[1].Value != key {
			return empty()
		}
		return row(f.rows[table]...)
	}
	for original, table := range legacyProfileWorkerReads {
		if flat == original {
			if len(args) != 2 || args[0].Value != string(f.job.Run.TenantID) {
				return nil, errors.New("foreign worker read")
			}
			return row(f.rows[table]...)
		}
	}
	if statement == localTerminalLegacyManifestsSQL(1) {
		if len(args) != 3 || args[0].Value != string(f.job.Run.TenantID) || args[1].Value != string(f.state.Manifest.ID) || f.state.Manifest.ID == "" {
			return empty()
		}
		raw, err := json.Marshal(f.state.Manifest)
		if err != nil {
			return nil, err
		}
		if int64(len(raw)) >= args[2].Value.(int64) {
			return nil, errors.New("manifest overflow sentinel")
		}
		return row(string(f.state.Manifest.ID), string(raw))
	}
	if statement == localTerminalLegacyHeadSQL {
		f.headReads++
		if len(args) != 2 || args[0].Value != string(f.job.Run.TenantID) || args[1].Value != string(f.job.Run.ID) {
			return nil, errors.New("foreign historical head")
		}
		return row(f.rows["run_explanation_heads_v1"]...)
	}
	if statement == localTerminalLegacyEvidenceSQL {
		f.evidenceReads++
		if len(args) != 3 || args[0].Value != string(f.job.Run.TenantID) || args[1].Value != string(f.job.Attempt.ID) || args[2].Value != string(domain.ProviderEffectTurnV1) {
			return nil, errors.New("foreign historical evidence")
		}
		if f.state.EvidenceRecord == "" || f.effect == "" {
			return empty()
		}
		var record attemptEffectRecordV1
		if json.Unmarshal([]byte(f.effect), &record) != nil {
			return nil, errors.New("malformed stored effect")
		}
		return row(string(f.job.Run.ID), string(f.job.Attempt.ID), string(record.Reservation.PhysicalInvocationClaimID), f.state.EvidenceDigest, f.state.EvidenceRecord, f.effect, int64(0))
	}
	switch flat {
	case "SELECT CurrentUtcTimestamp()":
		return row(f.at)
	case "SELECT run_id FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2":
		if len(args) != 2 || args[0].Value != string(f.job.Run.TenantID) || args[1].Value != string(f.job.Run.IdempotencyKey) {
			return nil, errors.New("foreign Run idempotency key")
		}
		return row(string(f.job.Run.ID))
	case "SELECT lease_id,attempt_id,worker_id,fence_token,expires_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2":
		if !f.state.LeasePresent {
			return empty()
		}
		return row(string(f.lease.ID), string(f.lease.AttemptID), f.lease.WorkerID, int64(f.lease.FenceToken), f.lease.ExpiresAt)
	case "SELECT payload FROM leases WHERE tenant_id=$1 AND lease_id=$2":
		raw, err := json.Marshal(f.lease)
		if err != nil {
			return nil, err
		}
		return row(string(raw))
	case "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if !f.state.LeasePresent {
			return empty()
		}
		return row(string(f.lease.ID), int64(f.lease.FenceToken), f.lease.ExpiresAt)
	case "SELECT terminal_status, content_digest FROM run_finalizations WHERE tenant_id = $1 AND run_id = $2":
		if f.state.Finalizations == 0 {
			return empty()
		}
		return row(f.state.FinalStatus, f.state.FinalDigest)
	case "SELECT evidence_digest FROM substrate_execution_evidence WHERE tenant_id=$1 AND attempt_id=$2":
		if f.state.EvidenceRecord == "" {
			return empty()
		}
		return row(f.state.EvidenceDigest)
	case "SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2":
		raw, err := json.Marshal(f.state.Session)
		if err != nil {
			return nil, err
		}
		return row(string(raw))
	case "SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2",
		"SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3",
		"SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2":
		return empty()
	case "SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1":
		return row(f.at)
	case "SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND status = $3 LIMIT $4":
		raw, err := json.Marshal(f.principal.participant)
		if err != nil {
			return nil, err
		}
		return row(string(raw))
	case "SELECT entitlement_state FROM subscription_connections WHERE tenant_id = $1 AND subscription_connection_id = $2":
		return row(string(domain.EntitlementActive))
	case "SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at FROM subscription_scheduler_slots WHERE tenant_id = $1 AND subscription_connection_id = $2":
		return row(string(domain.SchedulerReady), string(f.job.Run.ID), string(f.job.Reservation.ID), time.Time{}, f.at)
	case "SELECT queue_depth, active_runs FROM tenant_scheduler_counters WHERE tenant_id = $1":
		return row(f.counters...)
	case completionSQL(sessionMaterialCurrentSQL):
		return row(f.state.material["current"]...)
	case completionSQL(sessionMaterialBoundarySQL):
		return row(f.state.material[fmt.Sprint(args[2].Value)]...)
	case "SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2":
		return row(f.rows["run_explanation_heads_v1"]...)
	}
	// The V1 effect reader has a closed bounded shape (record plus five flags).
	// Match the complete SELECT, not a substring/table-name fallback.
	if statement == managedHistoryEffectReadSQL {
		if len(args) != 3 || args[0].Value != string(f.job.Run.TenantID) || args[1].Value != string(f.job.Attempt.ID) || args[2].Value != string(domain.ProviderEffectTurnV1) {
			return nil, errors.New("foreign effect read")
		}
		if f.effect == "" {
			return empty()
		}
		return row(f.effect, int64(0), int64(0), int64(0), int64(0), int64(0))
	}
	return nil, fmt.Errorf("unmodeled managed V1 history SQL: %s", flat)
}

const managedHistoryEffectReadSQL = `SELECT SUBSTRING(CAST(record AS String),0,32769),
 CASE WHEN owned_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_started_at IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN allocation_original_record IS NOT NULL OR allocation_original_digest IS NOT NULL
 OR allocation_current_record IS NOT NULL OR allocation_current_digest IS NOT NULL
 OR allocation_publication_digest IS NOT NULL OR allocation_transition_parent_digest IS NOT NULL
 OR proxy_original_record IS NOT NULL OR proxy_original_digest IS NOT NULL
 OR proxy_current_record IS NOT NULL OR proxy_current_digest IS NOT NULL
 OR proxy_policy_body IS NOT NULL OR proxy_attestation_body IS NOT NULL
 OR operation_accounting_record IS NOT NULL OR operation_accounting_digest IS NOT NULL
 OR web_search_consent_record IS NOT NULL OR web_search_consent_digest IS NOT NULL
 THEN CAST(1 AS Uint64) ELSE CAST(0 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

type managedHistorySink struct{ calls int }

func (s *managedHistorySink) Emit(ctx context.Context, e ports.ExecutionEvent) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if e.Sequence != uint64(s.calls+1) || len(e.CheckpointState) == 0 {
		return errors.New("unexpected original deterministic event")
	}
	s.calls++
	return nil
}

func TestManagedV1OriginalExecutionFinalizerHistoricalReader(t *testing.T) {
	principal, job := currentSessionAccessFixture(t)
	_, base, _ := canonicalFixture(t)
	f := &managedV1HistorySQL{canonicalBudgetFixture: base, principal: principal, job: job, counters: []driver.Value{int64(0), int64(1)}, state: LocalTerminalCompletionSnapshot{
		Session: principal.session, LeasePresent: true, CountersPresent: true, ActiveRuns: 1,
		material: map[string][]driver.Value{}, eventIndex: map[string][]driver.Value{}, eventRecords: map[string]string{},
	}}
	f.at = principal.at
	f.lease = domain.Lease{ID: "managed-v1-history-lease", TenantID: job.Run.TenantID, RunID: job.Run.ID, AttemptID: job.Attempt.ID, WorkerID: job.Attempt.WorkerID, FenceToken: 1, AcquiredAt: job.Run.CreatedAt, ExpiresAt: f.at.Add(time.Minute)}
	if err := f.lease.ValidateForAttempt(job.Run, job.Attempt); err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return f.at }, nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db, Options{AttemptEffectGrantIssuer: issuer})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	// Use the original admission projection writer, not a fabricated locator.
	if err := store.Transact(ctx, job.Run.TenantID, func(state ports.StateTx) error {
		return selectExplanationTx(ctx, state.(*stateTx), job.Run, job.Attempt, job.Job.ExecutionPlacementV2, f.at)
	}); err != nil {
		t.Fatalf("original selection writer: %v SQL=%s", err, f.lastSQL)
	}
	owned, err := store.ReserveAttemptEffect(ctx, ports.ReserveAttemptEffectRequestV1{TenantID: job.Run.TenantID, RunID: job.Run.ID, AttemptID: job.Attempt.ID, LeaseID: f.lease.ID, FenceToken: f.lease.FenceToken, PhysicalInvocationClaimID: "managed-v1-history-original-claim"})
	if err != nil || owned.Status != ports.AttemptEffectOwnedV1 || owned.Grant == nil {
		t.Fatalf("public fresh reserve: status=%s err=%v SQL=%s", owned.Status, err, f.lastSQL)
	}
	substrate, err := serverlessharness.NewInProcessExecutionSubstrateV1(func() time.Time { return f.at }, issuer, sessionlessharness.DeterministicFixtureDescriptorV1())
	if err != nil {
		t.Fatal(err)
	}
	preparer, err := serverlessharness.NewExactExecutionPreparerV1(func() time.Time { return f.at }, issuer, func(a domain.ServerlessInvocationAuthorityV1) (serverlessharness.SubstrateRegistrationV1, error) {
		if err := sessionlessharness.ValidateDeterministicFixtureInvocationAuthorityV2(a); err != nil {
			return serverlessharness.SubstrateRegistrationV1{}, err
		}
		return serverlessharness.SubstrateRegistrationV1{Binding: a.SubstrateBinding, Enabled: true, Driver: substrate}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := preparer.PrepareExecution(ctx, owned)
	if err != nil {
		t.Fatal("original exact preparation", err)
	}
	harness, err := deterministicharness.New(deterministicharness.Config{Turns: 1, Artifacts: 1})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return f.at }, sessionlessharness.Registration{Descriptor: sessionlessharness.DeterministicFixtureDescriptorV1(), Enabled: true, ValidateBinding: sessionlessharness.ValidateDeterministicFixtureBindingV1, Driver: harness})
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	if err := os.Mkdir(filepath.Join(work, "outputs"), 0o700); err != nil {
		t.Fatal(err)
	}
	sink := &managedHistorySink{}
	result, evidence, err := execution.Execute(ctx, ports.ExecutionRequest{TenantID: job.Run.TenantID, OwnerUserID: job.Job.CredentialOwnerUserID, RunID: job.Run.ID, SessionID: job.Run.SessionID, TriggerEventID: job.Run.TriggerEventID, AttemptID: job.Attempt.ID, WorkDir: work, ContextSnapshot: job.Job.ContextSnapshot, ExecutionPlacementV2: job.Job.ExecutionPlacementV2, HarnessBinding: job.Job.HarnessBinding, SubstrateBinding: job.Job.SubstrateBinding, AdmissionCostCeiling: job.Job.AdmissionCostCeiling}, sink, registry)
	if err != nil || sink.calls != 1 || len(result.Outputs) != 1 || result.ProviderEvidence == nil {
		t.Fatalf("actual in-process execution: err=%v events=%d outputs=%d", err, sink.calls, len(result.Outputs))
	}
	blobs := &localEntryBlobs{}
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(job.Run.ID), TenantID: job.Run.TenantID, RunID: job.Run.ID, CreatedAt: f.at}
	// The real harness file is retained unchanged. This is a bounded storage
	// fixture, not a claim that private Manager.uploadOutputs was exercised.
	for _, output := range result.Outputs {
		if filepath.Base(output.RelativePath) != output.RelativePath {
			t.Fatal("unexpected deterministic path")
		}
		file, err := os.Open(filepath.Join(work, "outputs", output.RelativePath))
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(body) > 1<<20 {
			t.Fatal("bounded actual output read", readErr, closeErr)
		}
		ref, err := blobs.Put(ctx, job.Run.TenantID, domain.SessionRunObjectPrefix(job.Run.TenantID, job.Run.SessionID, job.Run.ID)+"managed-proof/"+output.Name, bytes.NewReader(body))
		clear(body)
		if err != nil {
			t.Fatal(err)
		}
		manifest.Artifacts = append(manifest.Artifacts, domain.Artifact{Name: output.Name, MediaType: output.MediaType, Blob: ref})
	}
	plan, err := canonicaloutput.EncodeCompletionV1(job.Run.ID, job.Job.Limits, result, manifest.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plan.Release)
	materials := ports.NewCanonicalEventMaterialSetV1()
	t.Cleanup(materials.Release)
	var drafts []domain.SessionEventDraft
	for _, event := range plan.Events() {
		draft, prepared, err := canonicaloutput.PutEventDraftV1(ctx, blobs, job.Run, event.ID, event.Kind, event.Payload, f.at)
		if err != nil {
			t.Fatal(err)
		}
		draft.DisplayText = event.DisplayText
		if !materials.Adopt(event.ID, prepared) {
			t.Fatal("original canonical material")
		}
		drafts = append(drafts, draft)
	}
	if err := store.CompleteWorkerJob(ctx, ports.WorkerCompletion{TenantID: job.Run.TenantID, RunID: job.Run.ID, AttemptID: job.Attempt.ID, ReservationID: job.Reservation.ID, LeaseID: f.lease.ID, Fence: f.lease.FenceToken, At: f.at, Manifest: manifest, Events: drafts, PreparedEvents: materials, SubstrateEvidence: &evidence}); err != nil {
		t.Fatalf("public canonical finalizer: %v SQL=%s", err, f.lastSQL)
	}
	if f.state.Finalizations != 1 || len(f.state.FinalDigest) != 64 || f.state.EvidenceRecord == "" || len(f.state.Events) != 1 {
		t.Fatal("actual V1 finalizer did not persist complete output")
	}
	nilSeal, err := runFinalizationDigest(domain.RunSucceeded, &manifest, drafts, nil, nil)
	if err != nil || nilSeal == f.state.FinalDigest {
		t.Fatal("managed proof must actually require non-nil original evidence", err)
	}
	var terminal domain.Run
	if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &terminal) != nil || terminal.Status != domain.RunSucceeded {
		t.Fatal("actual terminal Run missing")
	}
	var records []sessioncontext.EventPayload
	for _, event := range f.state.Events {
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: bytes.Clone(blobs.objects[event.Payload.Key])})
	}
	t.Cleanup(func() {
		for _, record := range records {
			clear(record.Payload)
		}
	})
	// The genuine finalizer above has finished. Remove the old operational
	// rows before the first positive read, retaining only historical facts.
	delete(f.rows, "attempts")
	delete(f.rows, "worker_jobs")
	f.lease = domain.Lease{}
	f.state.LeasePresent = false
	f.historyOnly = true
	beforeWrites, beforeBlobs := f.state.Mutations, blobs.calls
	read := func() error {
		return store.ownedAuthorityTx(ctx, func(call context.Context, q *ownedAuthorityQuery) error {
			ids := []domain.RunID{terminal.ID}
			runs := map[domain.RunID]domain.Run{terminal.ID: terminal}
			markers := map[domain.RunID]string{terminal.ID: f.state.FinalDigest}
			mat, err := prepareLocalTerminalLegacyMaterials(q, ids, map[domain.RunID][]sessioncontext.EventPayload{terminal.ID: records}, runs, markers)
			if err != nil {
				return err
			}
			manifests, err := readLocalTerminalLegacyManifests(call, q, ids, mat, runs)
			if err != nil {
				return err
			}
			v, err := readLocalTerminalLegacySnapshot(call, q, terminal, f.state.FinalDigest, mat[terminal.ID], manifests)
			if err == nil && (v.Evidence == nil || v.Effect == nil || v.Head == nil || !reflect.DeepEqual(v.Manifest, manifest)) {
				return errors.New("historical reader omitted genuine persisted evidence or manifest")
			}
			return err
		})
	}
	if err := read(); err != nil {
		t.Fatalf("actual persisted managed V1 history: %v SQL=%s", err, f.lastSQL)
	}
	if f.headReads == 0 || f.evidenceReads == 0 {
		t.Fatal("managed non-nil-evidence locator was not used")
	}
	if f.historicalTTLReads != 0 || f.state.Mutations != beforeWrites || blobs.calls != beforeBlobs {
		t.Fatal("positive historical validation used TTL state, wrote or replayed output")
	}
	for _, fault := range []string{"missing_head", "corrupt_head", "missing_evidence", "corrupt_evidence", "missing_manifest", "corrupt_manifest"} {
		t.Run(fault, func(t *testing.T) {
			head := append([]driver.Value(nil), f.rows["run_explanation_heads_v1"]...)
			record, digest, originalManifest := f.state.EvidenceRecord, f.state.EvidenceDigest, f.state.Manifest
			t.Cleanup(func() {
				f.rows["run_explanation_heads_v1"] = head
				f.state.EvidenceRecord, f.state.EvidenceDigest, f.state.Manifest = record, digest, originalManifest
				if err := read(); err != nil {
					t.Errorf("restored clean V1 history after %s: %v", fault, err)
				}
				if f.historicalTTLReads != 0 || f.state.Mutations != beforeWrites || blobs.calls != beforeBlobs {
					t.Error("restored historical validation used TTL state, wrote or replayed output")
				}
			})
			switch fault {
			case "missing_head":
				f.rows["run_explanation_heads_v1"] = nil
			case "corrupt_head":
				f.rows["run_explanation_heads_v1"] = []driver.Value{"{}"}
			case "missing_evidence":
				f.state.EvidenceRecord = ""
			case "corrupt_evidence":
				f.state.EvidenceRecord = "{}"
			case "missing_manifest":
				f.state.Manifest = domain.ArtifactManifest{}
			case "corrupt_manifest":
				f.state.Manifest.Artifacts = nil
			}
			if err := read(); err == nil {
				t.Fatal("corrupt/missing persisted V1 material admitted")
			}
			if f.state.Mutations != beforeWrites || blobs.calls != beforeBlobs {
				t.Fatal("historical validation repaired or replayed output")
			}
		})
	}
}
