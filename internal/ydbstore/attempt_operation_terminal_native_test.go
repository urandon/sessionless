//go:build ydbintegration

package ydbstore

import (
	"bytes"
	"context"
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

// Exercise the canonical worker-failure transaction, not a second operation
// terminal machine. The original result came from a genuine credential-free
// TLS fixture and its authenticated observation was committed beforehand.
// This proves retained storage on failure; it is not a production backend loop
// or a successful public serving/rollout gate.
func TestOwnedOperationYDBCanonicalWorkerFailureRetainsOriginalObservation(t *testing.T) {
	g, keeper, policy := nativeConsentGraph(t)
	model := nativeConsentModel(t, g, keeper, policy, []byte(`{"version":1,"kind":"model_response","text":"retained bounded result","calls":[]}`))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := g.f.store.db.ExecContext(ctx, `DELETE FROM run_finalizations WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID); err != nil {
			t.Error("cleanup exact own terminal receipt", err)
		}
	})
	var originalRaw, originalObservation string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT record,observation_record FROM attempt_operations_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND sequence=$3`, g.run.TenantID, g.attempt.ID, uint64(1)).Scan(&originalRaw, &originalObservation); err != nil {
		t.Fatal("actual immutable model reservation/observation", err)
	}
	var reservation domain.AttemptOperationReservationV1
	if err := json.Unmarshal([]byte(originalRaw), &reservation); err != nil || reservation.Validate() != nil {
		t.Fatal("actual original persisted reservation", err)
	}
	rd, err := reservation.Digest()
	if err != nil || rd != model.ReservationDigest {
		t.Fatal("model observation is not joined to its actual original reservation", err)
	}
	accounting, accountingRaw, _ := nativeAccountingBytes(t, g)
	loaded, found, err := g.f.store.LoadWorkerJob(g.ctx, g.run.TenantID, g.run.ID)
	if err != nil || !found || loaded.Run.Status != domain.RunRunning || loaded.Attempt.Status != domain.AttemptRunning {
		t.Fatalf("real current canonical worker before failure: found=%t run=%s attempt=%s err=%v", found, loaded.Run.Status, loaded.Attempt.Status, err)
	}
	at := nativeOperationClock{g.ctx, g.f.store}.Now()
	// Original bounded canonical material is prepared from actual bytes. This
	// test does not claim a persistent Object Storage write for that notice.
	body := []byte(`{"schema":"sessionless.run-terminal-notice.v1","code":"harness_failed","cancelled":false}`)
	eventID := domain.SessionEventID("operation-terminal-" + string(g.run.ID))
	draft := domain.SessionEventDraft{ID: eventID, Kind: domain.SessionEventSystemNotice, IdempotencyKey: domain.IdempotencyKey("operation-terminal-key-" + string(g.run.ID)), CreatedAt: at,
		Payload: domain.BlobRef{TenantID: g.run.TenantID, Key: domain.SessionEventObjectPrefix(g.run.TenantID, g.run.SessionID, eventID) + "payloads/sha256/" + operationBodyDigest(body) + ".json", SHA256: operationBodyDigest(body), Size: int64(len(body))}}
	if err := draft.ValidateForRun(loaded.Run); err != nil {
		t.Fatal("canonical worker failure notice", err)
	}
	materials := ports.NewCanonicalEventMaterialSetV1()
	t.Cleanup(materials.Release)
	if !materials.Adopt(draft.ID, ports.PrepareCanonicalDraftV1(loaded.Run, draft, body)) {
		t.Fatal("actual bounded canonical failure material rejected")
	}
	failure := ports.WorkerFailure{TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, AttemptID: loaded.Attempt.ID, ReservationID: loaded.Reservation.ID, LeaseID: g.lease.ID, Fence: g.lease.FenceToken, At: at, Code: "harness_failed", Events: []domain.SessionEventDraft{draft}, PreparedEvents: materials}
	foreignFence := failure
	foreignFence.Fence++
	if err := g.f.store.FailWorkerJob(g.ctx, foreignFence); err == nil {
		t.Fatal("foreign physical lease fence finalized original Run")
	}
	g.requireCount(t, "run_finalizations", 0)
	beforeFailure, found, err := g.f.store.LoadWorkerJob(g.ctx, g.run.TenantID, g.run.ID)
	if err != nil || !found || beforeFailure.Run.Status != domain.RunRunning || beforeFailure.Attempt.Status != domain.AttemptRunning {
		t.Fatalf("failed foreign fence mutated canonical state: found=%t run=%s attempt=%s err=%v", found, beforeFailure.Run.Status, beforeFailure.Attempt.Status, err)
	}
	if err := g.f.store.FailWorkerJob(g.ctx, failure); err != nil {
		t.Fatal("actual canonical worker failure transaction", err)
	}
	if err := g.f.store.FailWorkerJob(g.ctx, failure); err != nil {
		t.Fatal("exact original worker failure lost-ACK replay", err)
	}
	var run domain.Run
	var attempt domain.Attempt
	if err := g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		var present bool
		var err error
		run, present, err = state.GetRun(g.ctx, g.run.ID)
		if err != nil || !present {
			return errors.New("original canonical Run absent after failure")
		}
		attempt, present, err = state.GetAttempt(g.ctx, g.attempt.ID)
		if err != nil || !present {
			return errors.New("original canonical Attempt absent after failure")
		}
		return nil
	}); err != nil || run.Status != domain.RunFailed || attempt.Status != domain.AttemptFailed || run.FinishedAt == nil || attempt.FinishedAt == nil {
		t.Fatalf("canonical terminal state not committed: run=%s attempt=%s err=%v", run.Status, attempt.Status, err)
	}
	g.requireCount(t, "run_finalizations", 1)
	g.requireCount(t, "attempt_operations_v1", 1)
	g.requireCount(t, "attempt_operation_calls_v1", 1)
	clock := nativeOperationClock{g.ctx, g.f.store}
	issuer, err := serverlessharness.NewOperationCapabilityIssuer(clock.Now, strings.NewReader(strings.Repeat("terminal-new-process-key", 4)), 16)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := New(g.f.store.db, Options{OwnedAttemptEffectSourceVerifier: g.f.store.ownedAttemptEffectSourceVerifier, AttemptOperationIssuer: issuer, AttemptOperationBodies: keeper})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := resumed.RecoverAttemptOperation(g.ctx, ports.RecoverAttemptOperationRequestV1{Operation: reservation.Operation, ExpectedReservationDigest: rd})
	if err != nil || recovered.Capability != nil || recovered.Status != ports.AttemptOperationReconcileOnlyV1 || !reflect.DeepEqual(recovered.Reservation, reservation) {
		t.Fatalf("fresh issuer recovered send authority or lost terminal history: err=%v capability=%t status=%s", err, recovered.Capability != nil, recovered.Status)
	}
	observed, err := resumed.ReadAttemptOperationObservation(g.ctx, recovered.ObservationGrant)
	if err != nil || !reflect.DeepEqual(observed.Observation, model) {
		t.Fatalf("fresh Store lost original committed result after backend failure: err=%v gotOutcome=%s", err, observed.Observation.Outcome)
	}
	var afterRaw, afterObservation string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT record,observation_record FROM attempt_operations_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND sequence=$3`, g.run.TenantID, g.attempt.ID, uint64(1)).Scan(&afterRaw, &afterObservation); err != nil {
		t.Fatal("retained terminal record read", err)
	}
	afterAccounting, afterAccountingRaw, _ := nativeAccountingBytes(t, g)
	if afterRaw != originalRaw || afterObservation != originalObservation || accounting != afterAccounting || !bytes.Equal([]byte(accountingRaw), []byte(afterAccountingRaw)) {
		t.Fatal("worker failure or new-process recovery mutated original operation/result/debits")
	}
	foreign := reservation.Operation.Clone()
	foreign.Scope.TenantID = "terminal-foreign-tenant"
	if _, err := resumed.RecoverAttemptOperation(g.ctx, ports.RecoverAttemptOperationRequestV1{Operation: foreign, ExpectedReservationDigest: rd}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("terminal history permitted a foreign tenant: %v", err)
	}
}
