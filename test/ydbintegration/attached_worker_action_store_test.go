//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerActionPlanClaimIsConcurrentReplayableAndOwnerScoped(t *testing.T) {
	store, _ := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	now := time.Now().UTC().Truncate(time.Microsecond)
	suffix := uniqueID("worker-action")
	plan := domain.AttachedWorkerActionPlan{
		Version: domain.AttachedWorkerActionPlanVersionV1, TenantID: domain.TenantID("tenant-" + suffix),
		OwnerUserID: domain.UserID("owner-" + suffix), ID: domain.AttachedWorkerActionPlanID("wap-" + suffix),
		WorkerID: domain.AttachedWorkerID("wrk-" + suffix), Action: domain.AttachedWorkerActionDrain,
		State: domain.AttachedWorkerActionPlanned, WorkerRevision: 4, EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ConfirmationDigest: actionStoreDigest("confirmation-" + suffix), CreatedAt: now, ExpiresAt: now.Add(time.Minute), Revision: 1,
	}
	if err := store.CreateAttachedWorkerActionPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateAttachedWorkerActionPlan(ctx, plan); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}

	const contenders = 8
	type outcome struct {
		claim  ports.AttachedWorkerActionClaim
		result ports.AttachedWorkerActionClaimResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, contenders)
	var wait sync.WaitGroup
	for contender := 0; contender < contenders; contender++ {
		contender := contender
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			claim := ports.AttachedWorkerActionClaim{
				TenantID: plan.TenantID, OwnerUserID: plan.OwnerUserID, WorkerID: plan.WorkerID, PlanID: plan.ID,
				OperationID: domain.AttachedWorkerActionOperationID(fmt.Sprintf("wao-%s-%d", suffix, contender)),
				Action:      plan.Action, ConfirmationDigest: plan.ConfirmationDigest,
				IdempotencyDigest: actionStoreDigest(fmt.Sprintf("idempotency-%s-%d", suffix, contender)), Now: now,
			}
			result, err := store.ClaimAttachedWorkerActionPlan(ctx, claim)
			results <- outcome{claim: claim, result: result, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var winner outcome
	claimed, conflicted := 0, 0
	for candidate := range results {
		if candidate.err != nil {
			t.Fatalf("concurrent claim %q: %v", candidate.claim.OperationID, candidate.err)
		}
		switch candidate.result.Status {
		case ports.AttachedWorkerActionClaimed:
			claimed++
			winner = candidate
		case ports.AttachedWorkerActionConflict:
			conflicted++
		default:
			t.Fatalf("concurrent claim %q status = %q", candidate.claim.OperationID, candidate.result.Status)
		}
	}
	if claimed != 1 || conflicted != contenders-1 {
		t.Fatalf("claimed=%d conflicted=%d, want 1/%d", claimed, conflicted, contenders-1)
	}

	replay, err := store.ClaimAttachedWorkerActionPlan(ctx, winner.claim)
	if err != nil || replay.Status != ports.AttachedWorkerActionReplayed || replay.Plan.OperationID != winner.claim.OperationID {
		t.Fatalf("claim replay = %#v, %v", replay, err)
	}

	completed := winner.result.Plan
	completed.State = domain.AttachedWorkerActionSucceeded
	completed.Result = domain.AttachedWorker{
		TenantID: plan.TenantID, OwnerUserID: plan.OwnerUserID, ID: plan.WorkerID, DisplayName: "Action worker",
		IdentityPublicKey: bytes.Repeat([]byte{0x51}, ed25519.PublicKeySize), EnrollmentGeneration: plan.EnrollmentGeneration,
		ConnectionGeneration: plan.ConnectionGeneration, DesiredState: domain.AttachedWorkerDesiredDrain,
		ObservedState: domain.AttachedWorkerObservedDraining, Revision: plan.WorkerRevision + 1,
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	}
	completed.CompletedAt = now.Add(time.Second)
	completed.Revision++
	if err := store.CompleteAttachedWorkerAction(ctx, completed); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteAttachedWorkerAction(ctx, completed); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}

	loaded, found, err := store.LoadAttachedWorkerActionOperation(ctx, plan.TenantID, plan.OwnerUserID, completed.OperationID)
	if err != nil || !found || loaded.State != domain.AttachedWorkerActionSucceeded || loaded.Result.Revision != plan.WorkerRevision+1 {
		t.Fatalf("load operation = %#v found=%t err=%v", loaded, found, err)
	}
	foreign, found, err := store.LoadAttachedWorkerActionOperation(ctx, plan.TenantID, domain.UserID("other-"+suffix), completed.OperationID)
	if err != nil || found || foreign.ID != "" {
		t.Fatalf("foreign operation = %#v found=%t err=%v", foreign, found, err)
	}
}

func actionStoreDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
