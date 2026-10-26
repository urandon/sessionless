package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Use the original platform failure transaction with its actual lease/fence,
// canonical notice uploader and prepared materials. Never fabricate a terminal
// Run or its deletion index. This is NOT a native-harness failure/TerminalACK
// report: that producer path remains a separately required qualification.
func failInlineNativeRunForDeletionV2(t *testing.T, f *inlineNativeFixtureV2) {
	t.Helper()
	auth := f.prepare.Authorization
	loaded, found, err := f.store.LoadWorkerJob(f.ctx, auth.TenantID, auth.RunID)
	if err != nil || !found || loaded.Run.Status != domain.RunRunning || loaded.Attempt.Status != domain.AttemptRunning {
		t.Fatalf("original canonical Run before platform failure: found=%t err=%v", found, err)
	}
	notice, err := canonicaloutput.EncodeFailureV1(auth.RunID, false, "harness_failed")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(notice.Payload) })
	// This distinct fake peer stores only the platform notice. It cannot issue
	// writes for the pending inline result, whose peer must remain untouched.
	notices := &attachedHistoryNativeBlobs{objects: make(map[string][]byte)}
	t.Cleanup(func() {
		notices.mu.Lock()
		defer notices.mu.Unlock()
		for _, body := range notices.objects {
			clear(body)
		}
	})
	at := inlineNativeAuthorityTimeV2(t, f)
	draft, material, err := canonicaloutput.PutEventDraftV1(f.ctx, notices, loaded.Run, notice.ID, notice.Kind, notice.Payload, at)
	if err != nil {
		t.Fatal("original canonical platform failure notice", err)
	}
	materials := ports.NewCanonicalEventMaterialSetV1()
	t.Cleanup(materials.Release)
	if !materials.Adopt(draft.ID, material) {
		t.Fatal("original platform failure material not adopted")
	}
	if err := f.store.FailWorkerJob(f.ctx, ports.WorkerFailure{TenantID: auth.TenantID, RunID: auth.RunID, AttemptID: auth.AttemptID,
		ReservationID: loaded.Reservation.ID, LeaseID: auth.LeaseID, Fence: auth.LeaseGeneration, At: at, Code: "harness_failed",
		Events: []domain.SessionEventDraft{draft}, PreparedEvents: materials}); err != nil {
		t.Fatal("actual platform failure finalizer before deletion", err)
	}
	if err := f.store.Transact(f.ctx, auth.TenantID, func(tx ports.StateTx) error {
		run, found, err := tx.GetRun(f.ctx, auth.RunID)
		if err != nil {
			return err
		}
		if !found || run.Status != domain.RunFailed || run.FinishedAt == nil {
			return errors.New("original canonical Run did not commit failed terminal state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Real native transactions prove pre-copy retirement fences a held reader.
// Producer metadata and both immutable storage peers are synthetic. No actual
// session/object purge, real harness, cloud or Object Storage is exercised.
func TestAttachedInlineOutputYDBDeletionRetiresHeldBodyBeforeCopy(t *testing.T) {
	f := newInlineNativeFixtureV2(t)
	auth := f.prepare.Authorization
	// The base fixture does not own this extra table. Register exact new-row
	// cleanup before the first possible deletion request, not after success.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := f.db.ExecContext(ctx, `DELETE FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`, auth.TenantID, f.prepare.Producer.SessionID); err != nil {
			t.Error("cleanup exact disposable inline deletion request", err)
		}
	})
	request := f.prepared(t)
	reader := &inlineNativeClaimReaderV2{ctx: f.ctx, db: f.db, auth: auth, nonce: request.Nonce, fp: request.Fingerprint,
		body: bytes.NewReader(f.body), entered: make(chan struct{}), observed: make(chan error, 1), resume: make(chan struct{})}
	done, joined := make(chan inlineNativeRaceResultV2, 1), make(chan struct{})
	go func() {
		defer close(joined)
		result, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), reader)
		done <- inlineNativeRaceResultV2{result: result, err: err}
	}()
	t.Cleanup(func() {
		f.cancel()
		reader.release.Do(func() { close(reader.resume) })
		bound, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-joined:
		case <-bound.Done():
			select {
			case <-joined:
			default:
				t.Error("held inline deletion Submit did not join cleanup")
			}
		}
	})
	select {
	case <-reader.entered:
	case early := <-done:
		t.Fatalf("Submit ended before deletion claim barrier: %v", early.err)
	case <-f.ctx.Done():
		t.Fatal("deletion native first Read barrier", f.ctx.Err())
	}
	if err := <-reader.observed; err != nil {
		t.Fatal("deletion reader lacked committed original claim", err)
	}
	before, err := readInlineNativeReceiptV2(f.ctx, f.db, auth)
	if err != nil || before.Stage != attachedworkeroutput.InlineBodyClaimedV2 || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("original no-write claim before deletion: stage=%s err=%v", before.Stage, err)
	}
	deletion := domain.SessionDeletion{TenantID: auth.TenantID, SessionID: f.prepare.Producer.SessionID, RequestedBy: auth.OwnerUserID,
		Reason: "controlled inline pre-copy retirement", State: domain.SessionDeletionRequested, RequestedAt: inlineNativeAuthorityTimeV2(t, f)}
	if _, err := f.store.RequestSessionDeletion(f.ctx, deletion); err == nil {
		t.Fatal("active original Run did not block deletion")
	} else {
		var invalid domain.ValidationError
		if !errors.As(err, &invalid) || invalid.Field != "session_deletion.runs" {
			t.Fatalf("active-Run deletion was denied for an unrelated reason: %v", err)
		}
	}
	unchanged, err := readInlineNativeReceiptV2(f.ctx, f.db, auth)
	if err != nil || !reflect.DeepEqual(before, unchanged) {
		t.Fatal("denied active-Run deletion changed receipt custody", err)
	}
	failInlineNativeRunForDeletionV2(t, f)
	requireInlineNativeAuthorityWindowV2(t, f)
	deletion.RequestedAt = inlineNativeAuthorityTimeV2(t, f)
	requested, err := f.store.RequestSessionDeletion(f.ctx, deletion)
	if err != nil || !reflect.DeepEqual(requested, deletion) {
		t.Fatal("actual terminal-Run deletion request did not retire pre-copy custody", err)
	}
	persisted, found, err := f.store.GetSessionDeletion(f.ctx, auth.TenantID, deletion.SessionID)
	if err != nil || !found || !reflect.DeepEqual(persisted, deletion) {
		t.Fatalf("native deletion request not committed: found=%t err=%v", found, err)
	}
	retired, err := readInlineNativeReceiptV2(f.ctx, f.db, auth)
	want := before
	want.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	if err != nil || !reflect.DeepEqual(retired, want) || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("native deletion changed identity or acquired copy authority: stage=%s writes=%v err=%v", retired.Stage, f.blobs.writes(), err)
	}
	reader.release.Do(func() { close(reader.resume) })
	select {
	case late := <-done:
		if !errors.Is(late.err, ydbstore.ErrAttachedWorkerAttemptConflict) || late.result != (attachedworkeroutput.AttachedWorkerInlineResultV2{}) {
			t.Fatalf("late body did not lose its retired stage comparison: %v", late.err)
		}
	case <-f.ctx.Done():
		t.Fatal("late deletion body did not finish in original window", f.ctx.Err())
	}
	<-joined
	requireInlineNativeAuthorityWindowV2(t, f)
	denied := new(inlineNativeDeniedReaderV2)
	if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 {
		t.Fatalf("retired native body acquired another Read: reads=%d err=%v", denied.reads, err)
	}
	reconciled, err := f.store.ReconcileAttachedWorkerInlineOutput(f.ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: auth,
		Nonce: request.Nonce, Fingerprint: request.Fingerprint, BodySendBurned: true})
	if !errors.Is(err, domain.ErrMembershipDenied) || reconciled != (attachedworkeroutput.AttachedWorkerInlineResultV2{}) {
		t.Fatalf("retired native deletion reconciliation revived export authority: %v", err)
	}
	after, err := readInlineNativeReceiptV2(f.ctx, f.db, auth)
	if err != nil || !reflect.DeepEqual(after, retired) || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("late body changed retired no-write custody: writes=%v err=%v", f.blobs.writes(), err)
	}
}
