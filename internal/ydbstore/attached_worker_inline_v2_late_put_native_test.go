package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The first physical Put is invoked but its cooperative peer has not accepted
// bytes yet. Revocation is ordered before acceptance, not inferred from a
// timeout or an absent object. This is a synthetic peer, NOT Object Storage.
type inlineNativeLatePutV2 struct {
	*inlineNativeBlobsV2
	entered chan struct{}
	resume  chan struct{}
	first   sync.Once
	release sync.Once
}

func (b *inlineNativeLatePutV2) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	b.first.Do(func() {
		close(b.entered)
		select {
		case <-b.resume:
		case <-ctx.Done():
		}
	})
	return b.inlineNativeBlobsV2.Put(ctx, tenant, key, body)
}

func TestAttachedInlineOutputYDBRevocationDuringCanonicalPut(t *testing.T) {
	f := newInlineNativeFixtureV2(t)
	request := f.prepared(t)
	peer := &inlineNativeLatePutV2{inlineNativeBlobsV2: f.blobs, entered: make(chan struct{}), resume: make(chan struct{})}
	type outcome struct {
		result attachedworkeroutput.AttachedWorkerInlineResultV2
		err    error
	}
	done, joined := make(chan outcome, 1), make(chan struct{})
	go func() {
		defer close(joined)
		result, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, peer, request, int64(len(f.body)), bytes.NewReader(f.body))
		done <- outcome{result: result, err: err}
	}()
	t.Cleanup(func() {
		f.cancel()
		peer.release.Do(func() { close(peer.resume) })
		bound, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-joined:
		case <-bound.Done():
			select {
			case <-joined:
			default:
				t.Error("late canonical Put did not join bounded cleanup")
			}
		}
	})
	select {
	case <-peer.entered:
	case early := <-done:
		t.Fatalf("Submit ended before original canonical Put: %v", early.err)
	case <-f.ctx.Done():
		t.Fatal("late Put barrier reached original caller bound", f.ctx.Err())
	}
	before, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || before.Stage != attachedworkeroutput.InlineCanonicalCopyingV2 || len(before.CopyOwnerID) != 64 || before.CanonicalDigest == "" || before.Materialization.Completion == nil || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("physical Put began without committed pending write custody: stage=%s writes=%v err=%v", before.Stage, f.blobs.writes(), err)
	}
	revokeInlineNativeWorkerV2(t, f)
	requireInlineNativeAuthorityWindowV2(t, f)
	peer.release.Do(func() { close(peer.resume) })
	var completed outcome
	select {
	case completed = <-done:
	case <-f.ctx.Done():
		t.Fatal("late Put completion reached original caller bound", f.ctx.Err())
	}
	<-joined
	requireInlineNativeAuthorityWindowV2(t, f)
	if !errors.Is(completed.err, domain.ErrMembershipDenied) || completed.result.Stage != "" || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("late accepted writes escaped revoked READY check: stage=%s writes=%v err=%v", completed.result.Stage, f.blobs.writes(), completed.err)
	}
	after, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || !reflect.DeepEqual(before, after) || f.blobs.copyOwner != before.CopyOwnerID || f.blobs.copyDigest != before.CanonicalDigest {
		t.Fatalf("revocation discarded or replaced possible-write custody: stage=%s err=%v", after.Stage, err)
	}
	denied := new(inlineNativeDeniedReaderV2)
	if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, peer, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 {
		t.Fatalf("late writes minted another body permission: reads=%d err=%v", denied.reads, err)
	}
	recovered, err := f.store.ReconcileAttachedWorkerInlineOutput(f.ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: request.Authorization, Nonce: request.Nonce, Fingerprint: request.Fingerprint, BodySendBurned: true})
	if !errors.Is(err, domain.ErrMembershipDenied) || recovered.Capability != "" || recovered.Stage != "" || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("revoked late-write reconciliation replayed or claimed success: stage=%s writes=%v err=%v", recovered.Stage, f.blobs.writes(), err)
	}
	requireInlineNativeAuthorityWindowV2(t, f)
	final, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || !reflect.DeepEqual(final, before) {
		t.Fatal("reconcile/replay cleared original pending canonical refs", err)
	}
}
