package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type inlineNativeAuthorityClockV2 struct{ at time.Time }

func (c inlineNativeAuthorityClockV2) Now() time.Time { return c.at }

// Revocation must use the original worker identity, never allocate another one.
type inlineNativeNoIDsV2 struct{}

func (inlineNativeNoIDsV2) NewID(context.Context, ports.IDKind) (string, error) {
	return "", errors.New("inline native revocation unexpectedly requested a new identity")
}

func inlineNativeAuthorityTimeV2(t *testing.T, f *inlineNativeFixtureV2) time.Time {
	t.Helper()
	var at time.Time
	if err := f.db.QueryRowContext(f.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil || at.IsZero() {
		t.Fatalf("sample original inline authority DB clock: %v", err)
	}
	return at.UTC().Truncate(time.Microsecond)
}

func requireInlineNativeAuthorityWindowV2(t *testing.T, f *inlineNativeFixtureV2) {
	t.Helper()
	at := inlineNativeAuthorityTimeV2(t, f)
	if err := f.ctx.Err(); err != nil || !at.Before(f.prepare.ExpiresAt) {
		t.Fatalf("authority-loss oracle reached original expiry/caller bound: at=%s expires=%s err=%v", at, f.prepare.ExpiresAt, err)
	}
}

func cancelInlineNativeRunV2(t *testing.T, f *inlineNativeFixtureV2) {
	t.Helper()
	auth := f.prepare.Authorization
	at := inlineNativeAuthorityTimeV2(t, f)
	if err := f.store.Transact(f.ctx, auth.TenantID, func(tx ports.StateTx) error {
		run, found, err := tx.GetRun(f.ctx, auth.RunID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("original inline Run absent before cancellation")
		}
		if err := run.RequestCancellation(at); err != nil {
			return err
		}
		run.UpdatedAt = at
		return tx.PutRun(f.ctx, run)
	}); err != nil {
		t.Fatal("cancel original canonical Run", err)
	}
	loaded, found, err := f.store.LoadWorkerJob(f.ctx, auth.TenantID, auth.RunID)
	if err != nil || !found || loaded.Run.CancellationRequestedAt == nil || !loaded.Run.CancellationRequestedAt.Equal(at) || loaded.Run.Status != domain.RunRunning {
		t.Fatalf("canonical cancellation was not durably observed: found=%t status=%s err=%v", found, loaded.Run.Status, err)
	}
}

func revokeInlineNativeWorkerV2(t *testing.T, f *inlineNativeFixtureV2) {
	t.Helper()
	auth := f.prepare.Authorization
	original, found, err := f.store.LoadAttachedWorker(f.ctx, auth.TenantID, auth.OwnerUserID, auth.WorkerID)
	if err != nil || !found {
		t.Fatalf("load original inline worker for revocation: found=%t err=%v", found, err)
	}
	at := inlineNativeAuthorityTimeV2(t, f)
	service, err := attachedworker.New(attachedworker.Config{Clock: inlineNativeAuthorityClockV2{at: at}, IDs: inlineNativeNoIDsV2{}, MaxEnrollmentTTL: time.Minute, EnrollmentRetention: time.Hour}, f.store)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := service.Revoke(f.ctx, auth.TenantID, auth.OwnerUserID, attachedworker.WorkerRevisionRequest{WorkerID: auth.WorkerID, ExpectedRevision: original.Revision})
	if err != nil || revoked.DesiredState != domain.AttachedWorkerDesiredRevoked || revoked.EnrollmentGeneration != original.EnrollmentGeneration+1 || revoked.ConnectionGeneration != original.ConnectionGeneration+1 {
		t.Fatalf("actual worker revocation did not advance both original fences: %v", err)
	}
	persisted, found, err := f.store.LoadAttachedWorker(f.ctx, auth.TenantID, auth.OwnerUserID, auth.WorkerID)
	if err != nil || !found || !reflect.DeepEqual(persisted, revoked) {
		t.Fatalf("worker revocation not committed before body resumes: found=%t err=%v", found, err)
	}
}

// Only native transactional currentness is under test. The declared producer
// metadata and immutable blob peer remain synthetic, not real OAuth/native
// execution or a proof that an already-started external Put can be recalled.
func TestAttachedInlineOutputYDBAuthorityLostBeforeCopy(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *inlineNativeFixtureV2)
	}{
		{name: "canonical_run_cancelled", mutate: cancelInlineNativeRunV2},
		{name: "worker_revoked", mutate: revokeInlineNativeWorkerV2},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newInlineNativeFixtureV2(t)
			request := f.prepared(t)
			reader := &inlineNativeClaimReaderV2{ctx: f.ctx, db: f.db, auth: request.Authorization, nonce: request.Nonce, fp: request.Fingerprint,
				body: bytes.NewReader(f.body), entered: make(chan struct{}), observed: make(chan error, 1), resume: make(chan struct{})}
			type outcome struct {
				result attachedworkeroutput.AttachedWorkerInlineResultV2
				err    error
			}
			done, joined := make(chan outcome, 1), make(chan struct{})
			go func() {
				defer close(joined)
				result, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), reader)
				done <- outcome{result: result, err: err}
			}()
			t.Cleanup(func() {
				f.cancel()
				reader.release.Do(func() { close(reader.resume) })
				bound, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				select {
				case <-joined:
				case <-bound.Done():
					select {
					case <-joined:
					default:
						t.Error("authority-loss Submit did not join bounded cleanup")
					}
				}
			})
			select {
			case <-reader.entered:
			case early := <-done:
				t.Fatalf("Submit failed before committed claim observation: %v", early.err)
			case <-f.ctx.Done():
				t.Fatal("authority-loss first Read barrier", f.ctx.Err())
			}
			if err := <-reader.observed; err != nil {
				t.Fatal("uncommitted body claim before authority mutation", err)
			}
			before, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
			if err != nil || before.Stage != attachedworkeroutput.InlineBodyClaimedV2 || f.blobs.writes() != [2]uint64{} {
				t.Fatalf("expected committed no-write body claim: stage=%s err=%v", before.Stage, err)
			}
			test.mutate(t, f)
			// Expiry returns the same denial class. Exclude it explicitly using
			// the original DB authority window, never a renewed test deadline.
			requireInlineNativeAuthorityWindowV2(t, f)
			reader.release.Do(func() { close(reader.resume) })
			var completed outcome
			select {
			case completed = <-done:
			case <-f.ctx.Done():
				t.Fatal("authority-loss Submit did not finish in original window", f.ctx.Err())
			}
			<-joined
			requireInlineNativeAuthorityWindowV2(t, f)
			if !errors.Is(completed.err, domain.ErrMembershipDenied) || completed.result.Stage != "" || f.blobs.writes() != [2]uint64{} {
				t.Fatalf("authority loss still promoted original body: stage=%s writes=%v err=%v", completed.result.Stage, f.blobs.writes(), completed.err)
			}
			denied := new(inlineNativeDeniedReaderV2)
			if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 {
				t.Fatalf("authority loss reopened original body claim: reads=%d err=%v", denied.reads, err)
			}
			recovered, err := f.store.ReconcileAttachedWorkerInlineOutput(f.ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: request.Authorization, Nonce: request.Nonce, Fingerprint: request.Fingerprint, BodySendBurned: true})
			if !errors.Is(err, domain.ErrMembershipDenied) || recovered.Capability != "" || recovered.Stage != "" {
				t.Fatalf("reconciliation revived revoked/cancelled authority: stage=%s err=%v", recovered.Stage, err)
			}
			requireInlineNativeAuthorityWindowV2(t, f)
			after, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
			if err != nil || !reflect.DeepEqual(before, after) || f.blobs.writes() != [2]uint64{} {
				t.Fatalf("authority loss erased/replaced no-write custody: writes=%v err=%v", f.blobs.writes(), err)
			}
		})
	}
}
