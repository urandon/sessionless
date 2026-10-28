//go:build ydbintegration && (darwin || linux)

package attachedworkerstack_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Withdrawal is not revocation of membership/worker/connection. It denies NEW
// generation admission without turning the published head into a fixed pin,
// but must not discard an accepted original producer's READY/ACK custody.
func TestAttachedResourceGenerationYDBJoinedGateRemovalPreservesOriginalCustody(t *testing.T) {
	f := newJoinedGenerationScenario(t)
	baseline := f.baseline.Clone()
	first := f.ingest(1)
	if err := f.wire.removeGateAfterAcceptedInput(); err != nil {
		t.Fatal(err)
	}
	daemon, err := attachedworkerdaemon.NewDaemon(attachedworkerdaemon.DaemonConfig{IdleBackoff: 10 * time.Millisecond, ShutdownGrace: 5 * time.Second, ReportGrace: 10 * time.Second}, f.adapter, f.stack, f.adapter)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = daemon.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("original gate-removal daemon: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("original gate-removal daemon did not stop")
		}
	})
	for {
		select {
		case status := <-daemon.Updates():
			if status.Failed != 0 {
				t.Fatalf("original accepted N failed: %s", status.LastFailureCode)
			}
			if status.Committed != 1 {
				continue
			}
			f.waitRetired(first, done)
			f.assertCanonicalResult(first, 1, 1)
			drainCtx, drainCancel := context.WithTimeout(f.ctx, 5*time.Second)
			drainErr := daemon.Drain(drainCtx)
			drainCancel()
			if drainErr != nil {
				t.Fatal(drainErr)
			}
			seen := f.wire.committedSnapshot()
			if !seen.gateRemoved || seen.gateCalls != 3 || f.installation.LaunchCount() != 1 {
				t.Fatalf("accepted producer did not use all actual withdrawn-gate inline routes: removed=%t calls=%d launches=%d", seen.gateRemoved, seen.gateCalls, f.installation.LaunchCount())
			}
			if err := f.stack.SynchronizePreparedGenerationV3(f.ctx, f.session); err != nil {
				t.Fatal("actual own successor ACK", err)
			}
			head, found, err := f.store.LoadAttachedResourceGenerationHeadV1(f.ctx, f.worker.TenantID, f.worker.OwnerUserID, f.resource)
			if err != nil || !found || head.CredentialGeneration != 2 || head.Revoked {
				t.Fatalf("not a valid acknowledged successor before denial: found=%t err=%v", found, err)
			}
			local, found, err := f.credentialStore.LoadCredentialBinding(f.ctx, f.worker.TenantID, f.resource)
			if err != nil || !found || local.Generation != 2 {
				t.Fatalf("own WB did not retain genuine successor: found=%t err=%v", found, err)
			}
			metadata, err := joinedGenerationReadResourceMetadata(f.ctx, f.db, f.worker.TenantID, f.resource)
			if err != nil || metadata != f.metadataBefore {
				t.Fatalf("gate test accidentally changed operational eligibility: %v", err)
			}
			// This is the actual binder/ingress-created NEW outbox, not an
			// authored placement/job or replay of the original admission.
			outbox := f.canonicalOutbox(2)
			if outbox.RunID == first.Job.RunID || outbox.AttemptID == first.Job.AttemptID {
				t.Fatal("negative reused the original job")
			}
			if err := outbox.HarnessBinding.ValidateForScope(f.worker.TenantID, f.worker.OwnerUserID, outbox.RunID, outbox.AttemptID, outbox.ExecutionPlacementV2); err != nil {
				t.Fatal("malformed successor cannot qualify gate denial", err)
			}
			if err := outbox.HarnessBinding.ValidateAt(f.installation.Now()); err != nil {
				t.Fatal("expired successor cannot qualify gate denial", err)
			}
			admitted, err := f.admitOutbox(f.gateRemovedStore, outbox, 2)
			if !errors.Is(err, domain.ErrMembershipDenied) || admitted.Admitted {
				t.Fatalf("withdrawn generation gate did not cause exact denial: admitted=%t err=%v", admitted.Admitted, err)
			}
			for _, query := range []string{
				`SELECT COUNT(*) FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`,
				`SELECT COUNT(*) FROM leases WHERE tenant_id=$1 AND run_id=$2`,
				`SELECT COUNT(*) FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`,
			} {
				var count uint64
				if err := f.db.QueryRowContext(f.ctx, query, f.worker.TenantID, outbox.RunID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("denied successor left allocation: count=%d err=%v", count, err)
				}
			}
			var reservations uint64
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, f.worker.TenantID, "joined-reservation-2").Scan(&reservations); err != nil || reservations != 0 {
				t.Fatalf("denied successor reserved quota: count=%d err=%v", reservations, err)
			}
			if err := f.store.Transact(f.ctx, f.worker.TenantID, func(state ports.StateTx) error {
				run, found, err := state.GetRun(f.ctx, outbox.RunID)
				if err != nil {
					return err
				}
				if !found || run.Status != domain.RunCreated {
					return errors.New("denied canonical successor unexpectedly admitted")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			retired, found, err := f.store.LoadAttachedWorkerAttempt(f.ctx, f.worker.TenantID, f.worker.OwnerUserID, f.worker.ID)
			if err != nil || !found || retired.State != domain.AttachedWorkerAttemptRetired || retired.RunID != first.Job.RunID || retired.AttemptID != first.Job.AttemptID {
				t.Fatalf("denied successor replaced original retired head: found=%t err=%v", found, err)
			}
			f.assertCanonicalResult(first, 1, 1)
			after := f.wire.committedSnapshot()
			if !reflect.DeepEqual(after, seen) || !reflect.DeepEqual(f.baseline, baseline) || f.installation.LaunchCount() != 1 {
				t.Fatal("fresh denied admission repeated an original invocation or changed its commitments")
			}
			return
		case <-done:
			t.Fatalf("original daemon stopped before accepted custody completed: %v", runErr)
		case <-ctx.Done():
			t.Fatal("withdrawn generation custody deadline", ctx.Err())
		}
	}
}
