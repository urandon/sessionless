package ydbstore

import (
	"context"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Storage-only Final child lifecycle. Fixture BlobRefs are not real objects;
// inventory acknowledgements below prove no cloud erasure or whole-#189
// retention. Per-Attempt funding/effect/consent lifecycle is still separate work.
func TestOwnedFinalAdmissionYDBSharedLifecycleHoldAndIndexedCleanup(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	registerOwnedFinalLifecycleCleanup(t, g)
	result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
	if err != nil {
		t.Fatal(err)
	}
	g.requireCommittedGraph(t, result)
	_, original := g.finalAdmission(t)
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	deletion := domain.SessionDeletion{TenantID: g.run.TenantID, SessionID: g.run.SessionID, RequestedBy: g.f.scope.owner, Reason: "test-owned final child cleanup", State: domain.SessionDeletionRequested, RequestedAt: at}
	if _, err := g.f.store.RequestSessionDeletion(g.ctx, deletion); err == nil {
		t.Fatal("active canonical Run did not fence destructive lifecycle")
	}
	terminalizeOwnedFinalTestRun(t, g)
	at = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	deletion.RequestedAt = at
	if _, err := g.f.store.RequestSessionDeletion(g.ctx, deletion); err != nil {
		t.Fatal(err)
	}
	hold := domain.SessionLegalHold{TenantID: g.run.TenantID, SessionID: g.run.SessionID, SetBy: g.f.scope.owner, State: domain.SessionLegalHoldActive, Reason: "test-owned legal hold", SetAt: at}
	if _, err := g.f.store.PutSessionLegalHold(g.ctx, hold); err != nil {
		t.Fatal(err)
	}
	if _, err := g.f.store.StartSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at); err == nil {
		t.Fatal("legal hold did not fence cleanup start")
	}
	if _, err := g.f.store.CompleteSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at, 0, 0); err == nil {
		t.Fatal("legal hold did not fence cleanup completion")
	}
	_, held := g.finalAdmission(t)
	if held != original {
		t.Fatal("denied lifecycle rewrote Final")
	}
	at = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if _, err := g.f.store.ReleaseSessionLegalHold(g.ctx, g.run.TenantID, g.run.SessionID, g.f.scope.owner, at); err != nil {
		t.Fatal(err)
	}
	if _, err := g.f.store.StartSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at); err != nil {
		t.Fatal(err)
	}
	inventory, err := g.f.store.BuildSessionDeletionInventory(g.ctx, g.run.TenantID, g.run.SessionID, maxSessionDeletionRows, maxSessionDeletionRows)
	if err != nil || inventory.OwnedAdmissionRows != 1 {
		t.Fatalf("indexed Final inventory count=%d err=%v", inventory.OwnedAdmissionRows, err)
	}
	at = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	completed, err := g.f.store.CompleteSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at, uint64(len(inventory.Objects)), inventory.TotalBytes)
	if err != nil || completed.State != domain.SessionDeletionCompleted {
		t.Fatalf("canonical Final child cleanup state=%s err=%v", completed.State, err)
	}
	for _, table := range []string{"attempt_execution_admissions_v1", "attempt_execution_admissions_by_session_v1", "sessions", "worker_jobs"} {
		g.requireCount(t, table, 0)
	}
	// Global current-profile revocation and its publication audit survive
	// Session erasure. This test intentionally claims no funding/effect erasure.
	g.requireCount(t, "owned_agent_profile_current_v1", 1)
	g.requireCount(t, "owned_profile_publication_audit_v1", 1)
	replayed, err := g.f.store.CompleteSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at, uint64(len(inventory.Objects)), inventory.TotalBytes)
	if err != nil || !reflect.DeepEqual(completed, replayed) {
		t.Fatalf("durable deletion tombstone replay changed: err=%v", err)
	}
}

func TestOwnedFinalAdmissionYDBOrphanIndexDeniesSharedCleanup(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	registerOwnedFinalLifecycleCleanup(t, g)
	result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
	if err != nil {
		t.Fatal(err)
	}
	g.requireCommittedGraph(t, result)
	terminalizeOwnedFinalTestRun(t, g)
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	deletion := domain.SessionDeletion{TenantID: g.run.TenantID, SessionID: g.run.SessionID, RequestedBy: g.f.scope.owner, Reason: "test-owned orphan index", State: domain.SessionDeletionRequested, RequestedAt: at}
	if _, err := g.f.store.RequestSessionDeletion(g.ctx, deletion); err != nil {
		t.Fatal(err)
	}
	if _, err := g.f.store.StartSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at); err != nil {
		t.Fatal(err)
	}
	// Deliberate corruption of this test's exact Final PK, never a supported
	// historical writer. The retained reverse key must block successful erasure.
	if _, err := g.f.store.db.ExecContext(g.ctx, `DELETE FROM attempt_execution_admissions_v1 WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID); err != nil {
		t.Fatal(err)
	}
	inventory, err := g.f.store.BuildSessionDeletionInventory(g.ctx, g.run.TenantID, g.run.SessionID, maxSessionDeletionRows, maxSessionDeletionRows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.f.store.CompleteSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at, uint64(len(inventory.Objects)), inventory.TotalBytes); err == nil {
		t.Fatal("orphan reverse key was reported as complete erasure")
	}
	g.requireCount(t, "attempt_execution_admissions_by_session_v1", 1)
	g.requireCount(t, "sessions", 1)
	g.requireCount(t, "worker_jobs", 1)
	stored, found, err := g.f.store.GetSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID)
	if err != nil || !found || stored.State != domain.SessionDeletionDeleting {
		t.Fatalf("failed cleanup escaped completed tombstone: found=%t state=%s err=%v", found, stored.State, err)
	}
}

func terminalizeOwnedFinalTestRun(t *testing.T, g nativeOwnedFinalGraph) {
	t.Helper()
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if err := g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		run, found, err := state.GetRun(g.ctx, g.run.ID)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		if err := run.Transition(domain.RunCancelled, at); err != nil {
			return err
		}
		return state.PutRun(g.ctx, run)
	}); err != nil {
		t.Fatal(err)
	}
}

func registerOwnedFinalLifecycleCleanup(t *testing.T, g nativeOwnedFinalGraph) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"session_legal_holds", "session_deletions"} {
			if _, err := g.f.store.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1 AND session_id=$2", g.run.TenantID, g.run.SessionID); err != nil {
				t.Errorf("cleanup exact lifecycle test %s: %v", table, err)
			}
		}
	})
}
