package ydbstore

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func TestOwnedAllocationYDBSharedRetentionPreservesUnresolvedPreparation(t *testing.T) {
	for _, mode := range []string{"never_prepared", "burn_without_observation", "registered_retired"} {
		t.Run(mode, func(t *testing.T) {
			g := newNativeOwnedParentGraph(t)
			registerOwnedFinalLifecycleCleanup(t, g.nativeOwnedFinalGraph)
			reserved, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
			if err != nil || reserved.Grant == nil {
				t.Fatalf("original native parent: %v", err)
			}
			_, original := g.persistedParent(t)
			g.f.store.ownedPreparationGrantVerifier = g.issuer
			switch mode {
			case "burn_without_observation":
				fresh, err := g.f.store.beginBoundedOwnedAttemptPreparation(g.ctx, *reserved.Grant)
				if err != nil || !fresh {
					t.Fatalf("original burn: fresh=%t err=%v", fresh, err)
				}
			case "registered_retired":
				issuer, now := nativeOwnedPreparationIssuer(t, g, "retention-observation-key")
				g.f.store.ownedPreparationGrantVerifier = issuer
				grant, err := issuer.MintAttemptEffectOwnershipGrant(reserved.Grant.Authority, reserved.Reservation, reserved.Grant.GrantExpiresAt)
				if err != nil {
					t.Fatal(err)
				}
				driver, err := serverlessharness.NewInProcessExecutionSubstrateV1(now, issuer, grant.Authority.HarnessBinding.Backend)
				if err != nil {
					t.Fatal(err)
				}
				producer, err := serverlessharness.NewBoundedOwnedAllocationPreparationV1(issuer, grant.Authority.HarnessBinding.Backend, grant.Authority.SubstrateBinding, driver, nativeOwnedPreparationBurnBridge{store: g.f.store})
				if err != nil {
					t.Fatal(err)
				}
				p, err := producer.PrepareAllocationPublication(g.ctx, grant)
				if err != nil {
					t.Fatalf("actual native preparation: %v", err)
				}
				g.f.store.ownedAllocationPublicationVerifier = producer
				receipt, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, p)
				if err != nil {
					t.Fatalf("actual native publication: %v", err)
				}
				if _, err := g.f.store.retireBoundedOwnedAllocation(g.ctx, g.request, receipt.Digest, domain.RegisteredPreparationRetiredV1); err != nil {
					t.Fatal(err)
				}
			}
			terminalizeOwnedFinalTestRun(t, g.nativeOwnedFinalGraph)
			at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
			deletion := domain.SessionDeletion{TenantID: g.run.TenantID, SessionID: g.run.SessionID, RequestedBy: g.f.scope.owner,
				Reason: "native original allocation retention", State: domain.SessionDeletionRequested, RequestedAt: at}
			if _, err := g.f.store.RequestSessionDeletion(g.ctx, deletion); err != nil {
				t.Fatal(err)
			}
			if _, err := g.f.store.StartSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at); err != nil {
				t.Fatal(err)
			}
			inventory, err := g.f.store.BuildSessionDeletionInventory(g.ctx, g.run.TenantID, g.run.SessionID, maxSessionDeletionRows, maxSessionDeletionRows)
			if err != nil || inventory.OwnedAdmissionRows != 1 {
				t.Fatalf("shared reverse inventory: rows=%d err=%v", inventory.OwnedAdmissionRows, err)
			}
			at = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
			completed, err := g.f.store.CompleteSessionDeletion(g.ctx, g.run.TenantID, g.run.SessionID, at, uint64(len(inventory.Objects)), inventory.TotalBytes)
			if mode == "never_prepared" {
				if err != nil || completed.State != domain.SessionDeletionCompleted {
					t.Fatalf("unprepared same-Tx cleanup: state=%s err=%v", completed.State, err)
				}
				g.requireCount(t, "attempt_effect_reservations", 0)
				g.requireCount(t, "attempt_execution_admissions_v1", 0)
			} else {
				if err == nil {
					t.Fatal("unknown preparation or retired-without-cleanup was reported as erased")
				}
				g.requireCount(t, "attempt_effect_reservations", 1)
				g.requireCount(t, "attempt_execution_admissions_v1", 1)
				g.requireCount(t, "attempt_execution_admissions_by_session_v1", 1)
				g.requireCount(t, "sessions", 1)
				_, after := g.persistedParent(t)
				if after != original {
					t.Fatal("denied cleanup changed original parent")
				}
				if mode == "registered_retired" {
					history, err := g.f.store.observeBoundedOwnedAllocation(g.ctx, g.request, false)
					if err != nil || history.Current.State != domain.RegisteredPreparationRetiredV1 || history.Original.State != domain.RegisteredPreparationActiveV1 {
						t.Fatalf("retained original/tombstone: %v", err)
					}
				}
			}
		})
	}
}
