package providercredential

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type observedCandidateRevoker struct {
	delegate ports.ProviderCredentialInvocationRevokerV1
	entered  chan struct{}
}

type candidateReloadBindings struct {
	*fakeBindingStore
	reloaded         bool
	failure, foreign bool
}

func (b *candidateReloadBindings) FenceProviderCredentialCandidate(ctx context.Context, c ports.ProviderCredentialSecretCandidateV1, at time.Time) (ports.ProviderCredentialCandidateFenceV1, error) {
	r, err := b.fakeBindingStore.FenceProviderCredentialCandidate(ctx, c, at)
	b.reloaded = true
	return r, err
}
func (b *candidateReloadBindings) LoadProviderCredential(ctx context.Context, l ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	if b.reloaded && b.failure {
		return domain.ProviderCredentialBindingV1{}, false, errors.New("synthetic reload unavailable")
	}
	v, found, err := b.fakeBindingStore.LoadProviderCredential(ctx, l)
	if b.reloaded && b.foreign {
		v.OwnerUserID = "foreign-owner"
	}
	return v, found, err
}

type candidateFailingRevoker struct {
	calls []uint64
	fail  bool
}

func (r *candidateFailingRevoker) FenceProviderCredentialInvocations(_ context.Context, _ ports.ProviderCredentialLocatorV1, generation uint64) error {
	r.calls = append(r.calls, generation)
	if r.fail {
		return errors.New("synthetic join unavailable")
	}
	return nil
}

func TestCandidateGCReloadAndGenerationBoundaries(t *testing.T) {
	for _, global := range []bool{false, true} {
		entry := "scoped"
		if global {
			entry = "abandoned"
		}
		t.Run(entry, func(t *testing.T) {
			for _, tc := range []struct {
				name                                         string
				candidateGeneration                          uint64
				loadFailure, foreign, joinFailure, wantError bool
				wantFence                                    uint64
			}{
				{"reload-error", 1, true, false, false, true, 0},
				{"foreign-row", 1, false, true, false, true, 0},
				{"join-error", 1, false, false, true, true, 2},
				{"older", 1, false, false, false, false, 2},
				{"equal-rival", 2, false, false, false, false, 0},
				{"future-orphan", 3, false, false, false, false, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					service, _, base, secrets := testService(t)
					locator := testLocator("user-a")
					scope := ports.ProviderCredentialCandidateScopeV1{Locator: locator, ResourceRevision: tc.candidateGeneration, CredentialGeneration: tc.candidateGeneration, MutationID: "candidate-orphan"}
					candidate, err := secrets.PutProviderCredentialCandidate(context.Background(), scope, []byte("synthetic-orphan"))
					if err != nil {
						t.Fatal(err)
					}
					ref, _ := domain.NewCredentialSecretRef("current-generation-two")
					current := domain.ProviderCredentialBindingV1{Version: 1, TenantID: locator.TenantID, OwnerUserID: locator.OwnerUserID, ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID, ResourceRevision: 2, CredentialGeneration: 2, CandidateMutationID: "current-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("synthetic-current")), UpdatedAt: time.Unix(99, 0).UTC()}
					base.bindings[locator] = current
					bindings := &candidateReloadBindings{fakeBindingStore: base, failure: tc.loadFailure, foreign: tc.foreign}
					revoker := &candidateFailingRevoker{fail: tc.joinFailure}
					service.bindings = bindings
					service.revoker = revoker
					if global {
						_, err = service.DrainAbandonedCandidates(context.Background(), time.Unix(200, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{})
					} else {
						err = service.reconcileCandidates(context.Background(), locator, current, true)
					}
					if (err != nil) != tc.wantError {
						t.Fatalf("error=%v wantError=%v", err, tc.wantError)
					}
					_, retained := secrets.values[candidate.Reference]
					if retained != tc.wantError {
						t.Errorf("retained=%v want%v", retained, tc.wantError)
					}
					if tc.wantFence == 0 && len(revoker.calls) != 0 {
						t.Fatalf("invalidated current generation with calls=%v", revoker.calls)
					}
					if tc.wantFence != 0 && (len(revoker.calls) != 1 || revoker.calls[0] != tc.wantFence) {
						t.Fatalf("fence calls=%v want[%d]", revoker.calls, tc.wantFence)
					}
				})
			}
		})
	}
}

func (r observedCandidateRevoker) FenceProviderCredentialInvocations(ctx context.Context, locator ports.ProviderCredentialLocatorV1, generation uint64) error {
	close(r.entered)
	return r.delegate.FenceProviderCredentialInvocations(ctx, locator, generation)
}

func TestCandidateGCJoinsRotatedLocalBorrower(t *testing.T) {
	for _, global := range []bool{false, true} {
		name := "scoped"
		if global {
			name = "abandoned"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				invocations, bindings, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
				handle, err := invocations.IssueProviderCredential(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				entered, release, materialized := make(chan struct{}), make(chan struct{}), make(chan error, 1)
				go func() {
					materialized <- invocations.MaterializeProviderCredential(ctx, handle, func(_ ports.ProviderCredentialMaterializationV1, _ []byte) error {
						close(entered)
						<-release
						return nil
					})
				}()
				<-entered
				locator := ports.ProviderCredentialLocatorV1{TenantID: handle.TenantID, OwnerUserID: handle.OwnerUserID, ResourceKind: handle.ProviderResource.Kind, ResourceID: handle.ProviderResource.ResourceID}
				old, _, _ := bindings.LoadProviderCredential(ctx, locator)
				candidate := ports.ProviderCredentialSecretCandidateV1{Scope: ports.ProviderCredentialCandidateScopeV1{Locator: locator, ResourceRevision: old.ResourceRevision, CredentialGeneration: old.CredentialGeneration, MutationID: old.CandidateMutationID}, Reference: old.SecretRef, Fingerprint: old.SecretFingerprint, CreatedAt: old.UpdatedAt.Add(-time.Second)}
				// Immutable object inventory includes this now-superseded committed object.
				secrets.candidates[candidate.Reference] = candidate
				next := old
				next.ResourceRevision++
				next.CredentialGeneration++
				next.CandidateMutationID = "rotated-mutation"
				next.SecretRef, err = domain.NewCredentialSecretRef("rotated-reference")
				if err != nil {
					t.Fatal(err)
				}
				next.SecretFingerprint = domain.FingerprintCredential([]byte("rotated-synthetic-secret"))
				if swap, err := bindings.CompareAndSwapProviderCredential(ctx, old.ResourceRevision, next); err != nil || !swap.Applied {
					t.Fatalf("rotate: applied=%v err=%v", swap.Applied, err)
				}
				joining := make(chan struct{})
				service, err := New(Config{Clock: fixedClock{now: old.UpdatedAt.Add(time.Second)}, IDs: &fixedIDs{}}, &fakeAuthorizer{}, observedCandidateRevoker{delegate: invocations, entered: joining}, bindings, secrets)
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					if global {
						_, err := service.DrainAbandonedCandidates(ctx, old.UpdatedAt.Add(time.Second), ports.ProviderCredentialCandidateCursorV1{})
						done <- err
						return
					}
					done <- service.reconcileCandidates(ctx, locator, next, true)
				}()
				select {
				case <-joining:
				case <-done:
					t.Error("GC completed without joining the active local borrower")
					done <- nil
				case <-ctx.Done():
					t.Error("GC did not reach its bounded join")
				}
				secrets.mu.Lock()
				_, retained := secrets.values[old.SecretRef]
				secrets.mu.Unlock()
				if !retained {
					t.Error("GC deleted rotated secret while its local materialization callback still borrowed it")
				}
				close(release)
				if err := <-materialized; err != nil {
					t.Errorf("materialize: %v", err)
				}
				if err := <-done; err != nil {
					t.Errorf("GC: %v", err)
				}
				if err := invocations.ReleaseProviderCredential(ctx, handle); err != nil {
					t.Errorf("release: %v", err)
				}
			})
		})
	}
}
