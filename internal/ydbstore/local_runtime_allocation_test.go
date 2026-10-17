package ydbstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// This callback models the confirmed burn's outcomes only. Its boolean and
// counter are NOT native Store/YDB proof or permission to compose a serving path.
type localRuntimeActualBurn struct {
	mu    sync.Mutex
	fresh bool
	err   error
	calls int
}

func (b *localRuntimeActualBurn) BeginOwnedAttemptPreparation(ctx context.Context, _ ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return b.fresh, b.err
}

func (b *localRuntimeActualBurn) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

type localRuntimeActualFixture struct {
	source    *localoperator.Source
	config    domain.ConfiguredInferenceProfileV1
	authority domain.ServerlessInvocationAuthorityV1
	parent    domain.AttemptEffectReservationV1
	execution domain.AttemptExecutionSourceV1
	issuer    *serverlessharness.CapabilityIssuer
	grant     ports.AttemptEffectOwnershipGrantV1
	ctx       context.Context
	cancel    context.CancelFunc
	at        time.Time
}

// Independent MCP/search rows below are synthetic canonical DATA, not funding
// issued by Source (which intentionally cannot issue those components). For
// the no-receipt negative only, provider/substrate are also synthetic DATA.
func localRuntimeStructuralSpending(t *testing.T, p domain.OwnedAgentProfileCurrentV1, scope domain.AttemptExecutionSourceScopeV1, kind domain.AttemptSpendingComponentKindV1) domain.AttemptSpendingAuthorizationCurrentV1 {
	t.Helper()
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	v := domain.AttemptSpendingAuthorizationCurrentV1{Version: 1,
		Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: scope.TenantID, AttemptID: scope.AttemptID, ComponentKind: kind, BillingOwnerUserID: p.BillingOwnerUserID},
		RunID:   scope.RunID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID, SourceKind: domain.AttemptSpendingOwnerLocalSourceV1,
		SourceInstallationID: p.InstallationID, SourceInstallationRevision: p.InstallationRevision,
		SourceProfile: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: digest},
		ResourceID:    p.Resource.ResourceID, ResourceRevision: p.Resource.Revision, ResourceGeneration: p.Resource.CredentialGeneration, ProfileID: string(p.Locator.Profile), ProfileRevision: p.ProfileRevision,
		ScopedGrantDigest: p.EffectivePolicyDigest, Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, PriceEvidenceDigest: p.ProviderPriceDigest, TermsEvidenceDigest: p.ProviderTermsDigest,
		PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1,
		Window: p.Window, UpdatedAt: p.UpdatedAt, MutationID: "synthetic-independent-" + string(kind)}
	if kind == domain.AttemptSpendingSubstrateV1 {
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.RevocationGeneration
		v.ProfileID, v.ProfileRevision, v.ScopedGrantDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest
	} else if kind == domain.AttemptSpendingMCPToolV1 || kind == domain.AttemptSpendingWebSearchV1 {
		service := p.CostCeiling.MCPToolCost
		if kind == domain.AttemptSpendingWebSearchV1 {
			service = p.CostCeiling.WebSearchCost
		}
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = service.ResourceID, service.ResourceRevision, service.RevocationGeneration
		v.ProfileID, v.ProfileRevision, v.ScopedGrantDigest = service.ProfileID, service.ProfileRevision, service.ScopedGrantDigest
		v.PriceRevision, v.PriceEvidenceDigest, v.TermsEvidenceDigest, v.PriceState = service.PriceRevision, service.PriceEvidenceDigest, service.TermsEvidenceDigest, service.PriceState
		amount := *service.MaxAmountMicrounits
		v.MaxAmountMicrounits = &amount
	}
	bindReaderSpendingSource(t, &v)
	if err := v.ValidateAgainstProfile(p, p.Window.PublishedAt); err != nil {
		t.Fatalf("synthetic independent %s shape: %v", kind, err)
	}
	return v
}

// Real private-file intake, OS executable/host/net observation and Store profile
// publication over a deterministic SQL model. No native YDB, model, socket or
// external-service funding/qualification is exercised.
func newLocalRuntimeActualFixture(t *testing.T, runningExecutable, acceptReceipt bool) localRuntimeActualFixture {
	t.Helper()
	root, executable, manifest := localOperatorFiles(t)
	if runningExecutable {
		var err error
		executable, err = os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		executable, err = filepath.EvalSymlinks(executable)
		if err != nil {
			t.Fatal(err)
		}
	}
	identity, err := localoperator.InspectRuntime(t.Context(), executable)
	if err != nil {
		t.Fatalf("inspect owned executable before source installation: %v", err)
	}
	connector, err := localoperator.LinkedConnectorIdentity(identity.WorkerExecutableDigest)
	if err != nil {
		t.Fatal(err)
	}
	c, p := &manifest.Configuration, &manifest.Profile
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.OuterHarnessArtifactDigest, p.Substrate.LocalHostIdentityDigest, p.Substrate.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.EgressProxyArtifactDigest, p.Substrate.EgressProxyIdentityDigest = identity.WorkerExecutableDigest, connector
	writeLocalOperatorManifest(t, root, manifest) // before OpenFresh or any seal
	source, err := localoperator.OpenFresh(t.Context(), root, executable)
	if err != nil {
		t.Fatalf("actual private source intake: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	publication, err := source.Publication()
	if err != nil {
		t.Fatal(err)
	}
	store, sqlFixture, _, _ := newLocalInstallationFixture(t)
	sqlFixture.at = publication.IssuedAt
	store.ownedProfilePublicationVerifier = source
	receipt, err := store.PublishOwnedProfile(t.Context(), publication)
	if err != nil {
		t.Fatalf("actual source -> Store profile receipt (scripted SQL): %v", err)
	}
	if acceptReceipt {
		if err := source.AcceptReceipt(receipt); err != nil {
			t.Fatal(err)
		}
	}
	config, err := source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	profile := receipt.Profile
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: profile.Locator.TenantID, OwnerUserID: profile.Locator.OwnerUserID, RunID: "actual-runtime-run", SessionID: "actual-runtime-session", AttemptID: "actual-runtime-attempt"}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range ownedSpendingKinds {
		if acceptReceipt && i < 2 {
			grant, err := source.SpendingPublication(t.Context(), scope, kind)
			if err != nil || source.VerifyAttemptSpendingPublication(grant, grant.IssuedAt) != nil {
				t.Fatalf("genuine retained-source %s invoice: %v", kind, err)
			}
			spending[i] = grant.Authorization
		} else {
			spending[i] = localRuntimeStructuralSpending(t, profile, scope, kind)
		}
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, profile, spending, at)
	if err != nil {
		t.Fatalf("canonical package (external funding declarations remain synthetic): %v", err)
	}
	authority := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost,
		Lease:                 domain.Lease{ID: "actual-runtime-lease", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "actual-runtime-worker", FenceToken: 4, AcquiredAt: at, ExpiresAt: at.Add(5 * time.Minute)},
		ContextManifestDigest: strings.Repeat("a", 64), InputManifestDigest: strings.Repeat("b", 64), InvocationDeadline: at.Add(profile.Limits.MaxActiveDuration)}
	parent, err := domain.BuildAttemptEffectReservationV1(authority, "actual-runtime-claim", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{5}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return localRuntimeActualFixture{source, config, authority, parent, pkg.Source, issuer, grant, ctx, cancel, at}
}

func (f localRuntimeActualFixture) bridge(t *testing.T, burn *localRuntimeActualBurn) *serverlessharness.BoundedOwnedAllocationPreparationV1 {
	t.Helper()
	bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(f.issuer, f.authority.HarnessBinding.Backend, f.authority.SubstrateBinding, f.source, burn)
	if err != nil {
		t.Fatalf("actual local preparation constructor: %v", err)
	}
	t.Cleanup(func() { _ = bridge.Close() })
	return bridge
}

func (f localRuntimeActualFixture) prepare(t *testing.T, bridge *serverlessharness.BoundedOwnedAllocationPreparationV1) ports.OwnedAllocationPublicationV2 {
	t.Helper()
	proof, err := bridge.PrepareAllocationPublication(f.ctx, f.grant.Clone())
	if err != nil {
		bindingErr := f.source.ValidateExecutionBinding(f.authority, f.at)
		_, runtimeErr := f.source.InspectExecutionRuntime(f.ctx, f.authority, f.at)
		executable, executableErr := os.Executable()
		_, rawErr := localoperator.InspectRuntime(f.ctx, executable)
		canonical, canonicalErr := filepath.EvalSymlinks(executable)
		_, canonicalRuntimeErr := localoperator.InspectRuntime(f.ctx, canonical)
		t.Fatalf("actual runtime observation: prepare=%v binding=%v runtime=%v executable=%v canonical=%v raw-is-canonical=%t raw-inspect=%v canonical-inspect=%v", err, bindingErr, runtimeErr, executableErr, canonicalErr, executable == canonical, rawErr, canonicalRuntimeErr)
	}
	return proof
}

func TestLocalRuntimeActualObservationSealsSourceAndRetainedIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeActualFixture(t, true, true)
		burn := &localRuntimeActualBurn{fresh: true}
		bridge := f.bridge(t, burn)
		proof := f.prepare(t, bridge)
		if burn.count() != 1 || proof.Allocation.ObservedImageDigest != "" || proof.Allocation.ChildProcess != nil || proof.Allocation.InProcess != nil || proof.Allocation.LocalRuntime == nil {
			t.Fatal("actual local allocation did not preserve exclusive no-image runtime observation and one burn")
		}
		if proof.Allocation.LocalRuntime.Limits != f.config.Limits || proof.Allocation.LocalRuntime.WorkerExecutableDigest != f.config.WorkerExecutableDigest || !strings.HasPrefix(proof.AllocationID, "local_") {
			t.Fatal("runtime limits/worker/identity were not derived from the actual retained source")
		}
		if bridge.VerifyOwnedAllocationPublication(proof.Clone(), f.at) != nil || bridge.VerifyOwnedAllocationPublicationCurrent(proof.Clone(), f.at) != nil {
			t.Fatal("exact retained original carrier clone failed authentication")
		}
		configurationCopy := f.config.Clone()
		configurationCopy.Limits.JSONDepth--
		stillOriginal, err := f.source.Configuration()
		if err != nil || stillOriginal.Limits != f.config.Limits {
			t.Fatal("caller configuration copy mutated actual source limits")
		}
		other := f.bridge(t, burn)
		if other.VerifyOwnedAllocationPublication(proof, f.at) == nil {
			t.Error("same-issuer unobserved allocation authenticated another retained handle")
		}
		for name, mutate := range map[string]func(*ports.OwnedAllocationPublicationV2){
			"limits":   func(p *ports.OwnedAllocationPublicationV2) { p.Allocation.LocalRuntime.Limits.JSONMembers-- },
			"identity": func(p *ports.OwnedAllocationPublicationV2) { p.AllocationID = "local_counterfeit" },
			"worker": func(p *ports.OwnedAllocationPublicationV2) {
				p.Allocation.LocalRuntime.WorkerExecutableDigest = strings.Repeat("e", 64)
			},
			"profile": func(p *ports.OwnedAllocationPublicationV2) {
				p.Allocation.LocalRuntime.ConfiguredProfileDigest = strings.Repeat("e", 64)
			},
			"setup-expiry": func(p *ports.OwnedAllocationPublicationV2) {
				p.PreparedInvocationExpiresAt = p.PreparedInvocationExpiresAt.Add(-time.Microsecond)
			},
			"authenticator": func(p *ports.OwnedAllocationPublicationV2) { p.Authenticator[0] ^= 1 },
			"authority": func(p *ports.OwnedAllocationPublicationV2) {
				p.Authority.ContextManifestDigest = strings.Repeat("e", 64)
			},
		} {
			changed := proof.Clone()
			mutate(&changed)
			if bridge.VerifyOwnedAllocationPublication(changed, f.at) == nil || bridge.VerifyOwnedAllocationPublicationCurrent(changed, f.at) == nil {
				t.Errorf("changed %s authenticated as original actual allocation", name)
			}
		}
		if bridge.VerifyOwnedAllocationPublication(proof, f.at) != nil {
			t.Fatal("substitution controls damaged original historical carrier")
		}
		if _, err := bridge.PrepareAllocationPublication(f.ctx, f.grant.Clone()); err == nil || burn.count() != 1 {
			t.Fatal("same-issuer grant replay performed a second preparation burn")
		}
		if bridge.VerifyOwnedAllocationPublicationCurrent(proof, f.at) != nil || bridge.VerifyOwnedAllocationPublication(proof, f.at) != nil {
			t.Error("denied same-bridge duplicate damaged the original live allocation or historical proof")
		}
	})
}

func TestLocalRuntimeActualRequiresReceiptAndExecutingWorker(t *testing.T) {
	for _, tc := range []struct {
		name    string
		running bool
		receipt bool
		burns   int
	}{{"installed-different-file", false, true, 1}, {"no-accepted-receipt", true, false, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalRuntimeActualFixture(t, tc.running, tc.receipt)
				if tc.receipt && f.source.ValidateExecutionBinding(f.authority, f.at) != nil {
					t.Fatal("different-file negative must have a valid accepted source binding")
				}
				burn := &localRuntimeActualBurn{fresh: true}
				bridge := f.bridge(t, burn)
				proof, err := bridge.PrepareAllocationPublication(f.ctx, f.grant)
				if err == nil || !reflect.DeepEqual(proof, ports.OwnedAllocationPublicationV2{}) || burn.count() != tc.burns {
					t.Fatalf("invalid runtime/source prepared: err=%v burn=%d want=%d", err, burn.count(), tc.burns)
				}
			})
		})
	}
	t.Run("caller-source-and-wrong-authority", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newLocalRuntimeActualFixture(t, true, true)
			var zero localoperator.Source
			// Deliberately copy the quiescent value with reflection to test its
			// self-identity defense, not to treat copied mutex state as a keeper.
			copied := new(localoperator.Source)
			reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(f.source).Elem())
			for _, source := range []*localoperator.Source{&zero, copied} {
				if source.ValidateExecutionBinding(f.authority, f.at) == nil {
					t.Error("zero/copied source became retained installation authority")
				}
				if _, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(f.issuer, f.authority.HarnessBinding.Backend, f.authority.SubstrateBinding, source, &localRuntimeActualBurn{fresh: true}); err == nil {
					t.Error("zero/copied source constructed an actual local producer")
				}
			}
			wrong := f.authority.Clone()
			wrong.HarnessBinding.Resource.Revision++
			wrong.HarnessBinding.AgentLoopPolicy.Resource = wrong.HarnessBinding.Resource
			if err := wrong.ValidateAt(f.at); err != nil {
				t.Fatalf("wrong-resource control must retain valid authority shape: %v", err)
			}
			if f.source.ValidateExecutionBinding(wrong, f.at) == nil {
				t.Error("source accepted a different resource revision")
			}
		})
	})
}

func TestLocalRuntimeActualBurnDenialAndConcurrentReplay(t *testing.T) {
	for _, mode := range []string{"not-fresh", "unknown-commit"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalRuntimeActualFixture(t, true, true)
				burn := &localRuntimeActualBurn{}
				if mode == "unknown-commit" {
					burn.fresh, burn.err = true, errors.New("synthetic lost acknowledgement")
				}
				for range 2 {
					bridge := f.bridge(t, burn)
					proof, err := bridge.PrepareAllocationPublication(f.ctx, f.grant.Clone())
					if err == nil || !reflect.DeepEqual(proof, ports.OwnedAllocationPublicationV2{}) {
						t.Fatal("false/unknown burn authorized actual runtime preparation")
					}
				}
				if burn.count() != 1 {
					t.Fatalf("consumed original issuer grant retried burn: got=%d want=1", burn.count())
				}
			})
		})
	}
	t.Run("concurrent-original-grant", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newLocalRuntimeActualFixture(t, true, true)
			burn := &localRuntimeActualBurn{fresh: true}
			bridge := f.bridge(t, burn)
			// All copies target the SAME owner, so a losing pre-preflight call
			// must not close the winner's retained original allocation.
			bridges := []*serverlessharness.BoundedOwnedAllocationPreparationV1{bridge, bridge, bridge}
			type result struct {
				proof ports.OwnedAllocationPublicationV2
				err   error
			}
			results := make([]result, len(bridges))
			start := make(chan struct{})
			var joined sync.WaitGroup
			for i, bridge := range bridges {
				joined.Add(1)
				go func() {
					defer joined.Done()
					<-start
					results[i].proof, results[i].err = bridge.PrepareAllocationPublication(f.ctx, f.grant.Clone())
				}()
			}
			close(start)
			joined.Wait()
			successes := 0
			for i, r := range results {
				if r.err == nil {
					successes++
					if err := bridges[i].VerifyOwnedAllocationPublicationCurrent(r.proof, f.at); err != nil {
						t.Fatalf("losing grant copies closed the sole actual winner: %v", err)
					}
				} else if !reflect.DeepEqual(r.proof, ports.OwnedAllocationPublicationV2{}) {
					t.Error("denied concurrency branch returned partial publication")
				}
			}
			if successes != 1 || burn.count() != 1 {
				t.Fatalf("one original grant prepared multiple allocations: successes=%d burns=%d", successes, burn.count())
			}
		})
	})
}

func TestLocalRuntimeActualEntryOnceOutlivesOnlySetup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeActualFixture(t, true, true)
		bridge := f.bridge(t, &localRuntimeActualBurn{fresh: true})
		proof := f.prepare(t, bridge)
		deadline, err := domain.LocalAllocationActiveDeadlineV2(f.authority, f.parent, f.execution)
		if err != nil || !deadline.After(proof.PreparedInvocationExpiresAt) {
			t.Fatalf("original active deadline must outlive setup: %v", err)
		}
		window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: proof.ObservedAt, PublishedAt: f.at, ExpiresAt: deadline}
		registered, err := domain.BuildRegisteredAllocationV2(f.authority, f.parent, proof.Allocation, domain.PreparedInvocationDigestV1(proof.PreparedInvocationDigest), proof.PreparedInvocationExpiresAt, f.execution, proof.AllocationID, proof.AllocationGeneration, window)
		if err != nil {
			t.Fatalf("structural original local registration: %v", err)
		}
		if bridge.VerifyEnteredLocalAllocation(registered, f.at) == nil {
			t.Fatal("observation alone claimed loop entry")
		}
		if err := bridge.EnterOwnedLocalAllocation(f.ctx, proof); err != nil {
			t.Fatalf("original one-use entry before setup deadline: %v", err)
		}
		if bridge.EnterOwnedLocalAllocation(f.ctx, proof.Clone()) == nil {
			t.Error("original prepared capability entered loop twice")
		}
		time.Sleep(proof.PreparedInvocationExpiresAt.Sub(time.Now())) // explicit virtual-time boundary, not readiness
		synctest.Wait()
		now := time.Now().UTC()
		if bridge.VerifyOwnedAllocationPublicationCurrent(proof, now) == nil || bridge.EnterOwnedLocalAllocation(f.ctx, proof) == nil {
			t.Error("expired setup authority was renewed by active allocation")
		}
		if err := bridge.VerifyEnteredLocalAllocation(registered, now); err != nil {
			t.Fatalf("entered retained actual allocation truncated at setup expiry: %v", err)
		}
		if !registered.PreparedInvocationExpiresAt.Equal(proof.PreparedInvocationExpiresAt) || bridge.VerifyOwnedAllocationPublication(proof, now) != nil {
			t.Fatal("active handle rewrote prepared expiry or lost historical authenticity")
		}
		time.Sleep(deadline.Sub(time.Now()))
		synctest.Wait()
		if bridge.VerifyEnteredLocalAllocation(registered, time.Now().UTC()) == nil {
			t.Error("entered runtime survived original absolute active expiry")
		}
	})
}

func TestLocalRuntimeActualCloseCancelAndLateEntry(t *testing.T) {
	for _, action := range []string{"close", "cancel", "late-entry"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalRuntimeActualFixture(t, true, true)
				bridge := f.bridge(t, &localRuntimeActualBurn{fresh: true})
				proof := f.prepare(t, bridge)
				switch action {
				case "close":
					if err := bridge.Close(); err != nil {
						t.Fatal(err)
					}
				case "cancel":
					f.cancel()
				case "late-entry":
					time.Sleep(proof.PreparedInvocationExpiresAt.Sub(time.Now()))
				}
				synctest.Wait()
				now := time.Now().UTC()
				if bridge.VerifyOwnedAllocationPublicationCurrent(proof, now) == nil || bridge.EnterOwnedLocalAllocation(f.ctx, proof) == nil {
					t.Errorf("%s retained fresh setup/entry authority", action)
				}
				if err := bridge.VerifyOwnedAllocationPublication(proof, now); err != nil {
					t.Errorf("%s erased original historical runtime observation: %v", action, err)
				}
				if _, err := f.source.Configuration(); err != nil {
					t.Errorf("%s closed caller-owned Source: %v", action, err)
				}
			})
		})
	}
}
