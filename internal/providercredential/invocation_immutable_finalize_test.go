package providercredential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This fixture constructs a genuine existing API-account canonical tuple. It
// is not a Pi eligibility, owner-spending receipt or remote-phase authorizer.
func immutableAPIInvocationFixture(t *testing.T, delivery domain.ProviderCredentialDeliveryKindV1) (*InvocationService, *fakeBindingStore, *fakeSecretStore, ports.ProviderCredentialIssueRequestV1, *mutableClock) {
	t.Helper()
	now := time.Unix(100, 0).UTC()
	expires := now.Add(time.Hour)
	clock := &mutableClock{now: now}
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "api-account-a", OwnerUserID: "user-a", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	binding := domain.HarnessBindingV2{
		Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionSingleTurnV2,
		TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a",
		Backend: domain.HarnessBackendDescriptorV1{
			HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1,
			ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1",
			BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: delivery,
		},
		Resource: resource, ModelVendorID: "openrouter", ModelID: "stealth-ox-alpha", InputDataClass: domain.ProviderDataPublicV1,
		ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64),
		CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &expires,
	}
	ref, err := domain.NewCredentialSecretRef("test/provider/api-account-a/generation-1")
	if err != nil {
		t.Fatalf("construct synthetic reference: %v", err)
	}
	marker := []byte("synthetic-immutable-api-marker")
	credential := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		ResourceKind: resource.Kind, ResourceID: resource.ResourceID, ResourceRevision: resource.Revision, CredentialGeneration: resource.CredentialGeneration,
		CandidateMutationID: "synthetic-api-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential(marker), UpdatedAt: now,
	}
	if err := credential.Validate(); err != nil {
		t.Fatalf("validate canonical API credential fixture: %v", err)
	}
	bindings, secrets := newFakeBindingStore(), newFakeSecretStore()
	bindings.bindings[locatorFor(credential)] = credential
	secrets.values[ref], secrets.committed[ref] = marker, struct{}{}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize test scratch: %v", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("protect test scratch: %v", err)
	}
	template := ports.ProviderCredentialDeliveryTemplateV1{Kind: delivery}
	if delivery == domain.ProviderCredentialDeliveryFileV1 {
		template.FileName = "api-key"
	} else if delivery == domain.ProviderCredentialDeliveryEnvironmentV1 {
		template.EnvironmentName = "SYNTHETIC_API_KEY"
	}
	service, err := NewInvocationService(InvocationConfig{Clock: clock, IDs: &fixedIDs{}, ScratchRoot: root}, &fixedDeliveryPlanner{template: template}, bindings, secrets)
	if err != nil {
		t.Fatalf("construct API invocation service: %v", err)
	}
	t.Cleanup(func() {
		if service.files != nil {
			if err := service.files.Close(); err != nil {
				t.Errorf("close owned API descriptors: %v", err)
			}
		}
	})
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker-a", LeaseID: "lease-a", LeaseFence: 1, ExpiresAt: now.Add(30 * time.Minute)}
	if err := request.Validate(); err != nil {
		t.Fatalf("validate canonical API issue fixture: %v", err)
	}
	return service, bindings, secrets, request, clock
}

func issueImmutableAPI(t *testing.T, service *InvocationService, request ports.ProviderCredentialIssueRequestV1) ports.ProviderInvocationCredentialV1 {
	t.Helper()
	handle, err := service.IssueProviderCredential(context.Background(), request)
	if err != nil {
		t.Fatalf("issue genuine API handle: %v", err)
	}
	t.Cleanup(func() {
		if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
			t.Errorf("release owned API invocation: %v", err)
		}
	})
	return handle
}

func materializeImmutableAPI(t *testing.T, service *InvocationService, handle ports.ProviderInvocationCredentialV1) ports.ProviderCredentialMaterializationV1 {
	t.Helper()
	var plan ports.ProviderCredentialMaterializationV1
	calls := 0
	err := service.MaterializeProviderCredential(context.Background(), handle, func(value ports.ProviderCredentialMaterializationV1, secret []byte) error {
		calls++
		plan = value
		if value.Kind == domain.ProviderCredentialDeliveryFileV1 && len(secret) != 0 {
			t.Error("API file callback received plaintext")
		}
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("materialize API invocation: callbacks=%d want=1 err=%v", calls, err)
	}
	return plan
}

func expectImmutableRelease(t *testing.T, service *InvocationService, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1) {
	t.Helper()
	if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
		t.Errorf("release after local finalization: %v", err)
	}
	if plan.FilePath != "" {
		if _, err := os.Lstat(plan.FilePath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("owned credential path survived Release: err=%v", err)
		}
	}
}

func TestImmutableAPIFinalizeOriginalFileOnceThenRelease(t *testing.T) {
	service, bindings, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueImmutableAPI(t, service, request)
	plan := materializeImmutableAPI(t, service, handle)
	beforeReads := bindings.loadCalls
	// Removing access to the entire secret port makes ANY secret backend call
	// during Finalize fail causally, not only Read/Recover counter assertions.
	service.secrets = nil
	if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err != nil {
		t.Fatalf("unchanged original API file Finalize: %v", err)
	}
	state := service.handles[handle.HandleID]
	if !state.callbackSucceeded || !state.finalizeAttempted || !state.finalized || state.file == nil {
		t.Fatal("local successful finalization state or retained file is missing")
	}
	if bindings.loadCalls-beforeReads != 2 || secrets.readCalls != 1 || secrets.recoverCalls != 1 || secrets.putCalls != 0 {
		t.Fatalf("Finalize backend boundary: binding_reads=%d want=2 read=%d want=1 recover=%d want=1 put=%d want=0", bindings.loadCalls-beforeReads, secrets.readCalls, secrets.recoverCalls, secrets.putCalls)
	}
	if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrConsumed) {
		t.Errorf("successful Finalize replay: err=%v want=%v", err, ErrConsumed)
	}
	expectImmutableRelease(t, service, handle, plan)
}

func TestImmutableAPIFinalizePreservesOriginalBindingClockDomain(t *testing.T) {
	service, bindings, _, request, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	locator := ports.ProviderCredentialLocatorV1{TenantID: request.HarnessBinding.TenantID, OwnerUserID: request.HarnessBinding.OwnerUserID, ResourceKind: request.HarnessBinding.Resource.Kind, ResourceID: request.HarnessBinding.Resource.ResourceID}
	original := bindings.bindings[locator]
	// The mutation store and this invocation use independently configured
	// clocks. An unchanged original timestamp is not a local expiry authority.
	original.UpdatedAt = clock.Now().Add(24 * time.Hour)
	bindings.bindings[locator] = original
	handle := issueImmutableAPI(t, service, request)
	plan := materializeImmutableAPI(t, service, handle)
	service.secrets = nil
	if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err != nil {
		t.Fatalf("unchanged original binding from independent clock denied: %v", err)
	}
	if !service.handles[handle.HandleID].finalized {
		t.Error("unchanged independent-clock binding did not finalize")
	}
	expectImmutableRelease(t, service, handle, plan)
}

func TestImmutableAPIFinalizeRequiresCompletedSuccessfulCallback(t *testing.T) {
	for _, scenario := range []string{"unmaterialized", "callback_error", "callback_panic", "busy_reentry"} {
		t.Run(scenario, func(t *testing.T) {
			service, _, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryFileV1, RootDir: service.root, FilePath: filepath.Join(service.root, "api-key")}
			var reentry error
			if scenario != "unmaterialized" {
				func() {
					defer func() {
						value := recover()
						if (value != nil) != (scenario == "callback_panic") {
							t.Errorf("callback panic observed=%t want=%t", value != nil, scenario == "callback_panic")
						}
					}()
					err := service.MaterializeProviderCredential(context.Background(), handle, func(value ports.ProviderCredentialMaterializationV1, _ []byte) error {
						plan = value
						switch scenario {
						case "callback_error":
							return errors.New("synthetic callback failure")
						case "callback_panic":
							panic("synthetic callback panic")
						case "busy_reentry":
							reentry = service.FinalizeProviderCredentialFile(context.Background(), handle, plan)
						}
						return nil
					})
					if scenario == "callback_error" && !errors.Is(err, ErrBackend) {
						t.Errorf("callback failure error=%v want=%v", err, ErrBackend)
					}
					if scenario == "busy_reentry" && err != nil {
						t.Errorf("materialize after rejected reentry: %v", err)
					}
				}()
			}
			service.secrets = nil
			state := service.handles[handle.HandleID]
			if scenario == "busy_reentry" {
				if !errors.Is(reentry, ErrConsumed) || state.finalizeAttempted {
					t.Fatalf("reentry did not reject without attempt: err=%v attempted=%t", reentry, state.finalizeAttempted)
				}
				if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err != nil {
					t.Errorf("post-unlock valid control: %v", err)
				}
			} else {
				if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err == nil || state.finalized || state.callbackSucceeded {
					t.Errorf("unsuccessful callback qualified: err=%v finalized=%t callback_succeeded=%t", err, state.finalized, state.callbackSucceeded)
				}
			}
			wantReads := 1
			if scenario == "unmaterialized" {
				wantReads = 0
			}
			if secrets.readCalls != wantReads || secrets.recoverCalls != 1 {
				t.Errorf("unexpected secret reads=%d want=%d recover=%d want=1", secrets.readCalls, wantReads, secrets.recoverCalls)
			}
			if scenario != "unmaterialized" {
				expectImmutableRelease(t, service, handle, plan)
			}
		})
	}
}

func TestImmutableAPIFinalizeRejectsEveryForeignHandleFieldBeforeBusy(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*ports.ProviderInvocationCredentialV1)
	}{
		{"handle_id", func(h *ports.ProviderInvocationCredentialV1) { h.HandleID = "foreign-handle" }},
		{"tenant", func(h *ports.ProviderInvocationCredentialV1) { h.TenantID = "tenant-b" }},
		{"owner", func(h *ports.ProviderInvocationCredentialV1) {
			h.OwnerUserID = "user-b"
			h.ProviderResource.OwnerUserID = "user-b"
		}},
		{"run", func(h *ports.ProviderInvocationCredentialV1) { h.RunID = "run-b" }},
		{"attempt", func(h *ports.ProviderInvocationCredentialV1) { h.AttemptID = "attempt-b" }},
		{"worker", func(h *ports.ProviderInvocationCredentialV1) { h.WorkerID = "worker-b" }},
		{"lease", func(h *ports.ProviderInvocationCredentialV1) { h.LeaseID = "lease-b" }},
		{"fence", func(h *ports.ProviderInvocationCredentialV1) { h.LeaseFence++ }},
		{"resource_kind", func(h *ports.ProviderInvocationCredentialV1) {
			h.ProviderResource.Kind = domain.ProviderResourceRouterAccountV1
		}},
		{"resource_id", func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.ResourceID = "api-account-b" }},
		{"resource_revision", func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.Revision++ }},
		{"generation", func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.CredentialGeneration++ }},
		{"credential_mode", func(h *ports.ProviderInvocationCredentialV1) {
			h.ProviderResource.CredentialMode = domain.ProviderCredentialNoneV1
		}},
		{"expires", func(h *ports.ProviderInvocationCredentialV1) { h.ExpiresAt = h.ExpiresAt.Add(time.Second) }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			service, bindings, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := materializeImmutableAPI(t, service, handle)
			before := bindings.loadCalls
			foreign := handle
			mutation.mutate(&foreign)
			state := service.handles[handle.HandleID]
			state.mu.Lock()
			err := service.FinalizeProviderCredentialFile(context.Background(), foreign, plan)
			state.mu.Unlock()
			if err == nil || errors.Is(err, ErrConsumed) || state.finalizeAttempted || bindings.loadCalls != before {
				t.Fatalf("foreign tuple saw busy state or backend: err=%v attempted=%t binding_reads=%d want=%d", err, state.finalizeAttempted, bindings.loadCalls, before)
			}
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err != nil {
				t.Errorf("original complete handle restored control: %v", err)
			}
		})
	}
}

func TestImmutableAPIFinalizeForeignPlanDoesNotConsumeOriginal(t *testing.T) {
	for _, scenario := range []string{"file", "root", "kind", "environment", "other_invocation"} {
		t.Run(scenario, func(t *testing.T) {
			service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := materializeImmutableAPI(t, service, handle)
			foreign := plan
			switch scenario {
			case "file":
				foreign.FilePath = filepath.Join(plan.RootDir, "other-key")
			case "root":
				foreign.RootDir = service.root
				foreign.FilePath = filepath.Join(service.root, "api-key")
			case "kind":
				foreign = ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryDirectV1}
			case "environment":
				foreign.EnvironmentName = "SYNTHETIC_API_KEY"
			case "other_invocation":
				other := issueImmutableAPI(t, service, request)
				foreign = materializeImmutableAPI(t, service, other)
			}
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, foreign); err == nil || service.handles[handle.HandleID].finalizeAttempted {
				t.Errorf("foreign plan consumed or qualified: err=%v", err)
			}
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err != nil {
				t.Errorf("owned plan restored control with legitimate concurrent namespace: %v", err)
			}
		})
	}
}

type immutableBindingProbe struct {
	ports.ProviderCredentialBindingStore
	load func(context.Context, ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error)
}

func (probe *immutableBindingProbe) LoadProviderCredential(ctx context.Context, locator ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	return probe.load(ctx, locator)
}

func TestImmutableAPIFinalizeCurrentBindingDenialsAreOneShot(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*domain.ProviderCredentialBindingV1)
	}{
		{"version", func(b *domain.ProviderCredentialBindingV1) { b.Version++ }},
		{"tenant", func(b *domain.ProviderCredentialBindingV1) { b.TenantID = "tenant-b" }},
		{"owner", func(b *domain.ProviderCredentialBindingV1) { b.OwnerUserID = "user-b" }},
		{"kind", func(b *domain.ProviderCredentialBindingV1) { b.ResourceKind = domain.ProviderResourceRouterAccountV1 }},
		{"id", func(b *domain.ProviderCredentialBindingV1) { b.ResourceID = "api-account-b" }},
		{"revision", func(b *domain.ProviderCredentialBindingV1) { b.ResourceRevision++ }},
		{"generation", func(b *domain.ProviderCredentialBindingV1) { b.CredentialGeneration++ }},
		{"mutation", func(b *domain.ProviderCredentialBindingV1) { b.CandidateMutationID = "different-mutation" }},
		{"revoked", func(b *domain.ProviderCredentialBindingV1) {
			b.State = domain.ProviderCredentialRevokedV1
			b.SecretRef = domain.CredentialSecretRef{}
			b.SecretFingerprint = ""
		}},
		{"ref", func(b *domain.ProviderCredentialBindingV1) {
			b.SecretRef, _ = domain.NewCredentialSecretRef("test/provider/other-ref")
		}},
		{"fingerprint", func(b *domain.ProviderCredentialBindingV1) {
			b.SecretFingerprint = domain.FingerprintCredential([]byte("different-synthetic-marker"))
		}},
		{"updated", func(b *domain.ProviderCredentialBindingV1) { b.UpdatedAt = b.UpdatedAt.Add(-time.Second) }},
		{"future_updated", func(b *domain.ProviderCredentialBindingV1) { b.UpdatedAt = b.UpdatedAt.Add(time.Second) }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			service, bindings, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := materializeImmutableAPI(t, service, handle)
			state := service.handles[handle.HandleID]
			locator := locatorFor(state.binding)
			original := bindings.bindings[locator]
			changed := original
			mutation.mutate(&changed)
			bindings.bindings[locator] = changed
			service.secrets = nil
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err == nil || !state.finalizeAttempted || state.finalized {
				t.Errorf("changed canonical binding qualified: err=%v attempted=%t finalized=%t", err, state.finalizeAttempted, state.finalized)
			}
			bindings.bindings[locator] = original
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrConsumed) {
				t.Errorf("failed attempt replay after restoring binding: %v", err)
			}
			expectImmutableRelease(t, service, handle, plan)
		})
	}
}

func TestImmutableAPIFinalizeBindingReadsUseSameContextAndCheckAfterWork(t *testing.T) {
	for _, scenario := range []string{"missing_first", "error_first", "cancel_first", "expire_first", "missing_second", "error_second", "cancel_second", "expire_second", "changed_second", "equivalent_time"} {
		t.Run(scenario, func(t *testing.T) {
			service, bindings, _, request, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := materializeImmutableAPI(t, service, handle)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			calls := 0
			service.bindings = &immutableBindingProbe{ProviderCredentialBindingStore: bindings, load: func(observed context.Context, locator ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
				calls++
				if observed != ctx || locator != locatorFor(service.handles[handle.HandleID].binding) {
					t.Error("Finalize changed caller context or exact locator")
				}
				current, found, err := bindings.LoadProviderCredential(observed, locator)
				affect := (calls == 1 && strings.HasSuffix(scenario, "first")) || (calls == 2 && strings.HasSuffix(scenario, "second"))
				if affect {
					switch {
					case strings.HasPrefix(scenario, "missing"):
						found = false
					case strings.HasPrefix(scenario, "error"):
						err = errors.New("synthetic backend detail")
					case strings.HasPrefix(scenario, "cancel"):
						cancel()
					case strings.HasPrefix(scenario, "expire"):
						clock.Set(handle.ExpiresAt)
					case strings.HasPrefix(scenario, "changed"):
						current.CredentialGeneration++
					}
				}
				if scenario == "equivalent_time" {
					current.UpdatedAt = current.UpdatedAt.In(time.FixedZone("equivalent", 3600))
				}
				return current, found, err
			}}
			service.secrets = nil
			err := service.FinalizeProviderCredentialFile(ctx, handle, plan)
			state := service.handles[handle.HandleID]
			if scenario == "equivalent_time" {
				if err != nil || !state.finalized || calls != 2 {
					t.Errorf("equal time semantics control: err=%v finalized=%t reads=%d want=2", err, state.finalized, calls)
				}
			} else if err == nil || state.finalized || !state.finalizeAttempted || calls > 2 {
				t.Errorf("uncertain backend work qualified: err=%v finalized=%t attempted=%t calls=%d", err, state.finalized, state.finalizeAttempted, calls)
			}
			expectImmutableRelease(t, service, handle, plan)
		})
	}
}

func TestImmutableAPIFinalizeRejectsCancelledExpiredAndMissingState(t *testing.T) {
	for _, scenario := range []string{"nil_context", "cancelled", "expired", "released", "fenced", "closed_files"} {
		t.Run(scenario, func(t *testing.T) {
			service, _, _, request, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			var handle ports.ProviderInvocationCredentialV1
			if scenario == "closed_files" {
				var err error
				handle, err = service.IssueProviderCredential(context.Background(), request)
				if err != nil {
					t.Fatalf("issue API for owned-descriptor Close: %v", err)
				}
				t.Cleanup(func() {
					if err := service.ReleaseProviderCredential(context.Background(), handle); !errors.Is(err, ErrDeliveryMismatch) {
						t.Errorf("Release must report lost namespace after explicit Close: %v", err)
					}
				})
			} else {
				handle = issueImmutableAPI(t, service, request)
			}
			plan := materializeImmutableAPI(t, service, handle)
			ctx := context.Background()
			switch scenario {
			case "nil_context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "expired":
				clock.Set(handle.ExpiresAt)
			case "released":
				expectImmutableRelease(t, service, handle, plan)
			case "fenced":
				if err := service.FenceProviderCredentialInvocations(ctx, locatorFor(service.handles[handle.HandleID].binding), 2); err != nil {
					t.Fatalf("fence old generation: %v", err)
				}
			case "closed_files":
				if err := service.files.Close(); err != nil {
					t.Fatalf("close original owned descriptors: %v", err)
				}
			}
			service.secrets = nil
			if err := service.FinalizeProviderCredentialFile(ctx, handle, plan); err == nil {
				t.Error("cancelled, expired or retired invocation qualified")
			}
			if scenario != "closed_files" {
				expectImmutableRelease(t, service, handle, plan)
			}
		})
	}
}

func TestImmutableAPIFinalizeFileIdentityAndBytesDenials(t *testing.T) {
	for _, scenario := range []string{"same_byte_replacement", "same_inode_changed_byte", "truncate", "append", "mode"} {
		t.Run(scenario, func(t *testing.T) {
			service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, service, request)
			plan := materializeImmutableAPI(t, service, handle)
			original, err := os.ReadFile(plan.FilePath)
			if err != nil {
				t.Fatalf("read bounded synthetic test fixture: %v", err)
			}
			defer zero(original)
			var saved string
			switch scenario {
			case "same_byte_replacement":
				// Keep the original inode alive in a separately test-owned path;
				// recreating a path after unlink must not recycle that identity.
				saved = filepath.Join(t.TempDir(), "original-key")
				if err := os.Rename(plan.FilePath, saved); err != nil {
					t.Fatalf("retain original test inode: %v", err)
				}
				if err := os.WriteFile(plan.FilePath, original, 0o600); err != nil {
					t.Fatalf("replace synthetic key path: %v", err)
				}
			case "same_inode_changed_byte":
				changed := append([]byte(nil), original...)
				changed[0] ^= 1
				if err := os.WriteFile(plan.FilePath, changed, 0o600); err != nil {
					t.Fatalf("modify synthetic bytes: %v", err)
				}
				zero(changed)
			case "truncate":
				if err := os.Truncate(plan.FilePath, 0); err != nil {
					t.Fatalf("truncate synthetic key: %v", err)
				}
			case "append":
				if err := os.WriteFile(plan.FilePath, append(original, '!'), 0o600); err != nil {
					t.Fatalf("grow synthetic key: %v", err)
				}
			case "mode":
				if err := os.Chmod(plan.FilePath, 0o644); err != nil {
					t.Fatalf("mutate synthetic mode: %v", err)
				}
			}
			service.secrets = nil
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrDeliveryMismatch) || service.handles[handle.HandleID].finalized {
				t.Errorf("changed original file qualified: err=%v finalized=%t", err, service.handles[handle.HandleID].finalized)
			}
			if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrConsumed) {
				t.Errorf("integrity failure replay: %v", err)
			}
			if saved != "" {
				// Cleanup cannot unlink a foreign replacement as if it were the
				// original. Restore the ORIGINAL test inode, not equivalent bytes.
				if err := os.Remove(plan.FilePath); err != nil {
					t.Fatalf("remove test-owned replacement: %v", err)
				}
				if err := os.Rename(saved, plan.FilePath); err != nil {
					t.Fatalf("restore original test inode before Release: %v", err)
				}
			}
			expectImmutableRelease(t, service, handle, plan)
		})
	}
}

func TestImmutableAPIFinalizeUnsupportedDeliveryMatrixUnchanged(t *testing.T) {
	for _, kind := range []domain.ProviderResourceKindV1{domain.ProviderResourceAPIAccountV1, domain.ProviderResourceRouterAccountV1} {
		for _, delivery := range []domain.ProviderCredentialDeliveryKindV1{domain.ProviderCredentialDeliveryFileV1, domain.ProviderCredentialDeliveryEnvironmentV1, domain.ProviderCredentialDeliveryDirectV1} {
			if kind == domain.ProviderResourceAPIAccountV1 && delivery == domain.ProviderCredentialDeliveryFileV1 {
				continue
			}
			t.Run(string(kind)+"/"+string(delivery), func(t *testing.T) {
				var service *InvocationService
				var secrets *fakeSecretStore
				var request ports.ProviderCredentialIssueRequestV1
				if kind == domain.ProviderResourceAPIAccountV1 {
					service, _, secrets, request, _ = immutableAPIInvocationFixture(t, delivery)
				} else {
					service, _, secrets, request, _ = invocationFixture(t, delivery)
				}
				handle := issueImmutableAPI(t, service, request)
				plan := materializeImmutableAPI(t, service, handle)
				if secrets.readCalls != 1 || secrets.recoverCalls != 1 {
					t.Error("existing delivery did not perform one genuine materialization")
				}
				service.secrets = nil
				if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrDeliveryMismatch) {
					t.Errorf("unsupported Finalize arm: err=%v want=%v", err, ErrDeliveryMismatch)
				}
				expectImmutableRelease(t, service, handle, plan)
			})
		}
	}
}

func waitImmutableEvent[T any](t *testing.T, channel <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s acknowledgement", operation)
		var zero T
		return zero
	}
}

func TestImmutableAPIFinalizeConcurrentAttemptIsNonblockingAndOneShot(t *testing.T) {
	service, bindings, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	handle := issueImmutableAPI(t, service, request)
	plan := materializeImmutableAPI(t, service, handle)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(resume) }) }
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	service.bindings = &immutableBindingProbe{ProviderCredentialBindingStore: bindings, load: func(observed context.Context, locator ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
		calls++
		if calls == 1 {
			close(entered)
			select {
			case <-resume:
			case <-observed.Done():
			}
		}
		return bindings.LoadProviderCredential(observed, locator)
	}}
	finalized := make(chan struct{})
	var result error
	go func() { defer close(finalized); result = service.FinalizeProviderCredentialFile(ctx, handle, plan) }()
	t.Cleanup(func() { cancel(); finish(); waitImmutableEvent(t, finalized, "finalizer cleanup") })
	waitImmutableEvent(t, entered, "first current-binding read")
	if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrConsumed) {
		t.Errorf("concurrent Finalize did not reject busy state: %v", err)
	}
	finish()
	waitImmutableEvent(t, finalized, "first Finalize")
	if result != nil {
		t.Errorf("first Finalize valid control: %v", result)
	}
	if calls != 2 {
		t.Errorf("concurrent Finalize read count=%d want=2", calls)
	}
	if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrConsumed) {
		t.Errorf("concurrent winner replay: %v", err)
	}
}

// This authorizer is ONLY the original mutation service's bounded synthetic
// principal dependency. It supplies no Pi phase or owner-spending authority.
type immutableMutationPrincipal struct {
	principal ports.ProviderCredentialPrincipalV1
	operation ports.ProviderCredentialMutationOperationV1
	calls     int
}

func (authorizer *immutableMutationPrincipal) AuthorizeProviderCredentialMutation(_ context.Context, principal ports.ProviderCredentialPrincipalV1, operation ports.ProviderCredentialMutationOperationV1) (bool, error) {
	authorizer.calls++
	return principal.Validate() == nil && principal == authorizer.principal && operation == authorizer.operation, nil
}

type immutableObservedFence struct {
	service *InvocationService
	locator ports.ProviderCredentialLocatorV1
	entered chan struct{}
}

func (fence *immutableObservedFence) FenceProviderCredentialInvocations(ctx context.Context, locator ports.ProviderCredentialLocatorV1, generation uint64) error {
	if locator != fence.locator || generation != 2 {
		return ErrInvalid
	}
	// Existing mutation service reaches this only after its durable store
	// operation. It delegates to the real local fence without replacing it.
	close(fence.entered)
	return fence.service.FenceProviderCredentialInvocations(ctx, locator, generation)
}

func TestImmutableAPIFinalizeDurableMutationPrecedesWaitingFence(t *testing.T) {
	for _, operation := range []ports.ProviderCredentialMutationOperationV1{ports.ProviderCredentialOperationIngestV1, ports.ProviderCredentialOperationRevokeV1} {
		t.Run(string(operation), func(t *testing.T) {
			invocations, bindings, secrets, request, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			handle := issueImmutableAPI(t, invocations, request)
			locator := ports.ProviderCredentialLocatorV1{TenantID: handle.TenantID, OwnerUserID: handle.OwnerUserID, ResourceKind: handle.ProviderResource.Kind, ResourceID: handle.ProviderResource.ResourceID}
			principal := ports.ProviderCredentialPrincipalV1{TenantID: handle.TenantID, OwnerUserID: handle.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1}
			authorizer := &immutableMutationPrincipal{principal: principal, operation: operation}
			// Explicitly prove deny-by-default on both foreign principal and op.
			foreign := principal
			foreign.OwnerUserID = "user-b"
			if ok, _ := authorizer.AuthorizeProviderCredentialMutation(context.Background(), foreign, operation); ok {
				t.Fatal("foreign synthetic mutation principal allowed")
			}
			if ok, _ := authorizer.AuthorizeProviderCredentialMutation(context.Background(), principal, "immutable_finalize"); ok {
				t.Fatal("local mutation dependency fabricated Pi phase authority")
			}
			fence := &immutableObservedFence{service: invocations, locator: locator, entered: make(chan struct{})}
			mutations, err := New(Config{Clock: clock, IDs: &fixedIDs{}}, authorizer, fence, bindings, secrets)
			if err != nil {
				t.Fatalf("construct existing mutation service: %v", err)
			}
			started, resume, materialized := make(chan ports.ProviderCredentialMaterializationV1, 1), make(chan struct{}), make(chan struct{})
			var stopOnce sync.Once
			stop := func() { stopOnce.Do(func() { close(resume) }) }
			var materializeErr error
			go func() {
				defer close(materialized)
				materializeErr = invocations.MaterializeProviderCredential(context.Background(), handle, func(plan ports.ProviderCredentialMaterializationV1, _ []byte) error {
					started <- plan
					<-resume
					return nil
				})
			}()
			t.Cleanup(func() { stop(); waitImmutableEvent(t, materialized, "materialization cleanup") })
			plan := waitImmutableEvent(t, started, "generation G API callback")
			clock.Set(time.Unix(101, 0).UTC())
			mutated := make(chan struct{})
			var mutationErr error
			go func() {
				defer close(mutated)
				if operation == ports.ProviderCredentialOperationIngestV1 {
					_, mutationErr = mutations.Ingest(context.Background(), ports.ProviderCredentialIngestRequestV1{Principal: principal, ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID}, []byte("synthetic-rotated-api-marker"))
				} else {
					_, mutationErr = mutations.Revoke(context.Background(), ports.ProviderCredentialRevokeRequestV1{Principal: principal, ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID})
				}
			}()
			t.Cleanup(func() { stop(); waitImmutableEvent(t, mutated, "mutation cleanup") })
			waitImmutableEvent(t, fence.entered, "durable G+1 then real Fence entry")
			current, found, err := bindings.LoadProviderCredential(context.Background(), locator)
			if err != nil || !found || current.CredentialGeneration != 2 || current.ResourceRevision != 2 {
				t.Fatalf("durable G+1 during blocked callback: found=%t generation=%d revision=%d err=%v", found, current.CredentialGeneration, current.ResourceRevision, err)
			}
			if operation == ports.ProviderCredentialOperationRevokeV1 && current.State != domain.ProviderCredentialRevokedV1 {
				t.Error("durable revoke was not published before Fence")
			}
			if err := invocations.FinalizeProviderCredentialFile(context.Background(), handle, plan); !errors.Is(err, ErrConsumed) {
				t.Errorf("active callback Finalize did not reject busy state: %v", err)
			}
			secrets.mu.Lock()
			readBefore, recoverBefore, putBefore, deleteBefore := secrets.readCalls, secrets.recoverCalls, secrets.putCalls, len(secrets.deleted)
			secrets.mu.Unlock()
			if deleteBefore != 0 {
				t.Error("old secret deleted before callback/Fence completion")
			}
			stop()
			waitImmutableEvent(t, materialized, "callback return and unlock")
			if materializeErr != nil {
				t.Errorf("materialization after durable mutation: %v", materializeErr)
			}
			// Either Finalize or Fence may win this lock. Both must deny old G.
			if err := invocations.FinalizeProviderCredentialFile(context.Background(), handle, plan); err == nil {
				t.Error("old generation finalized after durable G+1")
			}
			waitImmutableEvent(t, mutated, "existing mutation completion")
			if mutationErr != nil {
				t.Errorf("existing mutation service error: %v", mutationErr)
			}
			secrets.mu.Lock()
			readAfter, recoverAfter, putAfter := secrets.readCalls, secrets.recoverCalls, secrets.putCalls
			secrets.mu.Unlock()
			if readAfter != readBefore || recoverAfter != recoverBefore || putAfter != putBefore {
				t.Errorf("stale Finalize crossed secret backend: read=%d/%d recover=%d/%d put=%d/%d", readAfter, readBefore, recoverAfter, recoverBefore, putAfter, putBefore)
			}
			if authorizer.calls != 3 {
				t.Errorf("synthetic mutation authorizer calls=%d want=3; no Pi phase/spend authority exists", authorizer.calls)
			}
			state := invocations.handles[handle.HandleID]
			if state.finalized || !state.revoked {
				t.Errorf("stale state finalized=%t revoked=%t", state.finalized, state.revoked)
			}
			expectImmutableRelease(t, invocations, handle, plan)
		})
	}
}

func TestImmutableAPIFinalizeOverlapsReleaseFenceAndClose(t *testing.T) {
	for _, operation := range []string{"release", "fence", "close"} {
		t.Run(operation, func(t *testing.T) {
			service, bindings, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			var handle ports.ProviderInvocationCredentialV1
			if operation == "close" {
				var err error
				handle, err = service.IssueProviderCredential(context.Background(), request)
				if err != nil {
					t.Fatalf("issue API for concurrent descriptor Close: %v", err)
				}
				t.Cleanup(func() {
					if err := service.ReleaseProviderCredential(context.Background(), handle); !errors.Is(err, ErrDeliveryMismatch) {
						t.Errorf("Release after explicit concurrent Close: %v", err)
					}
				})
			} else {
				handle = issueImmutableAPI(t, service, request)
			}
			plan := materializeImmutableAPI(t, service, handle)
			entered, resume := make(chan struct{}), make(chan struct{})
			var resumeOnce sync.Once
			resumeWork := func() { resumeOnce.Do(func() { close(resume) }) }
			calls := 0
			service.bindings = &immutableBindingProbe{ProviderCredentialBindingStore: bindings, load: func(ctx context.Context, locator ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
				calls++
				if calls == 1 {
					close(entered)
					<-resume
				}
				return bindings.LoadProviderCredential(ctx, locator)
			}}
			finished := make(chan struct{})
			var result error
			go func() {
				defer close(finished)
				result = service.FinalizeProviderCredentialFile(context.Background(), handle, plan)
			}()
			t.Cleanup(func() { resumeWork(); waitImmutableEvent(t, finished, "overlapping Finalize cleanup") })
			waitImmutableEvent(t, entered, "overlapping Finalize binding read")
			retired := make(chan struct{})
			var retireErr error
			go func() {
				defer close(retired)
				switch operation {
				case "release":
					retireErr = service.ReleaseProviderCredential(context.Background(), handle)
				case "fence":
					retireErr = service.FenceProviderCredentialInvocations(context.Background(), locatorFor(service.handles[handle.HandleID].binding), 2)
				case "close":
					retireErr = service.files.Close()
				}
			}()
			t.Cleanup(func() { resumeWork(); waitImmutableEvent(t, retired, "overlapping retirement cleanup") })
			if operation == "close" {
				waitImmutableEvent(t, retired, "wrapper Close before file verification")
			}
			resumeWork()
			waitImmutableEvent(t, finished, "overlapping Finalize completion")
			waitImmutableEvent(t, retired, "overlapping retirement completion")
			if retireErr != nil {
				t.Errorf("overlapping %s error: %v", operation, retireErr)
			}
			if operation == "close" {
				if !errors.Is(result, ErrDeliveryMismatch) {
					t.Errorf("Finalize after descriptor Close: %v", result)
				}
				// Explicit Close preserves crash residue. Release honestly reports
				// cleanup failure rather than manufacturing namespace removal.
				if err := service.ReleaseProviderCredential(context.Background(), handle); !errors.Is(err, ErrDeliveryMismatch) {
					t.Errorf("Release after Close error=%v", err)
				}
			} else {
				if result != nil {
					t.Errorf("Finalize ordered before %s: %v", operation, result)
				}
				if err := service.FinalizeProviderCredentialFile(context.Background(), handle, plan); err == nil {
					t.Error("retired finalization replay succeeded")
				}
				expectImmutableRelease(t, service, handle, plan)
			}
		})
	}
}
