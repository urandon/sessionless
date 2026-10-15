package providercredential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func prepareFileFixture(t *testing.T, service *InvocationService, request ports.ProviderCredentialIssueRequestV1) *PreparedImmutableAPIFileV1 {
	t.Helper()
	p, err := service.PrepareImmutableAPIFileV1(t.Context(), request, "api-key", service.maxBytes)
	if err != nil {
		t.Fatal("original pre-Issue file source:", err)
	}
	return p
}

func TestPreparedImmutableFileOriginalCallbackOnly(t *testing.T) {
	service, bindings, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	// Preserve the actual Issue's UTC/microsecond canonicalization.
	request.ExpiresAt = request.ExpiresAt.Add(123456789 * time.Nanosecond).In(time.FixedZone("fixture", 3600))
	source := prepareFileFixture(t, service, request)
	n, err := source.FilePathBound(t.Context(), uint64(service.maxBytes))
	if err != nil || n == 0 || len(service.handles) != 0 || bindings.loadCalls != 0 || secrets.readCalls != 0 || secrets.recoverCalls != 0 || service.planner.(*fixedDeliveryPlanner).calls != 0 {
		t.Fatal("pre-Issue source created a handle or touched planner/backend:", err)
	}
	handle := issueImmutableAPI(t, service, request)
	var original ports.ProviderCredentialMaterializationV1
	if err = source.VerifyMaterialized(t.Context(), handle, ports.ProviderCredentialMaterializationV1{}); err == nil {
		t.Error("pre-callback verification admitted")
	}
	err = service.MaterializeProviderCredential(t.Context(), handle, func(plan ports.ProviderCredentialMaterializationV1, secret []byte) error {
		original = plan
		if len(secret) != 0 || uint64(len(plan.FilePath)) != n {
			t.Error("file-only delivery/bound changed")
		}
		before := bindings.loadCalls
		// Any new secret/planner operation is now causally unavailable.
		service.secrets, service.planner = nil, nil
		if err := source.VerifyMaterialized(t.Context(), handle, plan); err != nil {
			t.Error("actual active file denied:", err)
		}
		if bindings.loadCalls != before {
			t.Error("verifier queried binding backend")
		}
		foreign := handle
		foreign.AttemptID = "foreign-attempt"
		if err := source.VerifyMaterialized(t.Context(), foreign, plan); err == nil {
			t.Error("foreign original tuple admitted")
		}
		for _, field := range []string{"tenant", "owner", "run", "worker", "lease", "fence", "resource", "revision", "generation", "expiry", "handle"} {
			foreign := handle
			switch field {
			case "tenant":
				foreign.TenantID = "other-tenant"
			case "owner":
				foreign.OwnerUserID = "other-owner"
				foreign.ProviderResource.OwnerUserID = foreign.OwnerUserID
			case "run":
				foreign.RunID = "other-run"
			case "worker":
				foreign.WorkerID = "other-worker"
			case "lease":
				foreign.LeaseID = "other-lease"
			case "fence":
				foreign.LeaseFence++
			case "resource":
				foreign.ProviderResource.ResourceID = "other-resource"
			case "revision":
				foreign.ProviderResource.Revision++
			case "generation":
				foreign.ProviderResource.CredentialGeneration++
			case "expiry":
				foreign.ExpiresAt = foreign.ExpiresAt.Add(time.Microsecond)
			case "handle":
				foreign.HandleID = "other-handle"
			}
			if foreign.Validate() != nil {
				t.Fatalf("invalid %s negative control", field)
			}
			if err := source.VerifyMaterialized(t.Context(), foreign, plan); err == nil {
				t.Error("foreign tuple admitted:", field)
			}
		}
		foreignPlan := plan
		foreignPlan.FilePath = filepath.Join(plan.RootDir, "other")
		if err := source.VerifyMaterialized(t.Context(), handle, foreignPlan); err == nil {
			t.Error("arbitrary path admitted")
		}
		if err := service.FinalizeProviderCredentialFile(t.Context(), handle, plan); !errors.Is(err, ErrConsumed) {
			t.Error("verification reentered Finalize:", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal("original Materialize:", err)
	}
	if err = source.VerifyMaterialized(t.Context(), handle, original); err == nil {
		t.Error("completed callback remained active")
	}
	if err = service.FinalizeProviderCredentialFile(t.Context(), handle, original); err != nil {
		t.Error("original after-unlock Finalize:", err)
	}
	expectImmutableRelease(t, service, handle, original)
	if err = source.VerifyMaterialized(t.Context(), handle, original); err == nil {
		t.Error("released file remained active")
	}
}

func TestPreparedImmutableFileCallbackFailureAndPanicRetire(t *testing.T) {
	for _, mode := range []string{"error", "panic", "cancel-original", "cancel-consumer", "expiry", "mutation", "foreign-same-filesystem"} {
		t.Run(mode, func(t *testing.T) {
			service, _, _, request, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source, err := service.PrepareImmutableAPIFileV1(ctx, request, "api-key", service.maxBytes)
			if err != nil {
				t.Fatal(err)
			}
			handle := issueImmutableAPI(t, service, request)
			var plan ports.ProviderCredentialMaterializationV1
			func() {
				defer func() {
					r := recover()
					if mode == "panic" && r == nil {
						t.Error("expected synthetic callback panic missing")
					}
					if mode != "panic" && r != nil {
						panic(r)
					}
				}()
				err = service.MaterializeProviderCredential(ctx, handle, func(value ports.ProviderCredentialMaterializationV1, _ []byte) error {
					plan = value
					if mode == "error" {
						return errors.New("public callback control")
					}
					if mode == "panic" {
						panic("public callback control")
					}
					verifyCtx := ctx
					switch mode {
					case "cancel-original":
						cancel()
						verifyCtx = t.Context()
					case "cancel-consumer":
						var stop context.CancelFunc
						verifyCtx, stop = context.WithCancel(ctx)
						stop()
					case "expiry":
						clock.Set(request.ExpiresAt)
					case "mutation":
						if err := os.WriteFile(value.FilePath, []byte("public-mutated-file"), 0600); err != nil {
							t.Fatal(err)
						}
					case "foreign-same-filesystem":
						foreign, err := service.files.CreateImmutable("api-key", []byte("public-foreign-file"), service.maxBytes)
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							if err := service.files.Cleanup(foreign); err != nil {
								t.Error(err)
							}
						}()
						value.RootDir, value.FilePath = foreign.RootDir, foreign.FilePath
					}
					if err := source.VerifyMaterialized(verifyCtx, handle, value); err == nil {
						t.Error("invalid active file admitted:", mode)
					}
					return errors.New("public denied callback")
				})
			}()
			if mode != "panic" && err == nil {
				t.Error("failed callback succeeded")
			}
			if err := source.VerifyMaterialized(t.Context(), handle, plan); err == nil {
				t.Error("failed callback remained active")
			}
			if service.handles[handle.HandleID].fileUse.Load() != nil {
				t.Error("callback record retained after error/panic")
			}
			expectImmutableRelease(t, service, handle, plan)
		})
	}
}

// The first caller check passes; the in-record check blocks while holding its
// real use gate. All channels are bubble-owned; no sleep decides correctness.
type fileVerificationContext struct {
	context.Context
	mu              sync.Mutex
	calls           int
	entered, resume chan struct{}
}

func (c *fileVerificationContext) Err() error {
	c.mu.Lock()
	c.calls++
	n := c.calls
	c.mu.Unlock()
	if n == 2 {
		close(c.entered)
		<-c.resume
	}
	return c.Context.Err()
}

func TestPreparedImmutableFileCallbackRetirementJoinsVerifier(t *testing.T) {
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	source := prepareFileFixture(t, service, request)
	handle := issueImmutableAPI(t, service, request)
	// Filesystem setup is outside the bubble; only local synchronous file IO
	// and channel/mutex synchronization occur inside. No process/network clock.
	synctest.Test(t, func(t *testing.T) {
		ctx := &fileVerificationContext{Context: context.Background(), entered: make(chan struct{}), resume: make(chan struct{})}
		returnCallback, callbackReturning, materialized, verified := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var materializeErr, verifyErr error
		var plan ports.ProviderCredentialMaterializationV1
		go func() {
			defer close(materialized)
			materializeErr = service.MaterializeProviderCredential(context.Background(), handle, func(value ports.ProviderCredentialMaterializationV1, _ []byte) error {
				plan = value
				go func() { defer close(verified); verifyErr = source.VerifyMaterialized(ctx, handle, value) }()
				<-returnCallback
				close(callbackReturning)
				return nil
			})
		}()
		<-ctx.entered
		close(returnCallback)
		<-callbackReturning
		synctest.Wait()
		select {
		case <-materialized:
			t.Error("Materialize returned before its verifier joined")
		default:
		}
		close(ctx.resume)
		<-verified
		<-materialized
		if !errors.Is(verifyErr, ErrExpired) || materializeErr != nil {
			t.Error("genuine verifier/join:", verifyErr, materializeErr)
		}
		if err := source.VerifyMaterialized(context.Background(), handle, plan); err == nil {
			t.Error("late verifier admitted")
		}
		expectImmutableRelease(t, service, handle, plan)
	})
}

func TestPreparedImmutableFilePrerequisitesAndOriginalScope(t *testing.T) {
	for _, mode := range []string{"nil-service", "zero-service", "nil-context", "cancelled", "wrong-max", "wrong-name", "environment", "router", "expired"} {
		t.Run(mode, func(t *testing.T) {
			service, _, _, request, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			ctx := t.Context()
			maxBytes := service.maxBytes
			name := "api-key"
			switch mode {
			case "nil-service":
				service = nil
			case "zero-service":
				service = new(InvocationService)
			case "nil-context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "wrong-max":
				maxBytes--
			case "wrong-name":
				name = "../api-key"
			case "environment":
				request.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryEnvironmentV1
			case "router":
				request.HarnessBinding.Resource.Kind = domain.ProviderResourceRouterAccountV1
			case "expired":
				clock.Set(request.ExpiresAt)
			}
			if p, err := service.PrepareImmutableAPIFileV1(ctx, request, name, maxBytes); err == nil || p != nil {
				t.Error("invalid pre-Issue source admitted:", mode)
			}
		})
	}
}

func TestPreparedImmutableFilePreparationCopiesScopeAndRejectsWrongTemplate(t *testing.T) {
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	original := request
	source := prepareFileFixture(t, service, request)
	request.HarnessBinding.RunID = "changed-after-prepare"
	request.ExpiresAt = request.ExpiresAt.Add(time.Minute)
	scope := source.Scope()
	scope.TenantID = "changed-returned-data"
	wrongName, err := service.PrepareImmutableAPIFileV1(t.Context(), original, "not-key", service.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	handle := issueImmutableAPI(t, service, original)
	err = service.MaterializeProviderCredential(t.Context(), handle, func(plan ports.ProviderCredentialMaterializationV1, _ []byte) error {
		if err := source.VerifyMaterialized(t.Context(), handle, plan); err != nil {
			t.Error("mutated external DATA changed original scope:", err)
		}
		if err := wrongName.VerifyMaterialized(t.Context(), handle, plan); err == nil {
			t.Error("different real Issue template admitted")
		}
		if err := new(PreparedImmutableAPIFileV1).VerifyMaterialized(t.Context(), handle, plan); err == nil {
			t.Error("zero source admitted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPreparedImmutableFileRejectsDifferentActualIssueBinding(t *testing.T) {
	for _, field := range []string{"model", "backend-profile", "policy", "evidence-expiry"} {
		t.Run(field, func(t *testing.T) {
			service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			source := prepareFileFixture(t, service, request)
			switch field {
			case "model":
				request.HarnessBinding.ModelID = "other-model"
			case "backend-profile":
				request.HarnessBinding.Backend.BackendProfileDigest = strings.Repeat("a", 64)
			case "policy":
				request.HarnessBinding.EffectivePolicyDigest = strings.Repeat("b", 64)
			case "evidence-expiry":
				value := request.HarnessBinding.EvidenceExpiresAt.Add(time.Second)
				request.HarnessBinding.EvidenceExpiresAt = &value
			}
			handle := issueImmutableAPI(t, service, request)
			if err := service.MaterializeProviderCredential(t.Context(), handle, func(plan ports.ProviderCredentialMaterializationV1, _ []byte) error {
				if err := source.VerifyMaterialized(t.Context(), handle, plan); err == nil {
					t.Error("different actual Issue binding admitted:", field)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
