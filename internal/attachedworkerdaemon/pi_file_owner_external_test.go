package attachedworkerdaemon_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	daemon "gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

type piFileBackend struct {
	ports.ProviderCredentialBindingStore
	ports.ProviderCredentialSecretStore
	binding      domain.ProviderCredentialBindingV1
	reads, plans int
	lastRead     []byte
}

func (b *piFileBackend) LoadProviderCredential(context.Context, ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	return b.binding, true, nil
}
func (b *piFileBackend) RecoverProviderCredentialCandidate(context.Context, domain.ProviderCredentialBindingV1) (bool, error) {
	return true, nil
}
func (b *piFileBackend) ReadProviderCredentialSecret(context.Context, domain.ProviderCredentialBindingV1, int64) ([]byte, error) {
	b.reads++
	b.lastRead = []byte("synthetic-original-file-marker")
	return b.lastRead, nil
}
func (b *piFileBackend) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	b.plans++
	return ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryFileV1, FileName: "api-key"}, nil
}

type piFileExecutor func(context.Context, daemon.PreparedInvocationV1) (daemon.AttemptResult, error)

func (f piFileExecutor) RunPrepared(c context.Context, i daemon.PreparedInvocationV1) (daemon.AttemptResult, error) {
	return f(c, i)
}

func TestPiFileOwnerOriginalRunnerBinding(t *testing.T) {
	for _, mode := range []string{"valid", "foreign context", "foreign handle", "foreign plan", "mutated body", "repeat", "close"} {
		t.Run(mode, func(t *testing.T) {
			var recipe *providercredential.PreparedPiFileUseV1
			var backend *piFileBackend
			var service *providercredential.InvocationService
			var preparationContext context.Context
			var originalBody []byte
			called := 0
			result, runErr, order := daemon.ExerciseOriginalAPIRunnerForTest(t, func(ctx context.Context, data *daemon.PreparedAPIDataV1) ports.ProviderResourceCredentialLifecycleV1 {
				preparationContext = ctx
				b := data.Issue.HarnessBinding
				ref, err := domain.NewCredentialSecretRef("test/pi/source-owned-file")
				if err != nil {
					t.Fatal(err)
				}
				backend = &piFileBackend{binding: domain.ProviderCredentialBindingV1{Version: domain.ProviderCredentialBindingVersionV1, TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, ResourceKind: b.Resource.Kind, ResourceID: b.Resource.ResourceID, ResourceRevision: b.Resource.Revision, CredentialGeneration: b.Resource.CredentialGeneration, CandidateMutationID: "test-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("synthetic-original-file-marker")), UpdatedAt: time.Now().UTC()}}
				root, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(root, 0700); err != nil {
					t.Fatal(err)
				}
				files, err := credentiallifecycle.NewPinnedSecretFileSystem(root)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := files.Close(); err != nil {
						t.Error(err)
					}
				})
				service, err = providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: apiReceiverClock{time.Now().UTC()}, IDs: apiReceiverIDs{}, FileSystem: files}, backend, backend, backend)
				if err != nil {
					t.Fatal(err)
				}
				file, err := service.PrepareImmutableAPIFileV1(ctx, data.Issue, "api-key", 8<<10)
				if err != nil {
					t.Fatal(err)
				}
				recipe, err = file.PreparePiFileUseV1(data.Ledger, data.ParentRequest.Body, b.Backend.BackendProfileDigest)
				if err != nil {
					t.Fatal(err)
				}
				originalBody = data.ParentRequest.Body
				if backend.reads != 0 || backend.plans != 0 {
					t.Fatal("preparation read credential backend/planner")
				}
				t.Cleanup(func() {
					if err := recipe.Close(); err != nil {
						t.Error(err)
					}
				})
				return service
			}, piFileExecutor(func(ctx context.Context, in daemon.PreparedInvocationV1) (daemon.AttemptResult, error) {
				called++
				if ctx == preparationContext {
					t.Error("fixture did not preserve distinct pre-Issue and Materialize contexts")
				}
				h, p := in.Credential, in.Materialization
				originalCtx := ctx
				switch mode {
				case "foreign context":
					ctx = context.WithValue(ctx, struct{}{}, true)
				case "foreign handle":
					h.HandleID += "x"
				case "foreign plan":
					p.FilePath += "x"
				case "mutated body":
					originalBody[0] ^= 1
				case "close":
					if err := recipe.Close(); !errors.Is(err, providercredential.ErrConsumed) {
						t.Errorf("active callback Close must defer live retirement: %v", err)
					}
				}
				err := recipe.BindMaterialized(ctx, h, p, in.PhysicalSend)
				if (err == nil) != (mode == "valid" || mode == "repeat") {
					t.Errorf("%s bind error=%v", mode, err)
				}
				if mode == "repeat" && recipe.BindMaterialized(originalCtx, h, p, in.PhysicalSend) == nil {
					t.Error("repeat bind admitted")
				}
				// No HMAC/fixed TLS consumer exists yet. Even swallowed bind
				// failures must not leave the old structural path usable.
				if _, err := in.PhysicalSend.Authorize(in.APIRequest.Body, in.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest); err == nil {
					t.Error("file binding bypassed through structural send")
				}
				return daemon.AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
			}))
			if called != 1 || backend.reads != 1 || backend.plans != 1 || !bytes.Equal(backend.lastRead, make([]byte, len(backend.lastRead))) {
				t.Fatalf("original lifecycle: calls=%d reads=%d plans=%d secretZeroed=%v", called, backend.reads, backend.plans, bytes.Equal(backend.lastRead, make([]byte, len(backend.lastRead))))
			}
			if runErr == nil || !result.CredentialReleased {
				t.Fatalf("incomplete transport became success/leaked credential: err=%v released=%v", runErr, result.CredentialReleased)
			}
			for _, phase := range order {
				if strings.Contains(phase, "physical_send") || phase == "immutable_finalize" {
					t.Fatalf("premature Source phase: %q", phase)
				}
			}
			if recipe.BindMaterialized(t.Context(), ports.ProviderInvocationCredentialV1{}, ports.ProviderCredentialMaterializationV1{}, nil) == nil {
				t.Error("late recipe admitted")
			}
		})
	}
}
