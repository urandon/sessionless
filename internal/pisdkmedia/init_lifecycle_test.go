package pisdkmedia

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

const initPublicLiteral = "PUBLIC-NONCREDENTIAL-INIT-CONTROL"

// Only these explicit local ports exist; unexpected backend methods panic
// through the nil embedded interface. There is no real credential or network.
type initLocalStore struct {
	ports.ProviderCredentialBindingStore
	ports.ProviderCredentialSecretStore
	binding                domain.ProviderCredentialBindingV1
	loads, reads, recovers int
}

func (s *initLocalStore) LoadProviderCredential(_ context.Context, loc ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	s.loads++
	b := s.binding
	return b, loc.TenantID == b.TenantID && loc.OwnerUserID == b.OwnerUserID && loc.ResourceKind == b.ResourceKind && loc.ResourceID == b.ResourceID, nil
}
func (s *initLocalStore) RecoverProviderCredentialCandidate(_ context.Context, b domain.ProviderCredentialBindingV1) (bool, error) {
	s.recovers++
	return b == s.binding, nil
}
func (s *initLocalStore) ReadProviderCredentialSecret(_ context.Context, b domain.ProviderCredentialBindingV1, limit int64) ([]byte, error) {
	s.reads++
	if b != s.binding || int64(len(initPublicLiteral)) > limit {
		return nil, providercredential.ErrInvalid
	}
	return []byte(initPublicLiteral), nil
}

type initLocalClock struct{ now time.Time }

func (c initLocalClock) Now() time.Time { return c.now }

type initLocalIDs struct{}

func (initLocalIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "public-init-handle", nil
}

type initLocalPlanner struct{}

func (initLocalPlanner) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	return ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryFileV1, FileName: "api-key"}, nil
}

type initCredentials struct {
	service *providercredential.InvocationService
	source  *providercredential.PreparedImmutableAPIFileV1
	request ports.ProviderCredentialIssueRequestV1
	store   *initLocalStore
}

func initFixtureCredentials(t *testing.T, p *RequestProjectionV1) *initCredentials {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(root, "unicode-é-quote\"-backslash\\")
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	fs, err := credentiallifecycle.NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Error("close actual original namespace:", err)
		}
	})
	now := time.Unix(100, 0).UTC()
	expires := now.Add(time.Hour)
	s := p.scope
	b := p.binding
	evidenceExpiry := p.bindingEvidenceExpiry
	b.EvidenceExpiresAt = &evidenceExpiry
	ref, err := domain.NewCredentialSecretRef("public/init/control")
	if err != nil {
		t.Fatal(err)
	}
	store := &initLocalStore{binding: domain.ProviderCredentialBindingV1{Version: 1, TenantID: s.TenantID, OwnerUserID: s.OwnerUserID,
		ResourceKind: s.ProviderResource.Kind, ResourceID: s.ProviderResource.ResourceID, ResourceRevision: s.ProviderResource.Revision,
		CredentialGeneration: s.ProviderResource.CredentialGeneration, CandidateMutationID: "public-init-control", State: domain.ProviderCredentialActiveV1,
		SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte(initPublicLiteral)), UpdatedAt: now}}
	service, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: initLocalClock{now}, IDs: initLocalIDs{}, FileSystem: fs, MaxSecretBytes: 128}, initLocalPlanner{}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: b, WorkerID: s.WorkerID, LeaseID: s.LeaseID, LeaseFence: s.LeaseFence, ExpiresAt: expires}
	source, err := service.PrepareImmutableAPIFileV1(t.Context(), request, "api-key", 128)
	if err != nil {
		t.Fatal("actual pre-Issue source:", err)
	}
	if store.loads != 0 || store.reads != 0 || store.recovers != 0 {
		t.Fatal("pre-Issue source touched backend")
	}
	return &initCredentials{service: service, source: source, request: request, store: store}
}

func (c *initCredentials) consume(t *testing.T, fn func(ports.ProviderInvocationCredentialV1, ports.ProviderCredentialMaterializationV1) error) {
	t.Helper()
	h, err := c.service.IssueProviderCredential(t.Context(), c.request)
	if err != nil {
		t.Fatal("original Issue:", err)
	}
	t.Cleanup(func() {
		if err := c.service.ReleaseProviderCredential(context.Background(), h); err != nil {
			t.Error("original Release:", err)
		}
	})
	var plan ports.ProviderCredentialMaterializationV1
	var callbackErr error
	err = c.service.MaterializeProviderCredential(t.Context(), h, func(v ports.ProviderCredentialMaterializationV1, secret []byte) error {
		plan = v
		if len(secret) != 0 {
			t.Error("file-only consumer received plaintext")
		}
		callbackErr = fn(h, v)
		return callbackErr
	})
	if err != nil {
		t.Fatal("original Materialize:", err, "public-literal test callback:", callbackErr)
	}
	if err = c.source.VerifyMaterialized(t.Context(), h, plan); err == nil {
		t.Error("retired callback remained valid")
	}
	if err = c.service.FinalizeProviderCredentialFile(t.Context(), h, plan); err != nil {
		t.Fatal("after-unlock original Finalize:", err)
	}
	if err = c.service.ReleaseProviderCredential(t.Context(), h); err != nil {
		t.Fatal("original Release:", err)
	}
	if _, err = os.Lstat(plan.FilePath); !os.IsNotExist(err) {
		t.Error("original file survived Release:", err)
	}
	if c.store.reads != 1 || c.store.recovers != 1 {
		t.Error("bridge added secret read or Issue")
	}
}

func (c *initCredentials) initBody(t *testing.T, p *InitPlanV1) ([]byte, ports.ProviderCredentialMaterializationV1) {
	t.Helper()
	var body []byte
	var plan ports.ProviderCredentialMaterializationV1
	c.consume(t, func(h ports.ProviderInvocationCredentialV1, v ports.ProviderCredentialMaterializationV1) error {
		plan = v
		var err error
		body, err = p.MaterializedBody(t.Context(), h, v)
		return err
	})
	return body, plan
}

func TestCompleteInitMaterializationFailureIsIrreversible(t *testing.T) {
	for _, mode := range []string{"wrong-handle", "wrong-kind", "wrong-path", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			p, _ := initFixtureProjection(t, initFixtureConfig(t), false)
			c := initFixtureCredentials(t, p)
			plan, err := PrepareInitV1(t.Context(), p, []byte(initFixtureModel), 1000, c.source)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(plan.Clear)
			c.consume(t, func(h ports.ProviderInvocationCredentialV1, value ports.ProviderCredentialMaterializationV1) error {
				badHandle, badValue, ctx := h, value, t.Context()
				switch mode {
				case "wrong-handle":
					badHandle.RunID = "foreign-run"
				case "wrong-kind":
					badValue.Kind = domain.ProviderCredentialDeliveryDirectV1
				case "wrong-path":
					badValue.FilePath = filepath.Join(value.RootDir, "other")
				case "cancelled":
					var stop context.CancelFunc
					ctx, stop = context.WithCancel(ctx)
					stop()
				}
				if body, err := plan.MaterializedBody(ctx, badHandle, badValue); err == nil || body != nil {
					t.Error("invalid materialization admitted")
				}
				if body, err := plan.MaterializedBody(t.Context(), h, value); err == nil || body != nil {
					t.Error("failed plan replayed with genuine file")
				}
				return nil
			})
		})
	}
}

func TestCompleteInitRejectsDifferentOriginalScopeBeforeIssue(t *testing.T) {
	p, ledger := initFixtureProjection(t, initFixtureConfig(t), false)
	c := initFixtureCredentials(t, p)
	p.scope.RunID = "different-canonical-run"
	before := ledger.Accounting()
	if plan, err := PrepareInitV1(t.Context(), p, []byte(initFixtureModel), 1000, c.source); err == nil || plan != nil || ledger.Accounting() != before {
		t.Error("different original scope admitted or spent later-stage ledger")
	}
	if c.store.loads != 0 || c.store.reads != 0 {
		t.Error("scope rejection reached backend")
	}
}

func TestCompleteProjectionRejectsUnsupportedLoopPolicy(t *testing.T) {
	for _, mode := range []string{"owned-agent", "unexpected-policy"} {
		t.Run(mode, func(t *testing.T) {
			id, input := fixtureInput(t, false)
			if mode == "owned-agent" {
				input.Job.HarnessBinding.Profile = domain.HarnessExecutionOwnedAgentV2
			} else {
				input.Job.HarnessBinding.AgentLoopPolicy = &domain.AgentLoopPolicyV1{}
			}
			cfg := initFixtureConfig(t)
			ledger := fixtureLedger(t, cfg.Profile.Limits)
			before := ledger.Accounting()
			if p, err := CompileOriginalRequestV1(t.Context(), id, input, cfg, ledger); err == nil || p != nil || ledger.Accounting() != before {
				t.Error("unsupported policy was admitted, stripped or processed")
			}
		})
	}
}
