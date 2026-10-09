package sessionlessharness_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type attachedBinderStore struct {
	worker     domain.AttachedWorker
	connection domain.AttachedWorkerConnection
	found      bool
	err        error
	reads      int
}

func (store *attachedBinderStore) LoadAttachedWorker(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	store.reads++
	return store.worker, store.found, store.err
}

func (store *attachedBinderStore) LoadAttachedWorkerConnection(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	store.reads++
	return store.connection, store.found, store.err
}

func attachedBinderFixture(t *testing.T) (sessionlessharness.AttachedResourceBinderConfig, *attachedBinderStore, ports.HarnessBindingRequest) {
	t.Helper()
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	request := ports.HarnessBindingRequest{TenantID: "tenant-a", OwnerUserID: "owner-a", RunID: "run-a", AttemptID: "attempt-a", SubscriptionConnectionID: "resource-a", At: now}
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(request.TenantID, request.OwnerUserID, request.RunID, request.AttemptID, request.SubscriptionConnectionID, now)
	if err != nil {
		t.Fatal(err)
	}
	expiry := now.Add(24 * time.Hour)
	binding := managed.HarnessBinding.Clone()
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "", "", ""
	binding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	binding.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1
	binding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	binding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: string(request.SubscriptionConnectionID), OwnerUserID: request.OwnerUserID,
		Revision: 2, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}
	binding.EvidenceExpiresAt = &expiry
	capability := domain.AttachedWorkerCapabilityDigest(strings.Repeat("a", 64))
	pin := sessionlessharness.AttachedResourcePin{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, SubscriptionConnectionID: request.SubscriptionConnectionID,
		WorkerID: "worker-a", EnrollmentGeneration: 1, CapabilityDigest: capability, PolicyDigest: domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest), Binding: binding}
	connected := now.Add(-time.Hour)
	store := &attachedBinderStore{found: true,
		worker: domain.AttachedWorker{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: pin.WorkerID, DisplayName: "Owner worker", IdentityPublicKey: bytes.Repeat([]byte{1}, 32),
			EnrollmentGeneration: 1, ConnectionGeneration: 1, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOnline,
			Revision: 1, CreatedAt: connected, UpdatedAt: now},
		connection: domain.AttachedWorkerConnection{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: pin.WorkerID,
			ID: "connection-a", ActivationChallengeID: "challenge-a", EnrollmentGeneration: 1, ConnectionGeneration: 1, ProtocolVersion: 1, CapabilityDigest: capability,
			SecretDigest: domain.AttachedWorkerConnectionSecretDigest(strings.Repeat("b", 64)), ChannelBinding: domain.AttachedWorkerChannelBinding(strings.Repeat("c", 64)),
			ManifestRevision: 1, ManifestIdentityKey: domain.AttachedWorkerIdentityKeyDigest(strings.Repeat("d", 64)), ManifestSignature: bytes.Repeat([]byte{1}, 64), ManifestObservedAt: connected,
			State: domain.AttachedWorkerConnectionOnline, ProtocolSnapshot: []byte("{}"), ConnectedAt: connected, LastCheckpointAt: now,
			PresenceExpiresAt: now.Add(time.Hour), AuthExpiresAt: expiry, Revision: 1},
	}
	return sessionlessharness.AttachedResourceBinderConfig{Enabled: true, Resources: []sessionlessharness.AttachedResourcePin{pin}}, store, request
}

func TestAttachedResourceBinderSealsExactOwnedTupleWithoutManagedAuthority(t *testing.T) {
	t.Parallel()
	config, store, request := attachedBinderFixture(t)
	binder, err := sessionlessharness.NewAttachedResourceBinder(config, store)
	if err != nil {
		t.Fatal(err)
	}
	// A caller cannot mutate constructor-owned template/evidence through aliases.
	config.Resources[0].Binding.Resource.Revision = 99
	*config.Resources[0].Binding.EvidenceExpiresAt = request.At
	authority, err := binder.BindHarness(context.Background(), request)
	if err != nil {
		t.Fatalf("bind exact owned tuple: %v", err)
	}
	if authority.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker || authority.SubstrateBinding != nil || authority.AdmissionCostCeiling != nil ||
		authority.HarnessBinding.Resource.Revision != 2 || authority.HarnessBinding.Resource.CredentialGeneration != 3 ||
		authority.HarnessBinding.Resource.ResourceID != string(request.SubscriptionConnectionID) || store.reads != 2 {
		t.Fatalf("bound tuple=%+v point reads=%d", authority, store.reads)
	}
	if err := authority.ValidateForScope(request); err != nil {
		t.Fatalf("validate exact sealed tuple: %v", err)
	}
}

func TestAttachedResourceBinderDeniesCrossScopeAndUnavailableHeads(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		mutate func(*attachedBinderStore, *ports.HarnessBindingRequest)
	}{
		{name: "cross tenant", mutate: func(_ *attachedBinderStore, request *ports.HarnessBindingRequest) { request.TenantID = "tenant-b" }},
		{name: "cross owner", mutate: func(_ *attachedBinderStore, request *ports.HarnessBindingRequest) { request.OwnerUserID = "owner-b" }},
		{name: "foreign resource", mutate: func(_ *attachedBinderStore, request *ports.HarnessBindingRequest) {
			request.SubscriptionConnectionID = "resource-b"
		}},
		{name: "missing worker", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) { store.found = false }},
		{name: "store failure", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) {
			store.err = errors.New("unavailable")
		}},
		{name: "foreign returned head", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) { store.worker.OwnerUserID = "owner-b" }},
		{name: "offline", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) {
			store.worker.ObservedState = domain.AttachedWorkerObservedOffline
		}},
		{name: "draining", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) {
			store.worker.DesiredState = domain.AttachedWorkerDesiredDrain
		}},
		{name: "revoked", mutate: func(store *attachedBinderStore, request *ports.HarnessBindingRequest) {
			store.worker.DesiredState = domain.AttachedWorkerDesiredRevoked
			store.worker.RevokedAt = request.At
		}},
		{name: "stale enrollment", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) { store.worker.EnrollmentGeneration++ }},
		{name: "stale connection", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) { store.worker.ConnectionGeneration++ }},
		{name: "capability changed", mutate: func(store *attachedBinderStore, _ *ports.HarnessBindingRequest) {
			store.connection.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("e", 64))
		}},
		{name: "presence expired boundary", mutate: func(store *attachedBinderStore, request *ports.HarnessBindingRequest) {
			store.connection.PresenceExpiresAt = request.At
			store.connection.LastCheckpointAt = request.At.Add(-time.Second)
		}},
		{name: "auth expired boundary", mutate: func(store *attachedBinderStore, request *ports.HarnessBindingRequest) {
			store.connection.AuthExpiresAt = request.At
		}},
		{name: "policy evidence expired boundary", mutate: func(_ *attachedBinderStore, request *ports.HarnessBindingRequest) {
			request.At = request.At.Add(24 * time.Hour)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			config, store, request := attachedBinderFixture(t)
			binder, err := sessionlessharness.NewAttachedResourceBinder(config, store)
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(store, &request)
			if got, err := binder.BindHarness(context.Background(), request); err == nil {
				t.Fatalf("invalid tuple accepted: request=%+v authority=%+v", request, got)
			}
		})
	}
}

func TestAttachedResourceBinderDisabledAndDuplicatePins(t *testing.T) {
	t.Parallel()
	config, store, request := attachedBinderFixture(t)
	config.Enabled = false
	binder, err := sessionlessharness.NewAttachedResourceBinder(config, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := binder.BindHarness(context.Background(), request); !errors.Is(err, sessionlessharness.ErrAttachedResourceUnavailable) || store.reads != 0 {
		t.Fatalf("disabled bind err=%v reads=%d", err, store.reads)
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*sessionlessharness.AttachedResourcePin)
	}{
		{name: "duplicate resource", mutate: func(pin *sessionlessharness.AttachedResourcePin) { pin.WorkerID = "worker-b" }},
		{name: "duplicate worker", mutate: func(pin *sessionlessharness.AttachedResourcePin) {
			pin.SubscriptionConnectionID = "resource-b"
			pin.Binding.Resource.ResourceID = "resource-b"
		}},
		{name: "mixed owner", mutate: func(pin *sessionlessharness.AttachedResourcePin) { pin.Binding.Resource.OwnerUserID = "owner-b" }},
		{name: "mixed policy", mutate: func(pin *sessionlessharness.AttachedResourcePin) {
			pin.PolicyDigest = domain.AttachedWorkerPolicyDigest(strings.Repeat("e", 64))
		}},
		{name: "mixed resource", mutate: func(pin *sessionlessharness.AttachedResourcePin) { pin.Binding.Resource.ResourceID = "resource-b" }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			config, store, _ := attachedBinderFixture(t)
			other := config.Resources[0]
			other.Binding = other.Binding.Clone()
			testCase.mutate(&other)
			config.Resources = append(config.Resources, other)
			if _, err := sessionlessharness.NewAttachedResourceBinder(config, store); err == nil {
				t.Fatalf("invalid pin configuration accepted: %+v", config.Resources)
			}
		})
	}
}

func TestAttachedResourceCapabilityDigestSealsEnrollmentGeneration(t *testing.T) {
	t.Parallel()
	manifest := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: "worker-a", EnrollmentGeneration: 1, Revision: 1,
		ProtocolOffer:   attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1}},
		OperatingSystem: "linux", Architecture: "amd64", BuildID: "test-build", HarnessName: "test-harness", HarnessVersion: "1",
		HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: bytes.Repeat([]byte{1}, 32), MaxConcurrentAttempts: 1,
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary, attachedworkerprotocol.IsolationProcessBoundary},
		Features:          []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect},
	}
	before, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.EnrollmentGeneration++
	after, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("new enrollment retained the persisted placement capability digest")
	}
}
