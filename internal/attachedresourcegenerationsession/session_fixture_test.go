//go:build darwin || linux

package attachedresourcegenerationsession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

var ownerTestTime = time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)

// This fixture uses the PUBLIC original Connector, real pinned local stores,
// signed challenge/attach/manifest verification and both protocol reducers.
// The peers are in-process; the synthetic executable is hashed but NEVER run.
type ownerFixture struct {
	clock       *testkit.FakeClock
	store       *ownerlocalcredential.Store
	lifecycle   *ownerlocalcredential.Lifecycle
	workerStore *attachedworkerlocal.Store
	manifest    attachedworkerlocal.ManifestV1
	input       attachedworkersession.ConnectInputV1
	config      attachedworkersession.Config
	baseline    domain.AttachedResourceGenerationBaselineV1
	peer        *sessionPeer
	owner       *OwnerV1
	session     *attachedworkersession.Session
}

func newOwnerFixture(t *testing.T, initialize bool) *ownerFixture {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(parent, "never-run")
	body := []byte("synthetic pinned file; never executable by these tests\n")
	if err := os.WriteFile(executable, body, 0o700); err != nil {
		t.Fatal(err)
	}
	cli := filepath.Join(parent, "cli")
	if err := os.Mkdir(cli, 0o700); err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, 32))
	public := private.Public().(ed25519.PublicKey)
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	clock := testkit.NewFakeClock(ownerTestTime)
	manifest := attachedworkerlocal.ManifestV1{Version: 1, Revision: 1, ControlPlaneOrigin: "https://generation.example", TenantID: "tenant-generation", OwnerUserID: "owner-generation", WorkerID: "worker-generation", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{DockerPath: executable, DockerSHA256: digest, CLIConfigDir: cli, Host: "unix:///run/docker.sock", EngineID: "synthetic-engine-abcdef", InstallationID: "synthetic-install", Boundary: attachedworkerlocal.BoundaryLinuxRootless,
			Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness: attachedworkerlocal.HarnessConfigV1{Executable: executable, SHA256: digest, Arguments: []string{"--attached"}}, Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: ownerTestTime, UpdatedAt: ownerTestTime}
	if runtime.GOOS == "darwin" {
		manifest.OCI.Boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	workerStore, err := attachedworkerlocal.NewStore(filepath.Join(parent, "worker"), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerStore.Initialize(t.Context(), manifest, attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: private}); err != nil {
		t.Fatal(err)
	}
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	executableHash, _ := hex.DecodeString(digest)
	capability := attachedworkerprotocol.CapabilityManifestV1{WorkerID: string(manifest.WorkerID), EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer, OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "generation-build", HarnessName: "codex", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: executableHash,
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary, attachedworkerprotocol.IsolationProcessBoundary},
		Features:          []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}, MaxConcurrentAttempts: 1}
	capDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	baseline := completeBaseline(t, manifest, domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capDigest)))
	root := filepath.Join(parent, "credentials")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := ownerlocalcredential.New(t.Context(), ownerlocalcredential.Config{Root: root, Scope: ownerlocalcredential.Scope{TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, SubscriptionConnectionID: baseline.SubscriptionConnectionID}, KeyEpoch: "synthetic-generation-key"}, bytes.Repeat([]byte{0x37}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close original credential store: %v", err)
		}
	})
	if initialize {
		if err := store.Initialize(t.Context(), ownerlocalcredential.Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: clock.Now()}, []byte("synthetic-original-credential")); err != nil {
			t.Fatal(err)
		}
	}
	scratch := filepath.Join(parent, "scratch")
	if err := os.Mkdir(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := ownerlocalcredential.NewLifecycle(store, credentiallifecycle.Config{ScratchRoot: scratch, Clock: clock, IDs: testkit.NewSequenceIDGenerator("generation-"), MaxAuthBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Close(); err != nil {
			t.Errorf("close original lifecycle: %v", err)
		}
	})
	f := &ownerFixture{clock: clock, store: store, lifecycle: lifecycle, workerStore: workerStore, manifest: manifest, input: attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability}, baseline: baseline,
		config: attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: offer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: 5 * time.Second, Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, 64)), Now: clock.Now}}
	f.peer = &sessionPeer{clock: clock, public: public, offer: offer}
	return f
}

func (f *ownerFixture) connect(t *testing.T, transport roundTripper) {
	t.Helper()
	owner, err := NewV1(f.manifest.ControlPlaneOrigin, transport.client(), f.peer, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	f.owner = owner
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Errorf("close generation owner: %v", err)
		}
	})
	connector, err := attachedworkersession.New(f.workerStore, f.peer, owner, f.config)
	if err != nil {
		t.Fatal(err)
	}
	session, err := connector.Connect(t.Context(), f.input)
	if err != nil {
		t.Fatal("genuine initial connect", err)
	}
	f.session = session
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close original Session: %v", err)
		}
	})
	if err := owner.BindInitialSessionV1(t.Context(), session); err != nil {
		t.Fatal("bind genuine Ready Session", err)
	}
}

func (f *ownerFixture) reconnect(t *testing.T, transport roundTripper) {
	t.Helper()
	previousConfig, previousSnapshot := f.peer.protocolState(t)
	if err := f.session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Close(); err != nil {
		t.Fatal(err)
	}
	peer := &sessionPeer{clock: f.clock, public: f.peer.public, offer: f.peer.offer, previousConfig: previousConfig, previousSnapshot: previousSnapshot, reconnect: true}
	owner, err := NewV1(f.manifest.ControlPlaneOrigin, transport.client(), peer, f.lifecycle, f.baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Errorf("close reconnect owner: %v", err)
		}
	})
	config := f.config
	config.Random = bytes.NewReader(bytes.Repeat([]byte{0x61}, 64))
	connector, err := attachedworkersession.New(f.workerStore, peer, owner, config)
	if err != nil {
		t.Fatal(err)
	}
	session, err := connector.Reconnect(t.Context(), attachedworkersession.ReconnectInputV1(f.input))
	if err != nil {
		t.Fatal("genuine reconnect", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close reconnect Session: %v", err)
		}
	})
	if err := owner.BindInitialSessionV1(t.Context(), session); err == nil {
		t.Fatal("reconnect accepted as initial")
	}
	if err := owner.BindReconnectedSessionV1(t.Context(), session); err != nil {
		t.Fatal("bind genuine reconnect", err)
	}
	f.peer, f.owner, f.session = peer, owner, session
}

type sessionPeer struct {
	mu               sync.Mutex
	clock            *testkit.FakeClock
	public           ed25519.PublicKey
	offer            attachedworkerprotocol.VersionOfferV1
	challenge        domain.AttachedWorkerAttachChallenge
	auth             attachedworkerprotocol.AuthContextV1
	machine          *attachedworkerprotocol.ConformanceMachine
	config           attachedworkerprotocol.MachineConfig
	previousConfig   attachedworkerprotocol.MachineConfig
	previousSnapshot attachedworkerprotocol.MachineSnapshotV1
	reconnect        bool
	retired          bool
	closed           bool
	manifestHook     func() error
	openHook         func()
	closeHook        func()
}

func (p *sessionPeer) IssueChallenge(_ context.Context, in attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	request := attachedworkertransport.IssueChallengeRequest{WorkerID: domain.AttachedWorkerID(in.Hello.WorkerID), ExpectedAudience: in.ExpectedAudience, ExpectedWorkerRevision: in.ExpectedWorkerRevision, Purpose: in.Purpose, Hello: in.Hello}
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(in.TenantLocator, in.OwnerLocator, request.WorkerID, in.Hello.EnrollmentGeneration, in.Hello.ConnectionGeneration-1, request)
	if err != nil || !ed25519.Verify(p.public, transcript, in.Proof) {
		return nil, errors.New("synthetic challenge signature invalid")
	}
	purpose := domain.AttachedWorkerAttachInitial
	id := "connection-generation-1"
	if p.reconnect {
		purpose = domain.AttachedWorkerAttachReconnect
		id = "connection-generation-2"
	}
	if in.Purpose != purpose {
		return nil, errors.New("unexpected connection purpose")
	}
	nonce := bytes.Repeat([]byte{0x52}, 32)
	p.challenge = domain.AttachedWorkerAttachChallenge{TenantID: in.TenantLocator, OwnerUserID: in.OwnerLocator, ID: "challenge-generation", WorkerID: request.WorkerID, ConnectionID: domain.AttachedWorkerConnectionID(id), Purpose: purpose, Audience: in.ExpectedAudience,
		ExpectedWorkerRevision: in.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: in.Hello.EnrollmentGeneration, ExpectedConnectionGeneration: in.Hello.ConnectionGeneration - 1, TargetConnectionGeneration: in.Hello.ConnectionGeneration,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1}, PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
		WorkerNonceDigest: domain.DigestAttachedWorkerChallenge(in.Hello.Hello.WorkerNonce), PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(nonce), CreatedAt: p.clock.Now(), ExpiresAt: p.clock.Now().Add(time.Minute), RetainUntil: p.clock.Now().Add(time.Hour), Revision: 1}
	if p.reconnect {
		raw, err := attachedworkerprotocol.EncodeMachineSnapshotV1(p.previousSnapshot)
		if err != nil {
			return nil, err
		}
		p.challenge.ExpectedConnectionID = "connection-generation-1"
		p.challenge.ExpectedConnectionRevision = 2
		p.challenge.ExpectedCapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(p.previousSnapshot.CapabilityDigest))
		p.challenge.ExpectedProtocolSnapshot = raw
	}
	return &attachedworkerhttp.ChallengeResponseV1{Challenge: p.challenge, Frame: attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1), WorkerID: in.Hello.WorkerID, EnrollmentGeneration: in.Hello.EnrollmentGeneration, ConnectionGeneration: in.Hello.ConnectionGeneration, Sequence: 1, Ack: 1, Kind: attachedworkerprotocol.MessageChallenge,
		Challenge: &attachedworkerprotocol.ChallengeV1{WorkerOffer: in.Hello.Hello.Offer, PlatformOffer: p.offer, SelectedVersion: 1, WorkerNonce: bytes.Clone(in.Hello.Hello.WorkerNonce), PlatformNonce: nonce}}}, nil
}

func (p *sessionPeer) Activate(_ context.Context, in attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	channel := attachedworkertransport.ConnectionChannelBinding(p.challenge.ID, p.challenge.WorkerNonceDigest, p.challenge.PlatformNonceDigest, in.ConnectionSecret.Digest())
	channelBytes, _ := hex.DecodeString(string(channel))
	auth := attachedworkerprotocol.AuthContextV1{TenantID: string(p.challenge.TenantID), OwnerUserID: string(p.challenge.OwnerUserID), WorkerID: string(p.challenge.WorkerID), IdentityPublicKey: p.public, EnrollmentGeneration: in.Attach.EnrollmentGeneration, ConnectionGeneration: in.Attach.ConnectionGeneration, Version: 1, ChannelBinding: channelBytes}
	config := attachedworkerprotocol.MachineConfig{Auth: auth, WorkerOffer: p.offer, PlatformOffer: p.offer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}}
	var accepted attachedworkerprotocol.FrameV1
	var snapshot attachedworkerprotocol.MachineSnapshotV1
	var err error
	var capability []byte
	if p.reconnect {
		if attachedworkerprotocol.VerifyReconnectV1(auth, in.Attach) != nil {
			return nil, errors.New("invalid signed reconnect")
		}
		accepted, snapshot, err = attachedworkerprotocol.BuildReconnectAcceptedSnapshotV1(p.previousConfig, p.previousSnapshot, auth, in.Attach)
		capability = in.Attach.Reconnect.CapabilityDigest
	} else {
		if attachedworkerprotocol.VerifyAttachV1(auth, in.Attach) != nil {
			return nil, errors.New("invalid signed attach")
		}
		capability = in.Attach.Attach.CapabilityDigest
		accepted = attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2), WorkerID: in.Attach.WorkerID, EnrollmentGeneration: in.Attach.EnrollmentGeneration, ConnectionGeneration: in.Attach.ConnectionGeneration, Sequence: 2, Ack: 2, Kind: attachedworkerprotocol.MessageAttachAccepted,
			AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{WorkerOffer: in.Attach.Attach.WorkerOffer, PlatformOffer: in.Attach.Attach.PlatformOffer, SelectedVersion: 1, WorkerNonce: bytes.Clone(in.Attach.Attach.WorkerNonce), PlatformNonce: bytes.Clone(in.Attach.Attach.PlatformNonce), CapabilityDigest: bytes.Clone(capability)}}
		snapshot, err = attachedworkerprotocol.BuildInitialAttachSnapshotV1(config, in.Attach, accepted)
	}
	if err != nil {
		return nil, err
	}
	machine, err := attachedworkerprotocol.RestoreConformanceMachine(config, snapshot)
	if err != nil {
		return nil, err
	}
	p.auth, p.config, p.machine = auth, config, machine
	return &attachedworkerhttp.ActivateResponseV1{Accepted: accepted, Connection: attachedworkerhttp.ActivateConnectionV1{TenantID: p.challenge.TenantID, OwnerUserID: p.challenge.OwnerUserID, WorkerID: p.challenge.WorkerID, ID: p.challenge.ConnectionID, ActivationChallengeID: p.challenge.ID,
		EnrollmentGeneration: in.Attach.EnrollmentGeneration, ConnectionGeneration: in.Attach.ConnectionGeneration, ProtocolVersion: 1, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capability)), SecretDigest: in.ConnectionSecret.Digest(), ChannelBinding: channel,
		State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1, ConnectedAt: p.clock.Now(), AuthExpiresAt: p.clock.Now().Add(time.Hour), Revision: 1}}, nil
}

func (p *sessionPeer) Open(b attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	p.mu.Lock()
	if p.retired || p.closed {
		p.mu.Unlock()
		return nil, errors.New("source retired")
	}
	if _, err := attachedworkertransport.ParseConnectionBearer(bearer); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if b.ConnectionID != p.challenge.ConnectionID || b.TenantID != p.challenge.TenantID || b.OwnerUserID != p.challenge.OwnerUserID {
		p.mu.Unlock()
		return nil, errors.New("foreign Open binding")
	}
	hook := p.openHook
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired || p.closed {
		return nil, errors.New("source retired during original Open")
	}
	return p, nil
}
func (p *sessionPeer) Exchange(_ context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.retired || p.closed || len(batch.Frames) != 1 || batch.Frames[0].Kind != attachedworkerprotocol.MessageManifest {
		return nil, errors.New("unexpected original manifest exchange")
	}
	if p.manifestHook != nil {
		if err := p.manifestHook(); err != nil {
			return nil, err
		}
	}
	frame := batch.Frames[0]
	if attachedworkerprotocol.VerifyManifestV1(p.auth, frame) != nil {
		return nil, errors.New("invalid signed manifest")
	}
	if err := p.machine.Accept(attachedworkerprotocol.DirectionWorkerToPlatform, frame, attachedworkerprotocol.AcceptanceContextV1{ChannelBinding: p.auth.ChannelBinding, NowUnixMicro: p.clock.Now().UnixMicro()}); err != nil {
		return nil, err
	}
	if p.machine.ConnectionState() != attachedworkerprotocol.ConnectionReady {
		return nil, errors.New("actual reducer not ready")
	}
	return nil, nil
}
func (p *sessionPeer) RetireConnectionSourcesV1() error {
	p.mu.Lock()
	p.retired = true
	p.mu.Unlock()
	return nil
}
func (p *sessionPeer) Close() error {
	p.mu.Lock()
	p.retired, p.closed = true, true
	hook := p.closeHook
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}
func (p *sessionPeer) protocolState(t *testing.T) (attachedworkerprotocol.MachineConfig, attachedworkerprotocol.MachineSnapshotV1) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot, err := p.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return p.config, snapshot
}

func completeBaseline(t *testing.T, m attachedworkerlocal.ManifestV1, capability domain.AttachedWorkerCapabilityDigest) domain.AttachedResourceGenerationBaselineV1 {
	t.Helper()
	at, expires := ownerTestTime, ownerTestTime.Add(30*time.Minute)
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-generation", OwnerUserID: m.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	backend := domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "native-v3", BackendProfileDigest: strings.Repeat("b", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1}
	scope := domain.ProviderEvidenceScopeV1{TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, Resource: resource, Backend: backend}
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "approved-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "openai", ModelID: "approved-model", CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: at, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: at, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: "approved-model", MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnknownV1, StructuredOutput: domain.ProviderEvidenceUnsupportedV1, ObservedAt: at, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: "approved-model", AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "approved-terms", ObservedAt: at, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: "openai", ModelID: "approved-model", ObservedAt: at, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "approved-policy", Revision: 1, DecisionOwner: "operator", EvidenceSource: "approved-terms", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ObservedAt: at, ExpiresAt: expires},
	}
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, Resource: resource, Backend: backend, ModelVendorID: "openai", ModelID: "approved-model", InputDataClass: domain.ProviderDataPublicV1, EvidenceExpiresAt: &expires}
	for _, item := range []struct {
		digest func() (domain.ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{bundle.Catalog.Digest, []*string{&binding.ProviderCatalogDigest}}, {bundle.Route.Digest, []*string{&binding.ProviderRouteDigest, &bundle.Policy.RoutePolicyDigest}},
		{bundle.Capability.Digest, []*string{&binding.CapabilityEvidenceDigest, &bundle.Policy.CapabilityEvidenceDigest}}, {bundle.Privacy.Digest, []*string{&binding.PrivacyPolicyDigest, &bundle.Policy.PrivacyEvidenceDigest}},
		{bundle.Price.Digest, []*string{&bundle.Policy.PriceObservationDigest}}, {func() (domain.ProviderEvidenceDigestV1, error) { return bundle.Policy.Digest() }, []*string{&binding.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, SubscriptionConnectionID: domain.SubscriptionConnectionID(resource.ResourceID), WorkerID: m.WorkerID, EnrollmentGeneration: m.EnrollmentGeneration, BaselineRevision: resource.Revision, CapabilityDigest: capability, GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
	if err := baseline.Validate(); err != nil {
		t.Fatal("complete synthetic baseline", err)
	}
	return baseline
}

func (f *ownerFixture) request(generation uint64) ports.CredentialIssueRequest {
	at := f.clock.Now()
	started := at.Add(-time.Minute)
	resource := f.baseline.Binding.Resource
	resource.CredentialGeneration = generation
	return ports.CredentialIssueRequest{OwnerUserID: f.manifest.OwnerUserID, Run: domain.Run{ID: "run-generation", TenantID: f.manifest.TenantID, SessionID: "session-generation", TriggerEventID: "event-generation", SubscriptionConnectionID: f.baseline.SubscriptionConnectionID, Status: domain.RunRunning, IdempotencyKey: "generation-run-key", StartedAt: &started, CreatedAt: started, UpdatedAt: at},
		Attempt: domain.Attempt{ID: "attempt-generation", TenantID: f.manifest.TenantID, RunID: "run-generation", Number: 1, Status: domain.AttemptRunning, WorkerID: "credential-worker", CreatedAt: started, UpdatedAt: at},
		Lease:   domain.Lease{ID: "lease-generation", TenantID: f.manifest.TenantID, RunID: "run-generation", AttemptID: "attempt-generation", WorkerID: "credential-worker", FenceToken: 9, AcquiredAt: started, ExpiresAt: at.Add(10 * time.Minute)}, ExpiresAt: at.Add(5 * time.Minute), ProviderResource: resource}
}

func (f *ownerFixture) writeback(t *testing.T, generation uint64, changed bool) {
	t.Helper()
	h, err := f.lifecycle.Issue(t.Context(), f.request(generation))
	if err != nil {
		t.Fatal("Issue original lifecycle", err)
	}
	released := false
	t.Cleanup(func() {
		if released {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		if err := f.lifecycle.Release(ctx, h); err != nil {
			t.Errorf("release original handle: %v", err)
		}
	})
	m, err := f.lifecycle.Materialize(t.Context(), h)
	if err != nil {
		t.Fatal("Materialize original lifecycle", err)
	}
	if changed {
		if err := os.WriteFile(m.AuthFile, []byte(fmt.Sprintf("synthetic-refresh-%d", generation)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	wb, err := f.lifecycle.WriteBack(t.Context(), h, m)
	if err != nil || wb.Changed != changed {
		t.Fatal("original WriteBack", wb, err)
	}
	if err := f.lifecycle.Release(t.Context(), h); err != nil {
		t.Fatal("original Release", err)
	}
	released = true
}
