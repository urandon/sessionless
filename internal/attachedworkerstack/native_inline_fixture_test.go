//go:build darwin || linux

package attachedworkerstack

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

const nativeInlineFixtureFinal = `{"answer":"two requested files","artifacts":[{"name":"data.json","media_type":"application/json","content":" { \"n\": 9007199254740993123456789 } "},{"name":"note.txt","media_type":"text/plain","content":"original text\n"}]}`

// NativeInlineFaultV2ForTest selects only these closed peer faults. It cannot
// replace the Stack, producer, runner, profile, admission or current predicate.
type NativeInlineFaultV2ForTest uint8

const (
	NativeInlineHeldACKV2ForTest NativeInlineFaultV2ForTest = iota
	NativeInlineLostACKV2ForTest
	NativeInlineForgedDigestACKV2ForTest
	NativeInlineForgedSequenceACKV2ForTest
	NativeInlineExportExpiryV2ForTest
)

// NativeInlineFixtureV2ForTest exists only in this package's test binary. It
// exposes one fixed construction, not an arbitrary runner/producer factory.
// Isolation, credential metadata and the server admission are synthetic; the
// Stack, supervisor, prepared owner, Connector, Session and journal are real.
type NativeInlineFixtureV2ForTest struct {
	Stack               *Stack
	Session             *attachedworkersession.Session
	Original            attachedworkerdaemon.Invocation
	Capability          attachedworkerprotocol.CapabilityManifestV1
	JournalPath         string
	MaterializationRoot string
	Now                 time.Time
	TeardownEntered     chan struct{}
	WriteBackEntered    chan struct{}
	AckEntered          chan struct{}
	LostACKEntered      chan struct{}
	continueTeardown    chan struct{}
	continueWriteBack   chan struct{}
	continueAck         chan struct{}
	teardownOnce        sync.Once
	writeBackOnce       sync.Once
	ackOnce             sync.Once
	mu                  sync.Mutex
	events              []string
	launches, terminals int
	terminalExchanges   int
	exactACKReplay      bool
	fault               NativeInlineFaultV2ForTest
}

func (f *NativeInlineFixtureV2ForTest) CurrentTime() time.Time {
	if f.fault == NativeInlineExportExpiryV2ForTest {
		return time.Now().UTC()
	}
	return f.Now
}

func (f *NativeInlineFixtureV2ForTest) TerminalExchanges() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.terminalExchanges, f.exactACKReplay
}

func (f *NativeInlineFixtureV2ForTest) ContinueTeardown() {
	f.teardownOnce.Do(func() { close(f.continueTeardown) })
}
func (f *NativeInlineFixtureV2ForTest) ContinueWriteBack() {
	f.writeBackOnce.Do(func() { close(f.continueWriteBack) })
}
func (f *NativeInlineFixtureV2ForTest) ContinueACK() {
	f.ackOnce.Do(func() { close(f.continueAck) })
}
func (f *NativeInlineFixtureV2ForTest) Events() ([]string, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...), f.launches, f.terminals
}
func (f *NativeInlineFixtureV2ForTest) record(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
}

type nativeInlineCredentialFixture struct {
	fixture *NativeInlineFixtureV2ForTest
	root    string
	binding domain.CredentialBinding
}

func (c *nativeInlineCredentialFixture) LoadCredentialBinding(ctx context.Context, tenant domain.TenantID, connection domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error) {
	if ctx.Err() != nil || tenant != c.binding.TenantID || connection != c.binding.SubscriptionConnectionID {
		return domain.CredentialBinding{}, false, errors.New("synthetic metadata scope mismatch")
	}
	return c.binding, true, nil
}
func (c *nativeInlineCredentialFixture) Issue(ctx context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	if ctx.Err() != nil || request.ValidateAt(c.fixture.CurrentTime()) != nil || request.ProviderResource.CredentialGeneration != c.binding.Generation {
		return ports.CredentialHandle{}, errors.New("synthetic credential issue scope mismatch")
	}
	c.fixture.record("issue")
	return ports.CredentialHandle{HandleID: "native-inline-synthetic-handle", TenantID: request.Run.TenantID, OwnerUserID: request.OwnerUserID,
		SubscriptionConnectionID: request.Run.SubscriptionConnectionID, RunID: request.Run.ID, AttemptID: request.Attempt.ID, WorkerID: request.Lease.WorkerID,
		LeaseID: request.Lease.ID, LeaseFence: request.Lease.FenceToken, BindingGeneration: c.binding.Generation, ProviderResource: request.ProviderResource, ExpiresAt: request.ExpiresAt}, nil
}
func (c *nativeInlineCredentialFixture) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	c.fixture.record("materialize")
	return ports.CredentialMaterialization{RootDir: c.root, AuthFile: filepath.Join(c.root, "auth.json")}, nil
}
func (c *nativeInlineCredentialFixture) WriteBack(ctx context.Context, handle ports.CredentialHandle, materialization ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	c.fixture.record("writeback-entered")
	close(c.fixture.WriteBackEntered)
	select {
	case <-c.fixture.continueWriteBack:
	case <-ctx.Done():
		return ports.CredentialWriteBackResult{}, ctx.Err()
	}
	body, err := os.ReadFile(materialization.AuthFile)
	defer clear(body)
	if err != nil || !bytes.Equal(body, []byte(`{"synthetic":true}`)) {
		return ports.CredentialWriteBackResult{}, errors.New("synthetic auth bytes changed")
	}
	c.fixture.record("writeback")
	return ports.CredentialWriteBackResult{Generation: handle.BindingGeneration}, nil
}
func (c *nativeInlineCredentialFixture) Release(context.Context, ports.CredentialHandle) error {
	if err := os.Remove(filepath.Join(c.root, "auth.json")); err != nil {
		return err
	}
	if err := os.Remove(c.root); err != nil {
		return err
	}
	c.fixture.record("release")
	return nil
}
func (*nativeInlineCredentialFixture) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return errors.New("unexpected synthetic credential revoke")
}

type nativeInlineLauncherFixture struct{ fixture *NativeInlineFixtureV2ForTest }

func (*nativeInlineLauncherFixture) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	return attachedworkerdaemon.IsolationProfileV3{Version: 3, Name: "synthetic-native-inline-process", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}
func (*nativeInlineLauncherFixture) Reconcile(context.Context) (int, error) { return 0, nil }
func (l *nativeInlineLauncherFixture) PrepareV3(ctx context.Context, envelope attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	if envelope.Execution.ValidateV3(ctx, envelope.Native) != nil || !envelope.Socket.MatchesExecutionV3(envelope.Execution) {
		return nil, errors.New("original native launch/readset mismatch")
	}
	l.fixture.mu.Lock()
	l.fixture.launches++
	l.fixture.mu.Unlock()
	command := exec.Command(envelope.Workload.Executable, envelope.Workload.Arguments...)
	command.Dir, command.Env = envelope.Workload.Directory, append([]string(nil), envelope.Workload.Environment...)
	return &nativeInlineBoundaryFixture{command: command, envelope: envelope, fixture: l.fixture}, nil
}

type nativeInlineBoundaryFixture struct {
	command  *exec.Cmd
	envelope attachedworkerdaemon.LaunchEnvelopeV3
	fixture  *NativeInlineFixtureV2ForTest
}

func (b *nativeInlineBoundaryFixture) Command() *exec.Cmd { return b.command }
func (b *nativeInlineBoundaryFixture) AttestedWorkloadV3() attachedworkerdaemon.WorkloadAttestationV3 {
	w := b.envelope.Workload
	return attachedworkerdaemon.WorkloadAttestationV3{Native: attachedworkerdaemon.WorkloadAttestation{Executable: w.Executable, ExecutableDigest: w.ExecutableDigest, Arguments: append([]string(nil), w.Arguments...)}, Bundle: b.envelope.Native.Clone(), Execution: b.envelope.Execution, ExecutionDigest: b.envelope.Execution.DigestV3(), Socket: b.envelope.Socket}
}
func (*nativeInlineBoundaryFixture) GracefulStop(context.Context) error  { return nil }
func (*nativeInlineBoundaryFixture) ForceStop(context.Context) error     { return nil }
func (*nativeInlineBoundaryFixture) Alive(context.Context) (bool, error) { return false, nil }
func (b *nativeInlineBoundaryFixture) Release(ctx context.Context) error {
	close(b.fixture.TeardownEntered)
	select {
	case <-b.fixture.continueTeardown:
		b.fixture.record("boundary-release")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// This peer only supplies protocol responses to the real Connector/Session.
// The real Session owns sequencing, signed handshake, attempt child and ACK.
type nativeInlineSessionPeer struct {
	*preparedSessionPeer
	fixture    *NativeInlineFixtureV2ForTest
	connection attachedworkersession.ConnectionBindingV1
	binding    attachedworkerprotocol.AttemptBindingV1
	step       uint64
	terminal   []byte
	cachedACK  []byte
	replayed   bool
}

func (p *nativeInlineSessionPeer) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	if _, err := p.preparedSessionPeer.Open(binding, bearer); err != nil {
		return nil, err
	}
	p.connection = binding
	return p, nil
}
func (p *nativeInlineSessionPeer) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if len(batch.Frames) != 1 {
		return nil, errors.New("synthetic peer expects exactly one original frame")
	}
	in := batch.Frames[0]
	if in.Kind == attachedworkerprotocol.MessageManifest && p.step == 0 {
		return nil, nil
	}
	if in.Kind == attachedworkerprotocol.MessageTerminal && len(p.terminal) != 0 {
		exact, err := attachedworkerprotocol.EncodeBatchV1(batch)
		defer clear(exact)
		if err != nil || p.fixture.fault != NativeInlineLostACKV2ForTest || p.replayed || !bytes.Equal(exact, p.terminal) {
			return nil, errors.New("terminal retry changed exact encoded request or exceeded closed replay budget")
		}
		reply, err := attachedworkerprotocol.DecodeBatchV1(p.cachedACK)
		if err != nil {
			return nil, errors.New("original cached terminal ACK malformed")
		}
		p.replayed = true
		p.fixture.mu.Lock()
		p.fixture.terminalExchanges++
		p.fixture.exactACKReplay = true
		p.fixture.mu.Unlock()
		close(p.fixture.AckEntered)
		select {
		case <-p.fixture.continueAck:
			return &reply, nil // same ACK, no platform-step/terminal-commit advance
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p.step++
	frame := attachedworkerprotocol.FrameV1{Version: 1, WorkerID: string(p.connection.WorkerID), EnrollmentGeneration: p.connection.EnrollmentGeneration, ConnectionGeneration: p.connection.ConnectionGeneration,
		Sequence: p.step + 2, Ack: in.Sequence, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, p.step+2)}
	switch {
	case p.step == 1 && in.Kind == attachedworkerprotocol.MessageHeartbeat:
		frame.Kind, frame.LeaseOffer = attachedworkerprotocol.MessageLeaseOffer, &attachedworkerprotocol.LeaseOfferV1{Binding: p.binding, AttemptSequence: 1}
	case p.step == 2 && in.Kind == attachedworkerprotocol.MessageLeaseClaim:
		frame.Kind, frame.LeaseAccepted = attachedworkerprotocol.MessageLeaseAccepted, &attachedworkerprotocol.LeaseAcceptedV1{Binding: p.binding, AttemptSequence: 2}
	case p.step == 3 && in.Kind == attachedworkerprotocol.MessageTerminal && in.Terminal != nil:
		p.fixture.mu.Lock()
		p.fixture.terminals++
		p.fixture.terminalExchanges++
		p.fixture.mu.Unlock()
		v := in.Terminal
		frame.Kind, frame.TerminalAck = attachedworkerprotocol.MessageTerminalAck, &attachedworkerprotocol.TerminalAckV1{Binding: p.binding, AttemptSequence: 3, TerminalSequence: v.TerminalSequence, Status: v.Status, Result: v.Result, EvidenceDigest: bytes.Clone(v.EvidenceDigest)}
		if p.fixture.fault == NativeInlineLostACKV2ForTest {
			var err error
			p.terminal, err = attachedworkerprotocol.EncodeBatchV1(batch)
			if err != nil {
				return nil, err
			}
			p.cachedACK, err = attachedworkerprotocol.EncodeBatchV1(attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}})
			if err != nil {
				return nil, err
			}
			close(p.fixture.LostACKEntered)
			return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorUnavailable, true)
		}
		switch p.fixture.fault {
		case NativeInlineForgedDigestACKV2ForTest:
			frame.TerminalAck.EvidenceDigest[0] ^= 0x80
		case NativeInlineForgedSequenceACKV2ForTest:
			frame.TerminalAck.TerminalSequence++
		}
		close(p.fixture.AckEntered)
		select {
		case <-p.fixture.continueAck:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	default:
		return nil, errors.New("unexpected synthetic protocol exchange")
	}
	return &attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}}, nil
}

// NewNativeInlineFixtureV2ForTest accepts no caller-selected runner, admission,
// profile or output. Its sole fixed workload is a local shell JSONL fixture;
// no Codex, bootstrap, relay, engine, model or provider is executed.
func NewNativeInlineFixtureV2ForTest(t *testing.T, fault NativeInlineFaultV2ForTest) *NativeInlineFixtureV2ForTest {
	t.Helper()
	if fault > NativeInlineExportExpiryV2ForTest {
		t.Fatal("unknown closed native inline fixture fault")
	}
	base, _, _ := nativeStackFixtureV3(t)
	f := &NativeInlineFixtureV2ForTest{fault: fault, Now: time.Now().UTC(), TeardownEntered: make(chan struct{}), WriteBackEntered: make(chan struct{}), AckEntered: make(chan struct{}), LostACKEntered: make(chan struct{}), continueTeardown: make(chan struct{}), continueWriteBack: make(chan struct{}), continueAck: make(chan struct{})}
	t.Cleanup(func() { f.ContinueTeardown(); f.ContinueWriteBack(); f.ContinueACK() })
	text, err := json.Marshal(nativeInlineFixtureFinal)
	if err != nil {
		t.Fatal(err)
	}
	protocol := "{\"type\":\"thread.started\",\"thread_id\":\"synthetic-inline\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":" + string(text) + "}}\n{\"type\":\"turn.completed\"}\n"
	// Fixed JSONL contains no single quotes; no caller-controlled shell input.
	script := "#!/bin/sh\n[ -f \"$CODEX_HOME/auth.json\" ] || exit 81\nwhile IFS= read -r line; do :; done\nprintf '%s' '" + protocol + "'\n"
	if err := os.WriteFile(base.manifest.Harness.Executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	digest, err := attachedworkerdaemon.DigestExecutable(base.manifest.Harness.Executable)
	if err != nil {
		t.Fatal(err)
	}
	profile := base.config.PreparedCodexNative.Profile.Clone()
	profile.Backend.ExecutableDigest, profile.Backend.Now = digest, f.CurrentTime
	profile.Launch.Workload.Digest, profile.Architecture = digest, runtime.GOARCH
	host, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	host, err = filepath.EvalSymlinks(host)
	if err != nil {
		t.Fatal(err)
	}
	hostDigest, err := attachedworkerdaemon.DigestExecutable(host)
	if err != nil {
		t.Fatal(err)
	}
	profile.Launch.Companion = attachedworkerdaemon.NativeArtifactV2{Executable: host, Digest: hostDigest}
	base.manifest.Harness.SHA256 = hex.EncodeToString(digest[:])
	base.manifest.Harness.Arguments, err = profile.ArgumentsV2()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := profile.PolicyDigestV2()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := profile.DescriptorV2()
	if err != nil {
		t.Fatal(err)
	}
	resource := base.config.PreparedCodexNative.Evidence.Policy.Scope.Resource
	bundle := base.config.PreparedCodexNative.Evidence.Clone()
	scope := domain.ProviderEvidenceScopeV1{TenantID: base.manifest.TenantID, OwnerUserID: base.manifest.OwnerUserID, Backend: descriptor, Resource: resource}
	bundle.Catalog.Scope, bundle.Route.Scope, bundle.Capability.Scope, bundle.Privacy.Scope, bundle.Price.Scope, bundle.Policy.Scope = scope, scope, scope, scope, scope, scope
	observed, expires := f.Now.Add(-time.Minute), f.Now.Add(time.Hour)
	bundle.Catalog.ObservedAt, bundle.Route.ObservedAt, bundle.Capability.ObservedAt, bundle.Privacy.ObservedAt, bundle.Price.ObservedAt, bundle.Policy.ObservedAt = observed, observed, observed, observed, observed, observed
	bundle.Catalog.ExpiresAt, bundle.Route.ExpiresAt, bundle.Capability.ExpiresAt, bundle.Privacy.ExpiresAt, bundle.Price.ExpiresAt, bundle.Policy.ExpiresAt = expires, expires, expires, expires, expires, expires
	digestEvidence := func(v interface {
		Digest() (domain.ProviderEvidenceDigestV1, error)
	}) string {
		t.Helper()
		d, err := v.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return string(d)
	}
	bundle.Policy.RoutePolicyDigest, bundle.Policy.CapabilityEvidenceDigest = digestEvidence(bundle.Route), digestEvidence(bundle.Capability)
	bundle.Policy.PrivacyEvidenceDigest, bundle.Policy.PriceObservationDigest = digestEvidence(bundle.Privacy), digestEvidence(bundle.Price)
	credentialRoot := filepath.Join(filepath.Dir(base.scratch), "inline-auth")
	if err := os.Mkdir(credentialRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(credentialRoot, "auth.json"), []byte(`{"synthetic":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := domain.NewCredentialSecretRef("synthetic-native-inline-metadata")
	if err != nil {
		t.Fatal(err)
	}
	credentials := &nativeInlineCredentialFixture{fixture: f, root: credentialRoot, binding: domain.CredentialBinding{Version: 1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, SubscriptionConnectionID: domain.SubscriptionConnectionID(resource.ResourceID), Provider: "openai", AuthMode: "chatgpt", SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte(`{"synthetic":true}`)), Entitlement: domain.EntitlementActive, Generation: resource.CredentialGeneration, UpdatedAt: observed}}
	base.config.PreparedCodexNative = &codexexec.NativePreparedConfigV2{Profile: profile, Evidence: bundle, CredentialBindings: credentials, InlineOutputV2: true}
	base.config.MaxStdoutBytes, base.config.MaxStderrBytes = 4<<20, 1<<20
	base.config.AllowedReadRoots = []string{filepath.Dir(credentialRoot)}
	// A short owned scratch root keeps real Unix socket names below sun_path's
	// limit, matching the existing native joined fixture. Remove only this root
	// after the actual Stack has cleaned its exact owned children.
	shortRoot, err := os.MkdirTemp("", "ni-")
	if err != nil {
		t.Fatal(err)
	}
	ownedShortRoot := shortRoot
	t.Cleanup(func() {
		if err := os.Remove(ownedShortRoot); err != nil {
			t.Errorf("owned native scratch residue: %v", err)
		}
	})
	shortRoot, err = filepath.EvalSymlinks(shortRoot)
	if err != nil {
		t.Fatal(err)
	}
	base.config.ScratchRoot = shortRoot
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	base.manifest.ConnectionGeneration, base.manifest.Revision = 0, 1
	base.manifest.IdentityKeyFingerprint = string(domain.DigestAttachedWorkerIdentityKey(public))
	base.manifest.CreatedAt, base.manifest.UpdatedAt = f.Now, f.Now
	stateRoot := filepath.Join(filepath.Dir(base.scratch), "inline-session-state")
	store, err := attachedworkerlocal.NewStore(stateRoot, f.CurrentTime)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(t.Context(), base.manifest, attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: base.manifest.TenantID, OwnerUserID: base.manifest.OwnerUserID, WorkerID: base.manifest.WorkerID, EnrollmentGeneration: base.manifest.EnrollmentGeneration, IdentityPrivateKey: private}); err != nil {
		t.Fatal(err)
	}
	f.JournalPath, f.MaterializationRoot = filepath.Join(stateRoot, attachedworkerlocal.ReceiptCheckpointFileName), filepath.Dir(base.scratch)
	peer := &nativeInlineSessionPeer{preparedSessionPeer: &preparedSessionPeer{now: f.Now, public: public}, fixture: f}
	t.Cleanup(func() { clear(peer.terminal); clear(peer.cachedACK) })
	f.Capability = preparedCapability(base.manifest)
	f.Capability.OperatingSystem, f.Capability.Architecture, f.Capability.HarnessVersion = "linux", runtime.GOARCH, "0.162.1"
	f.Capability.Features = append(f.Capability.Features, attachedworkerprotocol.FeatureInlineOutputV2)
	slices.Sort(f.Capability.Features)
	launcher := &nativeInlineLauncherFixture{fixture: f}
	deps := nativeDependenciesV3{hostOS: "linux", hostArchitecture: runtime.GOARCH, digest: attachedworkerdaemon.DigestExecutable, newSupervisor: attachedworkerdaemon.NewSupervisorV3,
		newLauncher: func(context.Context, attachedworkeroci.Config) (nativeReconcilingLauncherV3, error) {
			return launcher, nil
		}}
	sessionConfig := attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: f.Capability.ProtocolOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: 5 * time.Second,
		Now: f.CurrentTime, Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, 64)), RuntimePreflight: func(ctx context.Context, manifest attachedworkerlocal.ManifestV1) error {
			var err error
			f.Stack, err = newNativeStackV3(ctx, manifest, base.config, credentials, deps)
			return err
		}}
	if fault == NativeInlineLostACKV2ForTest {
		// Backoff is a bounded transport-replay policy, never readiness proof.
		sessionConfig.MaxExchangeAttempts = 2
		sessionConfig.RetryInitialBackoff, sessionConfig.RetryMaxBackoff = 5*time.Millisecond, 5*time.Millisecond
		sessionConfig.RetryRandom = bytes.NewReader(bytes.Repeat([]byte{0x63}, 8))
	}
	connector, err := attachedworkersession.New(store, peer, peer, sessionConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.ContinueTeardown()
		f.ContinueWriteBack()
		f.ContinueACK()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if f.Stack != nil {
			if err := f.Stack.Close(ctx); err != nil {
				t.Errorf("close original native Stack: %v", err)
			}
		}
		if f.Session != nil {
			if err := f.Session.Close(ctx); err != nil {
				t.Errorf("close original Session: %v", err)
			}
		}
	})
	f.Session, err = connector.Connect(t.Context(), attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: f.Capability})
	if err != nil {
		t.Fatalf("original Connector.Connect: %v", err)
	}
	connected := f.Session.Snapshot()
	i := attachedworkerdaemon.InvocationIdentity{TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, WorkerID: base.manifest.WorkerID, RunID: "native-inline-run", AttemptID: "native-inline-attempt", LeaseID: "native-inline-lease", FenceToken: 7}
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: i.OwnerUserID, WorkerID: i.WorkerID, CapabilityDigest: connected.CapabilityDigest, PolicyDigest: policy}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: i.TenantID, OwnerUserID: i.OwnerUserID, RunID: i.RunID, AttemptID: i.AttemptID, Backend: descriptor, Resource: resource, ModelVendorID: codexexec.ModelVendorIDV1, ModelID: profile.Backend.Model, InputDataClass: domain.ProviderDataPrivateV1,
		ProviderCatalogDigest: digestEvidence(bundle.Catalog), ProviderRouteDigest: digestEvidence(bundle.Route), CapabilityEvidenceDigest: digestEvidence(bundle.Capability), PrivacyPolicyDigest: digestEvidence(bundle.Privacy), EffectivePolicyDigest: digestEvidence(bundle.Policy), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires}
	if err := bundle.ValidateForBinding(binding); err != nil {
		t.Fatalf("original evidence binding: %v", err)
	}
	job := domain.WorkerJob{TenantID: i.TenantID, RunID: i.RunID, SessionID: "native-inline-session", TriggerEventID: "native-inline-trigger", AttemptID: i.AttemptID, ReservationID: "native-inline-reservation", InputManifestID: "native-inline-manifest", ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, CredentialOwnerUserID: i.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 2, MaxToolEvents: 1, MaxToolEventBytes: 4096}}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: i.TenantID, RunID: i.RunID, CreatedAt: f.Now}
	payload := []byte(`{"version":1,"text":"produce two bounded original files"}`)
	hash := sha256.Sum256(payload)
	event := domain.SessionEvent{ID: job.TriggerEventID, TenantID: i.TenantID, SessionID: job.SessionID, Sequence: 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &i.OwnerUserID, IdempotencyKey: "native-inline-trigger-key", Payload: domain.BlobRef{TenantID: i.TenantID, Key: domain.SessionEventObjectPrefix(i.TenantID, job.SessionID, job.TriggerEventID) + "message.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])}, CreatedAt: f.Now}
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: i.TenantID, SessionID: job.SessionID, Events: []domain.SessionEvent{event}}, EventBodies: [][]byte{payload}}
	history, _, err := sessioncontext.ProjectCanonical(job, proof, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(i.TenantID, i.OwnerUserID, i.WorkerID, i.RunID, i.AttemptID, i.LeaseID, i.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	head := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: i.TenantID, OwnerUserID: i.OwnerUserID, WorkerID: i.WorkerID, ConnectionID: connected.ConnectionID, EnrollmentGeneration: connected.EnrollmentGeneration, ConnectionGeneration: connected.ConnectionGeneration, RunID: i.RunID, AttemptID: i.AttemptID, ReservationID: job.ReservationID, LeaseID: i.LeaseID, LeaseGeneration: i.FenceToken, FenceToken: fence, ContextDigest: contextDigest, CapabilityDigest: placement.CapabilityDigest, PolicyDigest: placement.PolicyDigest, State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1, Revision: 1, CreatedAt: f.Now, UpdatedAt: f.Now, LeaseExpiresAt: expires}
	credential := &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: codexexec.CredentialHomeEnvironmentV1, ExpectedBindingGeneration: resource.CredentialGeneration, IssueRequest: ports.CredentialIssueRequest{OwnerUserID: i.OwnerUserID, ProviderResource: resource, ExpiresAt: expires,
		Run:     domain.Run{ID: i.RunID, TenantID: i.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID, SubscriptionConnectionID: domain.SubscriptionConnectionID(resource.ResourceID), Status: domain.RunRunning, IdempotencyKey: "native-inline-run-key", StartedAt: &f.Now, CreatedAt: f.Now, UpdatedAt: f.Now},
		Attempt: domain.Attempt{ID: i.AttemptID, TenantID: i.TenantID, RunID: i.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: string(i.WorkerID), CreatedAt: f.Now, UpdatedAt: f.Now},
		Lease:   domain.Lease{ID: i.LeaseID, TenantID: i.TenantID, RunID: i.RunID, AttemptID: i.AttemptID, WorkerID: string(i.WorkerID), FenceToken: i.FenceToken, AcquiredAt: f.Now, ExpiresAt: expires}}}
	input := &attachedworkerdaemon.VerifiedAttachedInputV1{AcceptedAttempt: head, Job: job, Manifest: manifest, Context: history, CanonicalContext: proof}
	t.Cleanup(input.Clear)
	f.Original = attachedworkerdaemon.Invocation{Identity: i, Process: attachedworkerdaemon.AttemptSpec{Executable: profile.Backend.Executable, ExecutableDigest: digest, Arguments: append([]string(nil), base.manifest.Harness.Arguments...)}, Credential: credential, VerifiedInput: input}
	decode := func(value string) []byte {
		body, err := hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	peer.binding = attachedworkerprotocol.AttemptBindingV1{RunID: string(i.RunID), AttemptID: string(i.AttemptID), LeaseID: string(i.LeaseID), LeaseGeneration: i.FenceToken, FenceToken: string(fence), ExpiresAtUnixMicro: expires.UnixMicro(), ContextDigest: decode(string(contextDigest)), CapabilityDigest: decode(string(placement.CapabilityDigest)), PolicyDigest: decode(string(policy))}
	if input.ValidateFor(i) != nil || f.Original.Validate() != nil {
		t.Fatal("fixed canonical input fixture invalid")
	}
	return f
}
