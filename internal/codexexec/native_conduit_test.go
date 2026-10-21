//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This fixture never invokes Run, a subprocess, Unix listen/dial or an engine.
// Only the real Session bootstrap/reducer/claim and original constructors run.
type nativeConstructorFixture struct {
	session *attachedworkersession.Session
	input   *attachedworkerdaemon.VerifiedAttachedInputV1
	config  PreparedProcessBoundaryConfigV1
	bundle  attachedworkerdaemon.NativeLaunchV2
}

func newNativeConstructorFixture(t *testing.T) nativeConstructorFixture {
	t.Helper()
	return newNativeConstructorFixtureWithExecutable(t, "")
}

func newNativeConstructorFixtureWithExecutable(t *testing.T, path string) nativeConstructorFixture {
	t.Helper()
	root := canonicalTempDir(t)
	if path == "" {
		path = filepath.Join(root, "inert-native")
		if err := os.WriteFile(path, []byte("inert fixture; never executed\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	digest, err := attachedworkerdaemon.DigestExecutable(path)
	if err != nil {
		t.Fatal(err)
	}
	artifact := attachedworkerdaemon.NativeArtifactV2{Executable: path, Digest: digest}
	bundle := attachedworkerdaemon.NativeLaunchV2{Version: 2, Bootstrap: artifact, Relay: artifact, Companion: artifact,
		BootstrapArguments: []string{"bootstrap"}, RelayArguments: []string{"relay"}, CompanionArguments: []string{"companion"}, SocketDestination: "/run/native.sock",
		ProviderHosts: []string{"provider.example"},
		Limits:        attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 64 << 10, OutboundBytes: 64 << 10, CompanionMemoryBytes: 1 << 20, CompanionGoroutines: 8, Active: 10 * time.Second, Cleanup: time.Second}}
	policy, err := bundle.InstallPolicyDigestV2()
	if err != nil {
		t.Fatal(err)
	}
	configDir := filepath.Join(root, "engine-config")
	if err := os.Mkdir(configDir, 0700); err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	manifest := attachedworkerlocal.ManifestV1{Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "worker-a", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{DockerPath: path, DockerSHA256: fmt.Sprintf("%x", digest), CLIConfigDir: configDir,
			Host: "unix:///run/docker.sock", EngineID: "engine-native-abcdef", InstallationID: "install-native", Boundary: attachedworkerlocal.BoundaryLinuxRootless,
			Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024,
			MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: path, SHA256: fmt.Sprintf("%x", digest), Arguments: []string{"--native"}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: driverNow, UpdatedAt: driverNow}
	if runtime.GOOS == "darwin" {
		manifest.OCI.Boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	store, err := attachedworkerlocal.NewStore(filepath.Join(root, "state"), func() time.Time { return driverNow })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID,
		OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: private}); err != nil {
		t.Fatal(err)
	}
	peer := &nativeConstructorPeer{public: public}
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	capability := attachedworkerprotocol.CapabilityManifestV1{WorkerID: string(manifest.WorkerID), EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "native-fixture", HarnessName: "codex", HarnessVersion: "1",
		HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: digest[:],
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary, attachedworkerprotocol.IsolationProcessBoundary},
		Features:          []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}, MaxConcurrentAttempts: 1}
	connector, err := attachedworkersession.New(store, peer, peer, attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: offer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second,
		Random: bytes.NewReader(append(bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 32)...)), Now: func() time.Time { return driverNow }})
	if err != nil {
		t.Fatal(err)
	}
	session, err := connector.Connect(context.Background(), attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 1, CapabilityManifest: capability})
	if err != nil {
		t.Fatalf("Connect phase=%s claim_step=%d err=%v", peer.phase, peer.step, err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close owned Session: %v", err)
		}
	})
	snapshot := session.Snapshot()
	request, authority := validDriverRequest(t)
	request.ExecutionPlacementV2.PolicyDigest = domain.AttachedWorkerPolicyDigest(hex.EncodeToString(policy[:]))
	authority.PolicyDigest = request.ExecutionPlacementV2.PolicyDigest
	request.ExecutionPlacementV2.CapabilityDigest = snapshot.CapabilityDigest
	placementDigest, err := domain.ExecutionPlacementDigest(request.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	request.HarnessBinding.ExecutionPlacementDigest = string(placementDigest)
	authority.ConnectionID = snapshot.ConnectionID
	authority.EnrollmentGeneration = snapshot.EnrollmentGeneration
	authority.ConnectionGeneration = snapshot.ConnectionGeneration
	authority.CapabilityDigest = snapshot.CapabilityDigest
	input := preparedOwnerTypedInput(t, request, authority)
	authority.ContextDigest = input.AcceptedAttempt.ContextDigest
	peer.binding = attachedworkerprotocol.AttemptBindingV1{RunID: string(authority.RunID), AttemptID: string(authority.AttemptID), LeaseID: string(authority.LeaseID),
		LeaseGeneration: authority.LeaseGeneration, FenceToken: string(authority.FenceToken), ExpiresAtUnixMicro: authority.LeaseExpiresAt.UnixMicro(),
		ContextDigest: nativeDecodeHex(t, string(authority.ContextDigest)), CapabilityDigest: nativeDecodeHex(t, string(authority.CapabilityDigest)), PolicyDigest: nativeDecodeHex(t, string(authority.PolicyDigest))}
	if _, err := session.ExchangeAction(context.Background(), attachedworkersession.ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: driverNow.UnixMicro(), Available: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ExchangeAction(context.Background(), attachedworkersession.ActionV1{LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: peer.binding, AttemptSequence: 1}}); err != nil {
		t.Fatal(err)
	}
	supervisor, err := attachedworkerdaemon.NewSupervisorV2(attachedworkerdaemon.SupervisorConfigV2{Base: attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: canonicalTempDir(t), Timeout: 10 * time.Second, TerminationGrace: time.Second, MaxStdoutBytes: 4096, MaxStderrBytes: 4096}, Launcher: &nativeConstructorLauncher{}})
	if err != nil {
		t.Fatal(err)
	}
	return nativeConstructorFixture{session: session, input: input, config: PreparedProcessBoundaryConfigV1{Supervisor: supervisor, Authority: authority,
		Executable: path, ExecutableDigest: digest, Arguments: []string{"--native"}, Now: func() time.Time { return driverNow }},
		bundle: bundle}
}

func nativeDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, e := hex.DecodeString(s)
	if e != nil {
		t.Fatal(e)
	}
	return b
}

type nativeConstructorLauncher struct{}

func (*nativeConstructorLauncher) ProfileV2() attachedworkerdaemon.IsolationProfileV2 {
	return attachedworkerdaemon.IsolationProfileV2{Version: 2, Name: "inert-native", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}
func (*nativeConstructorLauncher) PrepareV2(context.Context, attachedworkerdaemon.LaunchEnvelopeV2) (attachedworkerdaemon.IsolationBoundaryV2, error) {
	panic("constructor fixture must never Prepare/Run")
}

type nativeConstructorPeer struct {
	public     ed25519.PublicKey
	challenge  domain.AttachedWorkerAttachChallenge
	binding    attachedworkerprotocol.AttemptBindingV1
	step       uint64
	phase      string
	connection attachedworkersession.ConnectionBindingV1
}

func (p *nativeConstructorPeer) IssueChallenge(_ context.Context, in attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	request := attachedworkertransport.IssueChallengeRequest{WorkerID: domain.AttachedWorkerID(in.Hello.WorkerID), ExpectedAudience: in.ExpectedAudience, ExpectedWorkerRevision: in.ExpectedWorkerRevision, Purpose: in.Purpose, Hello: in.Hello}
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(in.TenantLocator, in.OwnerLocator, request.WorkerID, in.Hello.EnrollmentGeneration, in.Hello.ConnectionGeneration-1, request)
	if err != nil || !ed25519.Verify(p.public, transcript, in.Proof) {
		return nil, fmt.Errorf("owned challenge proof invalid")
	}
	nonce := bytes.Repeat([]byte{0x52}, 32)
	p.challenge = domain.AttachedWorkerAttachChallenge{TenantID: in.TenantLocator, OwnerUserID: in.OwnerLocator, ID: "native-challenge", WorkerID: request.WorkerID, ConnectionID: "native-connection",
		Purpose: in.Purpose, Audience: in.ExpectedAudience, ExpectedWorkerRevision: in.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: in.Hello.EnrollmentGeneration,
		ExpectedConnectionGeneration: in.Hello.ConnectionGeneration - 1, TargetConnectionGeneration: in.Hello.ConnectionGeneration,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1}, PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
		WorkerNonceDigest: domain.DigestAttachedWorkerChallenge(in.Hello.Hello.WorkerNonce), PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(nonce), CreatedAt: driverNow, ExpiresAt: driverNow.Add(time.Minute), RetainUntil: driverNow.Add(time.Hour), Revision: 1}
	return &attachedworkerhttp.ChallengeResponseV1{Challenge: p.challenge, Frame: attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1), WorkerID: in.Hello.WorkerID,
		EnrollmentGeneration: in.Hello.EnrollmentGeneration, ConnectionGeneration: in.Hello.ConnectionGeneration, Sequence: 1, Ack: 1, Kind: attachedworkerprotocol.MessageChallenge,
		Challenge: &attachedworkerprotocol.ChallengeV1{WorkerOffer: in.Hello.Hello.Offer, PlatformOffer: in.Hello.Hello.Offer, SelectedVersion: 1, WorkerNonce: in.Hello.Hello.WorkerNonce, PlatformNonce: nonce}}}, nil
}
func (p *nativeConstructorPeer) Activate(_ context.Context, in attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	channel := attachedworkertransport.ConnectionChannelBinding(p.challenge.ID, p.challenge.WorkerNonceDigest, p.challenge.PlatformNonceDigest, in.ConnectionSecret.Digest())
	raw, err := hex.DecodeString(string(channel))
	if err != nil {
		return nil, err
	}
	auth := attachedworkerprotocol.AuthContextV1{TenantID: string(p.challenge.TenantID), OwnerUserID: string(p.challenge.OwnerUserID), WorkerID: string(p.challenge.WorkerID), IdentityPublicKey: p.public,
		EnrollmentGeneration: p.challenge.ExpectedEnrollmentGeneration, ConnectionGeneration: p.challenge.TargetConnectionGeneration, Version: in.Attach.Version, ChannelBinding: raw}
	if err := attachedworkerprotocol.VerifyAttachV1(auth, in.Attach); err != nil {
		return nil, err
	}
	return &attachedworkerhttp.ActivateResponseV1{Connection: attachedworkerhttp.ActivateConnectionV1{TenantID: p.challenge.TenantID, OwnerUserID: p.challenge.OwnerUserID, WorkerID: p.challenge.WorkerID, ID: p.challenge.ConnectionID,
		ActivationChallengeID: p.challenge.ID, EnrollmentGeneration: in.Attach.EnrollmentGeneration, ConnectionGeneration: in.Attach.ConnectionGeneration, ProtocolVersion: 1,
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", in.Attach.Attach.CapabilityDigest)), SecretDigest: in.ConnectionSecret.Digest(), ChannelBinding: channel,
		State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1, ConnectedAt: driverNow.Add(time.Minute), AuthExpiresAt: driverNow.Add(time.Hour), Revision: 1},
		Accepted: attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2), WorkerID: in.Attach.WorkerID, EnrollmentGeneration: in.Attach.EnrollmentGeneration,
			ConnectionGeneration: in.Attach.ConnectionGeneration, Sequence: 2, Ack: 2, Kind: attachedworkerprotocol.MessageAttachAccepted, AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{
				WorkerOffer: in.Attach.Attach.WorkerOffer, PlatformOffer: in.Attach.Attach.PlatformOffer, SelectedVersion: 1, WorkerNonce: in.Attach.Attach.WorkerNonce, PlatformNonce: in.Attach.Attach.PlatformNonce, CapabilityDigest: in.Attach.Attach.CapabilityDigest}}}, nil
}
func (p *nativeConstructorPeer) Open(b attachedworkersession.ConnectionBindingV1, _ []byte) (attachedworkersession.ExchangePort, error) {
	p.connection = b
	return p, nil
}
func (p *nativeConstructorPeer) Exchange(_ context.Context, b attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if len(b.Frames) != 1 {
		return nil, fmt.Errorf("owned peer expects one frame, phase=%s", p.phase)
	}
	if b.Frames[0].Kind == attachedworkerprotocol.MessageManifest && p.phase == "" && p.binding.RunID == "" {
		// The real Connect sends its signed capability Manifest before returning
		// Ready. Like the existing Session fixture, acknowledge it with no new
		// platform frame; do not offer an unconfigured zero-binding Attempt.
		p.phase = "ready-manifest-confirmed"
		return nil, nil
	}
	if p.binding.RunID == "" {
		return nil, fmt.Errorf("owned attempt not installed, phase=%s", p.phase)
	}
	p.step++
	f := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, p.step+2), WorkerID: string(p.connection.WorkerID),
		EnrollmentGeneration: p.connection.EnrollmentGeneration, ConnectionGeneration: p.connection.ConnectionGeneration, Sequence: p.step + 2, Ack: b.Frames[0].Sequence}
	if p.step == 1 {
		if p.phase != "ready-manifest-confirmed" || b.Frames[0].Kind != attachedworkerprotocol.MessageHeartbeat {
			return nil, fmt.Errorf("owned offer phase=%s kind=%s", p.phase, b.Frames[0].Kind)
		}
		p.phase = "offered"
		f.Kind = attachedworkerprotocol.MessageLeaseOffer
		f.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: p.binding, AttemptSequence: 1}
	} else if p.step == 2 {
		if p.phase != "offered" || b.Frames[0].Kind != attachedworkerprotocol.MessageLeaseClaim {
			return nil, fmt.Errorf("owned accept phase=%s kind=%s", p.phase, b.Frames[0].Kind)
		}
		p.phase = "claimed"
		f.Kind = attachedworkerprotocol.MessageLeaseAccepted
		f.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: p.binding, AttemptSequence: 2}
	} else {
		return nil, fmt.Errorf("unexpected owned exchange")
	}
	return &attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{f}}, nil
}

func TestNativeConduitConstructorLosingBorrowCannotCancelWinner(t *testing.T) {
	for _, kind := range []string{"different-bundle", "same-bundle"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeConstructorFixture(t)
			ctxA, cancelA := context.WithCancel(context.Background())
			defer cancelA()
			ctxB, cancelB := context.WithCancel(context.Background())
			defer cancelB()
			// A only borrows. B is the first original entry committed under the
			// real Session lock. No callback or fabricated current predicate.
			child, err := f.session.BorrowNativeAttemptV2(ctxA, f.input.AcceptedAttempt, f.input.Job, f.input.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			winner, err := NewNativePreparedProcessBoundaryV2(ctxB, f.config, f.session, f.input, f.bundle)
			if err != nil {
				t.Fatal(err)
			}
			life, err := child.Lifetime()
			if err != nil {
				t.Fatal(err)
			}
			bundleA := f.bundle.Clone()
			if kind == "different-bundle" {
				bundleA.RelayArguments = []string{"valid-but-losing-relay"}
			}
			joined, err := NewNativePreparedProcessBoundaryV2(ctxA, f.config, f.session, f.input, bundleA)
			if kind == "different-bundle" && err == nil {
				t.Fatal("different bundle joined")
			}
			if kind == "same-bundle" && (err != nil || joined != winner) {
				t.Fatalf("same bundle did not join original: same=%t err=%v", joined == winner, err)
			}
			cancelA()
			if err := winner.native.child.Current(); err != nil || life.Err() != nil {
				t.Fatalf("losing/joining borrow context retired winner: current=%v lifetime=%v", err, life.Err())
			}
		})
	}
}

func TestNativeConduitOriginalConstructorEntry(t *testing.T) {
	f := newNativeConstructorFixture(t)
	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	entries := make([]*PreparedProcessBoundaryV1, n)
	errs := make([]error, n)
	for i := range entries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			entries[i], errs[i] = NewNativePreparedProcessBoundaryV2(context.Background(), f.config, f.session, f.input, f.bundle)
		}()
	}
	close(start)
	wg.Wait()
	for i, entry := range entries {
		if errs[i] != nil || entry == nil || entry != entries[0] {
			t.Fatalf("constructor %d did not join original: same=%t err=%v", i, entry == entries[0], errs[i])
		}
	}
	original := entries[0]
	bad := f.config
	bad.ExecutableDigest[0] ^= 1
	if _, err := NewNativePreparedProcessBoundaryV2(context.Background(), bad, f.session, f.input, f.bundle); err == nil {
		t.Fatal("invalid loser accepted")
	}
	other := f.bundle
	other.RelayArguments = []string{"different-relay"}
	if _, err := NewNativePreparedProcessBoundaryV2(context.Background(), f.config, f.session, f.input, other); err == nil {
		t.Fatal("different immutable bundle accepted")
	}
	if original.native.child.Current() != nil {
		t.Fatal("invalid/losing constructor retired winner")
	}
	if original.consumed || original.active != nil {
		t.Fatal("constructor added another execution consumption")
	}
}

type fakeNativeEntry struct{ n int }

func (*fakeNativeEntry) NativePreparedEntryIdentityV2() { panic("passive marker cannot approve") }
func TestNativeConduitConstructorRejectsForeignEntry(t *testing.T) {
	for _, kind := range []string{"fake-marker", "typed-nil", "foreign-boundary"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeConstructorFixture(t)
			child, err := f.session.BorrowNativeAttemptV2(context.Background(), f.input.AcceptedAttempt, f.input.Job, f.input.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			var entry attachedworkersession.NativePreparedEntryV2
			switch kind {
			case "fake-marker":
				entry = &fakeNativeEntry{}
			case "typed-nil":
				var p *PreparedProcessBoundaryV1
				entry = p
			case "foreign-boundary":
				other := newNativeConstructorFixture(t)
				entry, err = NewNativePreparedProcessBoundaryV2(context.Background(), other.config, other.session, other.input, other.bundle)
				if err != nil {
					t.Fatal(err)
				}
			}
			_, bindErr := child.BindPreparedEntryV2(entry)
			if kind == "typed-nil" {
				if bindErr == nil {
					t.Fatal("typed nil attached")
				}
				return
			}
			if bindErr != nil {
				t.Fatal(bindErr)
			}
			if _, err := NewNativePreparedProcessBoundaryV2(context.Background(), f.config, f.session, f.input, f.bundle); err == nil {
				t.Fatal("foreign/passive marker became original boundary")
			}
			if child.Current() != nil {
				t.Fatal("rejected foreign association closed live child")
			}
		})
	}
}
