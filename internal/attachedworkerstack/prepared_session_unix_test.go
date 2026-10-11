//go:build darwin || linux

package attachedworkerstack

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This peer verifies the Connector's original key proofs. It does not create a
// Session, modify Session state, or provide execution/cleanup observations.
type preparedSessionPeer struct {
	now                                      time.Time
	public                                   ed25519.PublicKey
	challenge                                domain.AttachedWorkerAttachChallenge
	previous                                 *attachedworkerlocal.ReconnectCheckpointV1
	challengeCalls, activateCalls, exchanges int
	exchangeErr                              error
}

func preparedCapability(manifest attachedworkerlocal.ManifestV1) attachedworkerprotocol.CapabilityManifestV1 {
	digest, _ := hex.DecodeString(manifest.Harness.SHA256)
	return attachedworkerprotocol.CapabilityManifestV1{WorkerID: string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration, Revision: 1,
		ProtocolOffer:   attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}},
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "prepared-receipt-fixture", HarnessName: "prepared-codex", HarnessVersion: "fixture-v1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: digest, IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary, attachedworkerprotocol.IsolationProcessBoundary},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureOutputReceipt, attachedworkerprotocol.FeatureReconnect}, MaxConcurrentAttempts: 1}
}

func (peer *preparedSessionPeer) IssueChallenge(_ context.Context, input attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	peer.challengeCalls++
	r := attachedworkertransport.IssueChallengeRequest{WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ExpectedAudience: input.ExpectedAudience, ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: input.Purpose, Hello: input.Hello}
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(input.TenantLocator, input.OwnerLocator, r.WorkerID, input.Hello.EnrollmentGeneration, input.Hello.ConnectionGeneration-1, r)
	if err != nil || !ed25519.Verify(peer.public, transcript, input.Proof) {
		return nil, errors.New("fixture challenge proof denied")
	}
	nonce := bytes.Repeat([]byte{0x52}, 32)
	peer.challenge = domain.AttachedWorkerAttachChallenge{TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, ID: domain.AttachedWorkerChallengeID(fmt.Sprintf("challenge-%d", input.Hello.ConnectionGeneration)),
		WorkerID: r.WorkerID, ConnectionID: domain.AttachedWorkerConnectionID(fmt.Sprintf("connection-%d", input.Hello.ConnectionGeneration)), Purpose: input.Purpose, Audience: input.ExpectedAudience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration, ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1, TargetConnectionGeneration: input.Hello.ConnectionGeneration,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1}, PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
		WorkerNonceDigest: domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce), PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(nonce), CreatedAt: peer.now, ExpiresAt: peer.now.Add(time.Minute), RetainUntil: peer.now.Add(time.Hour), Revision: 1}
	if peer.previous != nil {
		body, err := attachedworkerprotocol.EncodeMachineSnapshotV1(peer.previous.MachineSnapshot)
		if err != nil {
			return nil, err
		}
		peer.challenge.ExpectedConnectionID = peer.previous.ConnectionID
		peer.challenge.ExpectedConnectionRevision = 2
		peer.challenge.ExpectedCapabilityDigest = peer.previous.CapabilityDigest
		peer.challenge.ExpectedProtocolSnapshot = body
	}
	return &attachedworkerhttp.ChallengeResponseV1{Challenge: peer.challenge, Frame: attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
		WorkerID: input.Hello.WorkerID, EnrollmentGeneration: input.Hello.EnrollmentGeneration, ConnectionGeneration: input.Hello.ConnectionGeneration, Sequence: 1, Ack: 1, Kind: attachedworkerprotocol.MessageChallenge,
		Challenge: &attachedworkerprotocol.ChallengeV1{WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: input.Hello.Hello.Offer, SelectedVersion: 1, WorkerNonce: bytes.Clone(input.Hello.Hello.WorkerNonce), PlatformNonce: nonce}}}, nil
}

func (peer *preparedSessionPeer) Activate(_ context.Context, input attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	peer.activateCalls++
	channel := attachedworkertransport.ConnectionChannelBinding(peer.challenge.ID, peer.challenge.WorkerNonceDigest, peer.challenge.PlatformNonceDigest, input.ConnectionSecret.Digest())
	channelBytes, err := hex.DecodeString(string(channel))
	if err != nil {
		return nil, err
	}
	auth := attachedworkerprotocol.AuthContextV1{TenantID: string(peer.challenge.TenantID), OwnerUserID: string(peer.challenge.OwnerUserID), WorkerID: string(peer.challenge.WorkerID), IdentityPublicKey: peer.public,
		EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration, Version: input.Attach.Version, ChannelBinding: channelBytes}
	var accepted attachedworkerprotocol.FrameV1
	var capability []byte
	if peer.previous == nil {
		if err := attachedworkerprotocol.VerifyAttachV1(auth, input.Attach); err != nil {
			return nil, err
		}
		capability = input.Attach.Attach.CapabilityDigest
		accepted = attachedworkerprotocol.FrameV1{Version: input.Attach.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2), WorkerID: input.Attach.WorkerID,
			EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration, Sequence: 2, Ack: 2, Kind: attachedworkerprotocol.MessageAttachAccepted,
			AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{WorkerOffer: input.Attach.Attach.WorkerOffer, PlatformOffer: input.Attach.Attach.PlatformOffer, SelectedVersion: input.Attach.Version,
				WorkerNonce: bytes.Clone(input.Attach.Attach.WorkerNonce), PlatformNonce: bytes.Clone(input.Attach.Attach.PlatformNonce), CapabilityDigest: bytes.Clone(capability)}}
	} else {
		if err := attachedworkerprotocol.VerifyReconnectV1(auth, input.Attach); err != nil {
			return nil, err
		}
		old := *peer.previous
		config := attachedworkerprotocol.MachineConfig{Auth: attachedworkerprotocol.AuthContextV1{TenantID: string(old.TenantID), OwnerUserID: string(old.OwnerUserID), WorkerID: string(old.WorkerID), IdentityPublicKey: peer.public,
			EnrollmentGeneration: old.EnrollmentGeneration, ConnectionGeneration: old.ConnectionGeneration, Version: old.ProtocolVersion, ChannelBinding: old.ChannelBinding}, WorkerOffer: old.WorkerOffer, PlatformOffer: old.PlatformOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}}
		accepted, _, err = attachedworkerprotocol.BuildReconnectAcceptedSnapshotV1(config, old.MachineSnapshot, auth, input.Attach)
		if err != nil {
			return nil, err
		}
		capability = input.Attach.Reconnect.CapabilityDigest
	}
	return &attachedworkerhttp.ActivateResponseV1{Accepted: accepted, Connection: attachedworkerhttp.ActivateConnectionV1{TenantID: peer.challenge.TenantID, OwnerUserID: peer.challenge.OwnerUserID, WorkerID: peer.challenge.WorkerID,
		ID: peer.challenge.ConnectionID, ActivationChallengeID: peer.challenge.ID, EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration, ProtocolVersion: uint32(input.Attach.Version),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capability)), SecretDigest: input.ConnectionSecret.Digest(), ChannelBinding: channel, State: domain.AttachedWorkerConnectionAttaching,
		PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1, ConnectedAt: peer.now, AuthExpiresAt: peer.now.Add(time.Hour), Revision: 1}}, nil
}
func (peer *preparedSessionPeer) Open(_ attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	if _, err := attachedworkertransport.ParseConnectionBearer(bearer); err != nil {
		return nil, err
	}
	return peer, nil
}
func (peer *preparedSessionPeer) Exchange(_ context.Context, _ attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	peer.exchanges++
	return nil, peer.exchangeErr
}
func (*preparedSessionPeer) Close() error { return nil }

func connectPreparedFixture(t *testing.T, manifest attachedworkerlocal.ManifestV1, now time.Time, preflight func(context.Context, attachedworkerlocal.ManifestV1) error) (*attachedworkersession.Session, *attachedworkerlocal.Store, *preparedSessionPeer) {
	t.Helper()
	connector, store, peer, capability := newPreparedConnectorFixture(t, manifest, now, preflight)
	session, err := connector.Connect(context.Background(), attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability})
	if err != nil {
		t.Fatalf("Connector.Connect: %v capability=%v challenge=%d activate=%d exchanges=%d", err, capability.Validate(), peer.challengeCalls, peer.activateCalls, peer.exchanges)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return session, store, peer
}

func newPreparedConnectorFixture(t *testing.T, manifest attachedworkerlocal.ManifestV1, now time.Time, preflight func(context.Context, attachedworkerlocal.ManifestV1) error) (*attachedworkersession.Connector, *attachedworkerlocal.Store, *preparedSessionPeer, attachedworkerprotocol.CapabilityManifestV1) {
	t.Helper()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	manifest.ConnectionGeneration = 0
	manifest.Revision = 1
	manifest.IdentityKeyFingerprint = string(domain.DigestAttachedWorkerIdentityKey(public))
	store, err := attachedworkerlocal.NewStore(filepath.Join(filepath.Dir(manifest.OCI.CLIConfigDir), "session-state"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	secret := attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: manifest.Revision, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID, EnrollmentGeneration: manifest.EnrollmentGeneration, IdentityPrivateKey: private}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	peer := &preparedSessionPeer{now: now, public: public}
	capability := preparedCapability(manifest)
	connector, err := attachedworkersession.New(store, peer, peer, attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: capability.ProtocolOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second,
		Now: func() time.Time { return now }, Random: bytes.NewReader(bytes.Repeat([]byte{0x41}, 64)), RuntimePreflight: preflight})
	if err != nil {
		t.Fatal(err)
	}
	return connector, store, peer, capability
}

func TestPreparedConnectionBindRejectsMissingRebindAndExpired(t *testing.T) {
	for _, mode := range []string{"missing", "nil", "zero", "closed", "expired", "expired-bind", "rebind"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			stack, invocation, owner, launcher, session, _, _ := newPreparedStackSessionFixture(t, false, mode == "expired" || mode == "rebind", func() time.Time { return now })
			switch mode {
			case "nil":
				if err := stack.BindPreparedConnection(nil); err == nil {
					t.Fatal("nil Session bound")
				}
			case "zero":
				if err := stack.BindPreparedConnection(&attachedworkersession.Session{}); err == nil {
					t.Fatal("zero Session bound")
				}
			case "closed":
				if err := session.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := stack.BindPreparedConnection(session); err == nil {
					t.Fatal("closed Session bound")
				}
			case "expired":
				now = now.Add(2 * time.Hour)
			case "expired-bind":
				now = now.Add(2 * time.Hour)
				if err := stack.BindPreparedConnection(session); err == nil {
					t.Fatal("expired Session bound")
				}
			case "rebind":
				if err := stack.BindPreparedConnection(session); err == nil {
					t.Fatal("second bind accepted")
				}
				return
			}
			if _, err := stack.Run(context.Background(), invocation); err == nil {
				t.Fatal("run acquired authority without a current bound Session")
			}
			if len(owner.order) != 0 || launcher.calls != 0 {
				t.Fatalf("denial caused effects %v/spawns=%d", owner.order, launcher.calls)
			}
		})
	}
}

func TestPreparedConnectionBindIsSingleOwner(t *testing.T) {
	stack, _, _, _, session, _, _ := newPreparedStackSessionFixture(t, false, false, nil)
	var workers sync.WaitGroup
	results := make(chan error, 16)
	for range 16 {
		workers.Go(func() { results <- stack.BindPreparedConnection(session) })
	}
	workers.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("original Session had %d binding owners", accepted)
	}
}

func TestPreparedConnectionPinPreflightHasZeroConnectionEffects(t *testing.T) {
	fixture := newFixture(t)
	fixture.manifest.ConnectionGeneration = 0
	now := time.Now().UTC()
	calls := 0
	connector, store, peer, capability := newPreparedConnectorFixture(t, fixture.manifest, now, func(ctx context.Context, manifest attachedworkerlocal.ManifestV1) error {
		calls++
		manifest.Harness.SHA256 = fmt.Sprintf("%064x", 8)
		_, err := newStack(ctx, manifest, fixture.config, fixture.credentials, fixture.dependencies(nil))
		return err
	})
	if session, err := connector.Connect(context.Background(), attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability}); session != nil || !errors.Is(err, attachedworkersession.ErrRuntimePreflight) {
		t.Fatalf("preflight session=%v err=%v", session, err)
	}
	manifest, err := store.Load(context.Background())
	if err != nil || manifest.ConnectionGeneration != 0 || manifest.Revision != 1 || calls != 1 || peer.challengeCalls != 0 || peer.activateCalls != 0 || peer.exchanges != 0 {
		t.Fatalf("preflight caused authority effects: err=%v gen=%d rev=%d calls=%d/%d/%d/%d", err, manifest.ConnectionGeneration, manifest.Revision, calls, peer.challengeCalls, peer.activateCalls, peer.exchanges)
	}
}

func TestPreparedConnectionBindDeniesGenerationOverflow(t *testing.T) {
	stack, invocation, owner, launcher, session, store, _ := newPreparedStackSessionFixture(t, false, false, nil)
	manifest, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manifest.ConnectionGeneration = math.MaxUint64
	now := invocation.Credential.IssueRequest.Run.CreatedAt
	prepared := codexexec.Config{Enabled: true, Executable: manifest.Harness.Executable, ExecutableDigest: invocation.Process.ExecutableDigest, ExecutableVersion: "fixture-v1", Model: "gpt-fixture", Now: func() time.Time { return now }}
	config := Config{PreparedCodex: &prepared, ScratchRoot: filepath.Join(filepath.Dir(owner.root), "scratch"), Timeout: 20 * time.Second, TerminationGrace: time.Second, MaxStdoutBytes: 4 << 20, MaxStderrBytes: 1 << 20, AllowedEnvironmentNames: []string{"CODEX_HOME"}, AllowedReadRoots: []string{filepath.Dir(owner.root)}, CredentialFinalizeGrace: 7 * time.Second}
	deps := defaultDependencies()
	deps.newLauncher = func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) { return launcher, nil }
	overflow, err := newStack(context.Background(), manifest, config, owner, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = overflow.Close(context.Background()) })
	if err := overflow.BindPreparedConnection(session); err == nil {
		t.Fatal("overflow acquired connection authority")
	}
	if _, err := overflow.Run(context.Background(), invocation); err == nil || len(owner.order) != 0 || launcher.calls != 0 {
		t.Fatal("overflow caused execution")
	}
	if err := stack.BindPreparedConnection(session); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedConnectionBindRejectsForeignAuthenticSession(t *testing.T) {
	for _, field := range []string{"tenant", "owner", "worker", "enrollment"} {
		t.Run(field, func(t *testing.T) {
			stack, invocation, owner, launcher, original, _, _ := newPreparedStackSessionFixture(t, false, false, nil)
			fixture := newFixture(t)
			switch field {
			case "tenant":
				fixture.manifest.TenantID = "foreign-tenant"
			case "owner":
				fixture.manifest.OwnerUserID = "foreign-owner"
			case "worker":
				fixture.manifest.WorkerID = "foreign-worker"
			case "enrollment":
				fixture.manifest.EnrollmentGeneration++
			}
			foreign, _, _ := connectPreparedFixture(t, fixture.manifest, invocation.Credential.IssueRequest.Run.CreatedAt, nil)
			if err := stack.BindPreparedConnection(foreign); err == nil {
				t.Fatal("foreign authentic Session bound")
			}
			if _, err := stack.Run(context.Background(), invocation); err == nil || len(owner.order) != 0 || launcher.calls != 0 {
				t.Fatal("foreign bind caused execution")
			}
			if err := stack.BindPreparedConnection(original); err != nil {
				t.Fatalf("failed bind consumed original binding slot: %v", err)
			}
		})
	}
}

func TestPreparedConnectionRunRejectsClosedAndChangedHead(t *testing.T) {
	for _, field := range []string{"closed", "unauthorized", "connection", "capability", "generation"} {
		t.Run(field, func(t *testing.T) {
			stack, invocation, owner, launcher, session, _, peer := newPreparedStackSessionFixture(t, false, true, nil)
			switch field {
			case "unauthorized":
				peer.exchangeErr = attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorUnauthorized, false)
				_, err := session.ExchangeAction(context.Background(), attachedworkersession.ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: peer.now.UnixMicro(), Available: true}})
				if err == nil || session.Snapshot().State != attachedworkersession.StateFenced {
					t.Fatalf("authentication loss did not fence actual Session: %v", err)
				}
			case "closed":
				if err := session.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "connection":
				invocation.VerifiedInput.AcceptedAttempt.ConnectionID = "foreign-connection"
			case "capability":
				input := invocation.VerifiedInput
				input.Job.ExecutionPlacementV2.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%064x", 7))
				placement, err := domain.ExecutionPlacementDigest(input.Job.ExecutionPlacementV2)
				if err != nil {
					t.Fatal(err)
				}
				input.Job.HarnessBinding.ExecutionPlacementDigest = string(placement)
				input.AcceptedAttempt.CapabilityDigest = input.Job.ExecutionPlacementV2.CapabilityDigest
				input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
				if err != nil {
					t.Fatal(err)
				}
			case "generation":
				invocation.VerifiedInput.AcceptedAttempt.ConnectionGeneration++
			}
			if err := invocation.VerifiedInput.ValidateFor(invocation.Identity); err != nil {
				t.Fatalf("negative head fixture not otherwise canonical: %v", err)
			}
			if _, err := stack.Run(context.Background(), invocation); err == nil || len(owner.order) != 0 || launcher.calls != 0 {
				t.Fatalf("changed %s caused execution: %v", field, err)
			}
		})
	}
}

func TestPreparedConnectionBindsActualIdleReconnectIncrement(t *testing.T) {
	oldStack, invocation, owner, _, initial, store, _ := newPreparedStackSessionFixture(t, false, true, nil)
	if oldStack.scope.connection != 0 || initial.Snapshot().ConnectionGeneration != 1 {
		t.Fatal("initial 0→1 authority was not genuine")
	}
	if err := oldStack.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := lease.LoadReconnectCheckpoint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	secret, err := lease.LoadSecret(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	public := ed25519.PrivateKey(secret.IdentityPrivateKey).Public().(ed25519.PublicKey)
	clear(secret.IdentityPrivateKey)
	clear(secret.ConnectionSecret)
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now := invocation.Credential.IssueRequest.Run.CreatedAt
	peer := &preparedSessionPeer{now: now, public: public, previous: &checkpoint}
	capability := preparedCapability(manifest)
	prepared := codexexec.Config{Enabled: true, Executable: manifest.Harness.Executable, ExecutableDigest: invocation.Process.ExecutableDigest, ExecutableVersion: "fixture-v1", Model: "gpt-fixture", Now: func() time.Time { return now }}
	config := Config{PreparedCodex: &prepared, ScratchRoot: filepath.Join(filepath.Dir(owner.root), "scratch"), Timeout: 20 * time.Second, TerminationGrace: time.Second, MaxStdoutBytes: 4 << 20, MaxStderrBytes: 1 << 20, AllowedEnvironmentNames: []string{"CODEX_HOME"}, AllowedReadRoots: []string{filepath.Dir(owner.root)}, CredentialFinalizeGrace: 7 * time.Second}
	launcher := &preparedStackLauncher{}
	deps := defaultDependencies()
	deps.newLauncher = func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) { return launcher, nil }
	var stack *Stack
	connector, err := attachedworkersession.New(store, peer, peer, attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: capability.ProtocolOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second,
		Now: func() time.Time { return now }, Random: bytes.NewReader(bytes.Repeat([]byte{0x62}, 64)), RuntimePreflight: func(ctx context.Context, m attachedworkerlocal.ManifestV1) error {
			if m.ConnectionGeneration != 1 {
				return errors.New("unexpected original reconnect generation")
			}
			var err error
			stack, err = newStack(ctx, m, config, owner, deps)
			return err
		}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := connector.Reconnect(context.Background(), attachedworkersession.ReconnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()); _ = stack.Close(context.Background()) })
	recovery, err := session.ReconnectRecovery()
	if err != nil || recovery.ConnectionState != attachedworkerprotocol.ConnectionReady || recovery.AttemptState != attachedworkerprotocol.AttemptIdle || recovery.TerminalDecision != attachedworkerprotocol.ReconnectTerminalNone || recovery.Terminal != nil {
		t.Fatalf("not genuinely idle: %v", err)
	}
	if stack.scope.connection != 1 || session.Snapshot().ConnectionGeneration != 2 {
		t.Fatal("reconnect did not preserve 1→2")
	}
	if err := stack.BindPreparedConnection(session); err != nil {
		t.Fatal(err)
	}
	invocation.VerifiedInput.AcceptedAttempt.ConnectionID = session.Snapshot().ConnectionID
	invocation.VerifiedInput.AcceptedAttempt.ConnectionGeneration = session.Snapshot().ConnectionGeneration
	result, err := stack.Run(context.Background(), invocation)
	if !result.Succeeded(err) || launcher.calls != 1 || len(owner.order) != 4 {
		t.Fatalf("genuine reconnect send result=%+v err=%v effects=%v spawns=%d", result, err, owner.order, launcher.calls)
	}
	var _ attachedworkerdaemon.Runner = stack
}
