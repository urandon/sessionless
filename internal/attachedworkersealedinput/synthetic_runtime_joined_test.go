package attachedworkersealedinput

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var joinedTestTime = time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)

// Joined protocol tests exercise real local-store and pinned-process setup, not
// a connection latency SLO. Match the ordinary session operation budget so a
// one-second setup race cannot decide the ownership/protocol assertion. Keep
// completion barriers and their bounded waits separate from this failure bound.
const joinedProtocolOperationTimeout = 15 * time.Second

// This exercises the exported, default-off composition. The pinned shell
// executable answers only read-only OCI preflight commands; any container
// creation or start is a test failure, not an implicit Docker dependency.
func TestSyntheticPinnedRuntimeCommitsMaterializationDenialWithoutProcess(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	image := "registry.example/worker@sha256:" + strings.Repeat("b", 64)
	commandLog := filepath.Join(root, "oci-commands.log")
	cli := filepath.Join(root, "pinned-oci")
	cliBytes := []byte(fmt.Sprintf(`#!/bin/sh
shift 4
printf '%%s\n' "$*" >> '%s'
case "$1:$2" in
  version:*) printf '%%s\n' '{"Client":{"ApiVersion":"1.45"},"Server":{"ApiVersion":"1.45","Os":"linux"}}';;
  info:*) printf '%%s\n' '{"ID":"engine-001-abcdef","OSType":"linux","CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"SwapLimit":true,"SecurityOptions":["name=seccomp,profile=builtin","name=rootless"]}';;
  image:inspect) printf '%%s\n' '{"Os":"linux","RepoDigests":["%s"],"Config":{}}';;
  container:ls) exit 0;;
  *) exit 97;;
esac
`, rootPathInShell(t, commandLog), image))
	if err := os.WriteFile(cli, cliBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	cliDigest := sha256.Sum256(cliBytes)
	cliConfig := filepath.Join(root, "oci-config")
	materializationRoot := filepath.Join(root, "materialized")
	scratchRoot := filepath.Join(root, "scratch")
	for _, dir := range []string{cliConfig, materializationRoot, scratchRoot} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-joined", OwnerUserID: "user-joined", WorkerID: "worker-joined", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{
			DockerPath: cli, DockerSHA256: hex.EncodeToString(cliDigest[:]), CLIConfigDir: cliConfig,
			Host: "unix:///run/docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-joined",
			Boundary: attachedworkerlocal.BoundaryLinuxRootless, Image: image,
			UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024,
			MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10,
		},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: cli, SHA256: hex.EncodeToString(cliDigest[:]), Arguments: []string{}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: joinedTestTime, UpdatedAt: joinedTestTime,
	}
	if runtime.GOOS == "darwin" {
		manifest.OCI.Boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	secret := attachedworkerlocal.SecretRecordV1{
		Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: append([]byte(nil), private...),
	}
	clock := &joinedClock{value: joinedTestTime}
	store, err := attachedworkerlocal.NewStore(filepath.Join(root, "state"), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	offer := attachedworkerprotocol.VersionOfferV1{
		Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{1},
	}
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "build-joined",
		HarnessName: "codex", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: append([]byte(nil), cliDigest[:]...),
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{
			attachedworkerprotocol.IsolationFilesystemBoundary,
			attachedworkerprotocol.IsolationNetworkBoundary,
			attachedworkerprotocol.IsolationProcessBoundary,
		},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation,
			attachedworkerprotocol.FeatureProgress,
			attachedworkerprotocol.FeatureReconnect,
		},
		MaxConcurrentAttempts: 1,
	}
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	binding := attachedworkerprotocol.AttemptBindingV1{
		RunID: "run-joined", AttemptID: "attempt-joined", LeaseID: "lease-joined",
		LeaseGeneration: 7, FenceToken: "fence-joined",
		ExpiresAtUnixMicro: joinedTestTime.Add(30 * time.Minute).UnixMicro(),
		ContextDigest:      bytes.Repeat([]byte{0x61}, sha256.Size),
		CapabilityDigest:   append([]byte(nil), capabilityDigest...),
		PolicyDigest:       bytes.Repeat([]byte{0x62}, sha256.Size),
	}
	if err := binding.Validate(); err != nil {
		t.Fatalf("joined binding invalid: %v", err)
	}
	exchange := &joinedExchange{binding: binding, terminalSeen: make(chan attachedworkerprotocol.TerminalV1, 1)}
	factory := &joinedFactory{exchange: exchange}
	authorizer := &joinedDenyAuthorizer{factory: factory, want: ports.AttachedWorkerSealedInputAuthorization{
		TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID,
		ConnectionID: "connection-joined", EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: 1,
		RunID: domain.RunID(binding.RunID), AttemptID: domain.AttemptID(binding.AttemptID), AttemptSequence: 1,
		LeaseID: domain.LeaseID(binding.LeaseID), LeaseGeneration: binding.LeaseGeneration,
		FenceToken: domain.AttachedWorkerFenceToken(binding.FenceToken), LeaseExpiresAtUnixMicro: binding.ExpiresAtUnixMicro,
		ContextDigest:    domain.AttachedWorkerContextDigest(hex.EncodeToString(binding.ContextDigest)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(binding.CapabilityDigest)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(hex.EncodeToString(binding.PolicyDigest)),
	}}
	service, err := NewService(authorizer, joinedNoJobStore{}, joinedNoBlobStore{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(Handler(service))
	t.Cleanup(server.Close)
	config := SyntheticRuntimeConfig{
		Store: store, Bootstrap: &joinedBootstrap{now: joinedTestTime, publicKey: public, offer: offer}, Exchange: factory,
		Session: attachedworkersession.Config{
			Audience: "sessionless:attached-worker:v1", WorkerOffer: offer,
			ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: joinedProtocolOperationTimeout,
			Random: bytes.NewReader(append(bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 32)...)), Now: clock.Now,
		},
		Connect:        attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability},
		SealedEndpoint: server.URL + PathV1, SealedClient: server.Client(), MaxInputBytes: 4096, Now: clock.Now,
		Adapter: attachedworkerdaemontransport.Config{
			Profile: attachedworkerdaemontransport.LocalProfileV1{
				Name: "codex-joined", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest)),
				Executable: cli, ExecutableDigest: attachedworkerdaemon.ExecutableDigest(cliDigest),
			},
			MaterializationRoot: materializationRoot, MaxInputBytes: 4096, Now: clock.Now,
		},
		Poll: attachedworkertransport.Config{
			Enabled: true, PollInterval: attachedworkertransport.MinimumHeartbeatInterval,
			InitialBackoff: time.Second, MaxBackoff: time.Minute, Random: bytes.NewReader(bytes.Repeat([]byte{0x43}, 8)), Now: clock.Now,
		},
		Stack: attachedworkerstack.Config{ScratchRoot: scratchRoot},
	}
	owner, err := ConnectSyntheticPinnedRuntime(context.Background(), config)
	if err != nil {
		t.Fatalf("connect joined pinned runtime: %v", err)
	}
	clock.Set(joinedTestTime.Add(16 * time.Minute))
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	done := make(chan error, 1)
	go func() { done <- owner.Run(runCtx) }()
	select {
	case terminal := <-exchange.terminalSeen:
		if terminal.Status != attachedworkerprotocol.TerminalFailed || terminal.Result != attachedworkerprotocol.TerminalResultFailed || terminal.AttemptSequence != 2 {
			t.Errorf("denied input terminal=%+v", terminal)
		}
	case err := <-done:
		local, localErr := store.LoadSnapshot(context.Background())
		t.Fatalf("runtime stopped before terminal: %v; exchange steps=%d; platform validation=%v; status=%+v; local error=%v; observation=%+v", err, exchange.steps, exchange.lastPlatformErr, owner.Status(), localErr, local.Observation)
	case <-time.After(5 * time.Second):
		t.Fatal("accepted attempt did not reach terminal")
	}
	select {
	case err := <-done:
		if !errors.Is(err, attachedworkerdaemontransport.ErrMaterializationFailed) {
			t.Fatalf("runtime result=%v, want materialization failure", err)
		}
	case <-time.After(5 * time.Second):
		cancelRun()
		t.Fatal("runtime did not stop after denied materialization")
	}
	if exchange.steps != 3 {
		t.Errorf("protocol exchange steps=%d, want 3", exchange.steps)
	}
	authorizer.mu.Lock()
	authorizationCalls, badScope := authorizer.calls, authorizer.badScope
	authorizer.mu.Unlock()
	if authorizationCalls != 1 || badScope {
		t.Errorf("sealed-input authorization calls=%d bad scope=%t, want one exact denied request", authorizationCalls, badScope)
	}
	commands, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	wantCommands := []string{
		"version --format {{json .}}",
		"info --format {{json .}}",
		"image inspect --format {{json .}} " + image,
		"container ls --all --quiet --filter label=dev.sessionless.attached-worker.profile=sessionless.oci.docker.v1 " +
			"--filter label=dev.sessionless.attached-worker.installation=install-joined " +
			"--filter label=dev.sessionless.attached-worker.engine=engine-001-abcdef",
	}
	gotCommands := strings.Split(strings.TrimSpace(string(commands)), "\n")
	if len(gotCommands) != len(wantCommands) {
		t.Errorf("pinned OCI command count=%d, want %d; commands=%q", len(gotCommands), len(wantCommands), gotCommands)
	} else {
		for index, command := range wantCommands {
			if gotCommands[index] != command {
				t.Errorf("pinned OCI command %d=%q, want %q", index, gotCommands[index], command)
			}
		}
	}
	if got := exchange.closed.Load(); got != 1 {
		t.Errorf("owned exchange close count=%d, want 1", got)
	}
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatalf("runtime lease was not released: %v", err)
	}
	defer func() {
		if err := lease.Close(); err != nil {
			t.Errorf("close proof lease: %v", err)
		}
	}()
	checkpoint, err := lease.LoadReconnectCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("terminal checkpoint missing: %v", err)
	}
	if checkpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptTerminalCommitted ||
		checkpoint.MachineSnapshot.Attempt.Summary.TerminalStatus != attachedworkerprotocol.TerminalFailed ||
		checkpoint.MachineSnapshot.Attempt.Summary.TerminalResult != attachedworkerprotocol.TerminalResultFailed {
		t.Errorf("persisted terminal checkpoint=%+v", checkpoint.MachineSnapshot.Attempt.Summary)
	}
	local, err := lease.LoadSnapshot(context.Background())
	if err != nil || local.ObservationPresent {
		t.Errorf("runtime observation not retired: present=%t error=%v", local.ObservationPresent, err)
	}
}

func rootPathInShell(t *testing.T, path string) string {
	t.Helper()
	if strings.Contains(path, "'") {
		t.Fatalf("test path cannot be quoted by pinned shell stub: %q", path)
	}
	return path
}

type joinedClock struct {
	mu    sync.Mutex
	value time.Time
}

func (clock *joinedClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.value
}

func (clock *joinedClock) Set(value time.Time) {
	clock.mu.Lock()
	clock.value = value
	clock.mu.Unlock()
}

type joinedBootstrap struct {
	now       time.Time
	publicKey ed25519.PublicKey
	offer     attachedworkerprotocol.VersionOfferV1
	challenge domain.AttachedWorkerAttachChallenge
}

func (fake *joinedBootstrap) IssueChallenge(_ context.Context, input attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(
		input.TenantLocator, input.OwnerLocator, domain.AttachedWorkerID(input.Hello.WorkerID),
		input.Hello.EnrollmentGeneration, input.Hello.ConnectionGeneration-1,
		attachedworkertransport.IssueChallengeRequest{
			WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ExpectedAudience: input.ExpectedAudience,
			ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: input.Purpose, Hello: input.Hello,
		},
	)
	if err != nil || !ed25519.Verify(fake.publicKey, transcript, input.Proof) {
		return nil, &attachedworkerhttp.ExchangeError{Kind: attachedworkerhttp.ErrorUnauthorized}
	}
	platformNonce := bytes.Repeat([]byte{0x52}, 32)
	fake.challenge = domain.AttachedWorkerAttachChallenge{
		TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, ID: "challenge-joined",
		WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ConnectionID: "connection-joined",
		Purpose: input.Purpose, Audience: input.ExpectedAudience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
		ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1,
		TargetConnectionGeneration:   input.Hello.ConnectionGeneration,
		WorkerProtocolMinimum:        1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1},
		SelectedProtocolVersion: 1,
		WorkerNonceDigest:       domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce),
		PlatformNonceDigest:     domain.DigestAttachedWorkerChallenge(platformNonce),
		CreatedAt:               fake.now, ExpiresAt: fake.now.Add(time.Minute), RetainUntil: fake.now.Add(time.Hour), Revision: 1,
	}
	return &attachedworkerhttp.ChallengeResponseV1{Challenge: fake.challenge, Frame: attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
		WorkerID: input.Hello.WorkerID, EnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ConnectionGeneration: input.Hello.ConnectionGeneration, Sequence: 1, Ack: 1,
		Kind: attachedworkerprotocol.MessageChallenge, Challenge: &attachedworkerprotocol.ChallengeV1{
			WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: fake.offer, SelectedVersion: 1,
			WorkerNonce: append([]byte(nil), input.Hello.Hello.WorkerNonce...), PlatformNonce: platformNonce,
		},
	}}, nil
}

func (fake *joinedBootstrap) Activate(_ context.Context, input attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	channel := attachedworkertransport.ConnectionChannelBinding(
		fake.challenge.ID, fake.challenge.WorkerNonceDigest, fake.challenge.PlatformNonceDigest, input.ConnectionSecret.Digest(),
	)
	channelBytes, err := hex.DecodeString(string(channel))
	if err != nil {
		return nil, err
	}
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(fake.challenge.TenantID), OwnerUserID: string(fake.challenge.OwnerUserID), WorkerID: string(fake.challenge.WorkerID),
		IdentityPublicKey: fake.publicKey, EnrollmentGeneration: fake.challenge.ExpectedEnrollmentGeneration,
		ConnectionGeneration: fake.challenge.TargetConnectionGeneration, Version: input.Attach.Version, ChannelBinding: channelBytes,
	}
	if attachedworkerprotocol.VerifyAttachV1(auth, input.Attach) != nil {
		return nil, &attachedworkerhttp.ExchangeError{Kind: attachedworkerhttp.ErrorUnauthorized}
	}
	accepted := attachedworkerprotocol.FrameV1{
		Version: input.Attach.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2),
		WorkerID: input.Attach.WorkerID, EnrollmentGeneration: input.Attach.EnrollmentGeneration,
		ConnectionGeneration: input.Attach.ConnectionGeneration, Sequence: 2, Ack: 2,
		Kind: attachedworkerprotocol.MessageAttachAccepted, AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{
			WorkerOffer: input.Attach.Attach.WorkerOffer, PlatformOffer: input.Attach.Attach.PlatformOffer,
			SelectedVersion: input.Attach.Version, WorkerNonce: append([]byte(nil), input.Attach.Attach.WorkerNonce...),
			PlatformNonce:    append([]byte(nil), input.Attach.Attach.PlatformNonce...),
			CapabilityDigest: append([]byte(nil), input.Attach.Attach.CapabilityDigest...),
		},
	}
	return &attachedworkerhttp.ActivateResponseV1{
		Connection: attachedworkerhttp.ActivateConnectionV1{
			TenantID: fake.challenge.TenantID, OwnerUserID: fake.challenge.OwnerUserID, WorkerID: fake.challenge.WorkerID,
			ID: fake.challenge.ConnectionID, ActivationChallengeID: fake.challenge.ID,
			EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration,
			ProtocolVersion:  uint32(input.Attach.Version),
			CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(input.Attach.Attach.CapabilityDigest)),
			SecretDigest:     input.ConnectionSecret.Digest(), ChannelBinding: channel,
			State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1,
			ConnectedAt: fake.now.Add(time.Minute), AuthExpiresAt: fake.now.Add(time.Hour), Revision: 1,
		},
		Accepted: accepted,
	}, nil
}

type joinedFactory struct {
	mu       sync.Mutex
	bearer   []byte
	exchange *joinedExchange
}

func (factory *joinedFactory) Open(_ attachedworkersession.ConnectionBindingV1, rawBearer []byte) (attachedworkersession.ExchangePort, error) {
	if _, err := attachedworkertransport.ParseConnectionBearer(rawBearer); err != nil {
		return nil, err
	}
	factory.mu.Lock()
	factory.bearer = append([]byte(nil), rawBearer...)
	factory.mu.Unlock()
	return factory.exchange, nil
}

type joinedDenyAuthorizer struct {
	factory  *joinedFactory
	want     ports.AttachedWorkerSealedInputAuthorization
	mu       sync.Mutex
	calls    int
	badScope bool
}

func (authorizer *joinedDenyAuthorizer) AuthorizeSealedInputBearer(_ context.Context, bearer []byte, request ports.AttachedWorkerSealedInputAuthorization) (uint64, error) {
	authorizer.factory.mu.Lock()
	validBearer := bytes.Equal(bearer, authorizer.factory.bearer)
	authorizer.factory.mu.Unlock()
	badScope := !validBearer || request != authorizer.want
	authorizer.mu.Lock()
	authorizer.calls++
	authorizer.badScope = authorizer.badScope || badScope
	authorizer.mu.Unlock()
	if badScope {
		return 0, errors.New("joined proof received wrong sealed-input scope")
	}
	return 0, ErrUnauthorized
}

type joinedNoJobStore struct{}

func (joinedNoJobStore) LoadWorkerJob(context.Context, domain.TenantID, domain.RunID) (ports.WorkerJobState, bool, error) {
	panic("sealed-input denial unexpectedly read a job")
}

type joinedNoBlobStore struct{}

func (joinedNoBlobStore) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	panic("sealed-input denial unexpectedly read a blob")
}

type joinedExchange struct {
	binding         attachedworkerprotocol.AttemptBindingV1
	terminalSeen    chan attachedworkerprotocol.TerminalV1
	idleHeartbeat   chan struct{}
	drainOnFirst    bool
	ambiguousClaim  bool
	steps           int
	lastWorkerKind  attachedworkerprotocol.MessageKind
	lastPlatformErr error
	closed          atomic.Int32
}

func (exchange *joinedExchange) Exchange(_ context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if len(batch.Frames) != 1 {
		return nil, fmt.Errorf("joined exchange got %d frames", len(batch.Frames))
	}
	worker := batch.Frames[0]
	exchange.lastWorkerKind = worker.Kind
	if worker.Kind == attachedworkerprotocol.MessageManifest {
		return nil, nil
	}
	if exchange.idleHeartbeat != nil {
		if worker.Kind != attachedworkerprotocol.MessageHeartbeat {
			return nil, fmt.Errorf("reconciled idle exchange got %s, want heartbeat", worker.Kind)
		}
		select {
		case exchange.idleHeartbeat <- struct{}{}:
		default:
		}
		return nil, nil
	}
	// The active-control watcher may send unavailable/one-active heartbeats
	// between acceptance and terminal. A nil response carries no new platform
	// sequence and must not create a second lease offer.
	if exchange.steps == 2 && worker.Kind == attachedworkerprotocol.MessageHeartbeat {
		return nil, nil
	}
	step := exchange.steps
	exchange.steps++
	platform := attachedworkerprotocol.FrameV1{
		Version:   worker.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, uint64(3+step)),
		WorkerID:  worker.WorkerID, EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: worker.ConnectionGeneration, Sequence: uint64(3 + step), Ack: worker.Sequence,
	}
	switch step {
	case 0:
		if worker.Kind != attachedworkerprotocol.MessageHeartbeat || worker.Sequence != 4 || worker.Ack != 2 {
			return nil, fmt.Errorf("joined heartbeat envelope kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
		}
		if exchange.drainOnFirst {
			platform.Kind = attachedworkerprotocol.MessageDrain
			platform.Drain = &attachedworkerprotocol.DrainV1{Revision: 1}
		} else {
			platform.Kind = attachedworkerprotocol.MessageLeaseOffer
			platform.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: exchange.binding, AttemptSequence: 1}
		}
	case 1:
		if worker.Kind != attachedworkerprotocol.MessageLeaseClaim || worker.LeaseClaim == nil || worker.Sequence != 5 || worker.Ack != 3 {
			return nil, fmt.Errorf("joined claim envelope kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
		}
		if exchange.ambiguousClaim {
			return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorUnavailable, true)
		}
		platform.Kind = attachedworkerprotocol.MessageLeaseAccepted
		platform.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: exchange.binding, AttemptSequence: 2}
	case 2:
		if worker.Kind != attachedworkerprotocol.MessageTerminal || worker.Terminal == nil || worker.Sequence < 6 || worker.Ack != 4 {
			return nil, fmt.Errorf("joined terminal envelope kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
		}
		platform.Kind = attachedworkerprotocol.MessageTerminalAck
		platform.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
			Binding: exchange.binding, AttemptSequence: 3,
			TerminalSequence: worker.Terminal.TerminalSequence, Status: worker.Terminal.Status,
			Result: worker.Terminal.Result, EvidenceDigest: append([]byte(nil), worker.Terminal.EvidenceDigest...),
		}
		exchange.terminalSeen <- *worker.Terminal
	default:
		return nil, fmt.Errorf("unexpected joined exchange step %d", step)
	}
	exchange.lastPlatformErr = platform.Validate()
	return &attachedworkerprotocol.BatchV1{Version: worker.Version, Frames: []attachedworkerprotocol.FrameV1{platform}}, nil
}

func (exchange *joinedExchange) Close() error {
	exchange.closed.Add(1)
	return nil
}
