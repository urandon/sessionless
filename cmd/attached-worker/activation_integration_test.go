package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeractivation"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This drives the shipped run CLI dispatch through an actual TLS bootstrap,
// exchange, and denied sealed-input endpoint. Only the clock is injected at
// the connector boundary. OCI preflight is a pinned stub: any container start
// or provider invocation is a test failure.
func TestActivatedRunAcceptsSyntheticAttemptAndDeniesInput(t *testing.T) {
	testActivatedSyntheticCommand(t, false, false, false, false)
}

func TestActivatedServeDrainsAcceptedSyntheticAttempt(t *testing.T) {
	testActivatedSyntheticCommand(t, true, false, false, false)
}

func TestActivatedServeStopsIdle(t *testing.T) {
	testActivatedSyntheticCommand(t, true, true, false, false)
}

func TestActivatedServeReconnectsAfterIdleStop(t *testing.T) {
	testActivatedSyntheticCommand(t, true, true, true, false)
}

func TestActivatedServeCancelsAcceptedSyntheticAttempt(t *testing.T) {
	testActivatedSyntheticCommand(t, true, false, false, true)
}

func testActivatedSyntheticCommand(t *testing.T, service, idle, restart, cancelActive bool) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("pinned OCI fixture is Unix-only")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var serviceStarted atomic.Bool
	serviceExited := make(chan struct{})
	var restartStarted atomic.Bool
	restartExited := make(chan struct{})
	if service {
		// Unix-domain socket paths are bounded (especially on Darwin), while
		// Go's nested test directory can exceed sockaddr_un before Serve starts.
		shortTemp, err := filepath.EvalSymlinks("/tmp")
		if err != nil {
			t.Fatal(err)
		}
		root, err = os.MkdirTemp(shortTemp, "aw165-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if serviceStarted.Load() {
				select {
				case <-serviceExited:
				default:
					t.Errorf("preserving test root while service may still own it: %s", root)
					return
				}
			}
			if restartStarted.Load() {
				select {
				case <-restartExited:
				default:
					t.Errorf("preserving test root while restarted service may still own it: %s", root)
					return
				}
			}
			if err := os.RemoveAll(root); err != nil {
				t.Errorf("remove finished test root: %v", err)
			}
		})
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	testNow := time.Now().UTC()
	image := "registry.example/worker@sha256:" + strings.Repeat("b", 64)
	commandLog := filepath.Join(root, "oci-commands.log")
	cli := filepath.Join(root, "pinned-oci")
	quotedLog := "'" + strings.ReplaceAll(commandLog, "'", "'\"'\"'") + "'"
	cliBytes := []byte(fmt.Sprintf(`#!/bin/sh
shift 4
printf '%%s\n' "$*" >> %s
case "$1:$2" in
  version:*) printf '%%s\n' '{"Client":{"ApiVersion":"1.45"},"Server":{"ApiVersion":"1.45","Os":"linux"}}';;
  info:*) printf '%%s\n' '{"ID":"engine-001-abcdef","OSType":"linux","CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"SwapLimit":true,"SecurityOptions":["name=seccomp,profile=builtin","name=rootless"]}';;
  image:inspect) printf '%%s\n' '{"Os":"linux","RepoDigests":["%s"],"Config":{}}';;
  container:ls) exit 0;;
  *) exit 97;;
esac
`, quotedLog, image))
	if err := os.WriteFile(cli, cliBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	cliDigest := sha256.Sum256(cliBytes)
	cliConfig := filepath.Join(root, "oci-config")
	materializationRoot := filepath.Join(root, "materialized")
	scratchRoot := filepath.Join(root, "scratch")
	activationDir := filepath.Join(root, "activation")
	for _, dir := range []string{cliConfig, materializationRoot, scratchRoot, activationDir} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1}}
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: "worker-command", EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "command-test",
		HarnessName: "synthetic", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: append([]byte(nil), cliDigest[:]...),
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{
			attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary,
			attachedworkerprotocol.IsolationProcessBoundary,
		},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress,
			attachedworkerprotocol.FeatureReconnect,
		}, MaxConcurrentAttempts: 1,
	}
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	binding := attachedworkerprotocol.AttemptBindingV1{
		RunID: "run-command", AttemptID: "attempt-command", LeaseID: "lease-command",
		LeaseGeneration: 7, FenceToken: "fence-command", ExpiresAtUnixMicro: testNow.Add(30 * time.Minute).UnixMicro(),
		ContextDigest: bytes.Repeat([]byte{0x61}, sha256.Size), CapabilityDigest: append([]byte(nil), capabilityDigest...),
		PolicyDigest: bytes.Repeat([]byte{0x62}, sha256.Size),
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	peer := &commandSyntheticPeer{public: public, offer: offer, binding: binding, now: testNow, idle: idle,
		terminal: make(chan attachedworkerprotocol.TerminalV1, 1)}
	if service {
		peer.sealedGate = make(chan struct{})
		peer.sealedStarted = make(chan struct{}, 1)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(peer.serveHTTP))
	t.Cleanup(server.Close)
	certificate := server.Certificate()
	trust := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	if _, err := x509.ParseCertificate(certificate.Raw); err != nil {
		t.Fatal(err)
	}
	trustPath := filepath.Join(activationDir, "trust.pem")
	if err := os.WriteFile(trustPath, trust, 0o600); err != nil {
		t.Fatal(err)
	}
	boundary := attachedworkerlocal.BoundaryLinuxRootless
	if runtime.GOOS == "darwin" {
		boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	now := testNow.Add(-time.Minute)
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: server.URL,
		TenantID: "tenant-command", OwnerUserID: "owner-command", WorkerID: "worker-command", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{
			DockerPath: cli, DockerSHA256: hex.EncodeToString(cliDigest[:]), CLIConfigDir: cliConfig,
			Host: "unix:///run/docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-command",
			Boundary: boundary, Image: image, UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30,
			CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10,
		},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: cli, SHA256: hex.EncodeToString(cliDigest[:]), Arguments: []string{}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now, UpdatedAt: now,
	}
	secret := attachedworkerlocal.SecretRecordV1{
		Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: append([]byte(nil), private...),
	}
	stateRoot := filepath.Join(root, "state")
	store, err := attachedworkerlocal.NewStore(stateRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	installationDigest, err := attachedworkeractivation.InstallationDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	profile := attachedworkeractivation.ProfileV1{
		Version: 1, Mode: "synthetic-denied", ManifestRevision: 1,
		ControlPlaneOrigin: server.URL, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, InstallationSHA256: installationDigest,
		ExpectedWorkerRevision: 1, Capability: capability,
		LocalProfile: attachedworkerdaemontransport.LocalProfileV1{
			Name: "synthetic", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest)),
			Executable: cli, ExecutableDigest: attachedworkerdaemon.ExecutableDigest(cliDigest),
		},
		TLSRootPEMPath: trustPath, MaterializationRoot: materializationRoot, ScratchRoot: scratchRoot,
		MaxInputBytes: 4096,
	}
	profilePath := filepath.Join(activationDir, "profile.json")
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profilePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	done := make(chan int, 1)
	var output bytes.Buffer
	clockStart := testNow
	var clockNanos atomic.Int64
	connected := make(chan *attachedworkersealedinput.SyntheticRuntime, 1)
	clockNanos.Store(clockStart.UnixNano())
	testClock := func() time.Time { return time.Unix(0, clockNanos.Load()).UTC() }
	connector := func(ctx context.Context, store *attachedworkerlocal.Store, profile attachedworkeractivation.ProfileV1) (*attachedworkersealedinput.SyntheticRuntime, error) {
		owner, err := attachedworkeractivation.ConnectWithClock(ctx, store, profile, testClock)
		if err == nil {
			clockNanos.Store(clockStart.Add(16 * time.Minute).UnixNano())
			connected <- owner
		}
		return owner, err
	}
	if service {
		testActivatedServiceControl(t, ctx, peer, store, stateRoot, profilePath, connector,
			connected, testClock, &serviceStarted, serviceExited, &restartStarted, restartExited, idle, restart, cancelActive)
		return
	}
	go func() {
		done <- runWithContextAndConnector(ctx, []string{"run", "--state-dir", stateRoot,
			"--activation-profile", profilePath}, &output, connector)
	}()
	select {
	case terminal := <-peer.terminal:
		if terminal.Status != attachedworkerprotocol.TerminalFailed || terminal.Result != attachedworkerprotocol.TerminalResultFailed {
			t.Errorf("denied synthetic terminal=%+v", terminal)
		}
	case code := <-done:
		commands, _ := os.ReadFile(commandLog)
		snapshot, snapshotErr := store.LoadSnapshot(context.Background())
		peer.mu.Lock()
		steps, challenges, activations, exchanges, lastError, lastKind := peer.steps, peer.challenges, peer.activations, peer.exchanges, peer.lastError, peer.lastKind
		peer.mu.Unlock()
		t.Fatalf("command exited before accepted synthetic terminal: code=%d output=%s commands=%q revision=%d snapshot_error=%v exchange_steps=%d challenges=%d activations=%d exchanges=%d last_kind=%s peer_error=%q", code, output.String(), commands, snapshot.Manifest.Revision, snapshotErr, steps, challenges, activations, exchanges, lastKind, lastError)
	case <-ctx.Done():
		var code int
		select {
		case code = <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("activated run ignored cancellation after test deadline")
		}
		peer.mu.Lock()
		steps, challenges, activations, exchanges, lastKind, lastError := peer.steps, peer.challenges, peer.activations, peer.exchanges, peer.lastKind, peer.lastError
		peer.mu.Unlock()
		t.Fatalf("accepted synthetic attempt did not reach terminal: exit=%d output=%s steps=%d challenges=%d activations=%d exchanges=%d last_kind=%s peer_error=%q", code, output.String(), steps, challenges, activations, exchanges, lastKind, lastError)
	}
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("denied input command exit=%d output=%s", code, output.String())
		}
	case <-ctx.Done():
		t.Fatal("command did not stop after denied synthetic input")
	}
	peer.mu.Lock()
	steps, denied := peer.steps, peer.denied
	peer.mu.Unlock()
	if steps != 3 || denied != 1 {
		t.Errorf("protocol steps=%d denied sealed requests=%d, want 3 and 1", steps, denied)
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
			"--filter label=dev.sessionless.attached-worker.installation=install-command " +
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
	if strings.Contains(output.String(), server.URL) || strings.Contains(output.String(), stateRoot) ||
		strings.Contains(strings.ToLower(output.String()), hex.EncodeToString(private)) ||
		strings.Contains(output.String(), "connection_secret") {
		t.Errorf("synthetic command leaked private or endpoint material: %s", output.String())
	}
	if got, err := store.LoadSnapshot(context.Background()); err != nil || got.Manifest.Revision != 2 || got.ObservationPresent {
		t.Fatalf("owned command did not retire runtime observation: snapshot=%+v error=%v", got, err)
	}
}

func testActivatedServiceControl(t *testing.T, ctx context.Context, peer *commandSyntheticPeer,
	store *attachedworkerlocal.Store, stateRoot, profilePath string, connector activationConnector,
	connected <-chan *attachedworkersealedinput.SyntheticRuntime, testClock func() time.Time,
	serviceStarted *atomic.Bool, serviceExited chan struct{}, restartStarted *atomic.Bool,
	restartExited chan struct{}, idle, restart, cancelActive bool) {
	t.Helper()
	profile, pin, err := attachedworkeractivation.ReadProfileWithDigest(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ManifestRevision != 1 {
		t.Fatalf("unexpected installation baseline: %d", profile.ManifestRevision)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	var output bytes.Buffer
	done := make(chan int, 1)
	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		serviceCancel()
		peer.releaseSealed()
		select {
		case <-serviceExited:
		case <-time.After(2 * time.Second):
			t.Errorf("test-owned service did not exit after cancellation; state root preserved")
		}
	})
	serviceStarted.Store(true)
	go func() {
		defer close(serviceExited)
		done <- runWithContextAndConnector(serviceCtx, []string{"serve", "--state-dir", stateRoot,
			"--expected-revision", "1", "--binary", binary, "--binary-sha256", hex.EncodeToString(sum[:]),
			"--activation-profile", profilePath, "--activation-sha256", pin}, &output, connector)
	}()
	var runtimeOwner *attachedworkersealedinput.SyntheticRuntime
	select {
	case runtimeOwner = <-connected:
	case code := <-done:
		t.Fatalf("service exited before connection: code=%d output=%s", code, output.String())
	case <-ctx.Done():
		t.Fatal("service did not connect")
	}
	if idle {
		for {
			var status bytes.Buffer
			if runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &status) == 0 {
				break
			}
			select {
			case code := <-done:
				t.Fatalf("idle service exited before readiness: code=%d output=%s", code, output.String())
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal("idle service did not become ready")
			}
		}
		var stop bytes.Buffer
		if code := runWithContext(ctx, []string{"stop", "--state-dir", stateRoot,
			"--expected-revision", "2"}, &stop); code != 0 {
			t.Fatalf("idle service stop exit=%d output=%s", code, stop.String())
		}
		select {
		case code := <-done:
			if code != 0 {
				t.Fatalf("idle service exit=%d output=%s", code, output.String())
			}
		case <-ctx.Done():
			t.Fatal("idle service ignored stop")
		}
		if snapshot, err := store.LoadSnapshot(context.Background()); err != nil || snapshot.ObservationPresent {
			t.Fatalf("idle service retained runtime observation: snapshot=%+v error=%v", snapshot, err)
		}
		if restart {
			lease, err := store.AcquireRuntime(ctx)
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, checkpointErr := lease.LoadReconnectCheckpoint(ctx)
			closeErr := lease.Close()
			if checkpointErr != nil || closeErr != nil {
				t.Fatalf("load durable reconnect checkpoint: load=%v close=%v", checkpointErr, closeErr)
			}
			peer.mu.Lock()
			peer.now = testClock()
			peer.previousCheckpoint = checkpoint
			peer.previousConfig = attachedworkerprotocol.MachineConfig{
				Auth: attachedworkerprotocol.AuthContextV1{
					TenantID: string(checkpoint.TenantID), OwnerUserID: string(checkpoint.OwnerUserID),
					WorkerID: string(checkpoint.WorkerID), IdentityPublicKey: peer.public,
					EnrollmentGeneration: checkpoint.EnrollmentGeneration,
					ConnectionGeneration: checkpoint.ConnectionGeneration,
					Version:              checkpoint.ProtocolVersion, ChannelBinding: append([]byte(nil), checkpoint.ChannelBinding...),
				},
				WorkerOffer: checkpoint.WorkerOffer, PlatformOffer: checkpoint.PlatformOffer,
				ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
			}
			peer.mu.Unlock()
			restartCtx, restartCancel := context.WithCancel(ctx)
			var restartOutput bytes.Buffer
			restartDone := make(chan int, 1)
			connectionErrors := make(chan error, 1)
			restartConnector := func(ctx context.Context, store *attachedworkerlocal.Store,
				profile attachedworkeractivation.ProfileV1) (*attachedworkersealedinput.SyntheticRuntime, error) {
				owner, err := connector(ctx, store, profile)
				if err != nil {
					connectionErrors <- err
				}
				return owner, err
			}
			t.Cleanup(func() {
				restartCancel()
				select {
				case <-restartExited:
				case <-time.After(2 * time.Second):
					t.Errorf("restarted service did not exit after cancellation; state root preserved")
				}
			})
			restartStarted.Store(true)
			go func() {
				defer close(restartExited)
				restartDone <- runWithContextAndConnector(restartCtx, []string{"serve", "--state-dir", stateRoot,
					"--expected-revision", "1", "--binary", binary, "--binary-sha256", hex.EncodeToString(sum[:]),
					"--activation-profile", profilePath, "--activation-sha256", pin}, &restartOutput, restartConnector)
			}()
			for {
				var status bytes.Buffer
				if runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &status) == 0 &&
					strings.Contains(status.String(), `"manifest_revision":3`) {
					break
				}
				select {
				case code := <-restartDone:
					var connectionErr error
					select {
					case connectionErr = <-connectionErrors:
					default:
					}
					peer.mu.Lock()
					lastError, lastKind, exchanges := peer.lastError, peer.lastKind, peer.exchanges
					peer.mu.Unlock()
					t.Fatalf("restarted service exited before readiness: code=%d output=%s connection=%v peer_error=%q peer_kind=%s exchanges=%d", code, restartOutput.String(), connectionErr, lastError, lastKind, exchanges)
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal("restarted service did not become ready")
				}
			}
			var stopAgain bytes.Buffer
			if code := runWithContext(ctx, []string{"stop", "--state-dir", stateRoot,
				"--expected-revision", "3"}, &stopAgain); code != 0 {
				t.Fatalf("restarted service stop exit=%d output=%s", code, stopAgain.String())
			}
			select {
			case code := <-restartDone:
				if code != 0 {
					t.Fatalf("restarted service exit=%d output=%s", code, restartOutput.String())
				}
			case <-ctx.Done():
				t.Fatal("restarted service ignored stop")
			}
			if snapshot, err := store.LoadSnapshot(context.Background()); err != nil ||
				snapshot.Manifest.Revision != 3 || snapshot.ObservationPresent {
				t.Fatalf("restarted service retained lease or wrong revision: snapshot=%+v error=%v", snapshot, err)
			}
			peer.mu.Lock()
			reconnectChallenges, reconnectActivations := peer.reconnectChallenges, peer.reconnectActivations
			peer.mu.Unlock()
			if reconnectChallenges != 1 || reconnectActivations != 1 {
				t.Fatalf("restart did not complete signed reconnect: challenges=%d activations=%d", reconnectChallenges, reconnectActivations)
			}
		}
		return
	}
	select {
	case <-peer.sealedStarted:
	case code := <-done:
		t.Fatalf("service exited before accepted attempt: code=%d output=%s", code, output.String())
	case <-ctx.Done():
		t.Fatal("service did not reach accepted synthetic attempt")
	}
	if cancelActive {
		serviceCancel()
		select {
		case code := <-done:
			if code != 0 {
				t.Fatalf("cancelled service exit=%d output=%s", code, output.String())
			}
		case <-ctx.Done():
			t.Fatal("cancelled service did not terminate by deadline")
		}
		if snapshot, err := store.LoadSnapshot(context.Background()); err != nil || snapshot.ObservationPresent {
			t.Fatalf("cancelled service retained runtime lease observation: snapshot=%+v error=%v", snapshot, err)
		}
		return
	}
	var status bytes.Buffer
	if code := runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &status); code != 0 {
		t.Fatalf("test-owned service status exit=%d output=%s", code, status.String())
	}
	if strings.Contains(status.String(), profile.ControlPlaneOrigin) || strings.Contains(status.String(), stateRoot) ||
		strings.Contains(status.String(), "connection_secret") {
		t.Fatalf("service status leaked authority: %s", status.String())
	}
	var drain bytes.Buffer
	drainDone := make(chan int, 1)
	go func() {
		drainDone <- runWithContext(ctx, []string{"drain", "--state-dir", stateRoot,
			"--expected-revision", "2"}, &drain)
	}()
	// Wait for the actual daemon transition, not an elapsed-time guess. The
	// held sealed read keeps the attempt active while drain closes admission.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	drainCompleted := false
drainLoop:
	for runtimeOwner.Status().State == attachedworkerdaemon.DaemonRunning {
		select {
		case code := <-drainDone:
			if code != 0 || runtimeOwner.Status().State == attachedworkerdaemon.DaemonRunning {
				t.Fatalf("drain completed before admission closed: code=%d output=%s", code, drain.String())
			}
			drainCompleted = true
			break drainLoop
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("daemon did not enter draining state")
		}
	}
	peer.releaseSealed()
	if !drainCompleted {
		select {
		case code := <-drainDone:
			if code != 0 {
				t.Fatalf("service drain exit=%d output=%s", code, drain.String())
			}
		case <-ctx.Done():
			t.Fatal("service drain did not complete")
		}
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("service exit=%d output=%s", code, output.String())
		}
	case <-ctx.Done():
		t.Fatal("service did not exit after drain")
	}
	peer.mu.Lock()
	steps, denied := peer.steps, peer.denied
	peer.mu.Unlock()
	if steps != 2 || denied != 1 {
		t.Fatalf("drained accepted attempt exchanged %d protocol steps, denied %d sealed reads", steps, denied)
	}
	if snapshot, err := store.LoadSnapshot(context.Background()); err != nil || snapshot.ObservationPresent {
		t.Fatalf("service did not retire runtime lease observation: snapshot=%+v error=%v", snapshot, err)
	}
}

type commandSyntheticPeer struct {
	mu                   sync.Mutex
	public               ed25519.PublicKey
	now                  time.Time
	offer                attachedworkerprotocol.VersionOfferV1
	binding              attachedworkerprotocol.AttemptBindingV1
	challenge            domain.AttachedWorkerAttachChallenge
	steps                int
	denied               int
	challenges           int
	activations          int
	reconnectChallenges  int
	reconnectActivations int
	exchanges            int
	lastError            string
	lastKind             attachedworkerprotocol.MessageKind
	terminal             chan attachedworkerprotocol.TerminalV1
	idle                 bool
	previousCheckpoint   attachedworkerlocal.ReconnectCheckpointV1
	previousConfig       attachedworkerprotocol.MachineConfig
	sealedGate           chan struct{}
	sealedStarted        chan struct{}
	sealedRelease        sync.Once
}

func (peer *commandSyntheticPeer) releaseSealed() {
	if peer != nil && peer.sealedGate != nil {
		peer.sealedRelease.Do(func() { close(peer.sealedGate) })
	}
}

func (peer *commandSyntheticPeer) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch request.URL.Path {
	case attachedworkerhttp.ChallengePathV1:
		var input attachedworkerhttp.ChallengeRequestV1
		if json.NewDecoder(request.Body).Decode(&input) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		response, err := peer.issueChallenge(input)
		if err != nil {
			peer.mu.Lock()
			peer.lastError = err.Error()
			peer.mu.Unlock()
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(response)
	case attachedworkerhttp.AttachPathV1:
		var input attachedworkerhttp.ActivateRequestV1
		if json.NewDecoder(request.Body).Decode(&input) != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		response, err := peer.activate(input)
		if err != nil {
			peer.mu.Lock()
			peer.lastError = err.Error()
			peer.mu.Unlock()
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(response)
	case attachedworkerhttp.ExchangePathV1:
		batch, err := attachedworkerprotocol.DecodeBatchV1(mustReadBounded(request))
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		response, err := peer.exchange(batch)
		if err != nil {
			peer.mu.Lock()
			peer.lastError = err.Error()
			peer.mu.Unlock()
			writer.WriteHeader(http.StatusConflict)
			return
		}
		if response == nil {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		encoded, err := attachedworkerprotocol.EncodeBatchV1(*response)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(encoded)
	case attachedworkersealedinput.PathV1:
		if peer.sealedGate != nil {
			select {
			case peer.sealedStarted <- struct{}{}:
			default:
			}
			select {
			case <-peer.sealedGate:
			case <-request.Context().Done():
				return
			}
		}
		peer.mu.Lock()
		peer.denied++
		peer.mu.Unlock()
		writer.WriteHeader(http.StatusUnauthorized)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func mustReadBounded(request *http.Request) []byte {
	data, _ := io.ReadAll(io.LimitReader(request.Body, attachedworkerprotocol.MaxBatchBytes+1))
	return data
}

func (peer *commandSyntheticPeer) issueChallenge(input attachedworkerhttp.ChallengeRequestV1) (attachedworkerhttp.ChallengeResponseV1, error) {
	peer.mu.Lock()
	peer.challenges++
	sequence, now := peer.challenges, peer.now
	previous := peer.previousCheckpoint
	peer.mu.Unlock()
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(input.TenantLocator, input.OwnerLocator,
		domain.AttachedWorkerID(input.Hello.WorkerID), input.Hello.EnrollmentGeneration, input.Hello.ConnectionGeneration-1,
		attachedworkertransport.IssueChallengeRequest{WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID),
			ExpectedAudience: input.ExpectedAudience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
			Purpose: input.Purpose, Hello: input.Hello})
	if err != nil || !ed25519.Verify(peer.public, transcript, input.Proof) {
		return attachedworkerhttp.ChallengeResponseV1{}, fmt.Errorf("invalid challenge proof")
	}
	if input.Purpose == domain.AttachedWorkerAttachReconnect {
		peer.mu.Lock()
		peer.reconnectChallenges++
		peer.mu.Unlock()
	}
	platformNonce := bytes.Repeat([]byte{0x52}, 32)
	challenge := domain.AttachedWorkerAttachChallenge{
		TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator,
		ID:           domain.AttachedWorkerChallengeID(fmt.Sprintf("challenge-command-%d", sequence)),
		WorkerID:     domain.AttachedWorkerID(input.Hello.WorkerID),
		ConnectionID: domain.AttachedWorkerConnectionID(fmt.Sprintf("connection-command-%d", sequence)),
		Purpose:      input.Purpose, Audience: input.ExpectedAudience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
		ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1, TargetConnectionGeneration: input.Hello.ConnectionGeneration,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1},
		SelectedProtocolVersion: 1, WorkerNonceDigest: domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce),
		PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(platformNonce),
		CreatedAt:           now, ExpiresAt: now.Add(time.Minute), RetainUntil: now.Add(time.Hour), Revision: 1,
	}
	if input.Purpose == domain.AttachedWorkerAttachReconnect {
		encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(previous.MachineSnapshot)
		if err != nil {
			return attachedworkerhttp.ChallengeResponseV1{}, err
		}
		challenge.ExpectedConnectionID = previous.ConnectionID
		challenge.ExpectedConnectionRevision = 2
		challenge.ExpectedCapabilityDigest = previous.CapabilityDigest
		challenge.ExpectedProtocolSnapshot = encoded
	}
	peer.mu.Lock()
	peer.challenge = challenge
	peer.mu.Unlock()
	return attachedworkerhttp.ChallengeResponseV1{Challenge: challenge, Frame: attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
		WorkerID: input.Hello.WorkerID, EnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ConnectionGeneration: input.Hello.ConnectionGeneration, Sequence: 1, Ack: 1,
		Kind: attachedworkerprotocol.MessageChallenge, Challenge: &attachedworkerprotocol.ChallengeV1{
			WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: peer.offer, SelectedVersion: 1,
			WorkerNonce: append([]byte(nil), input.Hello.Hello.WorkerNonce...), PlatformNonce: platformNonce,
		},
	}}, nil
}

func (peer *commandSyntheticPeer) activate(input attachedworkerhttp.ActivateRequestV1) (attachedworkerhttp.ActivateResponseV1, error) {
	peer.mu.Lock()
	peer.activations++
	challenge := peer.challenge
	now := peer.now
	previous := peer.previousCheckpoint
	previousConfig := peer.previousConfig
	peer.mu.Unlock()
	channel := attachedworkertransport.ConnectionChannelBinding(challenge.ID, challenge.WorkerNonceDigest,
		challenge.PlatformNonceDigest, input.ConnectionSecretDigest)
	channelBytes, err := hex.DecodeString(string(channel))
	if err != nil {
		return attachedworkerhttp.ActivateResponseV1{}, err
	}
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(challenge.TenantID), OwnerUserID: string(challenge.OwnerUserID), WorkerID: string(challenge.WorkerID),
		IdentityPublicKey: peer.public, EnrollmentGeneration: challenge.ExpectedEnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Version: input.Attach.Version,
		ChannelBinding: channelBytes,
	}
	var accepted attachedworkerprotocol.FrameV1
	var capabilityDigest []byte
	if challenge.Purpose == domain.AttachedWorkerAttachReconnect {
		if attachedworkerprotocol.VerifyReconnectV1(auth, input.Attach) != nil {
			return attachedworkerhttp.ActivateResponseV1{}, fmt.Errorf("invalid reconnect proof")
		}
		accepted, _, err = attachedworkerprotocol.BuildReconnectAcceptedSnapshotV1(
			previousConfig, previous.MachineSnapshot, auth, input.Attach)
		if err != nil {
			return attachedworkerhttp.ActivateResponseV1{}, err
		}
		peer.mu.Lock()
		peer.reconnectActivations++
		peer.mu.Unlock()
		capabilityDigest = input.Attach.Reconnect.CapabilityDigest
	} else {
		if attachedworkerprotocol.VerifyAttachV1(auth, input.Attach) != nil {
			return attachedworkerhttp.ActivateResponseV1{}, fmt.Errorf("invalid attach proof")
		}
		capabilityDigest = input.Attach.Attach.CapabilityDigest
		accepted = attachedworkerprotocol.FrameV1{
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
	}
	return attachedworkerhttp.ActivateResponseV1{Connection: attachedworkerhttp.ActivateConnectionV1{
		TenantID: challenge.TenantID, OwnerUserID: challenge.OwnerUserID, WorkerID: challenge.WorkerID,
		ID: challenge.ConnectionID, ActivationChallengeID: challenge.ID,
		EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration,
		ProtocolVersion:  uint32(input.Attach.Version),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest)),
		SecretDigest:     input.ConnectionSecretDigest, ChannelBinding: channel,
		State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2,
		PlatformAck: 2, WorkerAck: 1, ConnectedAt: now, AuthExpiresAt: now.Add(time.Hour), Revision: 1,
	}, Accepted: accepted}, nil
}

func (peer *commandSyntheticPeer) exchange(batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	peer.mu.Lock()
	peer.exchanges++
	peer.mu.Unlock()
	if len(batch.Frames) != 1 {
		return nil, fmt.Errorf("expected one frame")
	}
	worker := batch.Frames[0]
	if peer.idle && worker.Kind == attachedworkerprotocol.MessageHeartbeat {
		return nil, nil
	}
	peer.mu.Lock()
	peer.lastKind = worker.Kind
	peer.mu.Unlock()
	if worker.Kind == attachedworkerprotocol.MessageManifest {
		return nil, nil
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.steps == 2 && worker.Kind == attachedworkerprotocol.MessageHeartbeat {
		return nil, nil
	}
	step := peer.steps
	peer.steps++
	platform := attachedworkerprotocol.FrameV1{
		Version:   worker.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, uint64(3+step)),
		WorkerID:  worker.WorkerID, EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: worker.ConnectionGeneration, Sequence: uint64(3 + step), Ack: worker.Sequence,
	}
	switch step {
	case 0:
		if worker.Kind != attachedworkerprotocol.MessageHeartbeat || worker.Sequence != 4 || worker.Ack != 2 {
			return nil, fmt.Errorf("invalid heartbeat envelope")
		}
		platform.Kind = attachedworkerprotocol.MessageLeaseOffer
		platform.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: peer.binding, AttemptSequence: 1}
	case 1:
		if worker.Kind != attachedworkerprotocol.MessageLeaseClaim || worker.LeaseClaim == nil || worker.Sequence != 5 || worker.Ack != 3 {
			return nil, fmt.Errorf("invalid lease claim envelope")
		}
		platform.Kind = attachedworkerprotocol.MessageLeaseAccepted
		platform.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: peer.binding, AttemptSequence: 2}
	case 2:
		if worker.Kind != attachedworkerprotocol.MessageTerminal || worker.Terminal == nil || worker.Sequence < 6 || worker.Ack != 4 {
			return nil, fmt.Errorf("invalid terminal envelope")
		}
		platform.Kind = attachedworkerprotocol.MessageTerminalAck
		platform.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
			Binding: peer.binding, AttemptSequence: 3, TerminalSequence: worker.Terminal.TerminalSequence,
			Status: worker.Terminal.Status, Result: worker.Terminal.Result,
			EvidenceDigest: append([]byte(nil), worker.Terminal.EvidenceDigest...),
		}
		peer.terminal <- *worker.Terminal
	default:
		return nil, fmt.Errorf("unexpected exchange step")
	}
	if err := platform.Validate(); err != nil {
		return nil, err
	}
	return &attachedworkerprotocol.BatchV1{Version: worker.Version, Frames: []attachedworkerprotocol.FrameV1{platform}}, nil
}
