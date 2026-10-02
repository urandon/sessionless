package attachedworkersealedinput

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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
)

type joinedSuccessFixture struct {
	config              SyntheticRuntimeConfig
	manifest            attachedworkerlocal.ManifestV1
	store               *attachedworkerlocal.Store
	clock               *joinedClock
	public              ed25519.PublicKey
	binding             attachedworkerprotocol.AttemptBindingV1
	exchange            *joinedExchange
	factory             *joinedFactory
	authorizer          *authorizerFixture
	jobs                *jobFixture
	blobs               *blobFixture
	commandLog          string
	materializationRoot string
	stateRoot           string
}

// The digest-pinned OCI test executable emulates strict inspect, consumes
// bounded synthetic stdin, and records every process boundary. No Docker is
// required for this protocol and ownership fixture.
func newJoinedSuccessFixture(t *testing.T) joinedSuccessFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	image := "registry.example/worker@sha256:" + strings.Repeat("b", 64)
	commandLog := filepath.Join(root, "oci-commands.log")
	cli := filepath.Join(root, "pinned-oci")
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatalf("locate host sleep for bounded OCI stub wait: %v", err)
	}
	sleepPath, err = filepath.Abs(sleepPath)
	if err != nil {
		t.Fatalf("resolve host sleep for bounded OCI stub wait: %v", err)
	}
	script := strings.NewReplacer(
		"@ROOT@", rootPathInShell(t, root),
		"@IMAGE@", image,
		"@SLEEP@", rootPathInShell(t, sleepPath),
	).Replace(`#!/bin/sh
shift 4
printf '%s\n' "$*" >> '@ROOT@/oci-commands.log'
case "$1:$2" in
  version:*) printf '%s\n' '{"Client":{"ApiVersion":"1.45"},"Server":{"ApiVersion":"1.45","Os":"linux"}}';;
  info:*) printf '%s\n' '{"ID":"engine-001-abcdef","OSType":"linux","CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"SwapLimit":true,"SecurityOptions":["name=seccomp,profile=builtin","name=rootless"]}';;
  image:inspect) printf '%s\n' '{"Os":"linux","RepoDigests":["@IMAGE@"],"Config":{}}';;
  container:ls) exit 0;;
  container:create)
    shift 2
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --name) printf '%s\n' "$2" > '@ROOT@/name'; shift 2;;
        --workdir) printf '%s\n' "$2" > '@ROOT@/workdir'; shift 2;;
        --tmpfs) printf '%s\n' "$2" > '@ROOT@/tmpfs'; shift 2;;
        --mount) printf '%s\n' "$2" > '@ROOT@/mount'; shift 2;;
        --entrypoint) printf '%s\n' "$2" > '@ROOT@/entrypoint'; shift 2;;
        *) shift;;
      esac
    done
    printf '%s\n' 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd';;
  container:inspect)
    case "$4" in
      '{{json .State}}') printf '%s\n' '{"Running":false}';;
      *)
        IFS= read -r name < '@ROOT@/name'
        IFS= read -r workdir < '@ROOT@/workdir'
        IFS= read -r tmpfs < '@ROOT@/tmpfs'
        IFS= read -r mount < '@ROOT@/mount'
        IFS= read -r entrypoint < '@ROOT@/entrypoint'
        tmpfs_path=${tmpfs%%:*}
        tmpfs_opts=${tmpfs#*:}
        mount_src=${mount#*src=}; mount_src=${mount_src%%,*}
        attempt_root=${workdir%/work}
        printf '{"Id":"%s","Name":"/%s","State":{"Status":"created"},"Config":{"Image":"@IMAGE@","User":"1000:1000","WorkingDir":"%s","StopSignal":"SIGTERM","StopTimeout":10,"Entrypoint":["%s"],"Cmd":[],"Env":["HOME=%s/home","TMPDIR=%s/tmp","XDG_CONFIG_HOME=%s/xdg/config","XDG_CACHE_HOME=%s/xdg/cache","XDG_DATA_HOME=%s/xdg/data","PATH=","LANG=C.UTF-8","LC_ALL=C.UTF-8","NO_COLOR=1"],"Healthcheck":{"Test":["NONE"]},"Labels":{"dev.sessionless.attached-worker.profile":"sessionless.oci.docker.v1","dev.sessionless.attached-worker.installation":"install-joined","dev.sessionless.attached-worker.engine":"engine-001-abcdef"}},"HostConfig":{"NetworkMode":"none","ReadonlyRootfs":true,"IpcMode":"private","CgroupnsMode":"private","CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true"],"PidsLimit":64,"Memory":67108864,"MemorySwap":67108864,"ShmSize":1048576,"Tmpfs":{"%s":"%s"},"Ulimits":[{"Name":"fsize","Soft":1024,"Hard":1024}],"Init":true,"LogConfig":{"Type":"none"},"RestartPolicy":{"Name":"no"}},"Mounts":[{"Type":"bind","Source":"%s","Destination":"%s","RW":false}]}\n' 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' "$name" "$workdir" "$entrypoint" "$attempt_root" "$attempt_root" "$attempt_root" "$attempt_root" "$attempt_root" "$tmpfs_path" "$tmpfs_opts" "$mount_src" "$mount_src";;
    esac;;
	  container:start)
	    attempts=0
	    while [ ! -f '@ROOT@/active-heartbeat-committed' ]; do
	      attempts=$((attempts + 1))
	      [ "$attempts" -lt 500 ] || exit 98
	      '@SLEEP@' 0.01
	    done
	    while IFS= read -r line; do :; done
	    exit 0;;
  container:rm) exit 0;;
  *) exit 97;;
esac
`)
	if err := os.WriteFile(cli, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cliDigest := sha256.Sum256([]byte(script))
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
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", EnrollmentGeneration: 1,
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
	stateRoot := filepath.Join(root, "state")
	store, err := attachedworkerlocal.NewStore(stateRoot, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "build-joined",
		HarnessName: "codex", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: append([]byte(nil), cliDigest[:]...),
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{
			attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary,
			attachedworkerprotocol.IsolationProcessBoundary,
		},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect,
		},
		MaxConcurrentAttempts: 1,
	}
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	_, _, jobs, blobs, request := fixtureInput(t)
	job := jobs.state.Job
	job.ExecutionPlacementV2.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest))
	placementDigest, err := domain.ExecutionPlacementDigest(job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	job.HarnessBinding.ExecutionPlacementDigest = string(placementDigest)
	jobs.state.Job = job
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, jobs.state.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	binding := request.Attempt
	binding.ContextDigest = decodeDigest(t, string(contextDigest))
	binding.CapabilityDigest = append([]byte(nil), capabilityDigest...)
	binding.ExpiresAtUnixMicro = joinedTestTime.Add(30 * time.Minute).UnixMicro()
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	exchange := &joinedExchange{binding: binding, terminalSeen: make(chan attachedworkerprotocol.TerminalV1, 1)}
	factory := &joinedFactory{exchange: exchange}
	authorizer := &authorizerFixture{revision: 7}
	service, err := NewService(authorizer, jobs, blobs)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(Handler(service))
	t.Cleanup(server.Close)
	config := SyntheticRuntimeConfig{
		Store: store, Bootstrap: &joinedBootstrap{now: joinedTestTime, publicKey: public, offer: offer}, Exchange: factory,
		Session: attachedworkersession.Config{
			Audience: "sessionless:attached-worker:v1", WorkerOffer: offer,
			ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second,
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
			InitialBackoff: time.Second, MaxBackoff: time.Minute,
			Random: bytes.NewReader(bytes.Repeat([]byte{0x43}, 8)), Now: clock.Now,
		},
		Stack: attachedworkerstack.Config{ScratchRoot: scratchRoot},
	}
	return joinedSuccessFixture{config: config, manifest: manifest, store: store, clock: clock,
		public: public, binding: binding, exchange: exchange, factory: factory,
		authorizer: authorizer, jobs: jobs, blobs: blobs, commandLog: commandLog,
		materializationRoot: materializationRoot, stateRoot: stateRoot}
}

// This is a protocol and ownership test, not a real Docker integration test.
func TestSyntheticPinnedRuntimeCommitsAcceptedAttemptAndLocalDrain(t *testing.T) {
	fixture := newJoinedSuccessFixture(t)
	config, manifest, store, clock := fixture.config, fixture.manifest, fixture.store, fixture.clock
	public, binding := fixture.public, fixture.binding
	exchange, factory := fixture.exchange, fixture.factory
	authorizer, jobs, blobs := fixture.authorizer, fixture.jobs, fixture.blobs
	commandLog, materializationRoot := fixture.commandLog, fixture.materializationRoot
	var activeHeartbeatSettled atomic.Uint64
	config.Adapter.ActiveHeartbeatSettled = &activeHeartbeatSettled
	// Keep the next active-control poll outside this bounded attempt. The
	// settled signal below, not elapsed time, releases the pinned process.
	config.Runtime.ActiveControlInterval = time.Minute
	owner, err := ConnectSyntheticPinnedRuntime(context.Background(), config)
	if err != nil {
		t.Fatalf("connect joined pinned runtime: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.Close(cleanupCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close joined runtime: %v", err)
		}
	})
	initialCloseCtx, cancelInitialClose := context.WithTimeout(context.Background(), 5*time.Second)
	if err := owner.Close(initialCloseCtx); err != nil {
		cancelInitialClose()
		t.Fatalf("retire idle owner before restart: %v", err)
	}
	cancelInitialClose()
	initialLease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatalf("idle owner retained lease: %v", err)
	}
	idleCheckpoint, err := initialLease.LoadReconnectCheckpoint(context.Background())
	if err != nil || idleCheckpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptIdle {
		_ = initialLease.Close()
		t.Fatalf("idle restart checkpoint=%+v error=%v", idleCheckpoint, err)
	}
	if err := initialLease.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Set(joinedTestTime.Add(2 * time.Minute))
	config.Bootstrap = newJoinedReconnectBootstrap(clock.Now(), manifest, idleCheckpoint, public)
	config.Session.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x51}, 32), bytes.Repeat([]byte{0x52}, 32)...))
	config.Poll.Random = bytes.NewReader(bytes.Repeat([]byte{0x53}, 8))
	exchange = &joinedExchange{binding: binding, terminalSeen: make(chan attachedworkerprotocol.TerminalV1, 1)}
	factory = &joinedFactory{exchange: exchange}
	config.Exchange = factory
	owner, err = ReconnectSyntheticPinnedRuntime(context.Background(), config)
	if err != nil || owner == nil {
		t.Fatalf("reconnect idle owner=%v error=%v", owner, err)
	}
	factory.mu.Lock()
	authorizer.bearer = append([]byte(nil), factory.bearer...)
	factory.mu.Unlock()
	clock.Set(joinedTestTime.Add(18 * time.Minute))
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	done := make(chan error, 1)
	go func() { done <- owner.Run(runCtx) }()
	// The active-control watcher sends an immediate unavailable heartbeat.
	// Let the pinned process exit only after ExchangeAction has returned and
	// the watcher is no longer in an ambiguous network operation. A checkpoint
	// can become visible just before that return and is not itself a barrier.
	checkpointFile := filepath.Join(fixture.stateRoot, attachedworkerlocal.ReconnectCheckpointFileName)
	settleDeadline := time.NewTimer(5 * time.Second)
	defer settleDeadline.Stop()
	settlePoll := time.NewTicker(10 * time.Millisecond)
	defer settlePoll.Stop()
	for activeHeartbeatSettled.Load() == 0 {
		select {
		case err := <-done:
			commands, commandErr := os.ReadFile(commandLog)
			checkpoint, checkpointErr := os.ReadFile(checkpointFile)
			t.Fatalf("runtime stopped before active heartbeat settled: %v; protocol steps=%d last-worker=%s platform=%v; status=%+v; commands error=%v commands=%q; checkpoint error=%v checkpoint=%s", err, exchange.steps, exchange.lastWorkerKind, exchange.lastPlatformErr, owner.Status(), commandErr, commands, checkpointErr, checkpoint)
		case <-settleDeadline.C:
			cancelRun()
			t.Fatal("active heartbeat did not settle")
		case <-settlePoll.C:
		}
	}
	encoded, readErr := os.ReadFile(checkpointFile)
	var activeCheckpoint attachedworkerlocal.ReconnectCheckpointV1
	decodeErr := json.Unmarshal(encoded, &activeCheckpoint)
	if readErr != nil || decodeErr != nil ||
		activeCheckpoint.ConnectionGeneration != 2 ||
		activeCheckpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptClaimed ||
		activeCheckpoint.MachineSnapshot.Worker.Sequence < 6 {
		cancelRun()
		t.Fatalf("settled active heartbeat lacks durable claimed checkpoint: read error=%v decode error=%v checkpoint=%+v", readErr, decodeErr, activeCheckpoint)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(fixture.stateRoot), "active-heartbeat-committed"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case terminal := <-exchange.terminalSeen:
		if terminal.Status != attachedworkerprotocol.TerminalSucceeded || terminal.Result != attachedworkerprotocol.TerminalResultCompleted {
			t.Errorf("accepted attempt terminal=%+v", terminal)
		}
	case err := <-done:
		commands, _ := os.ReadFile(commandLog)
		var terminal attachedworkerprotocol.TerminalV1
		select {
		case terminal = <-exchange.terminalSeen:
		default:
		}
		t.Fatalf("runtime stopped before terminal: %v; protocol steps=%d last-worker=%s; terminal=%+v; platform=%v; status=%+v; commands=%q", err, exchange.steps, exchange.lastWorkerKind, terminal, exchange.lastPlatformErr, owner.Status(), commands)
	case <-time.After(5 * time.Second):
		cancelRun()
		t.Fatal("accepted attempt did not reach terminal")
	}
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDrain()
	if err := owner.Drain(drainCtx); err != nil {
		t.Fatalf("local drain: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			commands, _ := os.ReadFile(commandLog)
			t.Fatalf("runtime after committed attempt and drain: %v; commands=%q", err, commands)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drained runtime did not stop")
	}
	if authorizer.calls != 2 || jobs.calls != 1 || blobs.calls != 2 {
		t.Errorf("sealed input effects: authorization=%d job=%d blobs=%d, want 2/1/2", authorizer.calls, jobs.calls, blobs.calls)
	}
	commands, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(commands), "container create ") != 1 || strings.Count(string(commands), "container start ") != 1 ||
		strings.Count(string(commands), "container rm ") != 1 {
		t.Errorf("expected exactly one owned OCI attempt, commands=%q", commands)
	}
	materialized, err := os.ReadDir(materializationRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(materialized) != 0 {
		t.Errorf("materialized attempt data was not removed: %v", materialized)
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
		t.Fatal(err)
	}
	if checkpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptTerminalCommitted ||
		checkpoint.MachineSnapshot.Attempt.Summary.TerminalStatus != attachedworkerprotocol.TerminalSucceeded {
		t.Errorf("persisted terminal checkpoint=%+v", checkpoint.MachineSnapshot.Attempt.Summary)
	}
	local, err := lease.LoadSnapshot(context.Background())
	if err != nil || local.ObservationPresent {
		t.Errorf("runtime observation not retired: present=%t error=%v", local.ObservationPresent, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	// A server-confirmed terminal head may reconnect as idle. Resume its poll
	// against a server with no new offer: it must not relaunch the locally
	// committed attempt merely from the durable checkpoint.
	config.Bootstrap = newJoinedReconnectBootstrap(clock.Now(), manifest, checkpoint, public)
	config.Session.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x61}, 32), bytes.Repeat([]byte{0x62}, 32)...))
	config.Poll.Random = bytes.NewReader(bytes.Repeat([]byte{0x63}, 8))
	restartExchange := &joinedExchange{idleHeartbeat: make(chan struct{}, 1)}
	config.Exchange = &joinedFactory{exchange: restartExchange}
	restarted, err := ReconnectSyntheticPinnedRuntime(context.Background(), config)
	if err != nil || restarted == nil {
		t.Fatalf("terminal restart owner=%v error=%v, want reconciled idle owner", restarted, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := restarted.Close(cleanupCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close terminal-reconnected owner: %v", err)
		}
	})
	if competing, err := store.AcquireRuntime(context.Background()); competing != nil || !errors.Is(err, attachedworkerlocal.ErrStateBusy) {
		if competing != nil {
			_ = competing.Close()
		}
		t.Errorf("restarted session allowed a second owner: lease=%v error=%v", competing, err)
	}
	clock.Set(joinedTestTime.Add(34 * time.Minute))
	restartRunCtx, cancelRestartRun := context.WithCancel(context.Background())
	defer cancelRestartRun()
	restartDone := make(chan error, 1)
	go func() { restartDone <- restarted.Run(restartRunCtx) }()
	select {
	case <-restartExchange.idleHeartbeat:
	case err := <-restartDone:
		t.Fatalf("reconciled idle runtime stopped before heartbeat: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("reconciled idle runtime did not poll")
	}
	restartDrainCtx, cancelRestartDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRestartDrain()
	if err := restarted.Drain(restartDrainCtx); err != nil {
		t.Fatalf("drain reconciled idle runtime: %v", err)
	}
	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatalf("reconciled idle runtime after drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciled idle runtime did not stop after drain")
	}
	commandsAfterRestart, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(commandsAfterRestart), "container create ") != 1 || strings.Count(string(commandsAfterRestart), "container start ") != 1 {
		t.Errorf("restart repeated process effect: commands=%q", commandsAfterRestart)
	}
	restartLease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restartLease.Close() }()
	restartCheckpoint, err := restartLease.LoadReconnectCheckpoint(context.Background())
	if err != nil || restartCheckpoint.ConnectionGeneration != 3 ||
		restartCheckpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptIdle {
		t.Errorf("reconciled restart checkpoint=%+v error=%v", restartCheckpoint, err)
	}
	if err := restartLease.Close(); err != nil {
		t.Fatal(err)
	}
	// The local head is now generation 3/idle. A server still claiming the
	// generation 2 terminal head cannot authorize generation 4 or a process.
	checkpointPath := filepath.Join(fixture.stateRoot, attachedworkerlocal.ReconnectCheckpointFileName)
	checkpointBeforeStale, err := os.ReadFile(checkpointPath)
	if err != nil || len(checkpointBeforeStale) == 0 {
		t.Fatalf("missing prior terminal-to-idle checkpoint before stale-server test: bytes=%d error=%v", len(checkpointBeforeStale), err)
	}
	config.Bootstrap = newJoinedReconnectBootstrap(clock.Now(), manifest, checkpoint, public)
	config.Session.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x71}, 32), bytes.Repeat([]byte{0x72}, 32)...))
	config.Poll.Random = bytes.NewReader(bytes.Repeat([]byte{0x73}, 8))
	if stale, err := ReconnectSyntheticPinnedRuntime(context.Background(), config); stale != nil ||
		!errors.Is(err, attachedworkersession.ErrReconciliationRequired) {
		if stale != nil {
			_ = stale.Close(context.Background())
		}
		t.Fatalf("stale server head was not fenced by reconciliation: owner=%v error=%v", stale, err)
	}
	staleSnapshot, err := store.LoadSnapshot(context.Background())
	if err != nil || staleSnapshot.Manifest.ConnectionGeneration != 4 {
		t.Fatalf("fenced stale reconnect local generation=%d error=%v, want 4", staleSnapshot.Manifest.ConnectionGeneration, err)
	}
	if _, err := os.Stat(checkpointPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale server head left reusable reconnect checkpoint: stat error=%v", err)
	}
	staleLease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatalf("stale server head leaked runtime lease: %v", err)
	}
	if err := staleLease.Close(); err != nil {
		t.Fatal(err)
	}
	commandsAfterStaleHead, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(commandsAfterStaleHead), "container create ") != 1 || strings.Count(string(commandsAfterStaleHead), "container start ") != 1 {
		t.Errorf("stale server head launched process: commands=%q", commandsAfterStaleHead)
	}
}

type joinedReconnectBootstrap struct {
	now                  time.Time
	publicKey            ed25519.PublicKey
	previousConfig       attachedworkerprotocol.MachineConfig
	previousSnapshot     attachedworkerprotocol.MachineSnapshotV1
	previousConnectionID domain.AttachedWorkerConnectionID
	challenge            domain.AttachedWorkerAttachChallenge
}

func newJoinedReconnectBootstrap(
	now time.Time, manifest attachedworkerlocal.ManifestV1,
	checkpoint attachedworkerlocal.ReconnectCheckpointV1, public ed25519.PublicKey,
) *joinedReconnectBootstrap {
	return &joinedReconnectBootstrap{
		now: now, publicKey: public, previousConnectionID: checkpoint.ConnectionID,
		previousConfig: attachedworkerprotocol.MachineConfig{
			Auth: attachedworkerprotocol.AuthContextV1{
				TenantID: string(manifest.TenantID), OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID),
				IdentityPublicKey: public, EnrollmentGeneration: manifest.EnrollmentGeneration,
				ConnectionGeneration: checkpoint.ConnectionGeneration, Version: checkpoint.ProtocolVersion,
				ChannelBinding: append([]byte(nil), checkpoint.ChannelBinding...),
			},
			WorkerOffer: checkpoint.WorkerOffer, PlatformOffer: checkpoint.PlatformOffer,
			ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1},
		},
		previousSnapshot: checkpoint.MachineSnapshot,
	}
}

func (fake *joinedReconnectBootstrap) IssueChallenge(_ context.Context, input attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	if input.Purpose != domain.AttachedWorkerAttachReconnect {
		return nil, errors.New("joined restart used non-reconnect challenge")
	}
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(
		input.TenantLocator, input.OwnerLocator, domain.AttachedWorkerID(input.Hello.WorkerID),
		input.Hello.EnrollmentGeneration, input.Hello.ConnectionGeneration-1,
		attachedworkertransport.IssueChallengeRequest{
			WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ExpectedAudience: input.ExpectedAudience,
			ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: input.Purpose, Hello: input.Hello,
		},
	)
	if err != nil || !ed25519.Verify(fake.publicKey, transcript, input.Proof) {
		return nil, errors.New("joined restart challenge proof invalid")
	}
	protocolSnapshot, err := attachedworkerprotocol.EncodeMachineSnapshotV1(fake.previousSnapshot)
	if err != nil {
		return nil, err
	}
	platformNonce := bytes.Repeat([]byte{0x82}, 32)
	fake.challenge = domain.AttachedWorkerAttachChallenge{
		TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, ID: "challenge-rejoined",
		WorkerID:     domain.AttachedWorkerID(input.Hello.WorkerID),
		ConnectionID: domain.AttachedWorkerConnectionID(fmt.Sprintf("connection-rejoined-%d", input.Hello.ConnectionGeneration)),
		Purpose:      input.Purpose, Audience: input.ExpectedAudience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
		ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1,
		TargetConnectionGeneration:   input.Hello.ConnectionGeneration,
		ExpectedConnectionID:         fake.previousConnectionID, ExpectedConnectionRevision: 2,
		ExpectedCapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", fake.previousSnapshot.CapabilityDigest)),
		ExpectedProtocolSnapshot: protocolSnapshot,
		WorkerProtocolMinimum:    1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
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
			WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: fake.previousConfig.PlatformOffer, SelectedVersion: 1,
			WorkerNonce: append([]byte(nil), input.Hello.Hello.WorkerNonce...), PlatformNonce: platformNonce,
		},
	}}, nil
}

func (fake *joinedReconnectBootstrap) Activate(_ context.Context, input attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	channel := attachedworkertransport.ConnectionChannelBinding(
		fake.challenge.ID, fake.challenge.WorkerNonceDigest, fake.challenge.PlatformNonceDigest, input.ConnectionSecret.Digest(),
	)
	channelBytes, err := hex.DecodeString(string(channel))
	if err != nil {
		return nil, err
	}
	nextAuth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(fake.challenge.TenantID), OwnerUserID: string(fake.challenge.OwnerUserID),
		WorkerID: string(fake.challenge.WorkerID), IdentityPublicKey: fake.publicKey,
		EnrollmentGeneration: fake.challenge.ExpectedEnrollmentGeneration,
		ConnectionGeneration: fake.challenge.TargetConnectionGeneration,
		Version:              input.Attach.Version, ChannelBinding: channelBytes,
	}
	if err := attachedworkerprotocol.VerifyReconnectV1(nextAuth, input.Attach); err != nil {
		return nil, err
	}
	accepted, _, err := attachedworkerprotocol.BuildReconnectAcceptedSnapshotV1(
		fake.previousConfig, fake.previousSnapshot, nextAuth, input.Attach,
	)
	if err != nil {
		return nil, err
	}
	return &attachedworkerhttp.ActivateResponseV1{
		Connection: attachedworkerhttp.ActivateConnectionV1{
			TenantID: fake.challenge.TenantID, OwnerUserID: fake.challenge.OwnerUserID, WorkerID: fake.challenge.WorkerID,
			ID: fake.challenge.ConnectionID, ActivationChallengeID: fake.challenge.ID,
			EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration,
			ProtocolVersion:  uint32(input.Attach.Version),
			CapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", input.Attach.Reconnect.CapabilityDigest)),
			SecretDigest:     input.ConnectionSecret.Digest(), ChannelBinding: channel,
			State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2,
			PlatformAck: 2, WorkerAck: 1, ConnectedAt: fake.now.Add(time.Minute),
			AuthExpiresAt: fake.now.Add(time.Hour), Revision: 1,
		},
		Accepted: accepted,
	}, nil
}
