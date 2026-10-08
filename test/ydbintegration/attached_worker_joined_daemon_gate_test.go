//go:build ydbintegration && (darwin || linux)

package ydbintegration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeractivation"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const aw07DaemonChildEnv = "SESSIONLESS_AW07_JOINED_DAEMON_CHILD"

type aw07DaemonChildInput struct {
	StateRoot          string `json:"state_root"`
	ProfilePath        string `json:"profile_path"`
	ClockNanos         int64  `json:"clock_nanos"`
	TestProvider       bool   `json:"test_provider,omitempty"`
	ProviderResource   string `json:"provider_resource,omitempty"`
	ProviderGeneration uint64 `json:"provider_generation,omitempty"`
}

// This subprocess owns the real activated foreground runtime. Its only
// authority inputs are the private local installation and activation profile.
func TestAW07JoinedDaemonChild(t *testing.T) {
	path := os.Getenv(aw07DaemonChildEnv)
	if path == "" {
		t.Skip("test-owned activated daemon subprocess")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var input aw07DaemonChildInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	os.Clearenv()
	store, err := attachedworkerlocal.NewStore(input.StateRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := attachedworkeractivation.ReadProfile(input.ProfilePath)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(0, input.ClockNanos).UTC()
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	var owner *attachedworkersealedinput.SyntheticRuntime
	if input.TestProvider {
		credentialRoot := filepath.Join(filepath.Dir(input.StateRoot), "credentials")
		if err := os.MkdirAll(credentialRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		credentials := &aw07TestCredentialLifecycle{root: credentialRoot,
			owner: profile.OwnerUserID, resource: input.ProviderResource, generation: input.ProviderGeneration, now: now}
		owner, err = attachedworkeractivation.ConnectTestProviderWithClock(ctx, store, profile, now, credentials, credentialRoot,
			func(identity attachedworkerdaemon.InvocationIdentity, result attachedworkerdaemon.InvocationResult, runErr error, status domain.AttachedWorkerTerminalStatus) (attachedworkeroutput.Candidate, error) {
				return attachedworkeroutput.Candidate{Status: status, Summary: "test provider result for " + string(identity.OwnerUserID)}, nil
			})
	} else {
		owner, err = attachedworkeractivation.ConnectTestSyntheticWithClock(ctx, store, profile, now)
	}
	if err != nil {
		t.Fatalf("activate joined daemon: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := owner.Close(closeCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close joined daemon: %v", err)
		}
	}()
	fmt.Println("AW07_DAEMON_CONNECTED")
	commands := bufio.NewScanner(os.Stdin)
	if !commands.Scan() || commands.Text() != "RUN" {
		t.Fatalf("expected RUN after connect: %v", commands.Err())
	}
	clock.Store(start.Add(16 * time.Minute).UnixNano())
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		err := owner.Run(runCtx)
		fmt.Printf("AW07_DAEMON_RUN_EXIT=%v\n", err)
		done <- err
	}()
	fmt.Println("AW07_DAEMON_RUNNING")
	if !commands.Scan() || commands.Text() != "STOP" {
		stop()
		t.Fatalf("expected STOP after run: %v", commands.Err())
	}
	stop()
	select {
	case err := <-done:
		// A Terminal frame is only pending evidence until a separate server-side
		// canonical materialization commits it. The foreground must stop for
		// reconciliation rather than treating the unacknowledged turn as done.
		if err != nil && !errors.Is(err, context.Canceled) &&
			!errors.Is(err, attachedworkerdaemontransport.ErrReconciliationRequired) {
			t.Fatalf("joined daemon run: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("joined daemon did not stop: %v", ctx.Err())
	}
	fmt.Println("AW07_DAEMON_STOPPED")
}

type aw07DaemonProcess struct {
	command *exec.Cmd
	stdin   ioWriteCloser
	ociRoot string
	events  chan string
	done    chan error
	stderr  aw07SafeBuffer
	stdout  aw07SafeBuffer
}

type aw07SafeBuffer struct {
	mu    sync.Mutex
	value bytes.Buffer
}

func (buffer *aw07SafeBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.value.Write(data)
}

func (buffer *aw07SafeBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.value.String()
}

type ioWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}

func aw07StartDaemonProcess(t *testing.T, input aw07DaemonChildInput) *aw07DaemonProcess {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "daemon-input.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestAW07JoinedDaemonChild$")
	// The fake OCI client is a descendant of this process. A failed assertion
	// must not leave its container:start shell polling forever after the Go
	// daemon child is killed.
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Env = []string{aw07DaemonChildEnv + "=" + path}
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &aw07DaemonProcess{
		command: command, stdin: stdin, ociRoot: filepath.Dir(filepath.Dir(input.ProfilePath)),
		events: make(chan string, 16), done: make(chan error, 1),
	}
	command.Stderr = &process.stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = fmt.Fprintln(&process.stdout, line)
			process.events <- line
		}
		if err := scanner.Err(); err != nil {
			process.events <- "SCAN_ERROR=" + err.Error()
		}
		close(process.events)
	}()
	go func() { process.done <- command.Wait() }()
	t.Cleanup(func() {
		// The supervisor deliberately starts the mock OCI client in a separate
		// process group. Stop that exact test-owned group before reaping/killing
		// the daemon; killing only the daemon group can orphan container:start.
		// Write this before checking oci-started so a late-starting client also
		// exits instead of racing the daemon teardown.
		if err := os.WriteFile(filepath.Join(process.ociRoot, "cancelled"), nil, 0o600); err != nil {
			t.Errorf("release mock OCI invocation: %v", err)
		}
		started := filepath.Join(process.ociRoot, "oci-started")
		if _, err := os.Stat(started); err == nil {
			exited := filepath.Join(process.ociRoot, "oci-exited")
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(exited); err == nil {
					break
				}
				if time.Now().After(deadline) {
					data, err := os.ReadFile(filepath.Join(process.ociRoot, "oci-start-pid"))
					pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
					if err != nil || parseErr != nil || pid <= 1 {
						t.Errorf("mock OCI did not exit and its group cannot be identified: read=%v parse=%v pid=%d", err, parseErr, pid)
					} else if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
						t.Errorf("kill mock OCI process group %d: %v", pid, err)
					}
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("inspect mock OCI invocation: %v", err)
		}
		_ = process.stdin.Close()
		// Closing stdin lets the test child run its own bounded shutdown. If it
		// cannot, kill its separate process group, including the fake OCI shell.
		select {
		case <-process.done:
			return
		case <-time.After(250 * time.Millisecond):
		}
		if process.command.Process != nil {
			if err := syscall.Kill(-process.command.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				select {
				case <-process.done:
					return
				default:
					t.Errorf("kill joined daemon process group: %v", err)
				}
			}
		}
		select {
		case <-process.done:
		case <-time.After(5 * time.Second):
			t.Errorf("joined daemon child did not reap after kill: stderr=%s", process.stderr.String())
		}
	})
	return process
}

func (process *aw07DaemonProcess) commandLine(t *testing.T, value string) {
	t.Helper()
	if _, err := fmt.Fprintln(process.stdin, value); err != nil {
		t.Fatalf("send %s to joined daemon: %v; stderr=%s", value, err, process.stderr.String())
	}
}

func (process *aw07DaemonProcess) await(t *testing.T, ctx context.Context, marker string) {
	t.Helper()
	for {
		select {
		case line, ok := <-process.events:
			if !ok {
				t.Fatalf("joined daemon exited before %s: stdout=%s stderr=%s", marker, process.stdout.String(), process.stderr.String())
			}
			if strings.Contains(line, marker) {
				return
			}
		case <-ctx.Done():
			t.Fatalf("joined daemon did not report %s: %v; stderr=%s", marker, ctx.Err(), process.stderr.String())
		}
	}
}

type aw07DaemonInstallation struct {
	worker              domain.AttachedWorker
	stateRoot           string
	profilePath         string
	materializationRoot string
	scratchRoot         string
	commandLog          string
	sentinel            string
	sentinelBody        []byte
}

// Record only failed exchange metadata; bearer bytes and frame payloads never
// enter test logs. This distinguishes a core rejection from an HTTP adapter
// failure when a joined daemon reports reconciliation.
type aw07ExchangeFailureRecorder struct {
	inner attachedworkerhttp.CoreExchange
	mu    sync.Mutex
	items []string
}

func (recorder *aw07ExchangeFailureRecorder) ExchangeBearer(ctx context.Context, bearer []byte,
	batch attachedworkerprotocol.BatchV1,
) (*attachedworkerprotocol.BatchV1, error) {
	response, err := recorder.inner.ExchangeBearer(ctx, bearer, batch)
	if err != nil {
		frame := attachedworkerprotocol.FrameV1{}
		if len(batch.Frames) > 0 {
			frame = batch.Frames[0]
		}
		recorder.mu.Lock()
		if len(recorder.items) < 16 {
			recorder.items = append(recorder.items, fmt.Sprintf("kind=%s sequence=%d ack=%d error=%v",
				frame.Kind, frame.Sequence, frame.Ack, err))
		}
		recorder.mu.Unlock()
	}
	return response, err
}

func (recorder *aw07ExchangeFailureRecorder) snapshot() string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return strings.Join(recorder.items, "; ")
}

type aw07ObservedAuthorizer struct {
	inner   *attachedworkertransport.Service
	mu      sync.Mutex
	calls   int
	lastErr error
}

func (authorizer *aw07ObservedAuthorizer) AuthorizeSealedInputBearer(ctx context.Context, bearer []byte,
	request ports.AttachedWorkerSealedInputAuthorization,
) (uint64, error) {
	revision, err := authorizer.inner.AuthorizeSealedInputBearer(ctx, bearer, request)
	authorizer.mu.Lock()
	authorizer.calls++
	authorizer.lastErr = err
	authorizer.mu.Unlock()
	return revision, err
}

func (authorizer *aw07ObservedAuthorizer) snapshot() (int, error) {
	authorizer.mu.Lock()
	defer authorizer.mu.Unlock()
	return authorizer.calls, authorizer.lastErr
}

type aw07ObservedJobs struct {
	inner *ydbstore.Store
	mu    sync.Mutex
	calls int
}

func (jobs *aw07ObservedJobs) LoadWorkerJob(ctx context.Context, tenant domain.TenantID,
	run domain.RunID,
) (ports.WorkerJobState, bool, error) {
	state, found, err := jobs.inner.LoadWorkerJob(ctx, tenant, run)
	jobs.mu.Lock()
	jobs.calls++
	jobs.mu.Unlock()
	return state, found, err
}

func (jobs *aw07ObservedJobs) count() int {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	return jobs.calls
}

func aw07CreateDaemonEnrollment(t *testing.T, store *ydbstore.Store, tenant domain.TenantID,
	owner domain.UserID, workerID domain.AttachedWorkerID, suffix string, now time.Time,
) (domain.AttachedWorker, ed25519.PrivateKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	enrollment, audit := attachedWorkerEnrollmentFixture(suffix, tenant, owner, now.Add(-time.Second))
	enrollment.ExpiresAt = now.Add(30 * time.Minute)
	enrollment.WorkerID, audit.WorkerID = workerID, workerID
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, audit); err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	claim := attachedWorkerClaimFixture(enrollment, 0x71)
	claim.IdentityPublicKey = append([]byte(nil), private.Public().(ed25519.PublicKey)...)
	result, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || result.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim owner %s: result=%+v err=%v", owner, result, err)
	}
	return result.Worker, private
}

func aw07PrepareDaemonInstallation(t *testing.T, root, origin string, trust []byte,
	worker domain.AttachedWorker, private ed25519.PrivateKey, now time.Time, testProvider bool,
) aw07DaemonInstallation {
	t.Helper()
	for _, dir := range []string{root, filepath.Join(root, "oci-config"), filepath.Join(root, "activation"),
		filepath.Join(root, "materialized"), filepath.Join(root, "scratch")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	image := "registry.example/worker@sha256:" + strings.Repeat("b", 64)
	commandLog := filepath.Join(root, "oci-commands.log")
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	startAction := "while [ ! -f '@ROOT@/cancelled' ]; do '@SLEEP@' 0.01; done"
	if testProvider {
		startAction = ": > '@ROOT@/oci-finished'"
	}
	startAction = strings.NewReplacer("@ROOT@", root, "@SLEEP@", sleepPath).Replace(startAction)
	providerEnv := ""
	if testProvider {
		providerEnv = `,"SESSIONLESS_PROVIDER_HOME=` + filepath.Join(root, "credentials", "handle-"+string(worker.OwnerUserID)) + `"`
	}
	cliBytes := []byte(strings.NewReplacer("@ROOT@", root, "@IMAGE@", image, "@SLEEP@", sleepPath,
		"@START_ACTION@", startAction, "@PROVIDER_ENV@", providerEnv).Replace(`#!/bin/sh
shift 4
printf '%s\n' "$*" >> '@ROOT@/oci-commands.log'
case "$1:$2" in
  version:*) printf '%s\n' '{"Client":{"ApiVersion":"1.45"},"Server":{"ApiVersion":"1.45","Os":"linux"}}';;
  info:*) printf '%s\n' '{"ID":"engine-001-abcdef","OSType":"linux","CgroupVersion":"2","MemoryLimit":true,"PidsLimit":true,"SwapLimit":true,"SecurityOptions":["name=seccomp,profile=builtin","name=rootless"]}';;
  image:inspect) printf '%s\n' '{"Os":"linux","RepoDigests":["@IMAGE@"],"Config":{}}';;
  container:ls) exit 0;;
  container:create)
    shift 2
    : > '@ROOT@/mounts'
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --name) printf '%s\n' "$2" > '@ROOT@/name'; shift 2;;
        --workdir) printf '%s\n' "$2" > '@ROOT@/workdir'; shift 2;;
        --tmpfs) printf '%s\n' "$2" > '@ROOT@/tmpfs'; shift 2;;
        --mount) printf '%s\n' "$2" >> '@ROOT@/mounts'; shift 2;;
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
        IFS= read -r entrypoint < '@ROOT@/entrypoint'
        tmpfs_path=${tmpfs%%:*}
        tmpfs_opts=${tmpfs#*:}
        attempt_root=${workdir%/work}
        printf '{"Id":"%s","Name":"/%s","State":{"Status":"created"},"Config":{"Image":"@IMAGE@","User":"1000:1000","WorkingDir":"%s","StopSignal":"SIGTERM","StopTimeout":10,"Entrypoint":["%s"],"Cmd":[],"Env":["HOME=%s/home","TMPDIR=%s/tmp","XDG_CONFIG_HOME=%s/xdg/config","XDG_CACHE_HOME=%s/xdg/cache","XDG_DATA_HOME=%s/xdg/data","PATH=","LANG=C.UTF-8","LC_ALL=C.UTF-8","NO_COLOR=1"@PROVIDER_ENV@],"Healthcheck":{"Test":["NONE"]},"Labels":{"dev.sessionless.attached-worker.profile":"sessionless.oci.docker.v1","dev.sessionless.attached-worker.installation":"install-aw07-daemon","dev.sessionless.attached-worker.engine":"engine-001-abcdef"}},"HostConfig":{"NetworkMode":"none","ReadonlyRootfs":true,"IpcMode":"private","CgroupnsMode":"private","CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true"],"PidsLimit":64,"Memory":67108864,"MemorySwap":67108864,"ShmSize":1048576,"Tmpfs":{"%s":"%s"},"Ulimits":[{"Name":"fsize","Soft":1024,"Hard":1024}],"Init":true,"LogConfig":{"Type":"none"},"RestartPolicy":{"Name":"no"}},"Mounts":[' 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' "$name" "$workdir" "$entrypoint" "$attempt_root" "$attempt_root" "$attempt_root" "$attempt_root" "$attempt_root" "$tmpfs_path" "$tmpfs_opts"
        first=1
        while IFS= read -r mount; do
          mount_src=${mount#*src=}; mount_src=${mount_src%%,*}
          mount_dst=${mount#*dst=}; mount_dst=${mount_dst%%,*}
          mount_rw=true
          case "$mount" in *,readonly) mount_rw=false;; esac
          if [ "$first" -eq 0 ]; then printf ','; fi
          printf '{"Type":"bind","Source":"%s","Destination":"%s","RW":%s}' "$mount_src" "$mount_dst" "$mount_rw"
          first=0
        done < '@ROOT@/mounts'
        printf ']}\n';;
    esac;;
  container:start)
    : > '@ROOT@/oci-started'
    printf '%s\n' "$$" > '@ROOT@/oci-start-pid'
    trap ': > "@ROOT@/oci-exited"' EXIT
    @START_ACTION@;;
  container:stop|container:kill) : > '@ROOT@/cancelled';;
  container:rm) exit 0;;
  *) exit 97;;
esac
`))
	cli := filepath.Join(root, "pinned-oci")
	if err := os.WriteFile(cli, cliBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	cliDigest := sha256.Sum256(cliBytes)
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, Revision: 1,
		ProtocolOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "aw07-daemon-test",
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
	if testProvider {
		capability.Features = []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureOutputReceipt,
			attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect,
		}
	}
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	boundary := attachedworkerlocal.BoundaryLinuxRootless
	if runtime.GOOS == "darwin" {
		boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: origin,
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		EnrollmentGeneration:   worker.EnrollmentGeneration,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(private.Public().(ed25519.PublicKey))),
		OCI: attachedworkerlocal.OCIConfigV1{
			DockerPath: cli, DockerSHA256: hex.EncodeToString(cliDigest[:]), CLIConfigDir: filepath.Join(root, "oci-config"),
			Host: "unix:///run/docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-aw07-daemon",
			Boundary: boundary, Image: image, UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30,
			CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10,
		},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: cli, SHA256: hex.EncodeToString(cliDigest[:]), Arguments: []string{}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
	}
	secret := attachedworkerlocal.SecretRecordV1{
		Version: 1, ManifestRevision: 1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID,
		WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration,
		IdentityPrivateKey: append([]byte(nil), private...),
	}
	stateRoot := filepath.Join(root, "state")
	local, err := attachedworkerlocal.NewStore(stateRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	installationDigest, err := attachedworkeractivation.InstallationDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	trustPath := filepath.Join(root, "activation", "trust.pem")
	if err := os.WriteFile(trustPath, trust, 0o600); err != nil {
		t.Fatal(err)
	}
	profile := attachedworkeractivation.ProfileV1{
		Version: 1, Mode: "synthetic-denied", ManifestRevision: 1,
		ControlPlaneOrigin: origin, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		EnrollmentGeneration: worker.EnrollmentGeneration, InstallationSHA256: installationDigest,
		ExpectedWorkerRevision: worker.Revision, Capability: capability,
		LocalProfile: attachedworkerdaemontransport.LocalProfileV1{
			Name: "synthetic", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest)),
			Executable: cli, ExecutableDigest: attachedworkerdaemon.ExecutableDigest(cliDigest),
		},
		TLSRootPEMPath: trustPath, MaterializationRoot: filepath.Join(root, "materialized"),
		ScratchRoot: filepath.Join(root, "scratch"), MaxInputBytes: 4096,
	}
	profilePath := filepath.Join(root, "activation", "profile.json")
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profilePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "external-sentinel")
	sentinelBody := []byte("outside attached-worker attempt authority\n")
	if err := os.WriteFile(sentinel, sentinelBody, 0o600); err != nil {
		t.Fatal(err)
	}
	return aw07DaemonInstallation{worker: worker, stateRoot: stateRoot, profilePath: profilePath,
		materializationRoot: filepath.Join(root, "materialized"), scratchRoot: filepath.Join(root, "scratch"),
		commandLog: commandLog,
		sentinel:   sentinel, sentinelBody: sentinelBody}
}

// This joined fixture exercises two activated foreground owners against the
// same YDB-backed control plane. It must not equate a pending Terminal frame
// with canonical run finalization or a successful provider turn.
func TestAW07TwoActivatedDaemonYDBJoin(t *testing.T) {
	store, client := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	suffix := attachedWorkerDrainTestSuffix(t, "aw07-daemons")
	tenant := domain.TenantID(uniqueID("tenant-" + suffix))
	workerID := domain.AttachedWorkerID(uniqueID("worker-" + suffix))
	aWorker, aPrivate := aw07CreateDaemonEnrollment(t, store, tenant, domain.UserID(uniqueID("owner-a-"+suffix)), workerID, suffix+"-a", now)
	bWorker, bPrivate := aw07CreateDaemonEnrollment(t, store, tenant, domain.UserID(uniqueID("owner-b-"+suffix)), workerID, suffix+"-b", now)
	seedCanonicalMembership(t, client.DB, tenant, aWorker.OwnerUserID, now)
	seedCanonicalMembership(t, client.DB, tenant, bWorker.OwnerUserID, now)
	probeCtx, cancelProbe := context.WithCancel(context.Background())
	backend := &aw07BackendRecorder{Store: store, probeDB: client.DB, probeCtx: probeCtx}
	t.Cleanup(func() {
		backend.probeMu.Lock()
		backend.probeClosed = true
		backend.probeMu.Unlock()
		cancelProbe()
		finished := make(chan struct{})
		go func() {
			backend.probeWG.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("joined synthetic YDB diagnostic probe did not stop after cancellation")
		}
	})
	service, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{
		IDs: testkit.NewSequenceIDGenerator("aw07-daemons-"), Audience: "sessionless:attached-worker:v1",
		PlatformOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, backend, backend)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := attachedworkerhttp.NewBootstrapHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	coreFailures := &aw07ExchangeFailureRecorder{inner: service}
	adapter, err := attachedworkerhttp.NewCoreExchangeAdapter(coreFailures)
	if err != nil {
		t.Fatal(err)
	}
	exchange, err := attachedworkerhttp.NewHandler(adapter)
	if err != nil {
		t.Fatal(err)
	}
	blobs := &aw07ArtifactBlobs{objects: make(map[string][]byte), opens: make(map[string]int)}
	observedAuth := &aw07ObservedAuthorizer{inner: service}
	observedJobs := &aw07ObservedJobs{inner: store}
	sealed, err := attachedworkersealedinput.NewService(observedAuth, observedJobs, blobs)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	statusRecorder := &aw07HTTPStatusRecorder{}
	mux.Handle(attachedworkerhttp.ChallengePathV1, bootstrap)
	mux.Handle(attachedworkerhttp.AttachPathV1, bootstrap)
	mux.Handle(attachedworkerhttp.ExchangePathV1, statusRecorder.wrap("exchange", exchange))
	mux.Handle(attachedworkersealedinput.PathV1, statusRecorder.wrap("sealed", attachedworkersealedinput.Handler(sealed)))
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	trust := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	root := t.TempDir()
	a := aw07PrepareDaemonInstallation(t, filepath.Join(root, "a"), server.URL, trust, aWorker, aPrivate, now, false)
	b := aw07PrepareDaemonInstallation(t, filepath.Join(root, "b"), server.URL, trust, bWorker, bPrivate, now, false)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	aProcess := aw07StartDaemonProcess(t, aw07DaemonChildInput{StateRoot: a.stateRoot, ProfilePath: a.profilePath, ClockNanos: now.UnixNano()})
	bProcess := aw07StartDaemonProcess(t, aw07DaemonChildInput{StateRoot: b.stateRoot, ProfilePath: b.profilePath, ClockNanos: now.UnixNano()})
	aProcess.await(t, ctx, "AW07_DAEMON_CONNECTED")
	bProcess.await(t, ctx, "AW07_DAEMON_CONNECTED")
	for _, installation := range []aw07DaemonInstallation{a, b} {
		connection, found, err := store.LoadAttachedWorkerConnection(ctx, tenant, installation.worker.OwnerUserID, workerID)
		if err != nil || !found || connection.ConnectionGeneration != 1 {
			t.Fatalf("owner %s did not activate YDB connection: found=%t connection=%+v err=%v",
				installation.worker.OwnerUserID, found, connection, err)
		}
	}
	aContext, aArtifact := []byte("owner A canonical context"), []byte("owner A private artifact")
	bContext, bArtifact := []byte("owner B canonical context"), []byte("owner B private artifact")
	aConnection, _, err := store.LoadAttachedWorkerConnection(ctx, tenant, aWorker.OwnerUserID, workerID)
	if err != nil {
		t.Fatal(err)
	}
	bConnection, _, err := store.LoadAttachedWorkerConnection(ctx, tenant, bWorker.OwnerUserID, workerID)
	if err != nil {
		t.Fatal(err)
	}
	aOffer := attachedWorkerOfferForDrainWithPayload(t, store, client, aWorker, aConnection, now,
		suffix+"-a", aContext, aArtifact, 20*time.Minute)
	bOffer := attachedWorkerOfferForDrainWithPayload(t, store, client, bWorker, bConnection, now,
		suffix+"-b", bContext, bArtifact, 20*time.Minute)
	aJob, found, err := store.LoadWorkerJob(ctx, tenant, aOffer.Attempt.RunID)
	if err != nil || !found || len(aJob.InputManifest.Artifacts) != 1 {
		t.Fatalf("load A job: found=%t err=%v", found, err)
	}
	bJob, found, err := store.LoadWorkerJob(ctx, tenant, bOffer.Attempt.RunID)
	if err != nil || !found || len(bJob.InputManifest.Artifacts) != 1 {
		t.Fatalf("load B job: found=%t err=%v", found, err)
	}
	blobs.mu.Lock()
	blobs.objects[aJob.Job.ContextSnapshot.Key] = aContext
	blobs.objects[aJob.InputManifest.Artifacts[0].Blob.Key] = aArtifact
	blobs.objects[bJob.Job.ContextSnapshot.Key] = bContext
	blobs.objects[bJob.InputManifest.Artifacts[0].Blob.Key] = bArtifact
	blobs.mu.Unlock()
	for _, process := range []*aw07DaemonProcess{aProcess, bProcess} {
		process.commandLine(t, "RUN")
		process.await(t, ctx, "AW07_DAEMON_RUNNING")
	}
	for _, installation := range []aw07DaemonInstallation{a, b} {
		waitUntil := time.Now().Add(15 * time.Second)
		for {
			attempt, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, installation.worker.OwnerUserID, workerID)
			if err != nil {
				t.Fatal(err)
			}
			if found && attempt.State == domain.AttachedWorkerAttemptClaimed {
				t.Logf("owner %s attempt state after daemon poll: %s", installation.worker.OwnerUserID, attempt.State)
				break
			}
			if found && attempt.State != domain.AttachedWorkerAttemptOffered {
				t.Fatalf("owner %s reached unexpected state before active invocation: %s", installation.worker.OwnerUserID, attempt.State)
			}
			if time.Now().After(waitUntil) {
				t.Fatalf("owner %s did not claim offer: found=%t state=%s A OCI=%s B OCI=%s",
					installation.worker.OwnerUserID, found, attempt.State,
					aw07ReadOCICommands(t, filepath.Join(root, "a", "oci-commands.log")),
					aw07ReadOCICommands(t, filepath.Join(root, "b", "oci-commands.log")))
			}
			if err := ctx.Err(); err != nil {
				t.Fatalf("owner %s never claimed offered attempt: %v", installation.worker.OwnerUserID, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for waitUntil := time.Now().Add(15 * time.Second); blobs.totalOpens() < 4; {
		if time.Now().After(waitUntil) {
			aState, _, _ := store.LoadAttachedWorkerAttempt(ctx, tenant, aWorker.OwnerUserID, workerID)
			bState, _, _ := store.LoadAttachedWorkerAttempt(ctx, tenant, bWorker.OwnerUserID, workerID)
			authCalls, authErr := observedAuth.snapshot()
			t.Fatalf("both daemon processes did not read their sealed context and artifact: opens=%d auth calls=%d last auth error=%v job calls=%d A state=%s B state=%s A events=%s B events=%s A OCI=%s B OCI=%s",
				blobs.totalOpens(), authCalls, authErr, observedJobs.count(), aState.State, bState.State,
				aw07DrainEvents(aProcess.events), aw07DrainEvents(bProcess.events),
				aw07ReadOCICommands(t, filepath.Join(root, "a", "oci-commands.log")),
				aw07ReadOCICommands(t, filepath.Join(root, "b", "oci-commands.log")))
		}
		time.Sleep(20 * time.Millisecond)
	}
	blobs.mu.Lock()
	for _, key := range []string{
		aJob.Job.ContextSnapshot.Key, aJob.InputManifest.Artifacts[0].Blob.Key,
		bJob.Job.ContextSnapshot.Key, bJob.InputManifest.Artifacts[0].Blob.Key,
	} {
		if got := blobs.opens[key]; got != 1 {
			blobs.mu.Unlock()
			t.Fatalf("joined daemon object reads for %s = %d, want exactly one own read", key, got)
		}
	}
	blobs.mu.Unlock()
	for _, installation := range []aw07DaemonInstallation{a, b} {
		started := filepath.Join(filepath.Dir(installation.commandLog), "oci-started")
		for waitUntil := time.Now().Add(15 * time.Second); ; {
			if _, err := os.Stat(started); err == nil {
				break
			}
			if time.Now().After(waitUntil) {
				t.Fatalf("owner %s did not start isolated invocation: OCI=%s", installation.worker.OwnerUserID,
					aw07ReadOCICommands(t, installation.commandLog))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for _, active := range []struct {
		installation aw07DaemonInstallation
		process      *aw07DaemonProcess
	}{{a, aProcess}, {b, bProcess}} {
		attempt, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, active.installation.worker.OwnerUserID, workerID)
		if err != nil || !found || attempt.State != domain.AttachedWorkerAttemptClaimed {
			t.Fatalf("owner %s is not claimed immediately before A revocation: found=%t state=%s err=%v",
				active.installation.worker.OwnerUserID, found, attempt.State, err)
		}
		commands := aw07ReadOCICommands(t, active.installation.commandLog)
		if !strings.Contains(commands, "container start --attach --interactive") ||
			strings.Contains(commands, "container stop") || strings.Contains(commands, "container kill") ||
			strings.Contains(commands, "container rm") ||
			strings.Contains(active.process.stdout.String(), "AW07_DAEMON_RUN_EXIT=") {
			t.Fatalf("owner %s invocation is not live before A revocation: stdout=%s OCI=%s",
				active.installation.worker.OwnerUserID, active.process.stdout.String(), commands)
		}
	}
	// Revoke A while both isolated invocations are running. Its local process
	// must be stopped without granting a terminal commit, while B's otherwise
	// identical worker locator and cloned identity remain live.
	aw07Revoke(t, store, ctx, aWorker)
	aProcess.await(t, ctx, "AW07_DAEMON_RUN_EXIT=")
	for waitUntil := time.Now().Add(20 * time.Second); ; {
		if strings.Contains(aw07ReadOCICommands(t, a.commandLog), "container rm --force --volumes") {
			break
		}
		if time.Now().After(waitUntil) {
			t.Fatalf("revoked A did not remove its local invocation: stdout=%s OCI=%s",
				aProcess.stdout.String(), aw07ReadOCICommands(t, a.commandLog))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(root, "b", "cancelled")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("A revocation touched B's invocation: stat error=%v B OCI=%s", err, aw07ReadOCICommands(t, b.commandLog))
	}
	bActive, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, bWorker.OwnerUserID, workerID)
	if err != nil || !found || bActive.State != domain.AttachedWorkerAttemptClaimed {
		t.Fatalf("B's claimed attempt changed after A revocation: found=%t state=%s err=%v", found, bActive.State, err)
	}
	if status := aw07RunStatus(t, store, ctx, tenant, aOffer.Attempt.RunID); status.Terminal() {
		t.Fatalf("revoked A finalized canonical run as %s", status)
	}
	cancelled, err := store.RequestAttachedWorkerCancellation(ctx, ports.AttachedWorkerCancellationRequest{
		TenantID: bOffer.Attempt.TenantID, OwnerUserID: bOffer.Attempt.OwnerUserID, WorkerID: bOffer.Attempt.WorkerID,
		AttemptID: bOffer.Attempt.AttemptID, LeaseGeneration: bOffer.Attempt.LeaseGeneration,
		AckTimeout: time.Minute,
	})
	if err != nil || cancelled.Status != ports.AttachedWorkerExecutionApplied {
		t.Fatalf("owner B cancellation request: result=%+v err=%v", cancelled, err)
	}
	for waitUntil := time.Now().Add(60 * time.Second); ; {
		attempt, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, bWorker.OwnerUserID, workerID)
		if err != nil {
			t.Fatal(err)
		}
		if found && attempt.State == domain.AttachedWorkerAttemptTerminalPending {
			break
		}
		if time.Now().After(waitUntil) {
			t.Fatalf("owner B cancellation did not reach pending terminal evidence: found=%t state=%s HTTP=%s core=%s backend=%s A stdout=%s B stdout=%s",
				found, attempt.State, statusRecorder.snapshot(), coreFailures.snapshot(), backend.snapshot(), aProcess.stdout.String(), bProcess.stdout.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, offer := range []ports.AttachedWorkerAttemptResult{bOffer} {
		if status := aw07RunStatus(t, store, ctx, tenant, offer.Attempt.RunID); status.Terminal() {
			t.Fatalf("owner %s pending terminal finalized canonical run as %s without server-side materialization",
				offer.Attempt.OwnerUserID, status)
		}
		pending, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, offer.Attempt.OwnerUserID, workerID)
		if err != nil || !found || pending.TerminalEvidenceDigest == "" {
			t.Fatalf("owner %s has no durable pending terminal evidence: found=%t attempt=%+v err=%v",
				offer.Attempt.OwnerUserID, found, pending, err)
		}
		materialization, _ := attachedWorkerFailureForDrain(t, store, pending, string(offer.Attempt.OwnerUserID))
		materialization.Failure.Cancelled = true
		materialization.EvidenceDigest = pending.TerminalEvidenceDigest
		_, err = store.CommitAttachedWorkerTerminal(ctx, ports.AttachedWorkerTerminalCommit{
			TenantID: tenant, OwnerUserID: pending.OwnerUserID, WorkerID: workerID,
			AttemptID: pending.AttemptID, LeaseGeneration: pending.LeaseGeneration,
			Materialization: materialization,
		})
		if !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) {
			t.Fatalf("owner %s accepted unrelated canonical output for process-only terminal digest: %v",
				pending.OwnerUserID, err)
		}
		if status := aw07RunStatus(t, store, ctx, tenant, offer.Attempt.RunID); status.Terminal() {
			t.Fatalf("owner %s digest mismatch finalized canonical run as %s", pending.OwnerUserID, status)
		}
	}
	for _, process := range []*aw07DaemonProcess{aProcess, bProcess} {
		process.commandLine(t, "STOP")
		process.await(t, ctx, "AW07_DAEMON_STOPPED")
	}
	for _, installation := range []aw07DaemonInstallation{a, b} {
		for _, path := range []string{installation.materializationRoot, installation.scratchRoot} {
			entries, err := os.ReadDir(path)
			if err != nil || len(entries) != 0 {
				t.Fatalf("owner %s attempt root not cleaned: path=%s entries=%v err=%v",
					installation.worker.OwnerUserID, path, entries, err)
			}
		}
		body, err := os.ReadFile(installation.sentinel)
		if err != nil || !bytes.Equal(body, installation.sentinelBody) {
			t.Fatalf("owner %s external sentinel changed: body=%q err=%v", installation.worker.OwnerUserID, body, err)
		}
	}
}

func aw07DrainEvents(events <-chan string) string {
	var lines []string
	for {
		select {
		case line, ok := <-events:
			if !ok {
				return strings.Join(lines, " | ")
			}
			lines = append(lines, line)
		default:
			return strings.Join(lines, " | ")
		}
	}
}

func aw07ReadOCICommands(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(data)
}
