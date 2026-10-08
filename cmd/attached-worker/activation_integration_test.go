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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// This drives the shipped run CLI dispatch through an actual TLS bootstrap,
// exchange, and denied sealed-input endpoint. Only the clock is injected at
// the connector boundary. OCI preflight is a pinned stub: any container start
// or provider invocation is a test failure.
func TestActivatedRunAcceptsSyntheticAttemptAndDeniesInput(t *testing.T) {
	testActivatedSyntheticCommand(t, false, false, false, false, false, false, false)
}

func TestActivatedRunAcknowledgesRemoteCancelBeforeMaterialization(t *testing.T) {
	testActivatedSyntheticCommand(t, false, false, false, false, false, true, false)
}

func TestActivatedServeDrainsAcceptedSyntheticAttempt(t *testing.T) {
	testActivatedSyntheticCommand(t, true, false, false, false, false, false, false)
}

func TestActivatedServeStopsIdle(t *testing.T) {
	testActivatedSyntheticCommand(t, true, true, false, false, false, false, false)
}

func TestActivatedServeReconnectsAfterIdleStop(t *testing.T) {
	testActivatedSyntheticCommand(t, true, true, true, false, false, false, false)
}

func TestActivatedServeCancelsAcceptedSyntheticAttempt(t *testing.T) {
	testActivatedSyntheticCommand(t, true, false, false, true, false, false, false)
}

func TestActivatedServeAcknowledgesRemoteCancelDuringActiveAttempt(t *testing.T) {
	testActivatedSyntheticCommand(t, true, false, false, false, false, false, true)
}

func TestActivatedServeCrashRestartCheckpoint(t *testing.T) {
	if os.Getenv("SESSIONLESS_ATTACHED_WORKER_BINARY") == "" {
		t.Skip("opt-in exact-binary crash/restart integration")
	}
	testActivatedSyntheticCommand(t, true, true, false, false, true, false, false)
}

func TestActivatedServeActiveCrashFencesRestart(t *testing.T) {
	if os.Getenv("SESSIONLESS_ATTACHED_WORKER_ACTIVE_CRASH_INTEGRATION") != "1" {
		t.Skip("opt-in test-binary active-crash integration")
	}
	testActivatedSyntheticCommand(t, true, false, false, false, true, false, false)
}

// This child is the ordinary command dispatch in a separate OS process, with
// only the test connector clock substituted so the 15-minute production
// heartbeat minimum can be exercised without waiting 15 minutes in CI.
func TestActivatedServeCrashChild(t *testing.T) {
	if os.Getenv("SESSIONLESS_ATTACHED_WORKER_CRASH_CHILD") != "1" {
		t.Skip("test-owned service child")
	}
	stateRoot := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_STATE")
	profilePath := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_PROFILE")
	binary := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_BINARY")
	binaryDigest := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_BINARY_DIGEST")
	profileDigest := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_PROFILE_DIGEST")
	clockString := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_CLOCK_NANOS")
	expectReconciliation := os.Getenv("SESSIONLESS_ATTACHED_WORKER_CHILD_EXPECT_RECONCILIATION") == "true"
	os.Clearenv()
	clockValue, err := strconv.ParseInt(clockString, 10, 64)
	if err != nil {
		t.Fatalf("parse child clock: %v", err)
	}
	var clockNanos atomic.Int64
	clockStart := time.Unix(0, clockValue).UTC()
	clockNanos.Store(clockStart.UnixNano())
	testClock := func() time.Time { return time.Unix(0, clockNanos.Load()).UTC() }
	connector := func(ctx context.Context, store *attachedworkerlocal.Store, profile attachedworkeractivation.ProfileV1) (*attachedworkersealedinput.SyntheticRuntime, error) {
		owner, err := attachedworkeractivation.ConnectWithClock(ctx, store, profile, testClock)
		if expectReconciliation && errors.Is(err, attachedworkerdaemontransport.ErrReconciliationRequired) {
			fmt.Fprintln(os.Stdout, "active_checkpoint_reconciliation_confirmed")
		}
		if err == nil {
			clockNanos.Store(clockStart.Add(16 * time.Minute).UnixNano())
		}
		return owner, err
	}
	var output bytes.Buffer
	code := runWithContextAndConnector(context.Background(), []string{"serve", "--state-dir", stateRoot,
		"--expected-revision", "1", "--binary", binary, "--binary-sha256", binaryDigest,
		"--activation-profile", profilePath, "--activation-sha256", profileDigest}, &output, connector)
	if code != 0 {
		t.Fatalf("child serve exit=%d output=%s", code, output.String())
	}
}

func commandSealedInput(t *testing.T, now time.Time, capabilityDigest []byte) (attachedworkerdaemontransport.SealedInputV1, []byte, []byte) {
	t.Helper()
	const tenant, owner, runID, attemptID = "tenant-command", "owner-command", "run-command", "attempt-command"
	contextBytes := []byte("synthetic command context")
	artifactBytes := []byte("synthetic command artifact")
	blob := func(name string, body []byte) domain.BlobRef {
		sum := sha256.Sum256(body)
		return domain.BlobRef{TenantID: tenant, Key: "tenants/" + tenant + "/" + name,
			Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
	}
	policy := domain.DigestAttachedWorkerCapability([]byte("command synthetic policy"))
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: owner, WorkerID: "worker-command",
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(policy),
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		tenant, owner, runID, attemptID, "subscription-command", now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	binding := authority.HarnessBinding.Clone()
	binding.ExecutionPlacementDigest = string(placementDigest)
	job := domain.WorkerJob{
		TenantID: tenant, RunID: runID, SessionID: "session-command", TriggerEventID: "event-command",
		AttemptID: attemptID, ReservationID: "reservation-command", InputManifestID: "manifest-command",
		ContextSnapshot: blob("context", contextBytes), CredentialOwnerUserID: owner,
		ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 10,
			MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 100,
			MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18,
		},
	}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: tenant, RunID: runID,
		CreatedAt: now.Add(-time.Minute), Artifacts: []domain.Artifact{{
			Name: "alpha", MediaType: "text/plain", Blob: blob("alpha", artifactBytes),
		}}}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(value string) []byte {
		t.Helper()
		result, err := hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	return attachedworkerdaemontransport.SealedInputV1{
		Job: job, Manifest: manifest, Context: contextBytes,
		Artifacts: []attachedworkerdaemontransport.SealedArtifactV1{{Name: "alpha", Body: artifactBytes}},
	}, decode(string(contextDigest)), decode(string(placement.PolicyDigest))
}

func testActivatedSyntheticCommand(t *testing.T, service, idle, restart, cancelActive, crashRestart, cancelBeforeMaterialization, cancelDuringActive bool) {
	t.Helper()
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
	crashChildrenExited := true
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
			if !crashChildrenExited {
				t.Errorf("preserving test root while crashed service child may still own it: %s", root)
				return
			}
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
	if cancelDuringActive {
		sleepPath, err := exec.LookPath("sleep")
		if err != nil {
			t.Fatalf("locate bounded OCI stub sleep: %v", err)
		}
		cliBytes = []byte(strings.NewReplacer("@ROOT@", root, "@IMAGE@", image, "@SLEEP@", sleepPath).Replace(`#!/bin/sh
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
        printf '{"Id":"%s","Name":"/%s","State":{"Status":"created"},"Config":{"Image":"@IMAGE@","User":"1000:1000","WorkingDir":"%s","StopSignal":"SIGTERM","StopTimeout":10,"Entrypoint":["%s"],"Cmd":[],"Env":["HOME=%s/home","TMPDIR=%s/tmp","XDG_CONFIG_HOME=%s/xdg/config","XDG_CACHE_HOME=%s/xdg/cache","XDG_DATA_HOME=%s/xdg/data","PATH=","LANG=C.UTF-8","LC_ALL=C.UTF-8","NO_COLOR=1"],"Healthcheck":{"Test":["NONE"]},"Labels":{"dev.sessionless.attached-worker.profile":"sessionless.oci.docker.v1","dev.sessionless.attached-worker.installation":"install-command","dev.sessionless.attached-worker.engine":"engine-001-abcdef"}},"HostConfig":{"NetworkMode":"none","ReadonlyRootfs":true,"IpcMode":"private","CgroupnsMode":"private","CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true"],"PidsLimit":64,"Memory":67108864,"MemorySwap":67108864,"ShmSize":1048576,"Tmpfs":{"%s":"%s"},"Ulimits":[{"Name":"fsize","Soft":1024,"Hard":1024}],"Init":true,"LogConfig":{"Type":"none"},"RestartPolicy":{"Name":"no"}},"Mounts":[{"Type":"bind","Source":"%s","Destination":"%s","RW":false}]}\n' 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' "$name" "$workdir" "$entrypoint" "$attempt_root" "$attempt_root" "$attempt_root" "$attempt_root" "$attempt_root" "$tmpfs_path" "$tmpfs_opts" "$mount_src" "$mount_src";;
    esac;;
  container:start)
    : > '@ROOT@/oci-started'
    while [ ! -f '@ROOT@/cancelled' ]; do '@SLEEP@' 0.01; done;;
  container:stop|container:kill) : > '@ROOT@/cancelled';;
  container:rm) exit 0;;
  *) exit 97;;
esac
`))
	}
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
	// Keep a test-owned path next to, but outside, every worker attempt root.
	// Shutdown, cancellation, and crash recovery may clean their own roots;
	// they must never remove or rewrite an unrelated host file.
	sentinelPath := filepath.Join(root, "external-sentinel")
	sentinelBody := []byte("outside attached-worker attempt authority\n")
	if err := os.WriteFile(sentinelPath, sentinelBody, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		got, err := os.ReadFile(sentinelPath)
		if err != nil || !bytes.Equal(got, sentinelBody) {
			t.Errorf("external sentinel changed: content=%q err=%v", got, err)
		}
		for _, dir := range []string{materializationRoot, scratchRoot} {
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Errorf("attempt root not cleaned: root=%s entries=%v err=%v", dir, entries, err)
			}
		}
	})
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
	var sealedInput *attachedworkerdaemontransport.SealedInputV1
	if cancelDuringActive {
		input, contextDigest, policyDigest := commandSealedInput(t, testNow, capabilityDigest)
		sealedInput = &input
		binding.ContextDigest = contextDigest
		binding.PolicyDigest = policyDigest
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	peer := &commandSyntheticPeer{public: public, offer: offer, binding: binding, now: testNow, idle: idle,
		cancelBeforeMaterialization: cancelBeforeMaterialization,
		cancelDuringActive:          cancelDuringActive,
		ociStartedPath:              filepath.Join(root, "oci-started"),
		sealedInput:                 sealedInput,
		terminal:                    make(chan attachedworkerprotocol.TerminalV1, 1)}
	if service {
		peer.sealedStarted = make(chan struct{}, 1)
		if !cancelDuringActive {
			peer.sealedGate = make(chan struct{})
		}
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
	timeout := 10 * time.Second
	if crashRestart {
		timeout = 30 * time.Second
	} else if cancelDuringActive {
		timeout = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
	if crashRestart {
		if !idle {
			t.Cleanup(peer.releaseSealed)
		}
		testActivatedCrashRestart(t, ctx, peer, store, stateRoot, profilePath, &crashChildrenExited)
		return
	}
	if service {
		testActivatedServiceControl(t, ctx, peer, store, stateRoot, profilePath, connector,
			connected, testClock, &serviceStarted, serviceExited, &restartStarted, restartExited, idle, restart, cancelActive, cancelDuringActive)
		return
	}
	go func() {
		done <- runWithContextAndConnector(ctx, []string{"run", "--state-dir", stateRoot,
			"--activation-profile", profilePath}, &output, connector)
	}()
	var terminal attachedworkerprotocol.TerminalV1
	var earlyExitCode *int
	select {
	case terminal = <-peer.terminal:
	case code := <-done:
		earlyExitCode = &code
		select {
		case terminal = <-peer.terminal:
		default:
			commands, _ := os.ReadFile(commandLog)
			snapshot, snapshotErr := store.LoadSnapshot(context.Background())
			peer.mu.Lock()
			steps, challenges, activations, exchanges, lastError, lastKind := peer.steps, peer.challenges, peer.activations, peer.exchanges, peer.lastError, peer.lastKind
			peer.mu.Unlock()
			t.Fatalf("command exited before synthetic terminal: code=%d output=%s commands=%q revision=%d snapshot_error=%v exchange_steps=%d challenges=%d activations=%d exchanges=%d last_kind=%s peer_error=%q", code, output.String(), commands, snapshot.Manifest.Revision, snapshotErr, steps, challenges, activations, exchanges, lastKind, lastError)
		}
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
	wantStatus, wantResult := attachedworkerprotocol.TerminalFailed, attachedworkerprotocol.TerminalResultFailed
	if cancelBeforeMaterialization {
		wantStatus, wantResult = attachedworkerprotocol.TerminalCancelled, attachedworkerprotocol.TerminalResultCancelled
	}
	if terminal.Status != wantStatus || terminal.Result != wantResult {
		t.Errorf("synthetic terminal=%+v, want status=%s result=%s", terminal, wantStatus, wantResult)
	}
	if earlyExitCode != nil {
		if *earlyExitCode != 1 {
			t.Fatalf("synthetic command exit=%d output=%s", *earlyExitCode, output.String())
		}
	} else {
		select {
		case code := <-done:
			if code != 1 {
				t.Fatalf("synthetic command exit=%d output=%s", code, output.String())
			}
		case <-ctx.Done():
			t.Fatal("command did not stop after synthetic terminal")
		}
	}
	peer.mu.Lock()
	steps, denied, cancelAcked := peer.steps, peer.denied, peer.cancelAcked
	peer.mu.Unlock()
	wantSteps, wantDenied := 3, 1
	if cancelBeforeMaterialization {
		wantSteps, wantDenied = 4, 0
	}
	if steps != wantSteps || denied != wantDenied {
		t.Errorf("protocol steps=%d denied sealed requests=%d, want %d and %d", steps, denied, wantSteps, wantDenied)
	}
	if cancelBeforeMaterialization && cancelAcked != 1 {
		t.Errorf("pre-materialization cancel acknowledgements=%d, want 1", cancelAcked)
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
	if cancelBeforeMaterialization {
		lease, err := store.AcquireRuntime(context.Background())
		if err != nil {
			t.Fatalf("acquire cancelled command proof lease: %v", err)
		}
		checkpoint, checkpointErr := lease.LoadReconnectCheckpoint(context.Background())
		closeErr := lease.Close()
		attempt := checkpoint.MachineSnapshot.Attempt
		if checkpointErr != nil || closeErr != nil ||
			attempt.Summary.State != attachedworkerprotocol.AttemptTerminalCommitted ||
			attempt.Summary.CancelRevision != 1 || attempt.Summary.TerminalStatus != attachedworkerprotocol.TerminalCancelled ||
			attempt.Summary.TerminalResult != attachedworkerprotocol.TerminalResultCancelled || attempt.PendingWorkerTerminal != nil ||
			checkpoint.MachineSnapshot.Worker.Sequence != 7 || checkpoint.MachineSnapshot.Platform.Sequence != 5 {
			t.Fatalf("cancelled command lacked durable terminal acknowledgement: attempt=%+v worker_sequence=%d platform_sequence=%d load=%v close=%v", attempt.Summary, checkpoint.MachineSnapshot.Worker.Sequence, checkpoint.MachineSnapshot.Platform.Sequence, checkpointErr, closeErr)
		}
	}
}

func testActivatedServiceControl(t *testing.T, ctx context.Context, peer *commandSyntheticPeer,
	store *attachedworkerlocal.Store, stateRoot, profilePath string, connector activationConnector,
	connected <-chan *attachedworkersealedinput.SyntheticRuntime, testClock func() time.Time,
	serviceStarted *atomic.Bool, serviceExited chan struct{}, restartStarted *atomic.Bool,
	restartExited chan struct{}, idle, restart, cancelActive, cancelDuringActive bool) {
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
	var output boundedCrashOutput
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
			peer.setPreviousCheckpoint(checkpoint, testClock())
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
	if cancelDuringActive {
		var terminal attachedworkerprotocol.TerminalV1
		select {
		case terminal = <-peer.terminal:
		case code := <-done:
			peer.mu.Lock()
			steps, acknowledgements, exchanges, peerError, lastKind := peer.steps, peer.cancelAcked, peer.exchanges, peer.lastError, peer.lastKind
			peer.mu.Unlock()
			commands, _ := os.ReadFile(filepath.Join(filepath.Dir(stateRoot), "oci-commands.log"))
			t.Fatalf("service exited before remote-cancel terminal: code=%d output=%s steps=%d ack=%d exchanges=%d peer_error=%q last_kind=%s status=%+v commands=%q",
				code, output.String(), steps, acknowledgements, exchanges, peerError, lastKind, runtimeOwner.Status(), commands)
		case <-ctx.Done():
			peer.mu.Lock()
			steps, acknowledgements, exchanges, peerError, lastKind := peer.steps, peer.cancelAcked, peer.exchanges, peer.lastError, peer.lastKind
			peer.mu.Unlock()
			t.Fatalf("active remote Cancel did not reach terminal: steps=%d ack=%d exchanges=%d peer_error=%q last_kind=%s status=%+v output=%s",
				steps, acknowledgements, exchanges, peerError, lastKind, runtimeOwner.Status(), output.String())
		}
		if terminal.Status != attachedworkerprotocol.TerminalCancelled || terminal.Result != attachedworkerprotocol.TerminalResultCancelled {
			t.Errorf("active remote-cancel terminal status=%s result=%s, want cancelled/cancelled", terminal.Status, terminal.Result)
		}
		ready := time.NewTicker(10 * time.Millisecond)
		defer ready.Stop()
		for runtimeOwner.Status().Completed != 1 || runtimeOwner.Status().Active {
			select {
			case code := <-done:
				t.Fatalf("service exited before remote-cancel commit: code=%d output=%s", code, output.String())
			case <-ready.C:
			case <-ctx.Done():
				t.Fatalf("remote-cancel terminal not reported: status=%+v", runtimeOwner.Status())
			}
		}
		var stop bytes.Buffer
		if code := runWithContext(ctx, []string{"stop", "--state-dir", stateRoot,
			"--expected-revision", "2"}, &stop); code != 0 {
			t.Fatalf("stop after remote cancellation: code=%d output=%s", code, stop.String())
		}
		select {
		case code := <-done:
			if code != 0 {
				t.Fatalf("stopped service exit=%d output=%s", code, output.String())
			}
		case <-ctx.Done():
			t.Fatal("service did not stop after remote cancellation")
		}
		peer.mu.Lock()
		steps, acknowledged, denied, peerError, activeHeartbeatSequence := peer.steps, peer.cancelAcked, peer.denied, peer.lastError, peer.activeCancelHeartbeatSequence
		peer.mu.Unlock()
		if steps != 5 || acknowledged != 1 || denied != 0 || peerError != "" {
			t.Errorf("remote-cancel exchange steps=%d ack=%d denied=%d error=%q, want 5/1/0/empty", steps, acknowledged, denied, peerError)
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(stateRoot), "oci-started")); err != nil {
			t.Errorf("accepted attempt never started pinned OCI process: %v", err)
		}
		commands, err := os.ReadFile(filepath.Join(filepath.Dir(stateRoot), "oci-commands.log"))
		if err != nil || strings.Count(string(commands), "container create ") != 1 ||
			strings.Count(string(commands), "container start ") != 1 || strings.Count(string(commands), "container rm ") != 1 {
			t.Errorf("remote cancel did not clean up one pinned OCI process: commands=%q read=%v", commands, err)
		}
		if snapshot, err := store.LoadSnapshot(context.Background()); err != nil || snapshot.ObservationPresent {
			t.Fatalf("remote-cancel service retained runtime observation: snapshot=%+v error=%v", snapshot, err)
		}
		lease, err := store.AcquireRuntime(ctx)
		if err != nil {
			t.Fatalf("acquire remote-cancel checkpoint: %v", err)
		}
		checkpoint, checkpointErr := lease.LoadReconnectCheckpoint(ctx)
		closeErr := lease.Close()
		attempt := checkpoint.MachineSnapshot.Attempt
		if checkpointErr != nil || closeErr != nil || attempt.Summary.State != attachedworkerprotocol.AttemptTerminalCommitted ||
			attempt.Summary.CancelRevision != 1 || attempt.Summary.TerminalStatus != attachedworkerprotocol.TerminalCancelled ||
			attempt.Summary.TerminalResult != attachedworkerprotocol.TerminalResultCancelled || attempt.PendingWorkerTerminal != nil ||
			checkpoint.MachineSnapshot.Worker.Sequence != activeHeartbeatSequence+2 || checkpoint.MachineSnapshot.Platform.Sequence != 6 {
			t.Fatalf("remote-cancel checkpoint state=%s cancel_revision=%d terminal=%s/%s pending=%+v worker_sequence=%d platform_sequence=%d load=%v close=%v",
				attempt.Summary.State, attempt.Summary.CancelRevision, attempt.Summary.TerminalStatus,
				attempt.Summary.TerminalResult, attempt.PendingWorkerTerminal,
				checkpoint.MachineSnapshot.Worker.Sequence, checkpoint.MachineSnapshot.Platform.Sequence, checkpointErr, closeErr)
		}
		return
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
	steps, denied, lastKind, lastError := peer.steps, peer.denied, peer.lastKind, peer.lastError
	peer.mu.Unlock()
	if steps != 2 || denied != 1 {
		t.Fatalf("drained accepted attempt exchanged %d protocol steps, denied %d sealed reads; last_kind=%s peer_error=%q status=%+v",
			steps, denied, lastKind, lastError, runtimeOwner.Status())
	}
	if snapshot, err := store.LoadSnapshot(context.Background()); err != nil || snapshot.ObservationPresent {
		t.Fatalf("service did not retire runtime lease observation: snapshot=%+v error=%v", snapshot, err)
	}
}

type activatedCrashChild struct {
	command *exec.Cmd
	done    chan error
	output  boundedCrashOutput
	reaped  bool
}

type boundedCrashOutput struct {
	mu   sync.Mutex
	data []byte
}

func (output *boundedCrashOutput) Write(p []byte) (int, error) {
	const limit = 64 << 10
	output.mu.Lock()
	if remaining := limit - len(output.data); remaining > 0 {
		output.data = append(output.data, p[:min(len(p), remaining)]...)
	}
	output.mu.Unlock()
	return len(p), nil
}

func (output *boundedCrashOutput) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return string(output.data)
}

func startActivatedCrashChild(binary, binaryDigest, stateRoot, profilePath, profileDigest string) (*activatedCrashChild, error) {
	child := &activatedCrashChild{done: make(chan error, 1)}
	child.command = exec.Command(binary, "serve", "--state-dir", stateRoot,
		"--expected-revision", "1", "--binary", binary, "--binary-sha256", binaryDigest,
		"--activation-profile", profilePath, "--activation-sha256", profileDigest)
	child.command.Env = []string{}
	child.command.Stdout = &child.output
	child.command.Stderr = &child.output
	if err := child.command.Start(); err != nil {
		return nil, err
	}
	go func() { child.done <- child.command.Wait() }()
	return child, nil
}

func startActivatedCrashTestChild(binary, binaryDigest, stateRoot, profilePath, profileDigest string, clockStart time.Time, expectReconciliation bool) (*activatedCrashChild, error) {
	child := &activatedCrashChild{done: make(chan error, 1)}
	child.command = exec.Command(binary, "-test.run=^TestActivatedServeCrashChild$")
	child.command.Env = []string{
		"SESSIONLESS_ATTACHED_WORKER_CRASH_CHILD=1",
		"SESSIONLESS_ATTACHED_WORKER_CHILD_STATE=" + stateRoot,
		"SESSIONLESS_ATTACHED_WORKER_CHILD_PROFILE=" + profilePath,
		"SESSIONLESS_ATTACHED_WORKER_CHILD_BINARY=" + binary,
		"SESSIONLESS_ATTACHED_WORKER_CHILD_BINARY_DIGEST=" + binaryDigest,
		"SESSIONLESS_ATTACHED_WORKER_CHILD_PROFILE_DIGEST=" + profileDigest,
		"SESSIONLESS_ATTACHED_WORKER_CHILD_CLOCK_NANOS=" + strconv.FormatInt(clockStart.UnixNano(), 10),
		"SESSIONLESS_ATTACHED_WORKER_CHILD_EXPECT_RECONCILIATION=" + strconv.FormatBool(expectReconciliation),
	}
	child.command.Stdout = &child.output
	child.command.Stderr = &child.output
	if err := child.command.Start(); err != nil {
		return nil, err
	}
	go func() { child.done <- child.command.Wait() }()
	return child, nil
}

func (child *activatedCrashChild) killAndWait(ctx context.Context) error {
	if child == nil || child.reaped {
		return nil
	}
	killErr := child.command.Process.Kill()
	select {
	case err := <-child.done:
		child.reaped = true
		if killErr != nil {
			return fmt.Errorf("kill exact child: %w (wait: %v)", killErr, err)
		}
		var exited *exec.ExitError
		if !errors.As(err, &exited) {
			return fmt.Errorf("killed child returned %v", err)
		}
		status, ok := exited.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			return fmt.Errorf("child did not die from SIGKILL: %v", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func testActivatedCrashRestart(t *testing.T, ctx context.Context, peer *commandSyntheticPeer,
	store *attachedworkerlocal.Store, stateRoot, profilePath string, childrenExited *bool) {
	t.Helper()
	binary := os.Getenv("SESSIONLESS_ATTACHED_WORKER_BINARY")
	if !peer.idle {
		var err error
		binary, err = os.Executable()
		if err != nil {
			t.Fatal(err)
		}
	}
	canonical, err := filepath.EvalSymlinks(binary)
	if err != nil || canonical != binary || !filepath.IsAbs(binary) {
		t.Fatalf("exact built binary path required: %v", err)
	}
	content, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	binaryDigest := sha256.Sum256(content)
	_, profileDigest, err := attachedworkeractivation.ReadProfileWithDigest(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	children := make([]*activatedCrashChild, 0, 2)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		allReaped := true
		for _, child := range children {
			if child.reaped {
				continue
			}
			if err := child.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill exact crash-test child: %v", err)
			}
			select {
			case <-child.done:
				child.reaped = true
			case <-cleanupCtx.Done():
				t.Errorf("crash-test child did not exit after cleanup kill: %v", cleanupCtx.Err())
				allReaped = false
			}
		}
		*childrenExited = allReaped
	})
	start := func() *activatedCrashChild {
		t.Helper()
		var child *activatedCrashChild
		var err error
		if !peer.idle {
			peer.mu.Lock()
			clockStart := peer.now
			peer.mu.Unlock()
			child, err = startActivatedCrashTestChild(binary, hex.EncodeToString(binaryDigest[:]), stateRoot, profilePath, profileDigest, clockStart, len(children) != 0)
		} else {
			child, err = startActivatedCrashChild(binary, hex.EncodeToString(binaryDigest[:]), stateRoot, profilePath, profileDigest)
		}
		if err != nil {
			t.Fatal(err)
		}
		*childrenExited = false
		children = append(children, child)
		return child
	}
	waitReady := func(child *activatedCrashChild, revision uint64) {
		t.Helper()
		want := fmt.Sprintf(`"manifest_revision":%d`, revision)
		for {
			var status bytes.Buffer
			if runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &status) == 0 &&
				strings.Contains(status.String(), want) {
				return
			}
			select {
			case err := <-child.done:
				child.reaped = true
				t.Fatalf("child exited before revision %d: %v; output=%s", revision, err, child.output.String())
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				t.Fatalf("child did not reach revision %d: %v", revision, ctx.Err())
			}
		}
	}
	first := start()
	waitReady(first, 2)
	if !peer.idle {
		select {
		case <-peer.sealedStarted:
		case err := <-first.done:
			first.reaped = true
			t.Fatalf("first child exited before accepted active attempt: %v; output=%s", err, first.output.String())
		case <-ctx.Done():
			t.Fatalf("first child did not reach accepted active attempt: %v", ctx.Err())
		}
	}
	for {
		snapshot, err := store.LoadSnapshot(ctx)
		if err == nil && snapshot.Manifest.Revision == 2 && snapshot.ObservationPresent {
			break
		}
		select {
		case err := <-first.done:
			first.reaped = true
			t.Fatalf("first child exited before durable observation: %v; output=%s", err, first.output.String())
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("first child did not persist observation before crash: %v", ctx.Err())
		}
	}
	if err := first.killAndWait(ctx); err != nil {
		if first.reaped {
			t.Fatalf("first service was not killed exactly: %v; output=%s", err, first.output.String())
		}
		t.Fatalf("first service was not killed exactly before deadline: %v", err)
	}
	var unavailable bytes.Buffer
	if code := runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &unavailable); code == 0 {
		t.Fatalf("crashed service still answered live control: %s", unavailable.String())
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Revision != 2 || !snapshot.ObservationPresent {
		t.Fatalf("crash did not leave exact local observation: snapshot=%+v error=%v", snapshot, err)
	}
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, checkpointErr := lease.LoadReconnectCheckpoint(ctx)
	closeErr := lease.Close()
	if checkpointErr != nil || closeErr != nil {
		t.Fatalf("load crash checkpoint: load=%v close=%v", checkpointErr, closeErr)
	}
	if peer.idle && checkpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptIdle {
		t.Fatalf("idle crash checkpoint attempt state=%s", checkpoint.MachineSnapshot.Attempt.Summary.State)
	}
	if !peer.idle && (checkpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptClaimed ||
		!reflect.DeepEqual(checkpoint.MachineSnapshot.Attempt.Summary.Binding, peer.binding)) {
		t.Fatalf("active crash checkpoint is not the accepted attempt: state=%s run=%s attempt=%s",
			checkpoint.MachineSnapshot.Attempt.Summary.State, checkpoint.MachineSnapshot.Attempt.Summary.Binding.RunID,
			checkpoint.MachineSnapshot.Attempt.Summary.Binding.AttemptID)
	}
	restartNow := time.Now().UTC()
	if !peer.idle {
		peer.mu.Lock()
		restartNow = peer.now.Add(16 * time.Minute)
		peer.mu.Unlock()
	}
	peer.setPreviousCheckpoint(checkpoint, restartNow)
	second := start()
	if !peer.idle {
		select {
		case err := <-second.done:
			second.reaped = true
			if err == nil {
				t.Fatalf("active-crash restart was admitted: %s", second.output.String())
			}
			if !strings.Contains(second.output.String(), "active_checkpoint_reconciliation_confirmed") ||
				!strings.Contains(second.output.String(), `"code":"local_io_failed"`) {
				t.Fatalf("active-crash reconnect failed outside reconciliation: %s", second.output.String())
			}
		case <-ctx.Done():
			t.Fatalf("active-crash restart did not fail closed: %v", ctx.Err())
		}
		peer.mu.Lock()
		reconnectChallenges, reconnectActivations := peer.reconnectChallenges, peer.reconnectActivations
		peerError := peer.lastError
		peer.mu.Unlock()
		if reconnectChallenges != 1 || reconnectActivations != 1 || peerError != "" {
			t.Fatalf("active-crash restart did not fence after signed reconnect acceptance: challenges=%d activations=%d peer_error=%q", reconnectChallenges, reconnectActivations, peerError)
		}
		after, err := store.LoadSnapshot(ctx)
		if err != nil || after.Manifest.Revision != 3 || after.Manifest.ConnectionGeneration != 2 {
			t.Fatalf("active-crash restart did not persist reconnect generation: revision=%d generation=%d error=%v", after.Manifest.Revision, after.Manifest.ConnectionGeneration, err)
		}
		restartedLease, err := store.AcquireRuntime(ctx)
		if err != nil {
			t.Fatal(err)
		}
		restartedCheckpoint, checkpointErr := restartedLease.LoadReconnectCheckpoint(ctx)
		closeErr = restartedLease.Close()
		if checkpointErr != nil || closeErr != nil || restartedCheckpoint.ManifestRevision != 3 ||
			restartedCheckpoint.ConnectionGeneration != 2 || restartedCheckpoint.ConnectionID == checkpoint.ConnectionID ||
			restartedCheckpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptClaimed ||
			!reflect.DeepEqual(restartedCheckpoint.MachineSnapshot.Attempt.Summary.Binding, checkpoint.MachineSnapshot.Attempt.Summary.Binding) {
			t.Fatalf("active-crash restart did not persist non-idle manifest checkpoint: revision=%d generation=%d connection=%s attempt=%s load=%v close=%v",
				restartedCheckpoint.ManifestRevision, restartedCheckpoint.ConnectionGeneration, restartedCheckpoint.ConnectionID,
				restartedCheckpoint.MachineSnapshot.Attempt.Summary.State, checkpointErr, closeErr)
		}
		select {
		case <-peer.sealedStarted:
			t.Fatal("active-crash restart replayed sealed credential read")
		default:
		}
		var status bytes.Buffer
		if code := runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &status); code == 0 {
			t.Fatalf("active-crash restart published live control: %s", status.String())
		}
		return
	}
	waitReady(second, 3)
	var stop bytes.Buffer
	if code := runWithContext(ctx, []string{"stop", "--state-dir", stateRoot, "--expected-revision", "3"}, &stop); code != 0 {
		t.Fatalf("restarted child stop exit=%d output=%s", code, stop.String())
	}
	select {
	case err := <-second.done:
		second.reaped = true
		if err != nil {
			t.Fatalf("restarted child exit=%v output=%s", err, second.output.String())
		}
	case <-ctx.Done():
		t.Fatal("restarted child did not exit after stop")
	}
	peer.mu.Lock()
	reconnectChallenges, reconnectActivations := peer.reconnectChallenges, peer.reconnectActivations
	peer.mu.Unlock()
	if reconnectChallenges != 1 || reconnectActivations != 1 {
		t.Fatalf("crashed service did not authenticate reconnect: challenges=%d activations=%d", reconnectChallenges, reconnectActivations)
	}
	if snapshot, err := store.LoadSnapshot(ctx); err != nil || snapshot.Manifest.Revision != 3 || snapshot.ObservationPresent {
		t.Fatalf("restarted service did not retire lease: snapshot=%+v error=%v", snapshot, err)
	}
}

type commandSyntheticPeer struct {
	mu                            sync.Mutex
	public                        ed25519.PublicKey
	now                           time.Time
	offer                         attachedworkerprotocol.VersionOfferV1
	binding                       attachedworkerprotocol.AttemptBindingV1
	challenge                     domain.AttachedWorkerAttachChallenge
	steps                         int
	denied                        int
	challenges                    int
	activations                   int
	reconnectChallenges           int
	reconnectActivations          int
	exchanges                     int
	lastError                     string
	lastKind                      attachedworkerprotocol.MessageKind
	terminal                      chan attachedworkerprotocol.TerminalV1
	idle                          bool
	allowPostTerminalIdle         bool
	cancelBeforeMaterialization   bool
	cancelDuringActive            bool
	activeCancelHeartbeatSequence uint64
	ociStartedPath                string
	cancelAcked                   int
	previousCheckpoint            attachedworkerlocal.ReconnectCheckpointV1
	previousConfig                attachedworkerprotocol.MachineConfig
	sealedGate                    chan struct{}
	sealedStarted                 chan struct{}
	sealedInput                   *attachedworkerdaemontransport.SealedInputV1
	sealedRelease                 sync.Once
}

func TestCommandSyntheticPeerPostTerminalIdle(t *testing.T) {
	peer := &commandSyntheticPeer{steps: 3, allowPostTerminalIdle: true}
	heartbeat := attachedworkerprotocol.FrameV1{Kind: attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ActiveAttempts: 0}}
	response, err := peer.exchange(attachedworkerprotocol.BatchV1{Frames: []attachedworkerprotocol.FrameV1{heartbeat}})
	if err != nil || response != nil || peer.steps != 3 {
		t.Fatalf("post-terminal idle heartbeat: response=%+v err=%v steps=%d", response, err, peer.steps)
	}
	heartbeat.Heartbeat.ActiveAttempts = 1
	if _, err := peer.exchange(attachedworkerprotocol.BatchV1{Frames: []attachedworkerprotocol.FrameV1{heartbeat}}); err == nil {
		t.Fatal("post-terminal heartbeat still claiming an active attempt was accepted")
	}
}

func (peer *commandSyntheticPeer) setPreviousCheckpoint(checkpoint attachedworkerlocal.ReconnectCheckpointV1, now time.Time) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	peer.now = now
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
		if peer.sealedStarted != nil {
			select {
			case peer.sealedStarted <- struct{}{}:
			default:
			}
		}
		if peer.sealedGate != nil {
			select {
			case <-peer.sealedGate:
			case <-request.Context().Done():
				return
			}
		}
		peer.mu.Lock()
		input := peer.sealedInput
		if input == nil {
			peer.denied++
		}
		peer.mu.Unlock()
		if input != nil {
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(input)
			return
		}
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
	peer.mu.Lock()
	postTerminalIdle := peer.allowPostTerminalIdle && peer.steps >= 3
	peer.lastKind = worker.Kind
	peer.mu.Unlock()
	if postTerminalIdle && worker.Kind == attachedworkerprotocol.MessageHeartbeat {
		if worker.Heartbeat == nil || worker.Heartbeat.ActiveAttempts != 0 {
			return nil, fmt.Errorf("invalid post-terminal idle heartbeat")
		}
		return nil, nil
	}
	if peer.idle && worker.Kind == attachedworkerprotocol.MessageHeartbeat {
		return nil, nil
	}
	if worker.Kind == attachedworkerprotocol.MessageManifest {
		return nil, nil
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.steps == 2 && worker.Kind == attachedworkerprotocol.MessageHeartbeat {
		if !peer.cancelDuringActive {
			return nil, nil
		}
		if _, err := os.Stat(peer.ociStartedPath); errors.Is(err, os.ErrNotExist) {
			return nil, nil
		} else if err != nil {
			return nil, fmt.Errorf("inspect active OCI start: %w", err)
		}
	}
	step := peer.steps
	peer.steps++
	platformSequence := uint64(3 + step)
	if peer.cancelBeforeMaterialization && step == 3 {
		platformSequence--
	}
	if peer.cancelDuringActive && step == 4 {
		platformSequence--
	}
	platform := attachedworkerprotocol.FrameV1{
		Version:   worker.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, platformSequence),
		WorkerID:  worker.WorkerID, EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: worker.ConnectionGeneration, Sequence: platformSequence, Ack: worker.Sequence,
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
		if peer.cancelBeforeMaterialization {
			platform.Kind = attachedworkerprotocol.MessageCancel
			platform.Cancel = &attachedworkerprotocol.CancelV1{
				Binding: peer.binding, AttemptSequence: 2, CancelRevision: 1, Code: attachedworkerprotocol.CancelRequested,
			}
			break
		}
		platform.Kind = attachedworkerprotocol.MessageLeaseAccepted
		platform.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: peer.binding, AttemptSequence: 2}
	case 2:
		if peer.cancelDuringActive {
			if worker.Kind != attachedworkerprotocol.MessageHeartbeat || worker.Heartbeat == nil ||
				worker.Heartbeat.Available || worker.Heartbeat.ActiveAttempts != 1 || worker.Ack != 4 {
				return nil, fmt.Errorf("invalid active-control heartbeat: kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
			}
			peer.activeCancelHeartbeatSequence = worker.Sequence
			platform.Kind = attachedworkerprotocol.MessageCancel
			platform.Cancel = &attachedworkerprotocol.CancelV1{
				Binding: peer.binding, AttemptSequence: 3, CancelRevision: 1, Code: attachedworkerprotocol.CancelRequested,
			}
			break
		}
		if peer.cancelBeforeMaterialization {
			if worker.Kind != attachedworkerprotocol.MessageCancelAck || worker.CancelAck == nil ||
				worker.Sequence != 6 || worker.CancelAck.AttemptSequence != 2 || worker.CancelAck.CancelRevision != 1 ||
				!reflect.DeepEqual(worker.CancelAck.Binding, peer.binding) || worker.Ack != 4 {
				return nil, fmt.Errorf("invalid pre-materialization cancel acknowledgement: kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
			}
			peer.cancelAcked++
			return nil, nil
		}
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
	case 3:
		if peer.cancelDuringActive {
			if worker.Kind != attachedworkerprotocol.MessageCancelAck || worker.CancelAck == nil ||
				worker.Sequence != peer.activeCancelHeartbeatSequence+1 || worker.CancelAck.AttemptSequence != 2 || worker.CancelAck.CancelRevision != 1 ||
				!reflect.DeepEqual(worker.CancelAck.Binding, peer.binding) || worker.Ack != 5 {
				return nil, fmt.Errorf("invalid active cancel acknowledgement: kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
			}
			peer.cancelAcked++
			return nil, nil
		}
		if !peer.cancelBeforeMaterialization || worker.Kind != attachedworkerprotocol.MessageTerminal || worker.Terminal == nil ||
			worker.Sequence != 7 || worker.Terminal.AttemptSequence != 3 || !reflect.DeepEqual(worker.Terminal.Binding, peer.binding) ||
			worker.Terminal.Status != attachedworkerprotocol.TerminalCancelled ||
			worker.Terminal.Result != attachedworkerprotocol.TerminalResultCancelled || worker.Ack != 4 {
			return nil, fmt.Errorf("invalid pre-materialization cancellation terminal: kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
		}
		platform.Kind = attachedworkerprotocol.MessageTerminalAck
		platform.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
			Binding: peer.binding, AttemptSequence: 3, TerminalSequence: worker.Terminal.TerminalSequence,
			Status: worker.Terminal.Status, Result: worker.Terminal.Result,
			EvidenceDigest: append([]byte(nil), worker.Terminal.EvidenceDigest...),
		}
		peer.terminal <- *worker.Terminal
	case 4:
		if !peer.cancelDuringActive || worker.Kind != attachedworkerprotocol.MessageTerminal || worker.Terminal == nil ||
			worker.Sequence != peer.activeCancelHeartbeatSequence+2 || worker.Terminal.AttemptSequence != 3 || !reflect.DeepEqual(worker.Terminal.Binding, peer.binding) ||
			worker.Terminal.Status != attachedworkerprotocol.TerminalCancelled ||
			worker.Terminal.Result != attachedworkerprotocol.TerminalResultCancelled || worker.Ack != 5 {
			return nil, fmt.Errorf("invalid active cancellation terminal: kind=%s sequence=%d ack=%d", worker.Kind, worker.Sequence, worker.Ack)
		}
		platform.Kind = attachedworkerprotocol.MessageTerminalAck
		platform.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
			Binding: peer.binding, AttemptSequence: 4, TerminalSequence: worker.Terminal.TerminalSequence,
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
