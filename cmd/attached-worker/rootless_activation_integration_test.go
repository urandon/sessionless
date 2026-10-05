package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeractivation"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerpackage"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Opt-in CI only: this starts one exact test-owned systemd user unit under an
// already provisioned rootless engine. The peer is local to the runner; no
// provider, cloud deployment, or external credential is involved.
func TestActivatedRootlessServiceAcceptsSyntheticAttempt(t *testing.T) {
	if os.Getenv("SESSIONLESS_ROOTLESS_ACTIVATION_INTEGRATION") != "1" {
		t.Skip("opt-in activated Linux rootless user-service integration")
	}
	if runtime.GOOS != "linux" || os.Getuid() == 0 {
		t.Fatal("activated rootless service test requires non-root Linux")
	}
	binary := os.Getenv("SESSIONLESS_ATTACHED_WORKER_BINARY")
	docker := os.Getenv("ATTACHED_WORKER_OCI_DOCKER_PATH")
	host := os.Getenv("ATTACHED_WORKER_OCI_DOCKER_HOST")
	engine := os.Getenv("ATTACHED_WORKER_OCI_ENGINE_ID")
	image := os.Getenv("ATTACHED_WORKER_OCI_IMAGE")
	serviceImage := os.Getenv("SESSIONLESS_ROOTLESS_IMAGE")
	if binary == "" || docker == "" || host != fmt.Sprintf("unix:///run/user/%d/docker.sock", os.Getuid()) ||
		engine == "" || image == "" || serviceImage == "" {
		t.Fatal("exact binary, OCI client/host/engine/image and service image are required")
	}
	binaryHash := fileSHA256(t, binary)
	dockerHash := fileSHA256(t, docker)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	root, err := os.MkdirTemp("/tmp", "aw165-rootless-")
	if err != nil {
		t.Fatal(err)
	}
	safeToRemove := true
	t.Cleanup(func() {
		if !safeToRemove {
			t.Errorf("preserving test-owned root after incomplete service cleanup: %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove finished rootless test root: %v", err)
		}
	})
	for _, name := range []string{"activation", "materialized", "scratch", "oci-config"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	workerID := "worker-rootless-" + hex.EncodeToString(nonce[:])
	now := time.Now().UTC()
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1}}
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: workerID, EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "rootless-integration",
		HarnessName: "synthetic", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: mustDecodeSHA256(t, dockerHash),
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
	peer := &commandSyntheticPeer{public: public, offer: offer, now: now,
		terminal: make(chan attachedworkerprotocol.TerminalV1, 1)}
	server, trust := rootlessPeerServer(t, peer, now)
	peer.binding = attachedworkerprotocol.AttemptBindingV1{
		RunID:           "run-rootless-" + hex.EncodeToString(nonce[:]),
		AttemptID:       "attempt-rootless-" + hex.EncodeToString(nonce[:]),
		LeaseID:         "lease-rootless-" + hex.EncodeToString(nonce[:]),
		LeaseGeneration: 7, FenceToken: "fence-rootless-" + hex.EncodeToString(nonce[:]),
		ExpiresAtUnixMicro: now.Add(30 * time.Minute).UnixMicro(),
		ContextDigest:      bytes.Repeat([]byte{0x61}, sha256.Size), CapabilityDigest: capabilityDigest,
		PolicyDigest: bytes.Repeat([]byte{0x62}, sha256.Size),
	}
	if err := peer.binding.Validate(); err != nil {
		t.Fatal(err)
	}
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: server.URL,
		TenantID: "tenant-rootless", OwnerUserID: "owner-rootless", WorkerID: domain.AttachedWorkerID(workerID),
		EnrollmentGeneration: 1, IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{
			DockerPath: docker, DockerSHA256: dockerHash, CLIConfigDir: filepath.Join(root, "oci-config"),
			Host: host, EngineID: engine, InstallationID: "install-rootless-" + hex.EncodeToString(nonce[:]),
			Boundary: attachedworkerlocal.BoundaryLinuxRootless, Image: image,
			UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024,
			MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10,
		},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: docker, SHA256: dockerHash},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
	}
	secret := attachedworkerlocal.SecretRecordV1{
		Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: private,
	}
	stateRoot := filepath.Join(root, "state")
	store, err := attachedworkerlocal.NewStore(stateRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(ctx, manifest, secret); err != nil {
		t.Fatal(err)
	}
	installationDigest, err := attachedworkeractivation.InstallationDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var executableDigest attachedworkerdaemon.ExecutableDigest
	copy(executableDigest[:], capability.HarnessExecutableDigest)
	profile := attachedworkeractivation.ProfileV1{
		Version: 1, Mode: "synthetic-denied", ManifestRevision: 1,
		ControlPlaneOrigin: server.URL, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, InstallationSHA256: installationDigest,
		ExpectedWorkerRevision: 1, Capability: capability,
		LocalProfile: attachedworkerdaemontransport.LocalProfileV1{
			Name: "synthetic", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest)),
			Executable: docker, ExecutableDigest: executableDigest,
		},
		TLSRootPEMPath:      filepath.Join(root, "activation", "trust.pem"),
		MaterializationRoot: filepath.Join(root, "materialized"), ScratchRoot: filepath.Join(root, "scratch"),
		MaxInputBytes: 4096,
	}
	profilePath := filepath.Join(root, "activation", "profile.json")
	if err := os.WriteFile(profile.TLSRootPEMPath, trust, 0o600); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profilePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	config := attachedworkerpackage.Config{
		Mode: attachedworkerpackage.ModeRootlessContainer, StateRoot: stateRoot,
		InstallDir: filepath.Join(root, "units"), BinaryPath: binary,
		BinarySHA256: binaryHash, ContainerImage: serviceImage, ActivationProfile: profilePath,
	}
	plan, err := attachedworkerpackage.Plan(ctx, config, 0)
	if err != nil {
		t.Fatalf("plan activated rootless package: %v", err)
	}
	if _, err := attachedworkerpackage.Apply(ctx, config, plan); err != nil {
		t.Fatalf("stage activated rootless package: %v", err)
	}
	registration, err := attachedworkerpackage.NativePlan(ctx, config, attachedworkerpackage.NativeRegister, 1)
	if err != nil {
		t.Fatalf("plan exact user-service registration: %v", err)
	}
	safeToRemove = false
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		inspection, err := attachedworkerpackage.NativeInspect(cleanupCtx, config)
		if err == nil && inspection.Status == "unregistered" && !inspection.OSActive && !inspection.ContainerPresent {
			safeToRemove = true
			return
		}
		if err != nil || inspection.Status != "registered" && inspection.Status != "reconciliation_required" {
			t.Errorf("preserving rootless test root; cannot inspect exact registered unit: %+v %v", inspection, err)
			return
		}
		show := exec.CommandContext(cleanupCtx, "/usr/bin/systemctl", "--user", "show",
			filepath.Base(plan.UnitPath), "--property=FragmentPath", "--value")
		fragment, showErr := show.CombinedOutput()
		resolved, resolveErr := filepath.EvalSymlinks(strings.TrimSpace(string(fragment)))
		if showErr != nil || resolveErr != nil || resolved != plan.UnitPath {
			t.Errorf("preserving rootless test root; loaded unit path is not exact: fragment=%q resolved=%q want=%q show=%v resolve=%v",
				strings.TrimSpace(string(fragment)), resolved, plan.UnitPath, showErr, resolveErr)
			return
		}
		if inspection.OSActive {
			var shutdown bytes.Buffer
			graceCtx, endGrace := context.WithTimeout(cleanupCtx, 2*time.Second)
			_ = runWithContext(graceCtx, []string{"stop", "--state-dir", stateRoot,
				"--expected-revision", fmt.Sprint(inspection.ManifestRevision)}, &shutdown)
			endGrace()
			command := exec.CommandContext(cleanupCtx, "/usr/bin/systemctl", "--user", "stop", filepath.Base(plan.UnitPath))
			if output, err := command.CombinedOutput(); err != nil {
				t.Errorf("exact test unit stop failed: %v: %s", err, output)
				return
			}
		}
		inspection, err = attachedworkerpackage.NativeInspect(cleanupCtx, config)
		if err != nil {
			t.Errorf("inspect exact rootless container after stop: %v", err)
			return
		}
		if inspection.ContainerPresent {
			sum := sha256.Sum256([]byte(workerID))
			name := fmt.Sprintf("sessionless-attached-worker-%x", sum[:8])
			command := exec.CommandContext(cleanupCtx, docker, "--host", host,
				"--config", filepath.Join(config.InstallDir, "docker-config"),
				"container", "stop", "--time", "10", name)
			if output, err := command.CombinedOutput(); err != nil {
				t.Errorf("stop exact orphan rootless container: %v: %s", err, output)
				return
			}
		}
		for {
			inspection, err = attachedworkerpackage.NativeInspect(cleanupCtx, config)
			if err == nil && inspection.Status == "registered" && !inspection.OSActive && !inspection.ContainerPresent {
				break
			}
			select {
			case <-cleanupCtx.Done():
				t.Errorf("preserving rootless test root; container or service remained: %+v %v", inspection, err)
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		unregister, err := attachedworkerpackage.NativePlan(cleanupCtx, config, attachedworkerpackage.NativeUnregister, 1)
		if err != nil {
			t.Errorf("plan exact test unit unregister: %v", err)
			return
		}
		if _, err := attachedworkerpackage.ApplyNative(cleanupCtx, config, unregister); err != nil {
			t.Errorf("unregister exact test unit: %v", err)
			return
		}
		final, err := attachedworkerpackage.NativeInspect(cleanupCtx, config)
		if err != nil || final.Status != "unregistered" || final.OSActive || final.ContainerPresent {
			t.Errorf("preserving rootless test root; cleanup unverified: %+v %v", final, err)
			return
		}
		safeToRemove = true
	})
	if _, err := attachedworkerpackage.ApplyNative(ctx, config, registration); err != nil {
		t.Fatalf("register exact rootless user service: %v; root=%s", err, root)
	}
	if _, err := attachedworkerpackage.NativeStart(ctx, config, 1, 1); err != nil {
		t.Fatalf("start activated rootless service: %v", err)
	}
	select {
	case terminal := <-peer.terminal:
		if terminal.Status != attachedworkerprotocol.TerminalFailed || terminal.Result != attachedworkerprotocol.TerminalResultFailed {
			t.Errorf("rootless denied-input terminal=%+v, want failed/failed", terminal)
		}
	case <-ctx.Done():
		peer.mu.Lock()
		steps, denied, lastError := peer.steps, peer.denied, peer.lastError
		peer.mu.Unlock()
		t.Fatalf("activated rootless service missed accepted attempt: steps=%d denied=%d peer_error=%q: %v", steps, denied, lastError, ctx.Err())
	}
	var status bytes.Buffer
	if code := runWithContext(ctx, []string{"live-status", "--state-dir", stateRoot}, &status); code != 0 {
		t.Fatalf("activated rootless live-status code=%d output=%s", code, status.String())
	}
	for _, command := range []string{"drain", "stop"} {
		var output bytes.Buffer
		if code := runWithContext(ctx, []string{command, "--state-dir", stateRoot,
			"--expected-revision", "2"}, &output); code != 0 {
			t.Fatalf("activated rootless %s code=%d output=%s", command, code, output.String())
		}
	}
	peer.mu.Lock()
	steps, denied, challenges, activations := peer.steps, peer.denied, peer.challenges, peer.activations
	peer.mu.Unlock()
	if steps != 3 || denied != 1 || challenges != 1 || activations != 1 {
		t.Errorf("rootless signed attempt proof: steps=%d denied=%d challenge=%d activate=%d", steps, denied, challenges, activations)
	}
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		t.Fatalf("noncanonical pinned executable %s: %v", path, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func mustDecodeSHA256(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func rootlessPeerServer(t *testing.T, peer *commandSyntheticPeer, now time.Time) (*httptest.Server, []byte) {
	t.Helper()
	// Selecting the runner's route does not send a packet. The TLS listener is
	// bound to that explicit non-loopback address so Docker bridge traffic can
	// reach it without modifying production service networking.
	route, err := net.Dial("udp4", "192.0.2.1:443")
	if err != nil {
		t.Fatalf("select runner IPv4 route: %v", err)
	}
	ip := route.LocalAddr().(*net.UDPAddr).IP
	if err := route.Close(); err != nil {
		t.Fatal(err)
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.To4() == nil {
		t.Fatalf("runner route selected non-routable IPv4: %s", ip)
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		t.Fatalf("bind runner TLS peer on %s: %v", ip, err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "sessionless-rootless-test"},
		NotBefore: now.Add(-time.Hour),
		NotAfter:  now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true, IsCA: true,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{ip}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(peer.serveHTTP))
	if err := server.Listener.Close(); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	server.Listener = listener
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	if !strings.Contains(server.URL, ip.String()) {
		t.Fatalf("TLS peer origin %q does not contain runner IPv4 %s", server.URL, ip)
	}
	return server, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
