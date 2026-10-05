package attachedworkeractivation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestReadProfileRequiresPrivateExactSyntheticAuthority(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	if err := profile.Capability.Validate(); err != nil {
		t.Fatalf("fixture capability: %v", err)
	}
	if err := profile.LocalProfile.CapabilityDigest.Validate(); err != nil {
		t.Fatalf("fixture digest: %v", err)
	}
	path := filepath.Join(root, "activation.json")
	write := func(data []byte, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	write(encoded, 0o600)
	if _, err := readPrivateFile(path, maxProfileBytes); err != nil {
		t.Fatalf("private file read: %v", err)
	}
	var decoded ProfileV1
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("fixture json: %v", err)
	}
	if err := decoded.Capability.Validate(); err != nil {
		t.Fatalf("decoded capability: %v", err)
	}
	got, err := ReadProfile(path)
	if err != nil || got.Mode != "synthetic-denied" || got.LocalProfile.CapabilityDigest != profile.LocalProfile.CapabilityDigest {
		t.Fatalf("read private profile: mode=%q error=%v", got.Mode, err)
	}
	_, initialDigest, err := ReadProfileWithDigest(path)
	if err != nil || len(initialDigest) != 64 {
		t.Fatalf("initial profile digest=%q error=%v", initialDigest, err)
	}
	changed := profile
	changed.MaxInputBytes--
	changedBytes, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	write(changedBytes, 0o600)
	_, changedDigest, err := ReadProfileWithDigest(path)
	if err != nil || changedDigest == initialDigest {
		t.Fatalf("same-path changed profile digest=%q error=%v", changedDigest, err)
	}
	write(encoded, 0o644)
	if _, err := ReadProfile(path); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("world-readable profile error = %v", err)
	}
	write(append(encoded[:len(encoded)-1], []byte(`,"unknown_authority":true}`)...), 0o600)
	if _, err := ReadProfile(path); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("unknown authority error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	privateTarget := filepath.Join(root, "target.json")
	if err := os.WriteFile(privateTarget, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(privateTarget, path); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadProfile(path); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("symlink profile error = %v", err)
	}
}

func TestActivationProfileFencesStaleIdentityBeforeNetwork(t *testing.T) {
	profile := testProfile()
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: profile.ManifestRevision, ControlPlaneOrigin: profile.ControlPlaneOrigin,
		TenantID: profile.TenantID, OwnerUserID: profile.OwnerUserID, WorkerID: profile.WorkerID,
		EnrollmentGeneration: profile.EnrollmentGeneration, ConnectionGeneration: profile.ConnectionGeneration,
		Lifecycle: attachedworkerlocal.LifecycleActive,
	}
	if !matchesManifest(profile, manifest) {
		t.Fatal("exact profile did not match local manifest")
	}
	reconnected := manifest
	reconnected.Revision += 2
	reconnected.ConnectionGeneration += 2
	if !matchesManifest(profile, reconnected) {
		t.Fatal("profile rejected connection-only restart progression")
	}
	for _, mutate := range []func(*attachedworkerlocal.ManifestV1){
		func(m *attachedworkerlocal.ManifestV1) { m.Revision++ },
		func(m *attachedworkerlocal.ManifestV1) { m.TenantID = "other-tenant" },
		func(m *attachedworkerlocal.ManifestV1) { m.OwnerUserID = "other-owner" },
		func(m *attachedworkerlocal.ManifestV1) { m.WorkerID = "other-worker" },
		func(m *attachedworkerlocal.ManifestV1) { m.EnrollmentGeneration++ },
		func(m *attachedworkerlocal.ManifestV1) { m.ConnectionGeneration++ },
		func(m *attachedworkerlocal.ManifestV1) { m.Revision += 2; m.ConnectionGeneration++ },
		func(m *attachedworkerlocal.ManifestV1) { m.ControlPlaneOrigin = "https://other.example" },
		func(m *attachedworkerlocal.ManifestV1) { m.OCI.Image = "other-image" },
		func(m *attachedworkerlocal.ManifestV1) { m.Harness.Executable = "/other/harness" },
		func(m *attachedworkerlocal.ManifestV1) { m.Lifecycle = attachedworkerlocal.LifecycleLoggedOut },
	} {
		candidate := manifest
		mutate(&candidate)
		if matchesManifest(profile, candidate) {
			t.Fatal("stale or cross-owner profile matched manifest")
		}
	}
	if _, err := Connect(context.Background(), nil, profile); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("nil store error = %v", err)
	}
}

func testProfile() ProfileV1 {
	installationDigest, err := InstallationDigestV1(attachedworkerlocal.ManifestV1{})
	if err != nil {
		panic(err)
	}
	harness := sha256.Sum256([]byte("pinned synthetic harness"))
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: "worker-1", EnrollmentGeneration: 1, Revision: 1,
		ProtocolOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "test-build",
		HarnessName: "synthetic", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: harness[:],
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{
			attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary,
			attachedworkerprotocol.IsolationProcessBoundary,
		},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress,
			attachedworkerprotocol.FeatureReconnect,
		}, MaxConcurrentAttempts: 1,
	}
	digest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		panic(err)
	}
	return ProfileV1{
		Version: 1, Mode: "synthetic-denied", ManifestRevision: 1,
		ControlPlaneOrigin: "https://control.example", TenantID: "tenant-1", OwnerUserID: "owner-1",
		WorkerID: "worker-1", EnrollmentGeneration: 1, ExpectedWorkerRevision: 1,
		InstallationSHA256: installationDigest,
		Capability:         capability,
		LocalProfile: attachedworkerdaemontransport.LocalProfileV1{
			Name: "synthetic", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)),
			Executable: "/pinned/harness", ExecutableDigest: attachedworkerdaemon.ExecutableDigest(harness),
		},
		MaterializationRoot: "/private/materialized", ScratchRoot: "/private/scratch", MaxInputBytes: 4096,
	}
}
