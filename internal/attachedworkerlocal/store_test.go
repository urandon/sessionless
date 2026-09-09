package attachedworkerlocal

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

var testTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func newFixture(t *testing.T) (*Store, ManifestV1, SecretRecordV1) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "state")
	executable := filepath.Join(parent, "docker")
	executableBytes := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(executable, executableBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	executableDigest := fmt.Sprintf("%x", sha256.Sum256(executableBytes))
	cliConfig := filepath.Join(parent, "docker-config")
	if err := os.Mkdir(cliConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	now := testTime
	store, err := NewStore(root, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	manifest := ManifestV1{Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: OCIConfigV1{DockerPath: executable, DockerSHA256: executableDigest, CLIConfigDir: cliConfig,
			Host: "unix:///run/docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-001", Boundary: BoundaryLinuxRootless,
			Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024,
			MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness:   HarnessConfigV1{Executable: executable, SHA256: executableDigest, Arguments: []string{"--attached"}},
		Lifecycle: LifecycleActive, CreatedAt: now, UpdatedAt: now}
	secret := SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: private}
	if runtime.GOOS == "darwin" {
		manifest.OCI.Boundary = BoundaryDarwinVM
	}
	if err := validateOrigin(manifest.ControlPlaneOrigin); err != nil {
		t.Fatalf("fixture origin: %v", err)
	}
	if err := validateOCI(manifest.OCI); err != nil {
		t.Fatalf("fixture oci: %v", err)
	}
	if err := validateHarness(manifest.Harness); err != nil {
		t.Fatalf("fixture harness: %v", err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("fixture manifest: %v fp=%s tenant=%q owner=%q worker=%q", err, manifest.IdentityKeyFingerprint, manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID)
	}
	if err := secret.Validate(manifest); err != nil {
		t.Fatalf("fixture secret: %v", err)
	}
	return store, manifest, secret
}

func initializeFixture(t *testing.T) (*Store, ManifestV1, SecretRecordV1) {
	t.Helper()
	store, manifest, secret := newFixture(t)
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	return store, manifest, secret
}

func TestStoreCreateLoadUpdate(t *testing.T) {
	store, manifest, secret := initializeFixture(t)
	got, err := store.Load(context.Background())
	if err != nil || !reflect.DeepEqual(got, manifest) {
		t.Fatalf("load=%+v err=%v", got, err)
	}
	loadedSecret, err := store.LoadSecret(context.Background())
	if err != nil || string(loadedSecret.IdentityPrivateKey) != string(secret.IdentityPrivateKey) {
		t.Fatalf("secret err=%v", err)
	}
	next := manifest
	next.Revision = 2
	next.UpdatedAt = testTime.Add(time.Minute)
	next.ConnectionGeneration = 1
	nextSecret := secret
	nextSecret.ManifestRevision = 2
	nextSecret.ConnectionGeneration = 1
	nextSecret.ConnectionSecret = []byte(strings.Repeat("x", 32))
	if err := store.Update(context.Background(), 1, next, nextSecret); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Load(context.Background()); err != nil || got.Revision != 2 {
		t.Fatalf("updated load=%+v err=%v", got, err)
	}
	if err := store.Update(context.Background(), 1, next, nextSecret); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("stale update=%v", err)
	}
}

func TestRuntimeLeaseOwnsGenerationUpdateWithoutSecondOwner(t *testing.T) {
	store, manifest, secret := initializeFixture(t)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	next := manifest
	next.Revision++
	next.ConnectionGeneration++
	next.UpdatedAt = manifest.UpdatedAt.Add(time.Minute)
	nextSecret := cloneSecret(secret)
	nextSecret.ManifestRevision = next.Revision
	nextSecret.ConnectionGeneration = next.ConnectionGeneration
	nextSecret.ConnectionSecret = []byte(strings.Repeat("s", 32))
	if err := lease.Update(context.Background(), manifest.Revision, next, nextSecret); err != nil {
		t.Fatal(err)
	}
	snapshot, err := lease.LoadSnapshot(context.Background())
	if err != nil || snapshot.Manifest.Revision != next.Revision || snapshot.Manifest.ConnectionGeneration != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot.Manifest, err)
	}
	loadedSecret, err := lease.LoadSecret(context.Background())
	if err != nil || loadedSecret.ManifestRevision != next.Revision || loadedSecret.ConnectionGeneration != 1 ||
		!bytes.Equal(loadedSecret.ConnectionSecret, nextSecret.ConnectionSecret) {
		t.Fatalf("secret generation=%d revision=%d err=%v", loadedSecret.ConnectionGeneration, loadedSecret.ManifestRevision, err)
	}
	if err := store.Update(context.Background(), next.Revision, next, nextSecret); !errors.Is(err, ErrStateBusy) {
		t.Fatalf("second runtime owner error=%v", err)
	}
}

func TestUpdateEnforcesMonotonicGenerationsAndSecrets(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ManifestV1, *SecretRecordV1)
	}{
		{name: "generation jump", mutate: func(manifest *ManifestV1, _ *SecretRecordV1) { manifest.EnrollmentGeneration = 3 }},
		{name: "secret changes without generation", mutate: func(_ *ManifestV1, secret *SecretRecordV1) { secret.IdentityPrivateKey[0] ^= 1 }},
		{name: "generation changes without secret", mutate: func(manifest *ManifestV1, _ *SecretRecordV1) { manifest.EnrollmentGeneration = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, manifest, secret := initializeFixture(t)
			next, nextSecret := manifest, cloneSecret(secret)
			next.Revision, next.UpdatedAt, nextSecret.ManifestRevision = 2, testTime.Add(time.Minute), 2
			test.mutate(&next, &nextSecret)
			if err := store.Update(context.Background(), 1, next, nextSecret); !errors.Is(err, ErrStateConflict) && !errors.Is(err, ErrInvalidState) {
				t.Fatalf("Update() error = %v, want conflict or invalid", err)
			}
		})
	}
}

func TestStrictJSONRejectsMalformedUnknownDuplicateOversized(t *testing.T) {
	valid := []byte(`{"version":1}`)
	deep := []byte(strings.Repeat(`{"x":`, strictJSONMaxDepth) + `1` + strings.Repeat(`}`, strictJSONMaxDepth))
	members := make([]string, strictJSONMaxMembers+1)
	for index := range members {
		members[index] = fmt.Sprintf(`"k%d":0`, index)
	}
	items := make([]string, strictJSONMaxArrayItems+1)
	for index := range items {
		items[index] = "0"
	}
	for name, input := range map[string][]byte{
		"malformed": []byte(`{"version":`), "unknown": []byte(`{"version":1,"extra":2}`),
		"duplicate": []byte(`{"version":1,"Version":1}`), "oversized": []byte(`{"version":1,"x":"` + strings.Repeat("a", strictJSONMaxStringBytes+1) + `"}`),
		"bom": append([]byte{0xef, 0xbb, 0xbf}, valid...), "invalid_utf8": {0xff},
		"trailing": []byte(`{"version":1}{"version":1}`), "null": []byte(`null`),
		"negative": []byte(`{"version":-1}`), "fraction": []byte(`{"version":1.0}`),
		"leading_zero": []byte(`{"version":01}`), "exponent": []byte(`{"version":1e0}`),
		"depth": deep, "members": []byte(`{` + strings.Join(members, ",") + `}`),
		"array_items": []byte(`[` + strings.Join(items, ",") + `]`),
	} {
		t.Run(name, func(t *testing.T) {
			var target ManifestV1
			if err := decodeStrictJSON(input, &target); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	var target struct {
		Version uint32 `json:"version"`
	}
	if err := decodeStrictJSON(valid, &target); err != nil {
		t.Fatal(err)
	}
}

func FuzzStrictJSONParser(f *testing.F) {
	for _, seed := range [][]byte{[]byte(`{"version":1}`), []byte(`null`), {0xff}, []byte(`{"version":1,"Version":2}`)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		var target struct {
			Version uint32 `json:"version"`
		}
		err := decodeStrictJSON(input, &target)
		if err != nil && !errors.Is(err, ErrInvalidState) {
			t.Fatalf("decode error = %v for %q", err, input)
		}
	})
}

func TestInitializeCreatesExactPrivateInventoryAndReadOnlyCommandsDoNotMutate(t *testing.T) {
	store, _, _ := initializeFixture(t)
	want := []string{ManifestFileName, RuntimeLockFileName, SecretFileName, StateLockFileName}
	entries, err := os.ReadDir(store.root)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	before := make(map[string][]byte)
	for _, entry := range entries {
		got = append(got, entry.Name())
		info, statErr := entry.Info()
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("entry %q info=%v err=%v", entry.Name(), info, statErr)
		}
		before[entry.Name()], err = os.ReadFile(filepath.Join(store.root, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory = %v, want %v", got, want)
	}
	_, _ = store.Check(context.Background())
	_, _ = store.Status(context.Background())
	_, _ = store.Doctor(context.Background())
	_, _ = store.UninstallPlan(context.Background())
	after, err := os.ReadDir(store.root)
	if err != nil || len(after) != len(entries) {
		t.Fatalf("after entries=%v err=%v", after, err)
	}
	for _, entry := range after {
		contents, readErr := os.ReadFile(filepath.Join(store.root, entry.Name()))
		if readErr != nil || !reflect.DeepEqual(contents, before[entry.Name()]) {
			t.Fatalf("read-only command mutated %q: err=%v", entry.Name(), readErr)
		}
	}
	if err := os.Remove(filepath.Join(store.root, RuntimeLockFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrStateIncomplete) {
		t.Fatalf("missing runtime lock error = %v", err)
	}

	store2, manifest, secret := newFixture(t)
	if err := os.Mkdir(store2.root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store2.root, "foreign"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store2.Initialize(context.Background(), manifest, secret); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("foreign inventory error = %v", err)
	}
}

func TestInitializeMakesRootCreationDurableOrAmbiguous(t *testing.T) {
	store, manifest, secret := newFixture(t)
	store.syncParentOverride = func() error { return ErrLocalIO }
	if err := store.Initialize(context.Background(), manifest, secret); !errors.Is(err, ErrStateAmbiguous) {
		t.Fatalf("Initialize() error = %v, want ambiguous", err)
	}
	if info, err := os.Stat(store.root); err != nil || !info.IsDir() {
		t.Fatalf("ambiguous root info=%v err=%v", info, err)
	}
	store.syncParentOverride = nil
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatalf("resumable Initialize() error = %v", err)
	}
	if _, err := store.Load(context.Background()); err != nil {
		t.Fatalf("Load() after resumed initialization = %v", err)
	}
}

func TestUnknownInventoryFailsClosedAndIsNeverOmittedAsOK(t *testing.T) {
	store, _, _ := initializeFixture(t)
	unknown := filepath.Join(store.root, "future-v2-state.json")
	if err := os.WriteFile(unknown, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Load() error = %v, want invalid", err)
	}
	plan, err := store.UninstallPlan(context.Background())
	if !errors.Is(err, ErrInvalidState) || plan.Code != CodeInvalid || plan.DestructiveAction != "not_performed" {
		t.Fatalf("UninstallPlan()=%+v err=%v", plan, err)
	}
	if _, err := os.Stat(unknown); err != nil {
		t.Fatalf("uninstall plan mutated unknown entry: %v", err)
	}
}

func TestStoreRejectsSymlinkAndModeSubstitution(t *testing.T) {
	store, _, _ := initializeFixture(t)
	manifestPath := filepath.Join(store.root, ManifestFileName)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(outside, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, manifestPath); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("symlink err=%v", err)
	}
	if err := os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("mode err=%v", err)
	}
}

func TestStoreRejectsSecretLockAndRootSubstitution(t *testing.T) {
	for _, name := range []string{SecretFileName, StateLockFileName, RuntimeLockFileName} {
		t.Run(name, func(t *testing.T) {
			store, _, _ := initializeFixture(t)
			path := filepath.Join(store.root, name)
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(outside, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidState) && !errors.Is(err, ErrStateIncomplete) {
				t.Fatalf("Load() error = %v, want invalid or incomplete", err)
			}
		})
	}
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	realRoot := filepath.Join(parent, "real")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedRoot := filepath.Join(parent, "linked")
	if err := os.Symlink(realRoot, linkedRoot); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(linkedRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidRoot) {
		t.Fatalf("symlink root error = %v", err)
	}
}

func TestStoreRejectsInterruptedReplacementAndFutureManifest(t *testing.T) {
	store, _, _ := initializeFixture(t)
	if err := os.WriteFile(filepath.Join(store.root, temporaryFilePrefix+"orphan"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrStateIncomplete) {
		t.Fatalf("orphan replacement error = %v", err)
	}
	store, _, _ = initializeFixture(t)
	path := filepath.Join(store.root, ManifestFileName)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents = []byte(strings.Replace(string(contents), `"version":1`, `"version":2`, 1))
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("future manifest error = %v", err)
	}
}

func TestFutureSecretAndPartialUpdateFailClosed(t *testing.T) {
	store, manifest, secret := initializeFixture(t)
	secret.Version = 99
	encoded, _ := encodeStrictJSON(secret)
	if err := os.WriteFile(filepath.Join(store.root, SecretFileName), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); !errors.Is(err, ErrStateIncomplete) {
		t.Fatalf("future secret err=%v", err)
	}
	_, manifest, secret = initializeFixture(t)
	next := manifest
	next.Revision = 2
	next.UpdatedAt = testTime.Add(time.Minute)
	nextSecret := secret
	nextSecret.ManifestRevision = 2
	if err := os.Remove(filepath.Join(store.root, ManifestFileName)); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(context.Background(), 1, next, nextSecret); !errors.Is(err, ErrStateMissing) && !errors.Is(err, ErrStateIncomplete) {
		t.Fatalf("partial update err=%v", err)
	}
}

func TestRuntimeFlockConcurrency(t *testing.T) {
	store, _, _ := initializeFixture(t)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if _, err := store.AcquireRuntime(context.Background()); !errors.Is(err, ErrStateBusy) {
		t.Fatalf("busy err=%v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	firstHeld := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		l, e := store.AcquireRuntime(context.Background())
		if e == nil {
			close(firstHeld)
			<-release
			_ = l.Close()
		}
		result <- e
	}()
	<-firstHeld
	if _, err := store.AcquireRuntime(context.Background()); !errors.Is(err, ErrStateBusy) {
		t.Fatalf("concurrent busy=%v", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeObservationIsLeaseBoundMonotonicAndVisible(t *testing.T) {
	store, manifest, _ := initializeFixture(t)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	observation := RuntimeObservationV1{Version: 1, Revision: 1, ManifestRevision: manifest.Revision,
		State: attachedworkerdaemon.DaemonRunning, Active: true, Accepted: 1, ObservedAt: testTime.Add(time.Minute)}
	if err := lease.PersistObservation(context.Background(), observation); err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(context.Background())
	if err != nil || status.DaemonObservation != "observed_local" || status.DaemonState != "running" ||
		status.ObservationRevision != 1 || status.ObservedAt == nil || !status.ObservedAt.Equal(observation.ObservedAt) {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	stale := observation
	stale.Revision = 2
	stale.Accepted = 0
	if err := lease.PersistObservation(context.Background(), stale); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("counter regression error = %v", err)
	}
	if err := lease.RetireObservation(context.Background(), 2); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("wrong retirement revision error = %v", err)
	}
	if err := lease.RetireObservation(context.Background(), 1); err != nil {
		t.Fatalf("retire observation: %v", err)
	}
	if err := lease.RetireObservation(context.Background(), 1); err != nil {
		t.Fatalf("repeat retirement: %v", err)
	}
	status, err = store.Status(context.Background())
	if err != nil || status.DaemonObservation != "unknown" || status.ObservationRevision != 0 {
		t.Fatalf("retired status=%+v err=%v", status, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.PersistObservation(context.Background(), observation); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("closed lease error = %v", err)
	}
}

func TestUpdateAndLogoutRejectBusyRuntime(t *testing.T) {
	store, manifest, secret := initializeFixture(t)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	next, nextSecret := manifest, cloneSecret(secret)
	next.Revision, next.UpdatedAt, nextSecret.ManifestRevision = 2, testTime.Add(time.Minute), 2
	if err := store.Update(context.Background(), 1, next, nextSecret); !errors.Is(err, ErrStateBusy) {
		t.Fatalf("Update() error = %v, want busy", err)
	}
	if result, err := store.Logout(context.Background(), LogoutInputV1{ExpectedRevision: 1, RequestID: "logout-busy"}); !errors.Is(err, ErrStateBusy) || result.Code != CodeBusy {
		t.Fatalf("Logout() result=%+v error=%v, want busy", result, err)
	}
}

func TestObservationRetiredByUpdateAndDurabilityAmbiguityIsExplicit(t *testing.T) {
	store, manifest, secret := initializeFixture(t)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.PersistObservation(context.Background(), RuntimeObservationV1{Version: 1, Revision: 1,
		ManifestRevision: 1, State: attachedworkerdaemon.DaemonStopped, ObservedAt: testTime.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	next, nextSecret := manifest, cloneSecret(secret)
	next.Revision, next.UpdatedAt, nextSecret.ManifestRevision = 2, testTime.Add(2*time.Minute), 2
	store.syncRootOverride = func() error { return ErrLocalIO }
	if err := store.Update(context.Background(), 1, next, nextSecret); !errors.Is(err, ErrStateAmbiguous) || Code(err) != CodeAmbiguous {
		t.Fatalf("Update() error=%v code=%q, want ambiguous", err, Code(err))
	}
}

func TestLogoutIdempotenceConflictAndReconciliation(t *testing.T) {
	store, _, _ := initializeFixture(t)
	input := LogoutInputV1{ExpectedRevision: 1, RequestID: "logout-001"}
	first, err := store.Logout(context.Background(), input)
	if err != nil || first.Code != CodeOK {
		t.Fatalf("logout=%+v err=%v", first, err)
	}
	second, err := store.Logout(context.Background(), input)
	if err != nil || second != first {
		t.Fatalf("repeat=%+v err=%v", second, err)
	}
	if _, err := store.Logout(context.Background(), LogoutInputV1{ExpectedRevision: 1, RequestID: "logout-002"}); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if _, err := store.LoadSecret(context.Background()); !errors.Is(err, ErrSecretRetired) {
		t.Fatalf("retired=%v", err)
	}

	store2, manifest, secret := initializeFixture(t)
	intent := logoutIntentV1{Version: 1, ExpectedRevision: 1, NextRevision: 2, RequestID: "logout-003", RequestedAt: testTime, TenantID: string(manifest.TenantID), OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID)}
	encoded, _ := encodeStrictJSON(intent)
	if err := os.WriteFile(filepath.Join(store2.root, LogoutIntentFileName), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(store2.root, SecretFileName)); err != nil {
		t.Fatal(err)
	}
	result, err := store2.Logout(context.Background(), LogoutInputV1{ExpectedRevision: 1, RequestID: "logout-003"})
	if err != nil || result.Code != CodeOK {
		t.Fatalf("reconcile=%+v err=%v", result, err)
	}
	_ = secret
}

func TestUninstallPlanRedactedAndIdempotent(t *testing.T) {
	store, _, _ := initializeFixture(t)
	one, err := store.UninstallPlan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	two, err := store.UninstallPlan(context.Background())
	if err != nil || !reflect.DeepEqual(one, two) {
		t.Fatalf("plans differ: %v %+v %+v", err, one, two)
	}
	if strings.Contains(strings.ToLower(string(mustJSON(t, one))), "private") || strings.Contains(string(mustJSON(t, one)), "identity") {
		t.Fatal("plan contains secret-like fields")
	}
	if !one.RuntimeStopped || len(one.Entries) != 7 {
		t.Fatalf("plan=%+v", one)
	}
	for index := 1; index < len(one.Entries); index++ {
		if one.Entries[index-1].Name >= one.Entries[index].Name {
			t.Fatalf("inventory is not sorted: %+v", one.Entries)
		}
	}
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	busy, err := store.UninstallPlan(context.Background())
	if err != nil || busy.RuntimeStopped {
		t.Fatalf("busy plan=%+v err=%v", busy, err)
	}
}

func TestCheckAndDoctorVerifyPinnedArtifactsWithoutLiveProbes(t *testing.T) {
	store, manifest, _ := initializeFixture(t)
	check, err := store.Check(context.Background())
	if err != nil || check.Code != CodeOK {
		t.Fatalf("Check()=%+v err=%v", check, err)
	}
	doctor, err := store.Doctor(context.Background())
	if err != nil || doctor.Code != CodeOK || doctor.DockerArtifactState != "verified" ||
		doctor.HarnessArtifactState != "verified" || doctor.CLIConfigState != "verified" ||
		doctor.EngineObservation != "unknown" || doctor.ExternalVerification != "unknown" ||
		doctor.ServerConnectionState != "unknown" {
		t.Fatalf("Doctor()=%+v err=%v", doctor, err)
	}
	if err := os.WriteFile(manifest.OCI.DockerPath, []byte("changed"), 0o700); err != nil {
		t.Fatal(err)
	}
	check, err = store.Check(context.Background())
	if !errors.Is(err, ErrStateConflict) || check.Code != CodeConflict {
		t.Fatalf("drift Check()=%+v err=%v", check, err)
	}
}

func TestLogoutReportsDurabilityAmbiguity(t *testing.T) {
	store, _, _ := initializeFixture(t)
	store.syncRootOverride = func() error { return ErrLocalIO }
	result, err := store.Logout(context.Background(), LogoutInputV1{ExpectedRevision: 1, RequestID: "logout-ambiguous"})
	if !errors.Is(err, ErrStateAmbiguous) || result.Code != CodeAmbiguous {
		t.Fatalf("Logout()=%+v err=%v", result, err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := encodeStrictJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
