package attachedworkeronboarding

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type refusingRandom struct{}

func (refusingRandom) Read([]byte) (int, error) {
	return 0, errors.New("unexpected new key generation")
}
func onboardingFixture(t *testing.T) (*attachedworkerlocal.Store, string, GrantV1, SetupV1) {
	t.Helper()
	parent, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(parent, 0700); e != nil {
		t.Fatal(e)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	bootstrap := bytes.Repeat([]byte{0x39}, 32)
	grant := GrantV1{Version: 1, ControlPlaneOrigin: "https://control.example", BootstrapSecret: bootstrap, Enrollment: domain.AttachedWorkerEnrollment{TenantID: "tenant-170", OwnerUserID: "owner-170", ID: "enroll-170", WorkerID: "worker-170", DisplayName: "own worker", Audience: "attached-onboarding", BootstrapDigest: domain.DigestWorkerBootstrap(bootstrap), CreatedAt: now, ExpiresAt: now.Add(time.Hour), RetainUntil: now.Add(2 * time.Hour), Revision: 1}}
	store, e := attachedworkerlocal.NewStore(filepath.Join(parent, "installation"), func() time.Time { return now })
	if e != nil {
		t.Fatal(e)
	}
	setup := SetupV1{Version: 1, ControlPlaneOrigin: grant.ControlPlaneOrigin, OCI: attachedworkerlocal.OCIConfigV1{DockerPath: filepath.Join(parent, "docker"), DockerSHA256: strings.Repeat("a", 64), CLIConfigDir: filepath.Join(parent, "docker-config"), Host: "unix:///run/docker.sock", EngineID: "engine-170-abcdef", InstallationID: "install-170", Boundary: attachedworkerlocal.BoundaryLinuxRootless, Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10}, Harness: attachedworkerlocal.HarnessConfigV1{Executable: filepath.Join(parent, "harness"), SHA256: strings.Repeat("c", 64), Arguments: []string{"--attached"}}}
	return store, filepath.Join(parent, "pending.json"), grant, setup
}
func receiptForClaim(c ClaimV1) ReceiptV1 {
	g := c.Enrollment
	w := domain.AttachedWorker{TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, ID: g.WorkerID, DisplayName: g.DisplayName, IdentityPublicKey: c.IdentityPublicKey, EnrollmentGeneration: 1, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOffline, Revision: 1, CreatedAt: g.CreatedAt.Add(time.Second), UpdatedAt: g.CreatedAt.Add(time.Second)}
	return ReceiptV1{Version: 1, Worker: w, ResourceID: ResourceIDForWorker(w.TenantID, w.OwnerUserID, w.ID), ActorID: ActorIDForOwner(w.TenantID, w.OwnerUserID), Entitlement: domain.EntitlementUnknown, Quota: domain.ProviderQuotaUnknown}
}

func TestOnboardingEnrollmentResumeAndExactCompletion(t *testing.T) {
	store, pending, grant, setup := onboardingFixture(t)
	ctx := context.Background()
	claim, e := PrepareEnrollment(ctx, store, pending, grant, setup, bytes.NewReader(bytes.Repeat([]byte{4}, 32)))
	if e != nil {
		t.Fatalf("prepare: %v", e)
	}
	again, e := PrepareEnrollment(ctx, store, pending, grant, setup, refusingRandom{})
	if e != nil || !reflect.DeepEqual(claim, again) {
		t.Fatalf("lost delivery resume changed claim: equal=%v err=%v", reflect.DeepEqual(claim, again), e)
	}
	if _, e = store.LoadSnapshot(ctx); !errors.Is(e, attachedworkerlocal.ErrStateMissing) {
		t.Fatalf("prepare initialized installation: %v", e)
	}
	receipt := receiptForClaim(claim)
	wrong := receipt
	wrong.Worker.IdentityPublicKey = bytes.Repeat([]byte{9}, 32)
	if e = CompleteEnrollment(ctx, store, pending, wrong); !errors.Is(e, ErrConflict) {
		t.Fatalf("wrong key accepted: %v", e)
	}
	if e = CompleteEnrollment(ctx, store, pending, receipt); e != nil {
		t.Fatalf("complete: %v", e)
	}
	before, e := store.LoadSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = CompleteEnrollment(ctx, store, pending, receipt); e != nil {
		t.Fatalf("exact complete replay: %v", e)
	}
	after, e := store.LoadSnapshot(ctx)
	if e != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("replay mutated installation: %v", e)
	}
	_, e = PrepareEnrollment(ctx, store, pending, grant, setup, refusingRandom{})
	if e != nil {
		t.Fatalf("same pending persists after complete: %v", e)
	}
	changed := setup
	changed.Harness.Arguments = []string{"--different"}
	if _, e = PrepareEnrollment(ctx, store, pending, grant, changed, refusingRandom{}); !errors.Is(e, ErrConflict) {
		t.Fatalf("changed setup reused key: %v", e)
	}
}
func TestOnboardingIncompleteInstallationDoesNotRekey(t *testing.T) {
	store, pending, grant, setup := onboardingFixture(t)
	state := filepath.Join(filepath.Dir(pending), "installation")
	if e := os.Mkdir(state, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(state, "manifest.json"), []byte("{}"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := PrepareEnrollment(context.Background(), store, pending, grant, setup, refusingRandom{}); e == nil {
		t.Fatal("partial installation accepted")
	}
	if _, e := os.Stat(pending); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("partial installation generated pending identity: %v", e)
	}
}
func TestOnboardingSignedRotationBusyStaleAndReplay(t *testing.T) {
	store, pending, grant, setup := onboardingFixture(t)
	ctx := context.Background()
	claim, e := PrepareEnrollment(ctx, store, pending, grant, setup, bytes.NewReader(bytes.Repeat([]byte{4}, 32)))
	if e != nil {
		t.Fatal(e)
	}
	receipt := receiptForClaim(claim)
	if e = CompleteEnrollment(ctx, store, pending, receipt); e != nil {
		t.Fatal(e)
	}
	rotationPath := filepath.Join(filepath.Dir(pending), "rotation.json")
	lease, e := store.AcquireRuntime(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = PrepareRotation(ctx, store, rotationPath, receipt.Worker, refusingRandom{}); !errors.Is(e, attachedworkerlocal.ErrStateBusy) {
		t.Fatalf("busy prepare: %v", e)
	}
	if e = lease.Close(); e != nil {
		t.Fatal(e)
	}
	stale := receipt.Worker
	stale.EnrollmentGeneration++
	if _, e = PrepareRotation(ctx, store, rotationPath, stale, refusingRandom{}); !errors.Is(e, ErrConflict) {
		t.Fatalf("stale server head accepted: %v", e)
	}
	rotation, e := PrepareRotation(ctx, store, rotationPath, receipt.Worker, bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if e != nil {
		t.Fatalf("prepare rotate: %v", e)
	}
	again, e := PrepareRotation(ctx, store, rotationPath, receipt.Worker, refusingRandom{})
	if e != nil || !reflect.DeepEqual(rotation, again) {
		t.Fatalf("rotation resume changed identity: %v", e)
	}
	if rotation.Validate() != nil || !bytes.Equal(rotation.Worker.IdentityPublicKey, claim.IdentityPublicKey) {
		t.Fatal("dual signed rotation invalid")
	}
	next := receipt
	next.Worker.IdentityPublicKey = rotation.NewPublicKey
	next.Worker.Revision++
	next.Worker.EnrollmentGeneration++
	next.Worker.UpdatedAt = next.Worker.UpdatedAt.Add(time.Second)
	wrong := next
	wrong.Worker.ConnectionGeneration++
	if e = CompleteRotation(ctx, store, rotationPath, wrong); !errors.Is(e, ErrConflict) {
		t.Fatalf("wrong-generation rotation: %v", e)
	}
	lease, e = store.AcquireRuntime(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = CompleteRotation(ctx, store, rotationPath, next); !errors.Is(e, attachedworkerlocal.ErrStateBusy) {
		t.Fatalf("busy complete: %v", e)
	}
	if e = lease.Close(); e != nil {
		t.Fatal(e)
	}
	if e = CompleteRotation(ctx, store, rotationPath, next); e != nil {
		t.Fatalf("complete rotation: %v", e)
	}
	if e = CompleteRotation(ctx, store, rotationPath, next); e != nil {
		t.Fatalf("rotation replay: %v", e)
	}
	secret, e := store.LoadSecret(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer clear(secret.IdentityPrivateKey)
	if !bytes.Equal(ed25519.PrivateKey(secret.IdentityPrivateKey).Public().(ed25519.PublicKey), rotation.NewPublicKey) {
		t.Fatal("new key not installed")
	}
}
func TestOnboardingPrivateCodecAndRedaction(t *testing.T) {
	_, pending, grant, _ := onboardingFixture(t)
	path := filepath.Join(filepath.Dir(pending), "grant.json")
	if e := WriteGrant(path, grant); e != nil {
		t.Fatal(e)
	}
	if e := WriteGrant(path, grant); e != nil {
		t.Fatalf("same bytes replay: %v", e)
	}
	got, e := ReadGrant(path)
	if e != nil || !reflect.DeepEqual(got, grant) {
		t.Fatalf("private roundtrip: %v", e)
	}
	changed := grant
	changed.ControlPlaneOrigin = "https://other.example"
	if e = WriteGrant(path, changed); !errors.Is(e, ErrConflict) {
		t.Fatalf("overwrite accepted: %v", e)
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%x"} {
		if formatted := fmt.Sprintf(format, grant); !strings.Contains(formatted, "REDACTED") {
			t.Fatalf("format %s leaked grant", format)
		}
	}
	public, e := json.Marshal(grant)
	if e != nil || bytes.Contains(public, []byte("bootstrap_secret")) {
		t.Fatalf("JSON leaked grant: %s err=%v", public, e)
	}
	for _, name := range []string{"unknown", "duplicate", "trailing", "oversize", "symlink", "permissions"} {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(filepath.Dir(pending), name+".json")
			switch name {
			case "duplicate":
				b := append([]byte(nil), raw[:len(raw)-1]...)
				b = append(b, []byte(`,"version":1}`)...)
				if e := os.WriteFile(file, b, 0600); e != nil {
					t.Fatal(e)
				}
			case "trailing":
				b := append(append([]byte(nil), raw...), []byte(` {}`)...)
				if e := os.WriteFile(file, b, 0600); e != nil {
					t.Fatal(e)
				}
			case "unknown":
				b := append([]byte(nil), raw[:len(raw)-1]...)
				b = append(b, []byte(`,"unknown":true}`)...)
				if e := os.WriteFile(file, b, 0600); e != nil {
					t.Fatal(e)
				}
			case "oversize":
				if e := os.WriteFile(file, bytes.Repeat([]byte{' '}, MaxPrivateBytes+1), 0600); e != nil {
					t.Fatal(e)
				}
			case "symlink":
				if e := os.Symlink(path, file); e != nil {
					t.Fatal(e)
				}
			case "permissions":
				if e := os.WriteFile(file, raw, 0644); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := ReadGrant(file); e == nil {
				t.Fatalf("%s private boundary accepted", name)
			}
		})
	}
	if _, e = ReadGrant(filepath.Join(filepath.Dir(pending), "missing")); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("missing identity not preserved: %v", e)
	}
	if e = ValidateOrigin("https://control.example?token=secret"); e == nil {
		t.Fatal("secret query origin accepted")
	}
	if ActorIDForOwner("tenant-a", "owner-a") == ActorIDForOwner("tenant-b", "owner-a") || ResourceIDForWorker("tenant-a", "owner-a", "worker-a") == ResourceIDForWorker("tenant-a", "owner-b", "worker-a") {
		t.Fatal("deterministic IDs cross owner scope")
	}
}

func TestOnboardingRotationAfterAttachRetainsGenerationBoundLocalState(t *testing.T) {
	store, pending, grant, setup := onboardingFixture(t)
	ctx := context.Background()
	claim, err := PrepareEnrollment(ctx, store, pending, grant, setup, bytes.NewReader(bytes.Repeat([]byte{4}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	receipt := receiptForClaim(claim)
	if err = CompleteEnrollment(ctx, store, pending, receipt); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := store.LoadSecret(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(secret.IdentityPrivateKey)
	attached := snapshot.Manifest
	attached.Revision++
	attached.ConnectionGeneration = 1
	attached.UpdatedAt = attached.UpdatedAt.Add(time.Hour)
	secret.ManifestRevision = attached.Revision
	secret.ConnectionGeneration = 1
	secret.ConnectionSecret = bytes.Repeat([]byte{0x44}, 32)
	if err = store.Update(ctx, snapshot.Manifest.Revision, attached, secret); err != nil {
		t.Fatalf("test attach generation: %v", err)
	}
	receipt.Worker.ConnectionGeneration = 1
	receipt.Worker.Revision++
	receipt.Worker.UpdatedAt = receipt.Worker.UpdatedAt.Add(time.Second)
	// Persist a valid, test-owned idle continuation through the normal local
	// API, then prove rotation retires it rather than exporting an old claim.
	checkpoint := rotationCheckpoint(t, attached)
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.PersistReconnectCheckpoint(ctx, 0, checkpoint); err != nil {
		lease.Close()
		t.Fatalf("persist old continuation: %v", err)
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	rotationPath := filepath.Join(filepath.Dir(pending), "rotation-after-attach.json")
	rotation, err := PrepareRotation(ctx, store, rotationPath, receipt.Worker, bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatalf("rotation after attachment: %v", err)
	}
	receipt.Worker.IdentityPublicKey = rotation.NewPublicKey
	receipt.Worker.EnrollmentGeneration++
	receipt.Worker.Revision++
	receipt.Worker.UpdatedAt = receipt.Worker.UpdatedAt.Add(time.Second)
	if err = CompleteRotation(ctx, store, rotationPath, receipt); err != nil {
		t.Fatalf("rotation completion after attachment: %v", err)
	}
	next, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nextSecret, err := store.LoadSecret(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(nextSecret.IdentityPrivateKey)
	if next.Manifest.EnrollmentGeneration != 2 || next.Manifest.ConnectionGeneration != 1 || !bytes.Equal(nextSecret.ConnectionSecret, secret.ConnectionSecret) {
		t.Fatal("rotation diverged from AW-01 enrollment-only generation transition")
	}
	if !next.Manifest.UpdatedAt.Equal(attached.UpdatedAt.Add(time.Microsecond)) || !next.Manifest.UpdatedAt.After(receipt.Worker.UpdatedAt) {
		t.Fatal("normal two-clock skew did not preserve deterministic local monotonic evidence")
	}
	// These bytes are installation-local continuity only. The old server
	// connection remains enrollment generation 1, while the authoritative
	// worker is generation 2; transport refuses that mismatched tuple.
	if rotation.Worker.EnrollmentGeneration == next.Manifest.EnrollmentGeneration {
		t.Fatal("old connection enrollment authority was retained")
	}
	lease, err = store.AcquireRuntime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lease.LoadReconnectCheckpoint(ctx); !errors.Is(err, attachedworkerlocal.ErrStateMissing) {
		lease.Close()
		t.Fatalf("rotation retained old continuation: %v", err)
	}
	if err = lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err = CompleteRotation(ctx, store, rotationPath, receipt); err != nil {
		t.Fatalf("rotation replay after attachment: %v", err)
	}
}

func rotationCheckpoint(t *testing.T, m attachedworkerlocal.ManifestV1) attachedworkerlocal.ReconnectCheckpointV1 {
	t.Helper()
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	capability := attachedworkerprotocol.CapabilityManifestV1{WorkerID: string(m.WorkerID), EnrollmentGeneration: m.EnrollmentGeneration, Revision: 1, ProtocolOffer: offer, OperatingSystem: "linux", Architecture: "arm64", BuildID: "onboarding-fixture", HarnessName: "sessionless", HarnessVersion: "1.0.0", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: bytes.Repeat([]byte{0x21}, 32), IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary, attachedworkerprotocol.IsolationProcessBoundary}, Features: []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}, MaxConcurrentAttempts: 1}
	digest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	config := attachedworkerprotocol.MachineConfig{Auth: attachedworkerprotocol.AuthContextV1{TenantID: string(m.TenantID), OwnerUserID: string(m.OwnerUserID), WorkerID: string(m.WorkerID), IdentityPublicKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{4}, 32)).Public().(ed25519.PublicKey), EnrollmentGeneration: m.EnrollmentGeneration, ConnectionGeneration: m.ConnectionGeneration, Version: 1, ChannelBinding: bytes.Repeat([]byte{0x61}, 32)}, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}}
	initial, err := attachedworkerprotocol.NewConformanceMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := initial.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	machine.Connection = attachedworkerprotocol.ConnectionReady
	machine.Hello = &attachedworkerprotocol.HelloV1{Offer: offer, WorkerNonce: bytes.Repeat([]byte{0x41}, 32)}
	machine.Challenge = &attachedworkerprotocol.ChallengeV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: bytes.Repeat([]byte{0x41}, 32), PlatformNonce: bytes.Repeat([]byte{0x51}, 32)}
	machine.CapabilityDigest = digest
	machine.Manifest = &capability
	machine.Digest, err = attachedworkerprotocol.MachineSnapshotDigestV1(machine)
	if err != nil {
		t.Fatalf("continuation machine fixture: %v", err)
	}
	// Use the protocol's persistence encoder to normalize nil slices to its
	// canonical empty values before embedding in the strict local-state codec.
	encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(machine)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded, &machine); err != nil {
		t.Fatal(err)
	}
	checkpoint := attachedworkerlocal.ReconnectCheckpointV1{Version: 1, Revision: 1, ManifestRevision: m.Revision, TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, WorkerID: m.WorkerID, EnrollmentGeneration: m.EnrollmentGeneration, ConnectionGeneration: m.ConnectionGeneration, ProtocolVersion: 1, ConnectionID: "connection-170", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", digest)), AuthenticationExpires: m.UpdatedAt.Add(time.Hour), ChannelBinding: bytes.Repeat([]byte{0x61}, 32), WorkerOffer: offer, PlatformOffer: offer, MachineSnapshot: machine, CheckpointedAt: m.UpdatedAt}
	if err = checkpoint.Validate(m); err != nil {
		t.Fatalf("continuation fixture: %v", err)
	}
	return checkpoint
}

func TestOnboardingCorruptedPendingKeyNeverRegenerates(t *testing.T) {
	store, pending, grant, setup := onboardingFixture(t)
	ctx := context.Background()
	if _, err := PrepareEnrollment(ctx, store, pending, grant, setup, bytes.NewReader(bytes.Repeat([]byte{4}, 32))); err != nil {
		t.Fatal(err)
	}
	var p enrollmentPending
	if err := ReadPrivate(pending, &p); err != nil {
		t.Fatal(err)
	}
	defer clear(p.PrivateKey)
	p.PrivateKey[0] ^= 1
	encoded, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(pending, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = PrepareEnrollment(ctx, store, pending, grant, setup, refusingRandom{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("corrupted seed reused or regenerated: %v", err)
	}
	if _, err = store.LoadSnapshot(ctx); !errors.Is(err, attachedworkerlocal.ErrStateMissing) {
		t.Fatalf("corrupted seed initialized installation: %v", err)
	}
}

func TestOnboardingPrivateReplayRequiresParentDurability(t *testing.T) {
	_, pending, grant, _ := onboardingFixture(t)
	path := filepath.Join(filepath.Dir(pending), "grant-parent-sync.json")
	if err := WriteGrant(path, grant); err != nil {
		t.Fatal(err)
	}
	var disk grantDisk
	if err := readPrivate(path, &disk, func(*os.File) error { return errors.New("test-owned parent fsync failure") }); !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("parent durability failure did not fence replay: %v", err)
	}
	// The original bytes remain intact; an explicit later successful replay can
	// establish durability without minting a new grant or changing identity.
	got, err := ReadGrant(path)
	if err != nil || !reflect.DeepEqual(got, grant) {
		t.Fatalf("durable replay changed grant: %v", err)
	}
}
