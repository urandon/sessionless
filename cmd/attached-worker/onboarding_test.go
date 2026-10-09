package main

import (
	"bytes"
	"context"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/domain"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestOnboardingCLIRejectsIncompletePrivateHandoffWithoutLeaks(t *testing.T) {
	for _, command := range []string{"enroll-prepare", "enroll-complete", "rotate-prepare", "rotate-complete"} {
		t.Run(command, func(t *testing.T) {
			var output bytes.Buffer
			exit := run([]string{command, "--state-dir", "/private/tmp/no-installation", "--pending-file", "/private/tmp/no-pending", "--grant-file", "/private/tmp/secret-should-not-leak"}, &output)
			if exit == 0 || !strings.Contains(output.String(), "invalid") || strings.Contains(output.String(), "secret-should-not-leak") {
				t.Fatalf("invalid command diagnostics: exit=%d output=%q", exit, output.String())
			}
		})
	}
}

func TestOnboardingCLIPrivateEnrollmentAndRotation(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := func(name string) string { return filepath.Join(parent, name) }
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	secret := bytes.Repeat([]byte{0x51}, 32)
	g := attachedworkeronboarding.GrantV1{Version: 1, ControlPlaneOrigin: "https://control.example", BootstrapSecret: secret, Enrollment: domain.AttachedWorkerEnrollment{TenantID: "tenant-cli-170", OwnerUserID: "owner-cli-170", WorkerID: "worker-cli-170", ID: "enroll-cli-170", DisplayName: "CLI owned worker", Audience: "cli-170", BootstrapDigest: domain.DigestWorkerBootstrap(secret), CreatedAt: now, ExpiresAt: now.Add(time.Hour), RetainUntil: now.Add(2 * time.Hour), Revision: 1}}
	s := attachedworkeronboarding.SetupV1{Version: 1, ControlPlaneOrigin: g.ControlPlaneOrigin, OCI: attachedworkerlocal.OCIConfigV1{DockerPath: path("docker"), DockerSHA256: strings.Repeat("a", 64), CLIConfigDir: path("docker-config"), Host: "unix:///run/docker.sock", EngineID: "engine-cli-170-abcdef", InstallationID: "install-cli-170", Boundary: attachedworkerlocal.BoundaryLinuxRootless, Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10}, Harness: attachedworkerlocal.HarnessConfigV1{Executable: path("harness"), SHA256: strings.Repeat("c", 64), Arguments: []string{"--attached"}}}
	if err = attachedworkeronboarding.WriteGrant(path("grant.json"), g); err != nil {
		t.Fatal(err)
	}
	if err = attachedworkeronboarding.WriteSetup(path("setup.json"), s); err != nil {
		t.Fatal(err)
	}
	invoke := func(args ...string) {
		t.Helper()
		var output bytes.Buffer
		if exit := run(args, &output); exit != 0 || output.String() != "{\"version\":1,\"code\":\"ok\"}\n" {
			t.Fatalf("private CLI %s exit=%d output=%q", args[0], exit, output.String())
		}
	}
	prepare := []string{"enroll-prepare", "--state-dir", path("installation"), "--pending-file", path("pending.json"), "--grant-file", path("grant.json"), "--setup-file", path("setup.json"), "--request-file", path("claim.json")}
	invoke(prepare...)
	claim, err := attachedworkeronboarding.ReadClaim(path("claim.json"))
	if err != nil {
		t.Fatal(err)
	}
	invoke(prepare...)
	again, err := attachedworkeronboarding.ReadClaim(path("claim.json"))
	if err != nil || !reflect.DeepEqual(claim, again) {
		t.Fatalf("CLI resume changed claim: %v", err)
	}
	w := domain.AttachedWorker{TenantID: g.Enrollment.TenantID, OwnerUserID: g.Enrollment.OwnerUserID, ID: g.Enrollment.WorkerID, DisplayName: g.Enrollment.DisplayName, IdentityPublicKey: claim.IdentityPublicKey, EnrollmentGeneration: 1, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOffline, Revision: 1, CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)}
	r := attachedworkeronboarding.ReceiptV1{Version: 1, Worker: w, ResourceID: attachedworkeronboarding.ResourceIDForWorker(w.TenantID, w.OwnerUserID, w.ID), ActorID: attachedworkeronboarding.ActorIDForOwner(w.TenantID, w.OwnerUserID), Entitlement: domain.EntitlementUnknown, Quota: domain.ProviderQuotaUnknown}
	if err = attachedworkeronboarding.WriteReceipt(path("receipt.json"), r); err != nil {
		t.Fatal(err)
	}
	complete := []string{"enroll-complete", "--state-dir", path("installation"), "--pending-file", path("pending.json"), "--receipt-file", path("receipt.json")}
	invoke(complete...)
	invoke(complete...)
	if err = attachedworkeronboarding.WriteWorkerHead(path("head.json"), w); err != nil {
		t.Fatal(err)
	}
	rotatePrepare := []string{"rotate-prepare", "--state-dir", path("installation"), "--pending-file", path("rotation-pending.json"), "--head-file", path("head.json"), "--request-file", path("rotation-request.json")}
	invoke(rotatePrepare...)
	invoke(rotatePrepare...)
	rotation, err := attachedworkeronboarding.ReadRotation(path("rotation-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	r.Worker.IdentityPublicKey = rotation.NewPublicKey
	r.Worker.Revision++
	r.Worker.EnrollmentGeneration++
	r.Worker.UpdatedAt = r.Worker.UpdatedAt.Add(time.Second)
	if err = attachedworkeronboarding.WriteReceipt(path("rotation-receipt.json"), r); err != nil {
		t.Fatal(err)
	}
	rotateComplete := []string{"rotate-complete", "--state-dir", path("installation"), "--pending-file", path("rotation-pending.json"), "--receipt-file", path("rotation-receipt.json")}
	invoke(rotateComplete...)
	invoke(rotateComplete...)
	store, err := attachedworkerlocal.NewStore(path("installation"), nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSnapshot(context.Background())
	if err != nil || snapshot.Manifest.EnrollmentGeneration != 2 {
		t.Fatalf("CLI rotation installation: generation=%d err=%v", snapshot.Manifest.EnrollmentGeneration, err)
	}
}
