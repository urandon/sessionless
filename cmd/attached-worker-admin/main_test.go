package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	onboarding "gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type adminFake struct {
	grant                                  onboarding.GrantV1
	prepareCalls, persistCalls, claimCalls int
	beforePersist                          func(onboarding.GrantV1) error
}

func (f *adminFake) PrepareGrant(context.Context, domain.TenantID, domain.UserID, attachedworker.CreateEnrollmentRequest, string) (onboarding.GrantV1, error) {
	f.prepareCalls++
	return f.grant, nil
}
func (f *adminFake) PersistGrant(_ context.Context, g onboarding.GrantV1) error {
	f.persistCalls++
	if f.beforePersist != nil {
		return f.beforePersist(g)
	}
	return nil
}
func (f *adminFake) Claim(context.Context, onboarding.ClaimV1) (onboarding.ReceiptV1, error) {
	f.claimCalls++
	return onboarding.ReceiptV1{}, errors.New("secret backend detail must not escape")
}
func (*adminFake) Head(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, error) {
	return domain.AttachedWorker{}, errors.New("denied")
}
func (*adminFake) Rotate(context.Context, onboarding.RotationV1) (onboarding.ReceiptV1, error) {
	return onboarding.ReceiptV1{}, errors.New("denied")
}
func adminFixture(t *testing.T) (*adminFake, string, []string, func() time.Time) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	secret := bytes.Repeat([]byte{7}, 32)
	f := &adminFake{grant: onboarding.GrantV1{Version: 1, ControlPlaneOrigin: "https://control.example", BootstrapSecret: secret, Enrollment: domain.AttachedWorkerEnrollment{
		TenantID: "tenant", OwnerUserID: "owner", ID: "enrollment", WorkerID: "worker", DisplayName: "laptop", Audience: "worker-v1", BootstrapDigest: domain.DigestWorkerBootstrap(secret), CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), RetainUntil: now.Add(time.Hour), Revision: 1,
	}}}
	path := filepath.Join(dir, "grant.json")
	args := []string{"enroll-create", "--tenant", "tenant", "--owner", "owner", "--display-name", "laptop", "--audience", "worker-v1", "--origin", "https://control.example", "--grant-file", path}
	return f, path, args, func() time.Time { return now }
}
func adminOpen(f *adminFake) adminOpener {
	return func(context.Context) (adminService, func(), error) { return f, func() {}, nil }
}
func TestAdminRetainsExactGrantBeforeAmbiguousPersistence(t *testing.T) {
	f, path, args, now := adminFixture(t)
	f.beforePersist = func(g onboarding.GrantV1) error {
		persisted, err := onboarding.ReadGrant(path)
		if err != nil || persisted.Enrollment.ID != g.Enrollment.ID || !bytes.Equal(persisted.BootstrapSecret, g.BootstrapSecret) {
			t.Fatalf("grant not durable before mutation: err=%v sameID=%v", err, persisted.Enrollment.ID == g.Enrollment.ID)
		}
		return errors.New("lost response including a secret detail")
	}
	var output bytes.Buffer
	confirmation := "ONBOARD enroll-create owner INTO tenant\n"
	if code := runAdmin(context.Background(), args, strings.NewReader(confirmation), &output, adminOpen(f), now); code != 1 || !strings.Contains(output.String(), `"code":"denied"`) {
		t.Fatalf("ambiguous persistence code=%d public output=%s", code, &output)
	}
	f.beforePersist = nil
	output.Reset()
	if code := runAdmin(context.Background(), args, strings.NewReader(confirmation), &output, adminOpen(f), now); code != 0 || f.prepareCalls != 1 || f.persistCalls != 2 {
		t.Fatalf("exact retry code=%d prepare=%d persist=%d output=%s", code, f.prepareCalls, f.persistCalls, &output)
	}
	for _, forbidden := range []string{"bootstrap", "secret", path, "tenant", "owner", "https://"} {
		if strings.Contains(output.String(), forbidden) {
			t.Errorf("public result contains forbidden material %q", forbidden)
		}
	}
}
func TestAdminRejectsBeforeOpeningBackend(t *testing.T) {
	for _, name := range []string{"confirmation", "irrelevant-flag", "ttl", "unknown-command"} {
		t.Run(name, func(t *testing.T) {
			_, _, args, now := adminFixture(t)
			confirmation := "ONBOARD enroll-create owner INTO tenant\n"
			switch name {
			case "confirmation":
				confirmation = "yes\n"
			case "irrelevant-flag":
				args = append(args, "--worker", "worker")
			case "ttl":
				args = append(args, "--ttl", "11m")
			case "unknown-command":
				args[0] = "activate"
			}
			opened := false
			open := func(context.Context) (adminService, func(), error) {
				opened = true
				return nil, nil, errors.New("must not open")
			}
			var output bytes.Buffer
			if code := runAdmin(context.Background(), args, strings.NewReader(confirmation), &output, open, now); code != 1 || opened {
				t.Fatalf("invalid command reached authority: code=%d opened=%v output=%s", code, opened, &output)
			}
		})
	}
}
func TestAdminDoesNotReplaceForeignGrant(t *testing.T) {
	f, path, args, now := adminFixture(t)
	foreign := f.grant
	foreign.Enrollment.OwnerUserID = "another-owner"
	if err := onboarding.WriteGrant(path, foreign); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if code := runAdmin(context.Background(), args, strings.NewReader("ONBOARD enroll-create owner INTO tenant\n"), &output, adminOpen(f), now); code != 1 || f.prepareCalls != 0 || f.persistCalls != 0 || !strings.Contains(output.String(), "scope_conflict") {
		t.Fatalf("foreign grant code=%d prepare=%d persist=%d output=%s", code, f.prepareCalls, f.persistCalls, &output)
	}
	stored, err := onboarding.ReadGrant(path)
	if err != nil || stored.Enrollment.OwnerUserID != "another-owner" {
		t.Fatalf("foreign grant overwritten: %v", err)
	}
}
func TestAdminBackendErrorIsRedactedAndClosed(t *testing.T) {
	_, _, args, now := adminFixture(t)
	var output bytes.Buffer
	open := func(context.Context) (adminService, func(), error) {
		return nil, nil, errors.New("bootstrap_secret=do-not-print")
	}
	if code := runAdmin(context.Background(), args, strings.NewReader("ONBOARD enroll-create owner INTO tenant\n"), &output, open, now); code != 1 || output.String() != "{\"version\":1,\"code\":\"backend_unavailable\"}\n" {
		t.Fatalf("backend failure code=%d output=%s", code, &output)
	}
}
