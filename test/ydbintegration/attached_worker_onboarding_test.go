//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboardingserver"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/scheduler"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Every timestamp is read from YDB or derived from one such sample. No process,
// provider, container, or credential runtime is started by this declarative gate.
type onboardingDatabaseClock struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
}

func (clock onboardingDatabaseClock) Now() time.Time {
	clock.t.Helper()
	var at time.Time
	if err := clock.db.QueryRowContext(clock.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		clock.t.Fatalf("sample onboarding YDB clock: %v", err)
	}
	return at.UTC().Truncate(time.Microsecond)
}

type onboardingYDBFixture struct {
	ctx    context.Context
	store  *ydbstore.Store
	db     *sql.DB
	clock  onboardingDatabaseClock
	server *attachedworkeronboardingserver.Service
	tenant domain.TenantID
	owner  domain.UserID
}

func newOnboardingYDBFixture(t *testing.T) *onboardingYDBFixture {
	t.Helper()
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	f := &onboardingYDBFixture{ctx: ctx, store: store, db: client.DB, tenant: domain.TenantID(uniqueID("tenant-onboarding"))}
	f.clock = onboardingDatabaseClock{t: t, ctx: ctx, db: client.DB}
	f.owner = f.member(t, domain.TenantMembershipOwner)
	var err error
	f.server, err = attachedworkeronboardingserver.New(f.config(), store.OnboardingStore())
	if err != nil {
		t.Fatalf("compose normal privileged onboarding: %v", err)
	}
	return f
}

func (f *onboardingYDBFixture) config() attachedworker.Config {
	return attachedworker.Config{Clock: f.clock, IDs: idgen.New(), MaxEnrollmentTTL: 8 * time.Hour, EnrollmentRetention: 24 * time.Hour}
}

func (f *onboardingYDBFixture) member(t *testing.T, role domain.TenantMembershipRole) domain.UserID {
	t.Helper()
	identity, created, err := f.store.ResolveOrCreateExternalIdentity(f.ctx, domain.ExternalSubject{Provider: "onboarding-test-oidc", Subject: uniqueID("external-onboarding")}, domain.UserID(uniqueID("user-onboarding")), f.clock.Now())
	if err != nil || !created {
		t.Fatalf("create fresh external identity: created=%t err=%v", created, err)
	}
	membership, err := f.store.BootstrapDevelopmentMembership(f.ctx, domain.DevelopmentBootstrapGrant{TenantID: f.tenant, UserID: identity.UserID, Role: role, Environment: domain.DevelopmentEnvironment, Operator: "onboarding-ci", Reason: "fresh native handoff contract proof", GrantedAt: f.clock.Now()})
	if err != nil || membership.UserID != identity.UserID || membership.Status != domain.TenantMembershipActive {
		t.Fatalf("create explicit development membership: %+v err=%v", membership, err)
	}
	return identity.UserID
}

// There is no public membership-revocation command in this slice. This explicit
// authorization fault injection changes only the existing production-created
// membership; enrollment, worker, actor and resource rows are never seeded.
func (f *onboardingYDBFixture) revokeMembership(t *testing.T, owner domain.UserID) {
	t.Helper()
	memberships, err := f.store.ListTenantMemberships(f.ctx, owner, 2)
	if err != nil || len(memberships) != 1 || memberships[0].TenantID != f.tenant {
		t.Fatalf("read membership before revocation: %+v err=%v", memberships, err)
	}
	membership := memberships[0]
	membership.Status = domain.TenantMembershipRevoked
	membership.SecurityVersion++
	membership.UpdatedAt = f.clock.Now()
	if err := membership.Validate(); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(owner))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(f.ctx, `UPDATE tenant_memberships SET status=$1, security_version=$2, updated_at=$3, record=CAST($4 AS JsonDocument) WHERE user_bucket=$5 AND user_id=$6 AND tenant_id=$7`, membership.Status, membership.SecurityVersion, membership.UpdatedAt, string(payload), bucket, owner, f.tenant); err != nil {
		t.Fatalf("revoke existing membership: %v", err)
	}
	var status domain.TenantMembershipStatus
	if err := f.db.QueryRowContext(f.ctx, `SELECT status FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, bucket, owner, f.tenant).Scan(&status); err != nil || status != domain.TenantMembershipRevoked {
		t.Fatalf("read back revoked membership: status=%s err=%v", status, err)
	}
}

func (f *onboardingYDBFixture) prepare(t *testing.T, owner domain.UserID) (*attachedworkerlocal.Store, string, attachedworkeronboarding.GrantV1, attachedworkeronboarding.ClaimV1) {
	t.Helper()
	grant, err := f.server.PrepareGrant(f.ctx, f.tenant, owner, attachedworker.CreateEnrollmentRequest{DisplayName: "fresh operator-assisted worker", Audience: "sessionless-onboarding-ci", ExpiresAt: f.clock.Now().Add(4 * time.Hour)}, "https://control.example")
	if err != nil {
		t.Fatalf("prepare normal server grant: %v", err)
	}
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	grantPath := filepath.Join(parent, "grant.json")
	if err := attachedworkeronboarding.WriteGrant(grantPath, grant); err != nil {
		t.Fatalf("retain private grant before server effect: %v", err)
	}
	if err := f.server.PersistGrant(f.ctx, grant); err != nil {
		t.Fatalf("persist retained exact grant: %v", err)
	}
	if err := f.server.PersistGrant(f.ctx, grant); err != nil {
		t.Fatalf("replay retained exact grant: %v", err)
	}
	local, err := attachedworkerlocal.NewStore(filepath.Join(parent, "installation"), f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	setup := attachedworkeronboarding.SetupV1{Version: 1, ControlPlaneOrigin: grant.ControlPlaneOrigin,
		OCI:     attachedworkerlocal.OCIConfigV1{DockerPath: filepath.Join(parent, "declarative-docker"), DockerSHA256: strings.Repeat("a", 64), CLIConfigDir: filepath.Join(parent, "declarative-cli-config"), Host: "unix:///run/user/1000/docker.sock", EngineID: "onboarding-test-engine", InstallationID: uniqueID("installation"), Boundary: attachedworkerlocal.BoundaryLinuxRootless, Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness: attachedworkerlocal.HarnessConfigV1{Executable: filepath.Join(parent, "declarative-harness"), SHA256: strings.Repeat("c", 64), Arguments: []string{"--attached"}},
	}
	pending := filepath.Join(parent, "pending-enrollment.json")
	claim, err := attachedworkeronboarding.PrepareEnrollment(f.ctx, local, pending, grant, setup, nil)
	if err != nil {
		t.Fatalf("prepare one native key and signed claim: %v", err)
	}
	return local, pending, grant, claim
}

func (f *onboardingYDBFixture) session(t *testing.T, owner domain.UserID) domain.Session {
	t.Helper()
	sessions, err := sessionapi.New(sessionapi.Config{CursorKey: bytes.Repeat([]byte{5}, 32), IDKey: bytes.Repeat([]byte{6}, 32)}, f.store, newSessionAPITestBlobs(), f.clock)
	if err != nil {
		t.Fatal(err)
	}
	session, _, err := sessions.Create(f.ctx, f.tenant, owner, domain.IdempotencyKey(uniqueID("create-onboarding-session")))
	if err != nil {
		t.Fatalf("create canonical owner session: %v", err)
	}
	return session
}

func TestAttachedWorkerOnboardingFreshOwnerNativeHandoffAndResourceProjection(t *testing.T) {
	f := newOnboardingYDBFixture(t)
	local, pending, _, claim := f.prepare(t, f.owner)
	receipt, err := f.server.Claim(f.ctx, claim)
	if err != nil || receipt.Validate() != nil {
		t.Fatalf("normal claim plus atomic registration: worker=%s err=%v", receipt.Worker.ID, err)
	}
	replay, err := f.server.Claim(f.ctx, claim)
	if err != nil || replay.Worker.ID != receipt.Worker.ID || replay.ResourceID != receipt.ResourceID || replay.Worker.Revision != 1 {
		t.Fatalf("exact pristine claim/registration replay: worker=%s err=%v", replay.Worker.ID, err)
	}
	changedClaim := claim
	changedKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{43}, ed25519.SeedSize))
	changedClaim.IdentityPublicKey = changedKey.Public().(ed25519.PublicKey)
	transcript, err := attachedworker.ClaimProofTranscript(changedClaim.Enrollment, changedClaim.Enrollment.Revision, changedClaim.IdentityPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	changedClaim.Proof = ed25519.Sign(changedKey, transcript)
	if _, err := f.server.Claim(f.ctx, changedClaim); !errors.Is(err, attachedworker.ErrEnrollmentConsumed) {
		t.Fatalf("consumed grant accepted changed signed key: %v", err)
	}
	head, err := f.server.Head(f.ctx, f.tenant, f.owner, receipt.Worker.ID)
	if err != nil || head.Revision != 1 || !bytes.Equal(head.IdentityPublicKey, receipt.Worker.IdentityPublicKey) {
		t.Fatalf("changed consumed claim altered worker head: rev=%d err=%v", head.Revision, err)
	}
	if err := attachedworkeronboarding.CompleteEnrollment(f.ctx, local, pending, receipt); err != nil {
		t.Fatalf("complete native local state using public receipt: %v", err)
	}
	if err := attachedworkeronboarding.CompleteEnrollment(f.ctx, local, pending, receipt); err != nil {
		t.Fatalf("replay native completion without rekey: %v", err)
	}
	manifest, err := local.Load(f.ctx)
	if err != nil || manifest.WorkerID != receipt.Worker.ID || manifest.OwnerUserID != f.owner || manifest.EnrollmentGeneration != 1 || manifest.ConnectionGeneration != 0 {
		t.Fatalf("native initialized authority: %+v err=%v", manifest, err)
	}
	session := f.session(t, f.owner)
	connections, err := f.store.ResolveComputeConnectionsForUser(f.ctx, ports.ComputeConnectionResolveRequest{TenantID: f.tenant, UserID: f.owner, SessionID: session.ID})
	if err != nil || len(connections) != 1 || connections[0].ID != receipt.ResourceID || connections[0].Provider != "codex" || connections[0].Entitlement != domain.EntitlementUnknown || connections[0].Quota != domain.ProviderQuotaUnknown {
		t.Fatalf("normal Web owned resource projection: %+v err=%v", connections, err)
	}
	// Feed the actual persisted Web projection into the existing canonical
	// admission policy. Identity enrollment alone cannot grant provider access.
	decision, err := scheduler.Evaluate(f.clock.Now(), domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: 10 * time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 64, MaxArtifacts: 8, MaxToolEvents: 16, MaxToolEventBytes: 1 << 18}, domain.WorkloadShape{Runtime: time.Minute, Turns: 1}, scheduler.Snapshot{Entitlement: connections[0].Entitlement, Quota: connections[0].Quota, Slot: domain.SubscriptionSchedulerSlot{TenantID: f.tenant, SubscriptionConnectionID: connections[0].ID, State: domain.SchedulerReady, UpdatedAt: connections[0].ObservedAt}})
	if err != nil || decision.Admit || decision.Code != "subscription_reauthentication_required" {
		t.Fatalf("unknown registered provider admitted: %+v err=%v", decision, err)
	}
	var credential string
	if err := f.db.QueryRowContext(f.ctx, `SELECT credential_ref FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`, f.tenant, receipt.ResourceID).Scan(&credential); err != nil || credential != "" {
		t.Fatalf("server credential reference=%q err=%v, want empty", credential, err)
	}
	other := f.member(t, domain.TenantMembershipMember)
	otherSession := f.session(t, other)
	foreign, err := f.store.ResolveComputeConnectionsForUser(f.ctx, ports.ComputeConnectionResolveRequest{TenantID: f.tenant, UserID: other, SessionID: otherSession.ID})
	if err != nil || len(foreign) != 0 {
		t.Fatalf("foreign owner resource projection leaked: %+v err=%v", foreign, err)
	}
	if _, found, err := f.store.OnboardingStore().LoadAttachedWorker(f.ctx, f.tenant, other, receipt.Worker.ID); err != nil || found {
		t.Fatalf("foreign owner worker read: found=%t err=%v", found, err)
	}
}

func TestAttachedWorkerOnboardingOwnerMembershipAndResourceConflicts(t *testing.T) {
	f := newOnboardingYDBFixture(t)
	viewer := f.member(t, domain.TenantMembershipViewer)
	viewerGrant, err := f.server.PrepareGrant(f.ctx, f.tenant, viewer, attachedworker.CreateEnrollmentRequest{DisplayName: "viewer cannot enroll", Audience: "sessionless-onboarding-ci", ExpiresAt: f.clock.Now().Add(time.Hour)}, "https://control.example")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.PersistGrant(f.ctx, viewerGrant); err == nil {
		t.Fatal("read-only membership persisted worker enrollment")
	}
	if _, found, err := f.store.LoadAttachedWorkerEnrollment(f.ctx, f.tenant, viewer, viewerGrant.Enrollment.ID); err != nil || found {
		t.Fatalf("denied enrollment persisted: found=%t err=%v", found, err)
	}
	revokedOwner := f.member(t, domain.TenantMembershipMember)
	_, _, revokedGrant, revokedClaim := f.prepare(t, revokedOwner)
	f.revokeMembership(t, revokedOwner)
	if denied, err := f.server.Claim(f.ctx, revokedClaim); err == nil || denied.Version != 0 {
		t.Fatalf("membership revoked after Persist accepted Claim: worker=%s err=%v", denied.Worker.ID, err)
	}
	if _, found, err := f.store.LoadAttachedWorker(f.ctx, f.tenant, revokedOwner, revokedGrant.Enrollment.WorkerID); err != nil || found {
		t.Fatalf("revoked membership created worker: found=%t err=%v", found, err)
	}
	storedGrant, found, err := f.store.LoadAttachedWorkerEnrollment(f.ctx, f.tenant, revokedOwner, revokedGrant.Enrollment.ID)
	if err != nil || !found || !storedGrant.ConsumedAt.IsZero() || storedGrant.Revision != 1 {
		t.Fatalf("revoked membership consumed grant: found=%t rev=%d err=%v", found, storedGrant.Revision, err)
	}
	var revokedProjectionCount uint64
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM subscription_connections_by_user WHERE tenant_id=$1 AND user_id=$2`, f.tenant, revokedOwner).Scan(&revokedProjectionCount); err != nil || revokedProjectionCount != 0 {
		t.Fatalf("revoked membership resource projection count=%d err=%v", revokedProjectionCount, err)
	}
	local, pending, _, claim := f.prepare(t, f.owner)
	receipt, err := f.server.Claim(f.ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachedworkeronboarding.CompleteEnrollment(f.ctx, local, pending, receipt); err != nil {
		t.Fatal(err)
	}
	_, _, _, secondClaim := f.prepare(t, f.owner)
	if second, err := f.server.Claim(f.ctx, secondClaim); err == nil || second.Version != 0 {
		t.Fatalf("second owner resource conflict exposed successful receipt: worker=%s err=%v", second.Worker.ID, err)
	}
	session := f.session(t, f.owner)
	connections, err := f.store.ResolveComputeConnectionsForUser(f.ctx, ports.ComputeConnectionResolveRequest{TenantID: f.tenant, UserID: f.owner, SessionID: session.ID})
	if err != nil || len(connections) != 1 || connections[0].ID != receipt.ResourceID {
		t.Fatalf("conflicting registration modified original resource: %+v err=%v", connections, err)
	}
	var actors uint64
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM actors WHERE tenant_id=$1 AND user_id=$2`, f.tenant, f.owner).Scan(&actors); err != nil || actors != 1 {
		t.Fatalf("atomic conflicting registration left actors=%d err=%v", actors, err)
	}
	// Expiry is a deterministic sampled-YDB boundary, not a sleep. The
	// retained original grant remains unchanged and cannot be persisted anew.
	at := f.clock.Now()
	expiryClock := sessionAPITestClock{at: at}
	expiryService, err := attachedworkeronboardingserver.New(attachedworker.Config{Clock: expiryClock, IDs: idgen.New(), MaxEnrollmentTTL: time.Hour, EnrollmentRetention: time.Hour}, f.store.OnboardingStore())
	if err != nil {
		t.Fatal(err)
	}
	expired, err := expiryService.PrepareGrant(f.ctx, f.tenant, f.owner, attachedworker.CreateEnrollmentRequest{DisplayName: "expiry boundary", Audience: "sessionless-onboarding-ci", ExpiresAt: at.Add(time.Minute)}, "https://control.example")
	if err != nil {
		t.Fatal(err)
	}
	expiryService, err = attachedworkeronboardingserver.New(attachedworker.Config{Clock: sessionAPITestClock{at: expired.Enrollment.ExpiresAt}, IDs: idgen.New(), MaxEnrollmentTTL: time.Hour, EnrollmentRetention: time.Hour}, f.store.OnboardingStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := expiryService.PersistGrant(f.ctx, expired); !errors.Is(err, attachedworker.ErrEnrollmentDenied) {
		t.Fatalf("exact expiry grant accepted: %v", err)
	}
}

func TestAttachedWorkerOnboardingSignedRotationReplayAndRevocation(t *testing.T) {
	f := newOnboardingYDBFixture(t)
	local, pending, _, claim := f.prepare(t, f.owner)
	receipt, err := f.server.Claim(f.ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachedworkeronboarding.CompleteEnrollment(f.ctx, local, pending, receipt); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Dir(pending)
	rotationPending := filepath.Join(parent, "pending-rotation.json")
	rotation, err := attachedworkeronboarding.PrepareRotation(f.ctx, local, rotationPending, receipt.Worker, nil)
	if err != nil {
		t.Fatalf("native prepare dual-proof rotation: %v", err)
	}
	rotated, err := f.server.Rotate(f.ctx, rotation)
	if err != nil || rotated.Worker.Revision != 2 || rotated.Worker.EnrollmentGeneration != 2 || rotated.ResourceID != receipt.ResourceID {
		t.Fatalf("normal server rotation: worker=%s rev=%d generation=%d err=%v", rotated.Worker.ID, rotated.Worker.Revision, rotated.Worker.EnrollmentGeneration, err)
	}
	replay, err := f.server.Rotate(f.ctx, rotation)
	if err != nil || replay.Worker.Revision != rotated.Worker.Revision || !bytes.Equal(replay.Worker.IdentityPublicKey, rotated.Worker.IdentityPublicKey) {
		t.Fatalf("exact audited rotation reconciliation failed: rev=%d err=%v", replay.Worker.Revision, err)
	}
	if err := attachedworkeronboarding.CompleteRotation(f.ctx, local, rotationPending, rotated); err != nil {
		t.Fatalf("complete native generation-bound rotation: %v", err)
	}
	if err := attachedworkeronboarding.CompleteRotation(f.ctx, local, rotationPending, rotated); err != nil {
		t.Fatalf("native exact rotation completion replay: %v", err)
	}
	manifest, err := local.Load(f.ctx)
	if err != nil || manifest.EnrollmentGeneration != 2 || manifest.ConnectionGeneration != 0 {
		t.Fatalf("rotated local generations: %+v err=%v", manifest, err)
	}
	if _, err := f.server.Claim(f.ctx, claim); !errors.Is(err, attachedworker.ErrEnrollmentConsumed) {
		t.Fatalf("original claim after rotation=%v, want consumed", err)
	}
	secondPending := filepath.Join(parent, "pending-rotation-second.json")
	second, err := attachedworkeronboarding.PrepareRotation(f.ctx, local, secondPending, rotated.Worker, nil)
	if err != nil {
		t.Fatalf("prepare second exact native rotation: %v", err)
	}
	config := f.config()
	config.Clock = sessionAPITestClock{at: rotated.Worker.UpdatedAt}
	nonAdvancing, err := attachedworkeronboardingserver.New(config, f.store.OnboardingStore())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nonAdvancing.Rotate(f.ctx, second); !errors.Is(err, attachedworker.ErrWorkerConflict) {
		t.Fatalf("equal-time second rotation committed: %v", err)
	}
	head, err := f.server.Head(f.ctx, f.tenant, f.owner, rotated.Worker.ID)
	if err != nil || head.Revision != rotated.Worker.Revision || !bytes.Equal(head.IdentityPublicKey, rotated.Worker.IdentityPublicKey) {
		t.Fatalf("denied non-advancing rotation changed head: rev=%d err=%v", head.Revision, err)
	}
	rotated, err = f.server.Rotate(f.ctx, second)
	if err != nil || rotated.Worker.Revision != 3 || !rotated.Worker.UpdatedAt.After(head.UpdatedAt) {
		t.Fatalf("fresh YDB-time second rotation: rev=%d err=%v", rotated.Worker.Revision, err)
	}
	if err := attachedworkeronboarding.CompleteRotation(f.ctx, local, secondPending, rotated); err != nil {
		t.Fatalf("commit locally completable second rotation: %v", err)
	}
	identity, err := attachedworker.New(f.config(), f.store)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := identity.Revoke(f.ctx, f.tenant, f.owner, attachedworker.WorkerRevisionRequest{WorkerID: rotated.Worker.ID, ExpectedRevision: rotated.Worker.Revision})
	if err != nil || revoked.DesiredState != domain.AttachedWorkerDesiredRevoked {
		t.Fatalf("normal deny-first revoke: %+v err=%v", revoked, err)
	}
	if _, err := f.server.Rotate(f.ctx, rotation); err == nil {
		t.Fatal("old rotation reconciled through later revoked worker head")
	}
	if _, err := f.store.OnboardingStore().RegisterAttachedComputeResource(f.ctx, ports.AttachedComputeRegistration{Worker: rotated.Worker, ResourceID: rotated.ResourceID, ActorID: rotated.ActorID}); err == nil {
		t.Fatal("stale pre-revocation resource registration succeeded")
	}
}
