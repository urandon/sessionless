//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This is normal product ingress and admission, not a provider execution gate.
// WorkerJob, quota reservation, attempt and LeaseOffer must all be created by
// AdmitDispatch. Downstream receipt/daemon tests can reuse aw169WebFixture.
func TestWebAttachedProductIngressUsesCanonicalScheduler(t *testing.T) {
	fixture := newAW169WebFixture(t)
	response, admissionRequest := fixture.submit(t, "normal browser text", "message-1")
	if _, found, err := fixture.store.LoadWorkerJob(fixture.ctx, fixture.worker.TenantID, response.RunID); err != nil || found {
		t.Fatalf("before canonical admission: worker job found=%t err=%v", found, err)
	}
	admission, err := fixture.store.AdmitDispatch(fixture.ctx, admissionRequest)
	if err != nil || !admission.Admitted || admission.Delivery != ports.DispatchDeliveryAttachedOffer {
		t.Fatalf("normal canonical admission=%+v err=%v", admission, err)
	}
	loaded, found, err := fixture.store.LoadWorkerJob(fixture.ctx, fixture.worker.TenantID, response.RunID)
	if err != nil || !found {
		t.Fatalf("load admitted canonical job: found=%t err=%v", found, err)
	}
	if loaded.Job.SubstrateBinding != nil || loaded.Job.AdmissionCostCeiling != nil || loaded.Job.ContextWindow == nil ||
		loaded.Job.ContextWindow.ThroughSequence != response.Sequence || loaded.Job.HarnessBinding.Resource.ResourceID != string(fixture.resourceID) ||
		loaded.Run.SessionID != fixture.session.ID || loaded.Run.TriggerEventID != response.EventID || loaded.Run.Status != domain.RunQueued ||
		loaded.Reservation.ID != admissionRequest.ReservationID || loaded.Reservation.Status != domain.ReservationHeld {
		t.Fatalf("admitted normal Web job=%+v reservation=%+v run=%+v", loaded.Job, loaded.Reservation, loaded.Run)
	}
	polled, err := fixture.store.PollAttachedWorkerAttempt(fixture.ctx, ports.AttachedWorkerAttemptPoll{
		TenantID: fixture.worker.TenantID, OwnerUserID: fixture.worker.OwnerUserID, WorkerID: fixture.worker.ID,
		ConnectionID: fixture.connection.ID, PresentedSecretDigest: fixture.secret,
	})
	if err != nil || polled.Status != ports.AttachedWorkerExecutionApplied || polled.Outbound == nil ||
		polled.Attempt.RunID != response.RunID || polled.Attempt.ReservationID != loaded.Reservation.ID ||
		loaded.Reservation.ExpiresAt.Before(polled.Attempt.LeaseExpiresAt) {
		t.Fatalf("durable scheduler offer=%+v err=%v", polled, err)
	}
	// The same admission reconciles the exact job/quota authority, not a second
	// attempt. A browser retry likewise returns before mutable resource state.
	replayed, err := fixture.store.AdmitDispatch(fixture.ctx, admissionRequest)
	if err != nil || !replayed.Admitted || replayed.Code != "already_admitted" {
		t.Fatalf("admission replay=%+v err=%v", replayed, err)
	}
	retry, err := fixture.web.SubmitMessage(fixture.ctx, fixture.worker.TenantID, fixture.worker.OwnerUserID, fixture.session.ID,
		webcontract.CreateMessageRequest{IdempotencyKey: "message-1", Text: "normal browser text"})
	if err != nil || retry.Created || retry.RunID != response.RunID || retry.EventID != response.EventID || retry.Sequence != response.Sequence {
		t.Fatalf("browser retry=%+v original=%+v err=%v", retry, response, err)
	}
	history, err := fixture.sessions.HistoryAfter(fixture.ctx, fixture.worker.TenantID, fixture.worker.OwnerUserID, fixture.session.ID, 0, 10)
	if err != nil || len(history.Items) != 1 || history.Items[0].Event.ID != response.EventID {
		t.Fatalf("canonical browser history=%+v err=%v", history, err)
	}
}

func TestWebAttachedProductIngressRevocationBeforeAdmissionRollsBackOffer(t *testing.T) {
	fixture := newAW169WebFixture(t)
	response, request := fixture.submit(t, "must not execute after revoke", "message-revoke")
	aw07Revoke(t, fixture.store, fixture.ctx, fixture.worker)
	if result, err := fixture.store.AdmitDispatch(fixture.ctx, request); err == nil {
		t.Fatalf("revoked resource was admitted: result=%+v", result)
	}
	if _, found, err := fixture.store.LoadWorkerJob(fixture.ctx, fixture.worker.TenantID, response.RunID); err != nil || found {
		t.Fatalf("denied admission left worker job: found=%t err=%v", found, err)
	}
	if _, found, err := fixture.store.LoadAttachedWorkerAttempt(fixture.ctx, fixture.worker.TenantID, fixture.worker.OwnerUserID, fixture.worker.ID); err != nil || found {
		t.Fatalf("denied admission left attached attempt: found=%t err=%v", found, err)
	}
	var reservations uint64
	if err := fixture.client.DB.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`,
		fixture.worker.TenantID, request.ReservationID).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("denied admission left reservation count=%d err=%v", reservations, err)
	}
	record, found, err := fixture.store.GetRunForUser(fixture.ctx, fixture.worker.TenantID, fixture.worker.OwnerUserID, response.RunID)
	if err != nil || !found || record.Run.Status != domain.RunCreated {
		t.Fatalf("denied admission mutated canonical run=%+v found=%t err=%v", record.Run, found, err)
	}
}

type aw169WebFixture struct {
	ctx        context.Context
	store      *ydbstore.Store
	client     *ydbclient.Client
	worker     domain.AttachedWorker
	connection domain.AttachedWorkerConnection
	secret     domain.AttachedWorkerConnectionSecretDigest
	privateKey ed25519.PrivateKey
	manifest   []byte
	now        time.Time
	resourceID domain.SubscriptionConnectionID
	session    domain.Session
	sessions   *sessionapi.Service
	web        *webapi.Service
	blobs      ports.BlobStore
	wake       aw169Wake
}

func newAW169WebFixture(t *testing.T) *aw169WebFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	store, client := openStore(t)
	var now time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&now); err != nil {
		t.Fatalf("sample authoritative YDB clock: %v", err)
	}
	now = now.UTC().Truncate(time.Microsecond)
	suffix := attachedWorkerDrainTestSuffix(t, "web-bridge")
	tenant, owner := domain.TenantID("tenant-"+suffix), domain.UserID("owner-"+suffix)
	aw169Membership(t, ctx, client, tenant, owner, now)
	enrollment, audit := attachedWorkerEnrollmentFixture(suffix, tenant, owner, now.Add(-time.Second))
	enrollment.ExpiresAt = now.Add(time.Hour)
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, audit); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	claim := attachedWorkerClaimFixture(enrollment, 0x71)
	claim.IdentityPublicKey = append([]byte(nil), privateKey.Public().(ed25519.PublicKey)...)
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim owner resource=%+v err=%v", claimed, err)
	}
	worker := claimed.Worker
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, attachedWorkerChallengeCreateFixture(worker, suffix))
	if err != nil {
		t.Fatal(err)
	}
	channel := bytes.Repeat([]byte{0x72}, 32)
	canonicalManifest, capability, attachedSnapshot, readySnapshot, signature := attachedWorkerProtocolSnapshotFixture(t, worker, challenge, privateKey, channel, attachedworkerprotocol.FeatureOutputReceipt)
	secret := domain.DigestAttachedWorkerConnectionSecret([]byte("test-bearer-" + suffix))
	activated, err := store.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: tenant, OwnerUserID: owner, WorkerID: worker.ID, ChallengeID: challenge.ID, Purpose: domain.AttachedWorkerAttachInitial,
		ExpectedChallengeRevision: challenge.Revision, ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration, PresentedWorkerNonceDigest: challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secret, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channel), ExpectedCapabilityDigest: capability,
		ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
	})
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated {
		t.Fatalf("activate owner resource=%+v err=%v", activated, err)
	}
	worker, found, err := store.LoadAttachedWorker(ctx, tenant, owner, worker.ID)
	if err != nil || !found {
		t.Fatalf("load attaching worker found=%t err=%v", found, err)
	}
	accepted, err := store.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: tenant, OwnerUserID: owner, WorkerID: worker.ID, ConnectionID: activated.Connection.ID,
		ConnectionGeneration: activated.Connection.ConnectionGeneration, ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: secret,
		Capability: ports.AttachedWorkerCapabilityTarget{ManifestRevision: 1, Digest: capability, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey), CanonicalManifest: canonicalManifest,
			ManifestPayload: attachedWorkerManifestPayloadFixture(t, readySnapshot), Signature: signature},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, ProtocolSnapshot: readySnapshot, PresenceTTL: 20 * time.Minute,
	})
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("accept exact capability=%+v err=%v", accepted, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenant, owner, worker.ID)
	if err != nil || !found {
		t.Fatalf("load ready worker found=%t err=%v", found, err)
	}
	resource := domain.SubscriptionConnectionID("subscription-" + suffix)
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(tenant, owner, "pin-run", "pin-attempt", resource, now)
	if err != nil {
		t.Fatal(err)
	}
	binding := managed.HarnessBinding.Clone()
	aw07TestProviderBinding(owner, suffix, 3, now)(&binding)
	fixture := newAW169OnlineWebFixture(t, ctx, store, client, worker, accepted.Connection, binding, newSessionAPITestBlobs(), now)
	fixture.privateKey, fixture.manifest = privateKey, canonicalManifest
	return fixture
}

// newAW169OnlineWebFixture composes normal Web ingress for a daemon that has
// already completed authenticated attach. It creates no WorkerJob, reservation
// or lease; those belong exclusively to the canonical scheduler transaction.
// at must come from YDB, and the supplied blob store remains test-owned.
func newAW169OnlineWebFixture(t *testing.T, ctx context.Context, store *ydbstore.Store, client *ydbclient.Client,
	worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, binding domain.HarnessBindingV1, blobs ports.BlobStore, at time.Time,
) *aw169WebFixture {
	t.Helper()
	now := at.UTC().Truncate(time.Microsecond)
	tenant, owner := worker.TenantID, worker.OwnerUserID
	resource := domain.SubscriptionConnectionID(binding.Resource.ResourceID)
	suffix := attachedWorkerDrainTestSuffix(t, "web-online")
	actor := domain.ActorID("actor-" + suffix)
	if _, err := client.DB.ExecContext(ctx, `INSERT INTO actors (tenant_id,actor_id,user_id,frontend,external_id,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		tenant, actor, owner, domain.FrontendWeb, string(actor), now, now); err != nil {
		t.Fatal(err)
	}
	// Empty credential_ref is intentional: no control-plane credential custody.
	if _, err := client.DB.ExecContext(ctx, `INSERT INTO subscription_connections (tenant_id,subscription_connection_id,actor_id,provider,credential_ref,entitlement_state,quota_state,observed_at,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		tenant, resource, actor, "test-provider", "", domain.EntitlementActive, domain.ProviderQuotaAvailable, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx, `INSERT INTO subscription_connections_by_user (tenant_id,user_id,subscription_connection_id) VALUES ($1,$2,$3)`, tenant, owner, resource); err != nil {
		t.Fatal(err)
	}
	binding = binding.Clone()
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "", "", ""
	binder, err := sessionlessharness.NewAttachedResourceBinder(sessionlessharness.AttachedResourceBinderConfig{Enabled: true,
		Resources: []sessionlessharness.AttachedResourcePin{{TenantID: tenant, OwnerUserID: owner, SubscriptionConnectionID: resource, WorkerID: worker.ID,
			EnrollmentGeneration: worker.EnrollmentGeneration, CapabilityDigest: connection.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest), Binding: binding}},
	}, store)
	if err != nil {
		t.Fatalf("construct exact owned binder: %v", err)
	}
	fixture := &aw169WebFixture{ctx: ctx, store: store, client: client, worker: worker, connection: connection, secret: connection.SecretDigest,
		now: now, resourceID: resource, blobs: blobs}
	clock := sessionAPITestClock{at: now}
	fixture.sessions, err = sessionapi.New(sessionapi.Config{CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("s"), 32)}, store, fixture.blobs, clock)
	if err != nil {
		t.Fatal(err)
	}
	fixture.session, _, err = fixture.sessions.Create(ctx, tenant, owner, "create-web-session")
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte("i"), 32), HarnessBinder: binder, DispatchWakePublisher: &fixture.wake}, store, fixture.blobs)
	if err != nil {
		t.Fatal(err)
	}
	fixture.web, err = webapi.New(webapi.Config{IDKey: bytes.Repeat([]byte("w"), 32)}, fixture.sessions, ingress, store, store, aw169UnusedWebObjects{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *aw169WebFixture) submit(t *testing.T, text string, key domain.IdempotencyKey) (webcontract.CreateMessageResponse, ports.DispatchAdmissionRequest) {
	t.Helper()
	response, err := fixture.web.SubmitMessage(fixture.ctx, fixture.worker.TenantID, fixture.worker.OwnerUserID, fixture.session.ID,
		webcontract.CreateMessageRequest{IdempotencyKey: key, Text: text})
	if err != nil {
		t.Fatalf("normal Web submission: %v", err)
	}
	candidate, status, found, err := fixture.store.GetDispatch(fixture.ctx, fixture.worker.TenantID, fixture.wake.dispatch)
	if err != nil || !found || status != domain.DispatchPending || candidate.RunID != response.RunID {
		t.Fatalf("canonical dispatch=%+v status=%s found=%t err=%v", candidate, status, found, err)
	}
	var admittedAt time.Time
	if err := fixture.client.DB.QueryRowContext(fixture.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&admittedAt); err != nil {
		t.Fatal(err)
	}
	return response, ports.DispatchAdmissionRequest{TenantID: fixture.worker.TenantID, OutboxID: candidate.OutboxID, RunID: candidate.RunID, AttemptID: candidate.AttemptID,
		ReservationID: domain.QuotaReservationID("qrs-" + string(candidate.OutboxID)), Now: admittedAt, HoldUntil: admittedAt.Add(time.Hour),
		Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: 10 * time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20,
			MaxContextEvents: 64, MaxArtifacts: 8, MaxToolEvents: 16, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1}}
}

func aw169Membership(t *testing.T, ctx context.Context, client *ydbclient.Client, tenant domain.TenantID, owner domain.UserID, now time.Time) {
	t.Helper()
	if _, err := client.DB.ExecContext(ctx, `UPSERT INTO tenants (tenant_id,status,created_at,updated_at) VALUES ($1,$2,$3,$4)`, tenant, "active", now, now); err != nil {
		t.Fatal(err)
	}
	membership := domain.TenantMembership{TenantID: tenant, UserID: owner, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: now, UpdatedAt: now}
	payload, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(owner))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx, `UPSERT INTO tenant_memberships (user_bucket,user_id,tenant_id,role,status,security_version,created_at,updated_at,record) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument))`,
		bucket, owner, tenant, membership.Role, membership.Status, membership.SecurityVersion, now, now, string(payload)); err != nil {
		t.Fatal(err)
	}
}

type aw169Wake struct{ dispatch domain.DispatchOutboxID }

func (wake *aw169Wake) PublishDispatchWake(_ context.Context, _ domain.TenantID, dispatch domain.DispatchOutboxID, _ time.Time) error {
	wake.dispatch = dispatch
	return nil
}

type aw169UnusedWebObjects struct{}

func (aw169UnusedWebObjects) PresignUpload(context.Context, ports.UploadCapabilityRequest) (ports.ObjectCapability, error) {
	return ports.ObjectCapability{}, errors.New("unexpected upload capability")
}
func (aw169UnusedWebObjects) StatObject(context.Context, domain.TenantID, string) (ports.ObjectMetadata, error) {
	return ports.ObjectMetadata{}, errors.New("unexpected object stat")
}
func (aw169UnusedWebObjects) PromoteObject(context.Context, ports.PromoteObjectRequest) (domain.BlobRef, error) {
	return domain.BlobRef{}, errors.New("unexpected object promotion")
}
func (aw169UnusedWebObjects) PresignDownload(context.Context, domain.TenantID, domain.BlobRef, time.Duration) (ports.ObjectCapability, error) {
	return ports.ObjectCapability{}, errors.New("unexpected download capability")
}
