//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func TestAttachedWorkerTransportTwoPhaseAttachAuthorizationAndExpiry(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-worker-transport"))
	ownerID := domain.UserID(uniqueID("owner-worker-transport"))
	otherOwnerID := domain.UserID(uniqueID("other-owner-worker-transport"))

	enrollment, createAudit := attachedWorkerEnrollmentFixture("transport", tenantID, ownerID, now.Add(-time.Second))
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	claim := attachedWorkerClaimFixture(enrollment, 0x41)
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	worker := claimed.Worker
	challengeCreate := attachedWorkerChallengeCreateFixture(worker, "first")
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, challengeCreate)
	if err != nil {
		t.Fatal(err)
	}
	replayedChallenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, challengeCreate)
	if err != nil || !replayedChallenge.CreatedAt.Equal(challenge.CreatedAt) || replayedChallenge.Revision != challenge.Revision {
		t.Fatalf("challenge replay = %#v, %v", replayedChallenge, err)
	}
	if _, found, err := store.LoadAttachedWorkerAttachChallenge(ctx, tenantID, otherOwnerID, worker.ID, challenge.ID); err != nil || found {
		t.Fatalf("cross-owner challenge load: found=%t err=%v", found, err)
	}

	canonicalManifest := []byte("attached-worker-capability-manifest-v1\x00stable-capabilities")
	capabilityDigest := domain.DigestAttachedWorkerCapability(canonicalManifest)
	secretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("transport-bearer"))
	activation := ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		ExpectedChallengeRevision: challenge.Revision, ExpectedWorkerRevision: worker.Revision,
		ExpectedEnrollmentGeneration: worker.EnrollmentGeneration, ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest: challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(bytes.Repeat([]byte{0x51}, 32)),
		ExpectedCapabilityDigest: capabilityDigest, AuthTTL: time.Hour,
	}
	activated, err := store.ActivateAttachedWorkerConnection(ctx, activation)
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated {
		t.Fatalf("activate = %#v, %v", activated, err)
	}
	if activated.Connection.State != domain.AttachedWorkerConnectionAttaching || !activated.Connection.PresenceExpiresAt.IsZero() {
		t.Fatalf("activation did not create a presence-free attaching head: %#v", activated.Connection)
	}
	activationReplay, err := store.ActivateAttachedWorkerConnection(ctx, activation)
	if err != nil || activationReplay.Status != ports.AttachedWorkerConnectionActivated || activationReplay.Connection.ID != activated.Connection.ID {
		t.Fatalf("activation replay = %#v, %v", activationReplay, err)
	}

	worker, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || worker.ConnectionGeneration != activated.Connection.ConnectionGeneration {
		t.Fatalf("worker after activation = %#v found=%t err=%v", worker, found, err)
	}
	manifestAcceptance := ports.AttachedWorkerManifestAcceptance{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID: activated.Connection.ID, ConnectionGeneration: activated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: secretDigest,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: capabilityDigest, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey),
			CanonicalManifest: canonicalManifest, ManifestPayload: []byte(`{"version":1,"surface":"codex-exec"}`),
			Signature: bytes.Repeat([]byte{0x61}, ed25519.SignatureSize),
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, PresenceTTL: time.Microsecond,
	}
	accepted, err := store.AcceptAttachedWorkerManifest(ctx, manifestAcceptance)
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized || !accepted.Checkpointed {
		t.Fatalf("manifest acceptance = %#v, %v", accepted, err)
	}
	if accepted.Connection.State != domain.AttachedWorkerConnectionOnline || accepted.Connection.ManifestRevision != 1 {
		t.Fatalf("accepted head = %#v", accepted.Connection)
	}
	acceptedReplay, err := store.AcceptAttachedWorkerManifest(ctx, manifestAcceptance)
	if err != nil || acceptedReplay.Status != ports.AttachedWorkerConnectionAuthorized || !acceptedReplay.Checkpointed {
		t.Fatalf("manifest ambiguous replay = %#v, %v", acceptedReplay, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || worker.ObservedState != domain.AttachedWorkerObservedOnline || worker.Revision != manifestAcceptance.ExpectedWorkerRevision+1 {
		t.Fatalf("worker after manifest = %#v found=%t err=%v", worker, found, err)
	}

	wrongBearer := ports.AttachedWorkerExchangeAuthorization{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ConnectionID: accepted.Connection.ID,
		ConnectionGeneration: accepted.Connection.ConnectionGeneration, PresentedSecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("wrong")),
		ExpectedConnectionRevision: accepted.Connection.Revision, PlatformSequence: 2, WorkerSequence: 3,
		PlatformAck: 2, WorkerAck: 2, CheckpointInterval: time.Minute, PresenceTTL: time.Minute,
	}
	if result, err := store.AuthorizeAttachedWorkerExchange(ctx, wrongBearer); err != nil || result.Status != ports.AttachedWorkerConnectionDenied {
		t.Fatalf("wrong bearer = %#v, %v", result, err)
	}

	bucket, err := ydbpartition.BucketV1(string(worker.ID))
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := store.ListExpiredAttachedWorkerPresence(ctx, bucket, time.Now().UTC().Add(time.Second), ports.AttachedWorkerPresenceCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	var candidate domain.AttachedWorkerPresenceExpiry
	for _, item := range candidates {
		if item.WorkerID == worker.ID {
			candidate = item
			break
		}
	}
	if candidate.WorkerID == "" {
		t.Fatalf("presence candidate missing: %#v", candidates)
	}
	expired, err := store.ExpireAttachedWorkerPresence(ctx, candidate)
	if err != nil || !expired {
		t.Fatalf("expire = %t, %v", expired, err)
	}
	expiredReplay, err := store.ExpireAttachedWorkerPresence(ctx, candidate)
	if err != nil || !expiredReplay {
		t.Fatalf("expiry ambiguous replay = %t, %v", expiredReplay, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || worker.ObservedState != domain.AttachedWorkerObservedOffline {
		t.Fatalf("worker after expiry = %#v found=%t err=%v", worker, found, err)
	}
	audits, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, worker.ID, 0, 10)
	if err != nil || len(audits) != 5 || audits[3].Action != domain.AttachedWorkerAuditConnectionManifestAccepted ||
		audits[4].Action != domain.AttachedWorkerAuditConnectionPresenceExpired {
		t.Fatalf("transport audits = %#v, %v", audits, err)
	}

	stale := candidate
	stale.ConnectionRevision++
	if changed, err := store.ExpireAttachedWorkerPresence(ctx, stale); err != nil || changed {
		t.Fatalf("stale candidate changed state: changed=%t err=%v", changed, err)
	}
}

func TestAttachedWorkerTransportRevocationCleansStalePresenceExpiry(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-worker-revoke-expiry"))
	ownerID := domain.UserID(uniqueID("owner-worker-revoke-expiry"))

	enrollment, createAudit := attachedWorkerEnrollmentFixture("revoke-expiry", tenantID, ownerID, now.Add(-time.Second))
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, attachedWorkerClaimFixture(enrollment, 0x42))
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	worker := claimed.Worker
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, attachedWorkerChallengeCreateFixture(worker, "revoke-expiry"))
	if err != nil {
		t.Fatal(err)
	}
	canonicalManifest := []byte("attached-worker-capability-manifest-v1\x00revoke-expiry")
	capabilityDigest := domain.DigestAttachedWorkerCapability(canonicalManifest)
	secretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("revoke-expiry-bearer"))
	activated, err := store.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		ExpectedChallengeRevision: challenge.Revision, ExpectedWorkerRevision: worker.Revision,
		ExpectedEnrollmentGeneration: worker.EnrollmentGeneration, ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest: challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(bytes.Repeat([]byte{0x52}, 32)),
		ExpectedCapabilityDigest: capabilityDigest, AuthTTL: time.Hour,
	})
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated {
		t.Fatalf("activate = %#v, %v", activated, err)
	}
	worker, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker after activation = %#v found=%t err=%v", worker, found, err)
	}
	accepted, err := store.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID: activated.Connection.ID, ConnectionGeneration: activated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: secretDigest,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: capabilityDigest, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey),
			CanonicalManifest: canonicalManifest, ManifestPayload: []byte(`{"version":1,"surface":"revoke-expiry"}`),
			Signature: bytes.Repeat([]byte{0x62}, ed25519.SignatureSize),
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, PresenceTTL: time.Microsecond,
	})
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("manifest acceptance = %#v, %v", accepted, err)
	}
	bucket, err := ydbpartition.BucketV1(string(worker.ID))
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := store.ListExpiredAttachedWorkerPresence(ctx, bucket, time.Now().UTC().Add(time.Second), ports.AttachedWorkerPresenceCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	var candidate domain.AttachedWorkerPresenceExpiry
	for _, item := range candidates {
		if item.WorkerID == worker.ID {
			candidate = item
			break
		}
	}
	if candidate.WorkerID == "" {
		t.Fatalf("presence candidate missing before revoke: %#v", candidates)
	}

	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker before revoke = %#v found=%t err=%v", worker, found, err)
	}
	revoked := worker
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	revoked.EnrollmentGeneration++
	revoked.ConnectionGeneration++
	revoked.Revision++
	revoked.UpdatedAt = worker.UpdatedAt.Add(time.Microsecond).UTC().Truncate(time.Microsecond)
	revoked.RevokedAt = revoked.UpdatedAt
	revokeAudit := attachedWorkerMutationAudit(revoked, domain.AttachedWorkerAuditWorkerRevoked, revoked.UpdatedAt)
	didRevoke, err := store.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ExpectedRevision: worker.Revision, Next: revoked, Audit: revokeAudit, At: revoked.UpdatedAt,
	})
	if err != nil || !didRevoke {
		t.Fatalf("revoke = %t, %v", didRevoke, err)
	}

	expired, err := store.ExpireAttachedWorkerPresence(ctx, candidate)
	if err != nil || expired {
		t.Fatalf("stale revoked candidate = expired=%t err=%v", expired, err)
	}
	candidates, err = store.ListExpiredAttachedWorkerPresence(ctx, bucket, time.Now().UTC().Add(time.Second), ports.AttachedWorkerPresenceCursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range candidates {
		if item.WorkerID == worker.ID {
			t.Fatalf("revoked worker left a poison expiry row: %#v", item)
		}
	}
	stored, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || stored.DesiredState != domain.AttachedWorkerDesiredRevoked ||
		stored.ConnectionGeneration != revoked.ConnectionGeneration || stored.Revision != revoked.Revision {
		t.Fatalf("expiry cleanup mutated revoked worker: %#v found=%t err=%v", stored, found, err)
	}
}

func attachedWorkerChallengeCreateFixture(worker domain.AttachedWorker, suffix string) ports.AttachedWorkerChallengeCreate {
	return ports.AttachedWorkerChallengeCreate{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ChallengeID:  domain.AttachedWorkerChallengeID(uniqueID("challenge-" + suffix)),
		ConnectionID: domain.AttachedWorkerConnectionID(uniqueID("connection-" + suffix)),
		Purpose:      domain.AttachedWorkerAttachInitial, Audience: "worker://sessionless/test",
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration,
		WorkerProtocolMinimum:        1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1},
		SelectedProtocolVersion: 1,
		WorkerNonceDigest:       domain.DigestAttachedWorkerChallenge([]byte("worker-nonce-" + suffix)),
		PlatformNonceDigest:     domain.DigestAttachedWorkerChallenge([]byte("platform-nonce-" + suffix)),
		Lifetime:                time.Minute, Retention: time.Hour,
	}
}
