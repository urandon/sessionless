//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func TestAttachedWorkerTransportTwoPhaseAttachAuthorizationAndExpiry(t *testing.T) {
	store, _ := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-worker-transport"))
	ownerID := domain.UserID(uniqueID("owner-worker-transport"))
	otherOwnerID := domain.UserID(uniqueID("other-owner-worker-transport"))

	enrollment, createAudit := attachedWorkerEnrollmentFixture("transport", tenantID, ownerID, now.Add(-time.Second))
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x41}, ed25519.SeedSize))
	claim := attachedWorkerClaimFixture(enrollment, 0x41)
	claim.IdentityPublicKey = append([]byte(nil), privateKey.Public().(ed25519.PublicKey)...)
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

	channelBytes := bytes.Repeat([]byte{0x51}, 32)
	canonicalManifest, capabilityDigest, attachedSnapshot, readySnapshot, manifestSignature := attachedWorkerProtocolSnapshotFixture(t, worker, challenge, privateKey, channelBytes)
	secretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("transport-bearer"))
	activation := ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		Purpose:                   domain.AttachedWorkerAttachInitial,
		ExpectedChallengeRevision: challenge.Revision, ExpectedWorkerRevision: worker.Revision,
		ExpectedEnrollmentGeneration: worker.EnrollmentGeneration, ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest: challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channelBytes),
		ExpectedCapabilityDigest: capabilityDigest, ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
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
			Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, ProtocolSnapshot: readySnapshot, PresenceTTL: time.Microsecond,
	}
	accepted, err := store.AcceptAttachedWorkerManifest(ctx, manifestAcceptance)
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized || !accepted.Checkpointed {
		t.Fatalf("manifest acceptance = %#v, %v", accepted, err)
	}
	if accepted.Connection.State != domain.AttachedWorkerConnectionOnline || accepted.Connection.ManifestRevision != 1 {
		t.Fatalf("accepted head = %#v", accepted.Connection)
	}
	loadedManifest, found, err := store.LoadAttachedWorkerCapabilityManifest(ctx, tenantID, ownerID, worker.ID, capabilityDigest)
	if err != nil || !found || loadedManifest.Digest != capabilityDigest || loadedManifest.WorkerID != worker.ID {
		t.Fatalf("load capability manifest = %#v found=%t err=%v", loadedManifest, found, err)
	}
	acceptedReplay, err := store.AcceptAttachedWorkerManifest(ctx, manifestAcceptance)
	if err != nil || acceptedReplay.Status != ports.AttachedWorkerConnectionAuthorized || !acceptedReplay.Checkpointed {
		t.Fatalf("manifest ambiguous replay = %#v, %v", acceptedReplay, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || worker.ObservedState != domain.AttachedWorkerObservedOnline || worker.Revision != manifestAcceptance.ExpectedWorkerRevision+1 {
		t.Fatalf("worker after manifest = %#v found=%t err=%v", worker, found, err)
	}

	reconnectCreate := attachedWorkerChallengeCreateFixture(worker, "reconnect")
	reconnectCreate.Purpose = domain.AttachedWorkerAttachReconnect
	reconnectCreate.ExpectedConnectionID = accepted.Connection.ID
	reconnectCreate.ExpectedConnectionRevision = accepted.Connection.Revision
	reconnectCreate.ExpectedCapabilityDigest = accepted.Connection.CapabilityDigest
	reconnectCreate.ExpectedProtocolSnapshot = append([]byte(nil), accepted.Connection.ProtocolSnapshot...)
	reconnectChallenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, reconnectCreate)
	if err != nil {
		t.Fatal(err)
	}
	loadedReconnectChallenge, found, err := store.LoadAttachedWorkerAttachChallenge(ctx, tenantID, ownerID, worker.ID, reconnectChallenge.ID)
	if err != nil || !found || loadedReconnectChallenge.ExpectedConnectionID != accepted.Connection.ID ||
		loadedReconnectChallenge.ExpectedConnectionRevision != accepted.Connection.Revision ||
		loadedReconnectChallenge.ExpectedCapabilityDigest != accepted.Connection.CapabilityDigest ||
		!bytes.Equal(loadedReconnectChallenge.ExpectedProtocolSnapshot, accepted.Connection.ProtocolSnapshot) {
		t.Fatalf("durable reconnect authority = %#v found=%t err=%v", loadedReconnectChallenge, found, err)
	}
	reconnectChannelBytes := bytes.Repeat([]byte{0x53}, 32)
	reconnectAttachedSnapshot, reconnectReadySnapshot, reconnectManifestSignature := attachedWorkerReconnectProtocolSnapshotFixture(
		t, worker, accepted.Connection, reconnectChallenge, privateKey, reconnectChannelBytes,
	)
	reconnectSecretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("transport-reconnect-bearer"))
	reconnectActivation := ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: reconnectChallenge.ID,
		Purpose: reconnectChallenge.Purpose, ExpectedChallengeRevision: reconnectChallenge.Revision,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration:     worker.ConnectionGeneration,
		ExpectedConnectionID:             accepted.Connection.ID,
		ExpectedConnectionRevision:       accepted.Connection.Revision,
		ExpectedPreviousCapabilityDigest: accepted.Connection.CapabilityDigest,
		ExpectedPreviousProtocolSnapshot: append([]byte(nil), accepted.Connection.ProtocolSnapshot...),
		PresentedWorkerNonceDigest:       reconnectChallenge.WorkerNonceDigest,
		PresentedPlatformNonceDigest:     reconnectChallenge.PlatformNonceDigest,
		ConnectionSecretDigest:           reconnectSecretDigest,
		ChannelBinding:                   domain.NewAttachedWorkerChannelBinding(reconnectChannelBytes),
		ExpectedCapabilityDigest:         accepted.Connection.CapabilityDigest,
		ProtocolSnapshot:                 reconnectAttachedSnapshot,
		AuthTTL:                          time.Hour,
	}
	divergentPrevious := attachedWorkerDrainingConnectionFixture(t, worker, accepted.Connection)
	divergentChannelBytes := bytes.Repeat([]byte{0x55}, 32)
	divergentAttachedSnapshot, _, _ := attachedWorkerReconnectProtocolSnapshotFixture(
		t, worker, divergentPrevious, reconnectChallenge, privateKey, divergentChannelBytes,
	)
	divergentAuthority := reconnectActivation
	divergentAuthority.ConnectionSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("transport-divergent-authority-bearer"))
	divergentAuthority.ChannelBinding = domain.NewAttachedWorkerChannelBinding(divergentChannelBytes)
	divergentAuthority.ProtocolSnapshot = divergentAttachedSnapshot
	if result, err := store.ActivateAttachedWorkerConnection(ctx, divergentAuthority); err != nil || result.Status != ports.AttachedWorkerConnectionConflict {
		t.Fatalf("divergent reconnect authority = %#v, %v", result, err)
	}
	otherReconnectChannelBytes := bytes.Repeat([]byte{0x54}, 32)
	otherReconnectAttachedSnapshot, otherReconnectReadySnapshot, otherReconnectManifestSignature := attachedWorkerReconnectProtocolSnapshotFixture(
		t, worker, accepted.Connection, reconnectChallenge, privateKey, otherReconnectChannelBytes,
	)
	otherReconnectActivation := reconnectActivation
	otherReconnectActivation.ConnectionSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("transport-other-reconnect-bearer"))
	otherReconnectActivation.ChannelBinding = domain.NewAttachedWorkerChannelBinding(otherReconnectChannelBytes)
	otherReconnectActivation.ProtocolSnapshot = otherReconnectAttachedSnapshot
	type reconnectContender struct {
		request           ports.AttachedWorkerConnectionActivation
		readySnapshot     []byte
		manifestSignature []byte
		result            ports.AttachedWorkerConnectionResult
		err               error
	}
	contenders := []reconnectContender{
		{request: reconnectActivation, readySnapshot: reconnectReadySnapshot, manifestSignature: reconnectManifestSignature},
		{request: otherReconnectActivation, readySnapshot: otherReconnectReadySnapshot, manifestSignature: otherReconnectManifestSignature},
	}
	start := make(chan struct{})
	results := make(chan reconnectContender, len(contenders))
	for _, contender := range contenders {
		contender := contender
		go func() {
			<-start
			contender.result, contender.err = store.ActivateAttachedWorkerConnection(ctx, contender.request)
			results <- contender
		}()
	}
	close(start)
	var winner, loser reconnectContender
	activatedCount, consumedCount := 0, 0
	for range contenders {
		var contender reconnectContender
		select {
		case contender = <-results:
		case <-ctx.Done():
			t.Fatalf("concurrent reconnect did not complete: %v", context.Cause(ctx))
		}
		if contender.err != nil {
			t.Fatalf("concurrent reconnect activation: %v", contender.err)
		}
		switch contender.result.Status {
		case ports.AttachedWorkerConnectionActivated:
			activatedCount++
			winner = contender
		case ports.AttachedWorkerConnectionConsumed:
			consumedCount++
			loser = contender
		default:
			t.Fatalf("concurrent reconnect status = %#v", contender.result)
		}
	}
	if activatedCount != 1 || consumedCount != 1 {
		t.Fatalf("concurrent reconnect activated=%d consumed=%d", activatedCount, consumedCount)
	}
	reconnectActivation = winner.request
	reconnectReadySnapshot = winner.readySnapshot
	reconnectManifestSignature = winner.manifestSignature
	reconnectSecretDigest = winner.request.ConnectionSecretDigest
	reconnectActivated := winner.result
	if reconnectActivated.Connection.ID == accepted.Connection.ID ||
		reconnectActivated.Connection.ConnectionGeneration != accepted.Connection.ConnectionGeneration+1 ||
		reconnectActivated.Connection.State != domain.AttachedWorkerConnectionAttaching ||
		!reconnectActivated.Connection.PresenceExpiresAt.IsZero() {
		t.Fatalf("reconnect did not rotate to a presence-free attaching head: %#v", reconnectActivated.Connection)
	}
	reconnectReplay, err := store.ActivateAttachedWorkerConnection(ctx, reconnectActivation)
	if err != nil || reconnectReplay.Status != ports.AttachedWorkerConnectionActivated ||
		reconnectReplay.Connection.ID != reconnectActivated.Connection.ID {
		t.Fatalf("reconnect activation replay = %#v, %v", reconnectReplay, err)
	}
	if result, err := store.ActivateAttachedWorkerConnection(ctx, loser.request); err != nil || result.Status != ports.AttachedWorkerConnectionConsumed {
		t.Fatalf("divergent reconnect replay = %#v, %v", result, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || worker.ConnectionGeneration != reconnectActivated.Connection.ConnectionGeneration ||
		worker.ObservedState != domain.AttachedWorkerObservedOffline {
		t.Fatalf("worker after reconnect activation = %#v found=%t err=%v", worker, found, err)
	}
	reconnectManifestAcceptance := ports.AttachedWorkerManifestAcceptance{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID:               reconnectActivated.Connection.ID,
		ConnectionGeneration:       reconnectActivated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: reconnectActivated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: reconnectSecretDigest,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: capabilityDigest, ProtocolVersion: reconnectChallenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey),
			CanonicalManifest: canonicalManifest, ManifestPayload: []byte(`{"version":1,"surface":"codex-exec"}`),
			Signature: reconnectManifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: reconnectReadySnapshot, PresenceTTL: time.Microsecond,
	}
	reconnectAccepted, err := store.AcceptAttachedWorkerManifest(ctx, reconnectManifestAcceptance)
	if err != nil || reconnectAccepted.Status != ports.AttachedWorkerConnectionAuthorized || !reconnectAccepted.Checkpointed {
		t.Fatalf("reconnect manifest acceptance = %#v, %v", reconnectAccepted, err)
	}
	accepted = reconnectAccepted
	secretDigest = reconnectSecretDigest
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found || worker.ObservedState != domain.AttachedWorkerObservedOnline {
		t.Fatalf("worker after reconnect manifest = %#v found=%t err=%v", worker, found, err)
	}

	wrongBearer := ports.AttachedWorkerExchangeAuthorization{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ConnectionID: accepted.Connection.ID,
		ConnectionGeneration: accepted.Connection.ConnectionGeneration, PresentedSecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("wrong")),
		ExpectedConnectionRevision: accepted.Connection.Revision, PlatformSequence: 2, WorkerSequence: 3,
		PlatformAck: 2, WorkerAck: 2, ProtocolSnapshot: append([]byte(nil), accepted.Connection.ProtocolSnapshot...),
		CheckpointInterval: time.Minute, PresenceTTL: time.Minute,
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
	if err != nil || len(audits) != 7 || audits[5].Action != domain.AttachedWorkerAuditConnectionManifestAccepted ||
		audits[6].Action != domain.AttachedWorkerAuditConnectionPresenceExpired {
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
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	claim := attachedWorkerClaimFixture(enrollment, 0x42)
	claim.IdentityPublicKey = append([]byte(nil), privateKey.Public().(ed25519.PublicKey)...)
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	worker := claimed.Worker
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, attachedWorkerChallengeCreateFixture(worker, "revoke-expiry"))
	if err != nil {
		t.Fatal(err)
	}
	channelBytes := bytes.Repeat([]byte{0x52}, 32)
	canonicalManifest, capabilityDigest, attachedSnapshot, readySnapshot, manifestSignature := attachedWorkerProtocolSnapshotFixture(t, worker, challenge, privateKey, channelBytes)
	secretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("revoke-expiry-bearer"))
	activated, err := store.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		Purpose:                   domain.AttachedWorkerAttachInitial,
		ExpectedChallengeRevision: challenge.Revision, ExpectedWorkerRevision: worker.Revision,
		ExpectedEnrollmentGeneration: worker.EnrollmentGeneration, ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest: challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channelBytes),
		ExpectedCapabilityDigest: capabilityDigest, ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
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
			Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, ProtocolSnapshot: readySnapshot, PresenceTTL: time.Microsecond,
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
	workerNonce, platformNonce := bytes.Repeat([]byte{0x61}, 32), bytes.Repeat([]byte{0x71}, 32)
	if suffix == "reconnect" {
		workerNonce, platformNonce = bytes.Repeat([]byte{0x62}, 32), bytes.Repeat([]byte{0x72}, 32)
	}
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
		WorkerNonceDigest:       domain.DigestAttachedWorkerChallenge(workerNonce),
		PlatformNonceDigest:     domain.DigestAttachedWorkerChallenge(platformNonce),
		Lifetime:                time.Minute, Retention: time.Hour,
	}
}

func attachedWorkerProtocolSnapshotFixture(
	t *testing.T,
	worker domain.AttachedWorker,
	challenge domain.AttachedWorkerAttachChallenge,
	privateKey ed25519.PrivateKey,
	channelBinding []byte,
) ([]byte, domain.AttachedWorkerCapabilityDigest, []byte, []byte, []byte) {
	t.Helper()
	offer := attachedworkerprotocol.VersionOfferV1{
		Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{1},
	}
	manifest := attachedWorkerCapabilityManifestFixture(worker, offer)
	canonicalManifest, err := attachedworkerprotocol.CanonicalManifestBytesV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	protocolDigest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	capabilityDigest := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(protocolDigest))
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
		IdentityPublicKey: append([]byte(nil), worker.IdentityPublicKey...), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Version: 1, ChannelBinding: append([]byte(nil), channelBinding...),
	}
	workerNonce, platformNonce := bytes.Repeat([]byte{0x61}, 32), bytes.Repeat([]byte{0x71}, 32)
	attach := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Sequence: 2, Ack: 1, Kind: attachedworkerprotocol.MessageAttach,
		Attach: &attachedworkerprotocol.AttachV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1,
			WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: protocolDigest},
	}
	if err := attachedworkerprotocol.SignAttachV1(privateKey, auth, &attach); err != nil {
		t.Fatal(err)
	}
	accepted := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Sequence: 2, Ack: 2, Kind: attachedworkerprotocol.MessageAttachAccepted,
		AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1,
			WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: protocolDigest},
	}
	config := attachedworkerprotocol.MachineConfig{Auth: auth, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}}
	attached, err := attachedworkerprotocol.BuildInitialAttachSnapshotV1(config, attach, accepted)
	if err != nil {
		t.Fatal(err)
	}
	attachedBytes, err := attachedworkerprotocol.EncodeMachineSnapshotV1(attached)
	if err != nil {
		t.Fatal(err)
	}
	manifestFrame := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Sequence: 3, Ack: 2, Kind: attachedworkerprotocol.MessageManifest,
		Manifest: &attachedworkerprotocol.ManifestV1{Manifest: manifest, Digest: protocolDigest},
	}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, auth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	ready, err := attachedworkerprotocol.ApplyMachineFrameV1(config, attached, attachedworkerprotocol.DirectionWorkerToPlatform, manifestFrame, time.Now().UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	readyBytes, err := attachedworkerprotocol.EncodeMachineSnapshotV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	return canonicalManifest, capabilityDigest, attachedBytes, readyBytes, append([]byte(nil), manifestFrame.Manifest.Signature...)
}

func attachedWorkerCapabilityManifestFixture(worker domain.AttachedWorker, offer attachedworkerprotocol.VersionOfferV1) attachedworkerprotocol.CapabilityManifestV1 {
	return attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: "linux", Architecture: "amd64", BuildID: "integration-build", HarnessName: "fixture",
		HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: bytes.Repeat([]byte{0x73}, 32),
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{
			attachedworkerprotocol.IsolationFilesystemBoundary,
			attachedworkerprotocol.IsolationNetworkBoundary,
			attachedworkerprotocol.IsolationProcessBoundary,
		},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation,
			attachedworkerprotocol.FeatureProgress,
			attachedworkerprotocol.FeatureReconnect,
		},
		MaxConcurrentAttempts: 1,
	}
}

func attachedWorkerReconnectProtocolSnapshotFixture(
	t *testing.T,
	worker domain.AttachedWorker,
	previous domain.AttachedWorkerConnection,
	challenge domain.AttachedWorkerAttachChallenge,
	privateKey ed25519.PrivateKey,
	channelBinding []byte,
) ([]byte, []byte, []byte) {
	t.Helper()
	previousSnapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(previous.ProtocolSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	previousChannelBinding, err := hex.DecodeString(string(previous.ChannelBinding))
	if err != nil {
		t.Fatal(err)
	}
	previousConfig := attachedworkerprotocol.MachineConfig{
		Auth: attachedworkerprotocol.AuthContextV1{
			TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
			IdentityPublicKey: append([]byte(nil), worker.IdentityPublicKey...), EnrollmentGeneration: worker.EnrollmentGeneration,
			ConnectionGeneration: previous.ConnectionGeneration, Version: attachedworkerprotocol.ProtocolVersion(previous.ProtocolVersion),
			ChannelBinding: append([]byte(nil), previousChannelBinding...),
		},
		WorkerOffer: previousSnapshot.Hello.Offer, PlatformOffer: previousSnapshot.Challenge.PlatformOffer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersion(previous.ProtocolVersion)},
	}
	previousMachine, err := attachedworkerprotocol.RestoreConformanceMachine(previousConfig, previousSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	nextAuth := previousConfig.Auth
	nextAuth.ConnectionGeneration = challenge.TargetConnectionGeneration
	nextAuth.ChannelBinding = append([]byte(nil), channelBinding...)
	claim, err := previousMachine.BeginReconnect(nextAuth)
	if err != nil {
		t.Fatal(err)
	}
	capabilityDigest, err := hex.DecodeString(string(previous.CapabilityDigest))
	if err != nil {
		t.Fatal(err)
	}
	reconnect, err := attachedworkerprotocol.BuildReconnectV1(claim, attachedworkerprotocol.ReconnectNegotiationV1{
		WorkerOffer: previousSnapshot.Hello.Offer, PlatformOffer: previousSnapshot.Challenge.PlatformOffer,
		SelectedVersion: attachedworkerprotocol.ProtocolVersion(previous.ProtocolVersion),
		WorkerNonce:     bytes.Repeat([]byte{0x62}, 32), PlatformNonce: bytes.Repeat([]byte{0x72}, 32),
		CapabilityDigest: capabilityDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconnectFrame := attachedworkerprotocol.FrameV1{
		Version:   attachedworkerprotocol.ProtocolVersion(previous.ProtocolVersion),
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID:  string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Sequence: 2, Ack: 1,
		Kind: attachedworkerprotocol.MessageReconnect, Reconnect: &reconnect,
	}
	if err := attachedworkerprotocol.SignReconnectV1(privateKey, nextAuth, &reconnectFrame); err != nil {
		t.Fatal(err)
	}
	_, attached, err := attachedworkerprotocol.BuildReconnectAcceptedSnapshotV1(previousConfig, previousSnapshot, nextAuth, reconnectFrame)
	if err != nil {
		t.Fatal(err)
	}
	attachedBytes, err := attachedworkerprotocol.EncodeMachineSnapshotV1(attached)
	if err != nil {
		t.Fatal(err)
	}
	nextConfig := previousConfig
	nextConfig.Auth = nextAuth
	manifest := attachedWorkerCapabilityManifestFixture(worker, previousSnapshot.Hello.Offer)
	manifestFrame := attachedworkerprotocol.FrameV1{
		Version:   attachedworkerprotocol.ProtocolVersion(previous.ProtocolVersion),
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID:  string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration, Sequence: 3, Ack: 2,
		Kind:     attachedworkerprotocol.MessageManifest,
		Manifest: &attachedworkerprotocol.ManifestV1{Manifest: manifest, Digest: capabilityDigest},
	}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, nextAuth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	ready, err := attachedworkerprotocol.ApplyMachineFrameV1(
		nextConfig, attached, attachedworkerprotocol.DirectionWorkerToPlatform, manifestFrame, time.Now().UnixMicro(),
	)
	if err != nil {
		t.Fatal(err)
	}
	readyBytes, err := attachedworkerprotocol.EncodeMachineSnapshotV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	return attachedBytes, readyBytes, append([]byte(nil), manifestFrame.Manifest.Signature...)
}

func attachedWorkerDrainingConnectionFixture(
	t *testing.T,
	worker domain.AttachedWorker,
	connection domain.AttachedWorkerConnection,
) domain.AttachedWorkerConnection {
	t.Helper()
	snapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(connection.ProtocolSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	channelBinding, err := hex.DecodeString(string(connection.ChannelBinding))
	if err != nil {
		t.Fatal(err)
	}
	config := attachedworkerprotocol.MachineConfig{
		Auth: attachedworkerprotocol.AuthContextV1{
			TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
			IdentityPublicKey: append([]byte(nil), worker.IdentityPublicKey...), EnrollmentGeneration: worker.EnrollmentGeneration,
			ConnectionGeneration: connection.ConnectionGeneration, Version: attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion),
			ChannelBinding: channelBinding,
		},
		WorkerOffer: snapshot.Hello.Offer, PlatformOffer: snapshot.Challenge.PlatformOffer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion)},
	}
	drain := attachedworkerprotocol.FrameV1{
		Version:   attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion),
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, snapshot.Platform.Sequence+1),
		WorkerID:  string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration,
		Sequence:             snapshot.Platform.Sequence + 1, Ack: snapshot.Worker.Sequence,
		Kind: attachedworkerprotocol.MessageDrain, Drain: &attachedworkerprotocol.DrainV1{Revision: 1},
	}
	snapshot, err = attachedworkerprotocol.ApplyMachineFrameV1(
		config, snapshot, attachedworkerprotocol.DirectionPlatformToWorker, drain, time.Now().UnixMicro(),
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	connection.State = domain.AttachedWorkerConnectionDraining
	connection.PlatformSequence = snapshot.Platform.Sequence
	connection.WorkerSequence = snapshot.Worker.Sequence
	connection.PlatformAck = snapshot.Platform.Ack
	connection.WorkerAck = snapshot.Worker.Ack
	connection.ProtocolSnapshot = encoded
	return connection
}
