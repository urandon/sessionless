// Package aw07native shares the existing native attached-worker Ready/Offer/Claim
// setup. It opens no database and applies no schema/cutover: callers provide
// their original Store and DB after their own native setup. All authenticated
// scopes below come from public Store results, not a protocol SQL simulator.
package aw07native

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// ReadyOptions preserves explicit owner/worker identity used by the two-owner
// tests; an omitted identity is independently generated for this test scope.
type ReadyOptions struct {
	Suffix   string
	TenantID domain.TenantID
	WorkerID domain.AttachedWorkerID
	Features []attachedworkerprotocol.ProtocolFeatureV1
}

type ReadyV1 struct {
	Worker            domain.AttachedWorker
	Connection        domain.AttachedWorkerConnection
	SecretDigest      domain.AttachedWorkerConnectionSecretDigest
	PrivateKey        ed25519.PrivateKey
	CanonicalManifest []byte
	At                time.Time
}

// At is an explicit test timestamp from the caller's DB sample, including
// intentional boundary offsets. Zero samples the SAME actual database clock.
// MaxRuntime and AmendBinding preserve existing pre-offer test declarations;
// they are not caller-authored authenticated attempt/receipt approvals.
type OfferOptions struct {
	Suffix                    string
	At                        time.Time
	ContextBody, ArtifactBody []byte
	AmendBinding              func(*domain.HarnessBindingV2)
	MaxRuntime                *time.Duration
}

type ClaimedV1 struct {
	Request            ports.AttachedWorkerSealedInputAuthorization
	Revision           uint64
	Attempt            domain.AttachedWorkerAttemptV1
	Binding            attachedworkerprotocol.AttemptBindingV1
	Accepted           attachedworkerprotocol.FrameV1
	NextWorkerSequence uint64
}

func databaseTime(t *testing.T, ctx context.Context, db *sql.DB) time.Time {
	t.Helper()
	var at time.Time
	if err := db.QueryRowContext(ctx, "SELECT CurrentUtcTimestamp()").Scan(&at); err != nil || at.IsZero() {
		t.Fatalf("sample original AW07 database clock: %v", err)
	}
	return at.UTC().Truncate(time.Microsecond)
}

func uniqueID(prefix string) string {
	return prefix + "-" + strings.ToLower(rand.Text())
}

func Ready(t *testing.T, store *ydbstore.Store, db *sql.DB, options ReadyOptions) ReadyV1 {
	t.Helper()
	if store == nil || db == nil {
		t.Fatal("AW07 setup requires the original Store and DB")
	}
	suffix := UniqueSuffix(t, options.Suffix)
	tenantID, workerID, features := options.TenantID, options.WorkerID, options.Features
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	now := databaseTime(t, ctx, db)
	if tenantID == "" {
		tenantID = domain.TenantID(uniqueID("tenant-worker-" + suffix))
	}
	ownerID := domain.UserID(uniqueID("owner-worker-" + suffix))
	SeedMembership(t, ctx, db, tenantID, ownerID, now)
	enrollment, createAudit := Enrollment(suffix, tenantID, ownerID, now.Add(-time.Second))
	// This helper establishes a ready worker before the scenario under test;
	// enrollment expiry is not the assertion and must outlive a slow CI run.
	enrollment.ExpiresAt = now.Add(30 * time.Minute)
	if workerID != "" {
		enrollment.WorkerID = workerID
		createAudit.WorkerID = workerID
	}
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	claim := EnrollmentClaim(enrollment, 0x71)
	claim.IdentityPublicKey = append([]byte(nil), privateKey.Public().(ed25519.PublicKey)...)
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	worker := claimed.Worker
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, Challenge(worker, suffix))
	if err != nil {
		t.Fatal(err)
	}
	channelBytes := bytes.Repeat([]byte{0x72}, 32)
	canonicalManifest, capabilityDigest, attachedSnapshot, readySnapshot, manifestSignature := ProtocolSnapshot(t, worker, challenge, privateKey, channelBytes, features...)
	// Keep the synthetic bearer recoverable by the transport-level integration
	// test while still unique to this test-owned worker scope.
	secretBytes := sha256.Sum256([]byte("drain-race-bearer-" + string(ownerID)))
	secretDigest := domain.DigestAttachedWorkerConnectionSecret(secretBytes[:])
	activated, err := store.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		Purpose: domain.AttachedWorkerAttachInitial, ExpectedChallengeRevision: challenge.Revision,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest:   challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
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
			CanonicalManifest: canonicalManifest, ManifestPayload: ManifestPayload(t, readySnapshot),
			Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: readySnapshot, PresenceTTL: 10 * time.Minute,
	})
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("manifest = %#v, %v", accepted, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("ready worker = %#v found=%t err=%v", worker, found, err)
	}
	return ReadyV1{Worker: worker, Connection: accepted.Connection, SecretDigest: secretDigest, PrivateKey: privateKey, CanonicalManifest: canonicalManifest, At: now}
}

func Offer(t *testing.T, store *ydbstore.Store, db *sql.DB, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, options OfferOptions) ports.AttachedWorkerAttemptResult {
	t.Helper()
	if store == nil || db == nil {
		t.Fatal("AW07 offer requires the original Store and DB")
	}
	suffix, contextBody, artifactBody, amendBinding := options.Suffix, options.ContextBody, options.ArtifactBody, options.AmendBinding
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	now := options.At
	if now.IsZero() {
		now = databaseTime(t, ctx, db)
	}
	runID := "run-drain-" + suffix
	updateDigest := sha256.Sum256([]byte(suffix))
	// YDB JsonDocument round-trips small decimal IDs without exponent notation;
	// larger 64-bit values are serialized as floats and cannot decode to int64.
	updateID := int64(binary.BigEndian.Uint32(updateDigest[:4]))
	ingress, err := Ingress(worker.TenantID, runID, updateID, now)
	if err != nil {
		t.Fatal(err)
	}
	if contextBody != nil {
		digest := sha256.Sum256(contextBody)
		ingress.Dispatch.ContextSnapshot.Size = int64(len(contextBody))
		ingress.Dispatch.ContextSnapshot.SHA256 = hex.EncodeToString(digest[:])
	}
	if artifactBody != nil {
		digest := sha256.Sum256(artifactBody)
		ingress.InputManifest.Artifacts = []domain.Artifact{{
			Name: "owner-artifact", MediaType: "text/plain",
			Blob: domain.BlobRef{
				TenantID: worker.TenantID,
				Key:      domain.SessionObjectPrefix(worker.TenantID, ingress.Run.SessionID) + "attached-worker/" + suffix,
				Size:     int64(len(artifactBody)), SHA256: hex.EncodeToString(digest[:]),
			},
		}}
	}
	// Keep subscription authority distinct when two owners share one tenant.
	// The generic ingress fixture derives this ID only from the tenant.
	ingress.Run.SubscriptionConnectionID = domain.SubscriptionConnectionID("subscription-" + suffix)
	policyDigest := domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy-" + suffix)))
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		CapabilityDigest: connection.CapabilityDigest, PolicyDigest: policyDigest,
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding := ingress.Dispatch.HarnessBinding
	binding.OwnerUserID = worker.OwnerUserID
	binding.Resource.OwnerUserID = worker.OwnerUserID
	binding.ExecutionPlacementDigest = string(placementDigest)
	if amendBinding != nil {
		amendBinding(&binding)
	}
	if err := binding.ValidateForScope(worker.TenantID, worker.OwnerUserID, ingress.Run.ID, ingress.Attempt.ID, placement); err != nil {
		t.Fatalf("owner %s test binding: %v", suffix, err)
	}
	ingress.Dispatch.ExecutionPlacementV2 = placement
	ingress.Dispatch.HarnessBinding = binding
	ingress.Dispatch.SubstrateBinding = nil
	ingress.Dispatch.AdmissionCostCeiling = nil
	ingress.Dispatch.CredentialOwnerUserID = worker.OwnerUserID
	if _, err := db.ExecContext(ctx,
		`INSERT INTO subscription_connections
		 (tenant_id, subscription_connection_id, actor_id, provider, credential_ref,
		  entitlement_state, quota_state, observed_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		worker.TenantID, ingress.Run.SubscriptionConnectionID, worker.OwnerUserID, "codex", "",
		domain.EntitlementActive, domain.ProviderQuotaAvailable, now, now, now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.IngestTelegram(ctx, ingress); err != nil {
		t.Fatal(err)
	}
	session, owner := Session(worker.TenantID, worker.OwnerUserID, ingress.Run.SessionID, now)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: 5 * time.Minute,
		MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20,
		MaxContextEvents: 64, MaxArtifacts: 8, MaxToolEvents: 16, MaxToolEventBytes: 1 << 18,
	}
	if options.MaxRuntime != nil {
		limits.MaxRuntime = *options.MaxRuntime
	}
	reservation := domain.QuotaReservation{
		ID: domain.QuotaReservationID("reservation-" + runID), TenantID: worker.TenantID, RunID: ingress.Run.ID,
		SubscriptionConnectionID: ingress.Run.SubscriptionConnectionID, Status: domain.ReservationHeld,
		CapacityUnits: 1, HeldAt: now, ExpiresAt: now.Add(30 * time.Minute), UpdatedAt: now,
	}
	job := domain.WorkerJob{
		TenantID: worker.TenantID, RunID: ingress.Run.ID, SessionID: ingress.Run.SessionID,
		TriggerEventID: ingress.Run.TriggerEventID, AttemptID: ingress.Attempt.ID, ReservationID: reservation.ID,
		InputManifestID: ingress.InputManifest.ID, ContextSnapshot: ingress.Dispatch.ContextSnapshot,
		CredentialOwnerUserID: worker.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: limits, DeliveryChat: ingress.Dispatch.DeliveryChat, ReplyToMessageID: ingress.Dispatch.ReplyToMessageID,
		CreatedAt: now,
	}
	err = store.Transact(ctx, worker.TenantID, func(tx ports.StateTx) error {
		run, found, getErr := tx.GetRun(ctx, ingress.Run.ID)
		if getErr != nil || !found {
			return getErr
		}
		run.Status, run.UpdatedAt = domain.RunQueued, now
		attempt, found, getErr := tx.GetAttempt(ctx, ingress.Attempt.ID)
		if getErr != nil || !found {
			return getErr
		}
		attempt.Status, attempt.UpdatedAt = domain.AttemptCreated, now
		if err := tx.PutRun(ctx, run); err != nil {
			return err
		}
		if err := tx.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if err := tx.PutQuotaReservation(ctx, reservation); err != nil {
			return err
		}
		return tx.PutWorkerJob(ctx, job)
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseTTL, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits)
	if err != nil {
		t.Fatal(err)
	}
	leaseID, err := domain.NewAttachedWorkerLeaseIDV1(worker.TenantID, ingress.Run.ID, ingress.Attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	offer, err := store.OfferAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptOffer{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID, ReservationID: reservation.ID,
		LeaseID: leaseID, LeaseTTL: leaseTTL,
	})
	if err != nil || offer.Status != ports.AttachedWorkerExecutionApplied || offer.Outbound == nil {
		t.Fatalf("offer = %#v, %v", offer, err)
	}
	return offer
}

func Claim(t *testing.T, store *ydbstore.Store, db *sql.DB, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, secret domain.AttachedWorkerConnectionSecretDigest, options OfferOptions) ClaimedV1 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	suffix := options.Suffix
	offer := Offer(t, store, db, worker, connection, options)
	batch, err := attachedworkerprotocol.DecodeBatchV1(offer.Outbound.Payload)
	if err != nil || len(batch.Frames) != 1 || batch.Frames[0].LeaseOffer == nil {
		t.Fatalf("decode owner %s offer: frames=%d err=%v", suffix, len(batch.Frames), err)
	}
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("load owner %s connection: found=%t err=%v", suffix, found, err)
	}
	offerFrame := batch.Frames[0]
	claimFrame := attachedworkerprotocol.FrameV1{
		Version:   offerFrame.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID:  string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: offerFrame.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offerFrame.LeaseOffer.Binding, AttemptSequence: 1},
	}
	claim, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, AttemptID: offer.Attempt.AttemptID,
		LeaseGeneration: offer.Attempt.LeaseGeneration, PresentedSecretDigest: secret,
		InboundFrame: claimFrame,
	})
	if err != nil || claim.Status != ports.AttachedWorkerExecutionApplied ||
		claim.Attempt.State != domain.AttachedWorkerAttemptClaimed || claim.Outbound == nil {
		t.Fatalf("claim owner %s: result=%+v err=%v", suffix, claim, err)
	}
	acceptedBatch, err := attachedworkerprotocol.DecodeBatchV1(claim.Outbound.Payload)
	if err != nil || len(acceptedBatch.Frames) != 1 || acceptedBatch.Frames[0].LeaseAccepted == nil {
		t.Fatalf("decode owner %s accepted claim: frames=%d err=%v", suffix, len(acceptedBatch.Frames), err)
	}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secret,
		EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration,
		RunID:                offer.Attempt.RunID, AttemptID: offer.Attempt.AttemptID, AttemptSequence: 1,
		LeaseID: offer.Attempt.LeaseID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		FenceToken:              offer.Attempt.FenceToken,
		LeaseExpiresAtUnixMicro: offer.Attempt.LeaseExpiresAt.UnixMicro(),
		ContextDigest:           offer.Attempt.ContextDigest,
		CapabilityDigest:        offer.Attempt.CapabilityDigest,
		PolicyDigest:            offer.Attempt.PolicyDigest,
	}
	authorized, err := store.AuthorizeAttachedWorkerSealedInput(ctx, request)
	if err != nil || authorized.Status != ports.AttachedWorkerExecutionApplied ||
		authorized.AttemptRevision != claim.Attempt.Revision || authorized.Attempt != claim.Attempt || !authorized.Matches(request) {
		t.Fatalf("authorize owner %s claimed head: result=%+v err=%v", suffix, authorized, err)
	}
	return ClaimedV1{
		Request: request, Revision: authorized.AttemptRevision, Attempt: claim.Attempt,
		Binding: offerFrame.LeaseOffer.Binding, Accepted: acceptedBatch.Frames[0],
		NextWorkerSequence: claimFrame.Sequence + 1,
	}
}

func Enrollment(
	suffix string,
	tenantID domain.TenantID,
	ownerID domain.UserID,
	createdAt time.Time,
) (domain.AttachedWorkerEnrollment, domain.AttachedWorkerAuditEvent) {
	enrollment := domain.AttachedWorkerEnrollment{
		TenantID: tenantID, OwnerUserID: ownerID,
		ID:          domain.AttachedWorkerEnrollmentID(uniqueID("worker-enrollment-" + suffix)),
		WorkerID:    domain.AttachedWorkerID(uniqueID("worker-" + suffix)),
		DisplayName: "worker " + suffix, Audience: "worker://sessionless/test",
		BootstrapDigest: domain.DigestWorkerBootstrap([]byte("bootstrap-" + suffix)),
		CreatedAt:       createdAt, ExpiresAt: createdAt.Add(time.Minute), RetainUntil: createdAt.Add(time.Hour),
		Revision: 1,
	}
	audit := domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: enrollment.WorkerID, EnrollmentID: enrollment.ID,
		Action: domain.AttachedWorkerAuditEnrollmentCreated, OccurredAt: createdAt,
	}
	return enrollment, audit
}

func EnrollmentClaim(
	enrollment domain.AttachedWorkerEnrollment,
	keyByte byte,
) ports.AttachedWorkerClaimMutation {
	return ports.AttachedWorkerClaimMutation{
		TenantID: enrollment.TenantID, OwnerUserID: enrollment.OwnerUserID, EnrollmentID: enrollment.ID,
		ExpectedEnrollmentRevision: enrollment.Revision, PresentedAudience: enrollment.Audience,
		PresentedDigest:   enrollment.BootstrapDigest,
		IdentityPublicKey: bytes.Repeat([]byte{keyByte}, ed25519.PublicKeySize),
	}
}

func Challenge(worker domain.AttachedWorker, suffix string) ports.AttachedWorkerChallengeCreate {
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

func ProtocolSnapshot(
	t *testing.T,
	worker domain.AttachedWorker,
	challenge domain.AttachedWorkerAttachChallenge,
	privateKey ed25519.PrivateKey,
	channelBinding []byte,
	features ...attachedworkerprotocol.ProtocolFeatureV1,
) ([]byte, domain.AttachedWorkerCapabilityDigest, []byte, []byte, []byte) {
	t.Helper()
	offer := attachedworkerprotocol.VersionOfferV1{
		Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{1},
	}
	manifest := CapabilityManifest(worker, offer)
	manifest.Features = append(manifest.Features, features...)
	slices.Sort(manifest.Features)
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
	ready, err := attachedworkerprotocol.ApplyMachineFrameV1(config, attached, attachedworkerprotocol.DirectionWorkerToPlatform, manifestFrame, challenge.CreatedAt.UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	readyBytes, err := attachedworkerprotocol.EncodeMachineSnapshotV1(ready)
	if err != nil {
		t.Fatal(err)
	}
	return canonicalManifest, capabilityDigest, attachedBytes, readyBytes, append([]byte(nil), manifestFrame.Manifest.Signature...)
}

func ManifestPayload(t *testing.T, snapshotBytes []byte) []byte {
	t.Helper()
	snapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(snapshotBytes)
	if err != nil || snapshot.Manifest == nil {
		t.Fatalf("decode signed manifest snapshot: %v", err)
	}
	payload, err := json.Marshal(snapshot.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func CapabilityManifest(worker domain.AttachedWorker, offer attachedworkerprotocol.VersionOfferV1) attachedworkerprotocol.CapabilityManifestV1 {
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

func SeedMembership(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tenantID domain.TenantID,
	userID domain.UserID,
	at time.Time,
) {
	t.Helper()
	if _, err := db.ExecContext(ctx,
		`UPSERT INTO tenants (tenant_id, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4)`,
		tenantID, "active", at, at,
	); err != nil {
		t.Fatal(err)
	}
	membership := domain.TenantMembership{
		TenantID: tenantID, UserID: userID, Role: domain.TenantMembershipOwner,
		Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at,
	}
	payload, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(userID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`UPSERT INTO tenant_memberships
		 (user_bucket, user_id, tenant_id, role, status, security_version,
		  created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		bucket, userID, tenantID, membership.Role, membership.Status,
		membership.SecurityVersion, at, at, string(payload),
	); err != nil {
		t.Fatal(err)
	}
}

func Session(
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	at time.Time,
) (domain.Session, domain.SessionParticipant) {
	return domain.Session{
			ID: sessionID, TenantID: tenantID, CreatedBy: userID,
			Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at,
		}, domain.SessionParticipant{
			TenantID: tenantID, SessionID: sessionID, UserID: userID,
			Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
			CreatedAt: at, UpdatedAt: at,
		}
}

func Ingress(
	tenantID domain.TenantID,
	runID string,
	updateID int64,
	now time.Time,
) (ydbstore.TelegramIngress, error) {
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		tenantID, "user-fixture", domain.RunID(runID), domain.AttemptID("attempt-"+runID),
		domain.SubscriptionConnectionID("subscription-"+string(tenantID)), now,
	)
	if err != nil {
		return ydbstore.TelegramIngress{}, err
	}
	substrate := authority.SubstrateBinding
	cost := authority.AdmissionCostCeiling.Clone()
	run := domain.Run{
		ID:                       domain.RunID(runID),
		TenantID:                 tenantID,
		SessionID:                domain.SessionID("session-" + runID),
		TriggerEventID:           domain.SessionEventID("event-" + runID),
		SubscriptionConnectionID: domain.SubscriptionConnectionID("subscription-" + string(tenantID)),
		Status:                   domain.RunCreated,
		IdempotencyKey:           domain.IdempotencyKey(fmt.Sprintf("telegram-%s-%d", tenantID, updateID)),
		CreatedAt:                now,
		UpdatedAt:                now,
	}
	attempt := domain.Attempt{
		ID: domain.AttemptID("attempt-" + runID), TenantID: tenantID, RunID: run.ID,
		Number: 1, Status: domain.AttemptCreated, CreatedAt: now, UpdatedAt: now,
	}
	dispatch := domain.DispatchOutbox{
		ID: domain.DispatchOutboxID("dispatch-" + runID), TenantID: tenantID,
		RunID: run.ID, AttemptID: attempt.ID, Status: domain.DispatchPending,
		InputManifestID:       domain.ArtifactManifestID("manifest-" + runID),
		ExecutionPlacementV2:  authority.ExecutionPlacementV2,
		HarnessBinding:        authority.HarnessBinding,
		SubstrateBinding:      &substrate,
		AdmissionCostCeiling:  &cost,
		CredentialOwnerUserID: "user-fixture",
		ContextSnapshot: domain.BlobRef{
			TenantID: tenantID, Key: "tenants/" + string(tenantID) + "/context/" + runID,
			Size: 1, SHA256: strings.Repeat("0", 64),
		},
		DeliveryChat:     domain.TelegramChatRef{TenantID: tenantID, ChatID: 442211},
		ReplyToMessageID: updateID,
		IdempotencyKey:   domain.IdempotencyKey("dispatch-key-" + runID),
		CreatedAt:        now, UpdatedAt: now,
	}
	manifest := domain.ArtifactManifest{
		ID: domain.ArtifactManifestID("manifest-" + runID), TenantID: tenantID,
		RunID: run.ID, CreatedAt: now,
	}
	return ydbstore.TelegramIngress{
		TenantID: tenantID, SourceID: "telegram-bot-primary", UpdateID: updateID,
		ExpireAt: now.Add(24 * time.Hour), Run: run, Attempt: attempt,
		InputManifest: manifest, Dispatch: dispatch,
	}, nil
}

func UniqueSuffix(t *testing.T, prefix string) string {
	t.Helper()
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	return prefix + "-" + hex.EncodeToString(entropy[:])
}
