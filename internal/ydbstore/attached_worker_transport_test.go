package ydbstore

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerActivationCreatesBoundAttachingHeadWithoutPresenceLease(t *testing.T) {
	at := time.Date(2026, 8, 25, 12, 0, 0, 123456000, time.UTC)
	worker := domain.AttachedWorker{
		TenantID: "tenant-transport", OwnerUserID: "owner-transport", ID: "worker-transport",
		DisplayName: "transport worker", IdentityPublicKey: bytes.Repeat([]byte{0x31}, ed25519.PublicKeySize),
		EnrollmentGeneration: 1, DesiredState: domain.AttachedWorkerDesiredActive,
		ObservedState: domain.AttachedWorkerObservedOffline, Revision: 4,
		CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute),
	}
	challenge := domain.AttachedWorkerAttachChallenge{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ID: "challenge-transport", ConnectionID: "connection-transport", Purpose: domain.AttachedWorkerAttachInitial,
		Audience: "worker://transport", ExpectedWorkerRevision: worker.Revision,
		ExpectedEnrollmentGeneration: 1, ExpectedConnectionGeneration: 0, TargetConnectionGeneration: 1,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1},
		SelectedProtocolVersion: 1,
		WorkerNonceDigest:       domain.DigestAttachedWorkerChallenge([]byte("worker-nonce")),
		PlatformNonceDigest:     domain.DigestAttachedWorkerChallenge([]byte("platform-nonce")),
		CreatedAt:               at.Add(-time.Second), ExpiresAt: at.Add(time.Minute), RetainUntil: at.Add(time.Hour), Revision: 1,
	}
	request := ports.AttachedWorkerConnectionActivation{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ChallengeID: challenge.ID, ExpectedChallengeRevision: challenge.Revision,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: 1, ExpectedConnectionGeneration: 0,
		PresentedWorkerNonceDigest: challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest:   domain.DigestAttachedWorkerConnectionSecret([]byte("secret")),
		ChannelBinding:           domain.NewAttachedWorkerChannelBinding(bytes.Repeat([]byte{0x42}, 32)),
		ExpectedCapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("canonical manifest")), AuthTTL: time.Hour,
		ProtocolSnapshot: []byte(`{"snapshot":"attached"}`),
	}

	connection, nextWorker, audit := attachedWorkerActivationTargets(request, challenge, worker, at)
	if err := connection.Validate(); err != nil {
		t.Fatal(err)
	}
	if connection.State != domain.AttachedWorkerConnectionAttaching || !connection.LastCheckpointAt.IsZero() ||
		!connection.PresenceExpiresAt.IsZero() || connection.PlatformSequence != 2 || connection.WorkerSequence != 2 ||
		connection.PlatformAck != 2 || connection.WorkerAck != 1 {
		t.Fatalf("invalid attaching head: %#v", connection)
	}
	if connection.ChannelBinding != request.ChannelBinding || connection.CapabilityDigest != request.ExpectedCapabilityDigest {
		t.Fatalf("attaching evidence was not pinned: %#v", connection)
	}
	if nextWorker.ObservedState != worker.ObservedState || nextWorker.ConnectionGeneration != 1 || nextWorker.Revision != worker.Revision+1 ||
		audit.Action != domain.AttachedWorkerAuditConnectionGenerationAdvanced {
		t.Fatalf("invalid activation worker/audit: worker=%#v audit=%#v", nextWorker, audit)
	}
}

func TestAttachedWorkerManifestAcceptanceHashesCanonicalProtocolBytesNotJSONSnapshot(t *testing.T) {
	canonical := []byte("attached-worker-capability-manifest-v1\x00canonical-length-prefixed-evidence")
	payload := []byte(`{"version":1,"surface":"codex exec"}`)
	request := ports.AttachedWorkerManifestAcceptance{
		TenantID: "tenant-manifest", OwnerUserID: "owner-manifest", WorkerID: "worker-manifest",
		ConnectionID: "connection-manifest", ConnectionGeneration: 2, ExpectedConnectionRevision: 1, ExpectedWorkerRevision: 2,
		PresentedSecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("secret")),
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: domain.DigestAttachedWorkerCapability(canonical), ProtocolVersion: 1,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(bytes.Repeat([]byte{0x22}, ed25519.PublicKeySize)),
			CanonicalManifest: canonical, ManifestPayload: payload, Signature: bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, PresenceTTL: time.Minute,
		ProtocolSnapshot: []byte(`{"snapshot":"ready"}`),
	}
	if domain.DigestAttachedWorkerCapability(payload) == request.Capability.Digest {
		t.Fatal("fixture failed to distinguish canonical protocol bytes from JSON evidence")
	}
	if err := validateAttachedWorkerManifestAcceptance(request); err != nil {
		t.Fatalf("valid canonical manifest rejected: %v", err)
	}
	request.Capability.CanonicalManifest = []byte("divergent canonical bytes")
	if err := validateAttachedWorkerManifestAcceptance(request); err == nil {
		t.Fatal("canonical manifest digest mismatch was accepted")
	}
}

func TestAttachedWorkerManifestAcceptanceRequiresExactHandshakeWatermarks(t *testing.T) {
	canonical := []byte("canonical-manifest")
	request := ports.AttachedWorkerManifestAcceptance{
		TenantID: "tenant-watermark", OwnerUserID: "owner-watermark", WorkerID: "worker-watermark",
		ConnectionID: "connection-watermark", ConnectionGeneration: 1, ExpectedConnectionRevision: 1, ExpectedWorkerRevision: 2,
		PresentedSecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("secret")),
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: domain.DigestAttachedWorkerCapability(canonical), ProtocolVersion: 1,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(bytes.Repeat([]byte{0x22}, ed25519.PublicKeySize)),
			CanonicalManifest: canonical, ManifestPayload: []byte(`{"version":1}`), Signature: bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, PresenceTTL: time.Minute,
		ProtocolSnapshot: []byte(`{"snapshot":"ready"}`),
	}
	request.WorkerSequence = 4
	if err := validateAttachedWorkerManifestAcceptance(request); err == nil {
		t.Fatal("manifest outside the canonical seq3/ack2 handshake was accepted")
	}
}

func TestAttachedWorkerCheckpointPreservesDrainingState(t *testing.T) {
	at := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	connection := domain.AttachedWorkerConnection{
		TenantID: "tenant-drain", OwnerUserID: "owner-drain", WorkerID: "worker-drain",
		ID: "connection-drain", ActivationChallengeID: "challenge-drain",
		EnrollmentGeneration: 1, ConnectionGeneration: 2, ProtocolVersion: 1,
		CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("canonical")),
		SecretDigest:     domain.DigestAttachedWorkerConnectionSecret([]byte("secret")),
		ChannelBinding:   domain.NewAttachedWorkerChannelBinding(bytes.Repeat([]byte{0x44}, 32)),
		ManifestRevision: 1, ManifestIdentityKey: domain.DigestAttachedWorkerIdentityKey(bytes.Repeat([]byte{0x22}, 32)),
		ManifestSignature: bytes.Repeat([]byte{0x55}, ed25519.SignatureSize), ManifestObservedAt: at.Add(-2 * time.Second),
		State: domain.AttachedWorkerConnectionDraining, PlatformSequence: 2, WorkerSequence: 3,
		PlatformAck: 2, WorkerAck: 2, ConnectedAt: at.Add(-3 * time.Second), LastCheckpointAt: at.Add(-time.Second),
		ProtocolSnapshot:  []byte(`{"snapshot":"draining"}`),
		PresenceExpiresAt: at.Add(time.Minute), AuthExpiresAt: at.Add(time.Hour), Revision: 3,
	}
	request := ports.AttachedWorkerExchangeAuthorization{
		PlatformSequence: 3, WorkerSequence: 4, PlatformAck: 3, WorkerAck: 3, PresenceTTL: time.Minute,
		ProtocolSnapshot: []byte(`{"snapshot":"draining-next"}`),
	}
	next := attachedWorkerCheckpointTarget(connection, request, at)
	if next.State != domain.AttachedWorkerConnectionDraining {
		t.Fatalf("checkpoint resurrected draining connection as %q", next.State)
	}
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	if !sameAppliedAttachedWorkerCheckpoint(next, request) {
		t.Fatal("exact checkpoint target was not reconciled")
	}
	request.PresenceTTL += time.Microsecond
	if sameAppliedAttachedWorkerCheckpoint(next, request) {
		t.Fatal("checkpoint replay with a different presence TTL was accepted")
	}
}

func TestAttachedWorkerCapabilityContentCanBeReusedAcrossConnectionGenerations(t *testing.T) {
	canonical := []byte("stable-canonical-capability")
	base := ports.AttachedWorkerManifestAcceptance{
		TenantID: "tenant-reconnect", OwnerUserID: "owner-reconnect", WorkerID: "worker-reconnect",
		ConnectionGeneration: 1,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: domain.DigestAttachedWorkerCapability(canonical), ProtocolVersion: 1,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(bytes.Repeat([]byte{0x22}, 32)),
			ManifestPayload:   []byte(`{"version":1}`), Signature: bytes.Repeat([]byte{0x51}, ed25519.SignatureSize),
		},
	}
	first := attachedWorkerManifestTarget(base, 1)
	reconnect := base
	reconnect.ConnectionGeneration = 2
	reconnect.Capability.Signature = bytes.Repeat([]byte{0x52}, ed25519.SignatureSize)
	second := attachedWorkerManifestTarget(reconnect, 1)
	if !sameAttachedWorkerManifest(first, second) {
		t.Fatalf("stable immutable capability conflicted across connection generations: first=%#v second=%#v", first, second)
	}
	if bytes.Equal(base.Capability.Signature, reconnect.Capability.Signature) {
		t.Fatal("fixture did not exercise distinct per-connection signed observations")
	}
}
