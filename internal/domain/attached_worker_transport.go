package domain

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

const AttachedWorkerCapabilityManifestVersionV1 uint32 = 1

const maxAttachedWorkerProtocolSnapshotBytes = 64 << 10

type (
	AttachedWorkerConnectionID           string
	AttachedWorkerChallengeID            string
	AttachedWorkerAttachPurpose          string
	AttachedWorkerConnectionState        string
	AttachedWorkerChallengeDigest        string
	AttachedWorkerConnectionSecretDigest string
	AttachedWorkerChannelBinding         string
	AttachedWorkerCapabilityDigest       string
	AttachedWorkerIdentityKeyDigest      string
)

func (id AttachedWorkerConnectionID) Validate() error {
	return ValidateOpaqueID("attached_worker_connection_id", string(id))
}

func (id AttachedWorkerChallengeID) Validate() error {
	return ValidateOpaqueID("attached_worker_challenge_id", string(id))
}

const (
	AttachedWorkerAttachInitial   AttachedWorkerAttachPurpose = "initial"
	AttachedWorkerAttachReconnect AttachedWorkerAttachPurpose = "reconnect"
)

func (purpose AttachedWorkerAttachPurpose) Valid() bool {
	return purpose == AttachedWorkerAttachInitial || purpose == AttachedWorkerAttachReconnect
}

const (
	AttachedWorkerConnectionAttaching  AttachedWorkerConnectionState = "attaching"
	AttachedWorkerConnectionOnline     AttachedWorkerConnectionState = "online"
	AttachedWorkerConnectionDraining   AttachedWorkerConnectionState = "draining"
	AttachedWorkerConnectionOffline    AttachedWorkerConnectionState = "offline"
	AttachedWorkerConnectionSuperseded AttachedWorkerConnectionState = "superseded"
	AttachedWorkerConnectionRevoked    AttachedWorkerConnectionState = "revoked"
)

func (state AttachedWorkerConnectionState) Valid() bool {
	switch state {
	case AttachedWorkerConnectionAttaching, AttachedWorkerConnectionOnline, AttachedWorkerConnectionDraining,
		AttachedWorkerConnectionOffline, AttachedWorkerConnectionSuperseded,
		AttachedWorkerConnectionRevoked:
		return true
	default:
		return false
	}
}

func DigestAttachedWorkerChallenge(value []byte) AttachedWorkerChallengeDigest {
	return AttachedWorkerChallengeDigest(attachedWorkerSHA256(value))
}

func DigestAttachedWorkerConnectionSecret(value []byte) AttachedWorkerConnectionSecretDigest {
	return AttachedWorkerConnectionSecretDigest(attachedWorkerSHA256(value))
}

func (digest AttachedWorkerConnectionSecretDigest) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_connection.secret_digest", string(digest))
}

func NewAttachedWorkerChannelBinding(value []byte) AttachedWorkerChannelBinding {
	return AttachedWorkerChannelBinding(hex.EncodeToString(value))
}

func (binding AttachedWorkerChannelBinding) Validate() error {
	return validateAttachedWorkerTransportDigest("attached_worker_connection.channel_binding", string(binding))
}

func DigestAttachedWorkerCapability(value []byte) AttachedWorkerCapabilityDigest {
	return AttachedWorkerCapabilityDigest(attachedWorkerSHA256(value))
}

func DigestAttachedWorkerIdentityKey(value []byte) AttachedWorkerIdentityKeyDigest {
	return AttachedWorkerIdentityKeyDigest(attachedWorkerSHA256(value))
}

func attachedWorkerSHA256(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func validateAttachedWorkerTransportDigest(field, value string) error {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return ValidationError{Field: field, Reason: "must be a lowercase SHA-256 digest"}
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return ValidationError{Field: field, Reason: "must be a lowercase SHA-256 digest"}
	}
	var nonzero byte
	for _, item := range decoded {
		nonzero |= item
	}
	if nonzero == 0 {
		return ValidationError{Field: field, Reason: "must not be the zero digest"}
	}
	return nil
}

type AttachedWorkerAttachChallenge struct {
	TenantID                     TenantID                      `json:"tenant_id"`
	OwnerUserID                  UserID                        `json:"owner_user_id"`
	ID                           AttachedWorkerChallengeID     `json:"challenge_id"`
	WorkerID                     AttachedWorkerID              `json:"worker_id"`
	ConnectionID                 AttachedWorkerConnectionID    `json:"connection_id"`
	Purpose                      AttachedWorkerAttachPurpose   `json:"purpose"`
	Audience                     string                        `json:"audience"`
	ExpectedWorkerRevision       uint64                        `json:"expected_worker_revision"`
	ExpectedEnrollmentGeneration uint64                        `json:"expected_enrollment_generation"`
	ExpectedConnectionGeneration uint64                        `json:"expected_connection_generation"`
	TargetConnectionGeneration   uint64                        `json:"target_connection_generation"`
	WorkerProtocolMinimum        uint32                        `json:"worker_protocol_minimum"`
	WorkerProtocolMaximum        uint32                        `json:"worker_protocol_maximum"`
	WorkerProtocolVersions       []uint32                      `json:"worker_protocol_versions"`
	PlatformProtocolMinimum      uint32                        `json:"platform_protocol_minimum"`
	PlatformProtocolMaximum      uint32                        `json:"platform_protocol_maximum"`
	PlatformProtocolVersions     []uint32                      `json:"platform_protocol_versions"`
	SelectedProtocolVersion      uint32                        `json:"selected_protocol_version"`
	WorkerNonceDigest            AttachedWorkerChallengeDigest `json:"worker_nonce_digest"`
	PlatformNonceDigest          AttachedWorkerChallengeDigest `json:"platform_nonce_digest"`
	CreatedAt                    time.Time                     `json:"created_at"`
	ExpiresAt                    time.Time                     `json:"expires_at"`
	RetainUntil                  time.Time                     `json:"retain_until"`
	ConsumedAt                   time.Time                     `json:"consumed_at"`
	Revision                     uint64                        `json:"revision"`
}

func (challenge AttachedWorkerAttachChallenge) Validate() error {
	if err := challenge.TenantID.Validate(); err != nil {
		return err
	}
	if err := challenge.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := challenge.ID.Validate(); err != nil {
		return err
	}
	if err := challenge.WorkerID.Validate(); err != nil {
		return err
	}
	if err := challenge.ConnectionID.Validate(); err != nil {
		return err
	}
	if !challenge.Purpose.Valid() || challenge.Audience == "" || challenge.Audience != strings.TrimSpace(challenge.Audience) || len(challenge.Audience) > 256 {
		return ValidationError{Field: "attached_worker_challenge.purpose", Reason: "purpose and bounded audience are required"}
	}
	if challenge.ExpectedWorkerRevision == 0 || challenge.ExpectedEnrollmentGeneration == 0 ||
		challenge.ExpectedConnectionGeneration == ^uint64(0) || challenge.TargetConnectionGeneration == 0 ||
		challenge.TargetConnectionGeneration != challenge.ExpectedConnectionGeneration+1 {
		return ValidationError{Field: "attached_worker_challenge.generations", Reason: "must describe the exact next connection generation"}
	}
	if !validAttachedWorkerProtocolOffer(challenge.WorkerProtocolMinimum, challenge.WorkerProtocolMaximum, challenge.WorkerProtocolVersions) ||
		!validAttachedWorkerProtocolOffer(challenge.PlatformProtocolMinimum, challenge.PlatformProtocolMaximum, challenge.PlatformProtocolVersions) ||
		challenge.SelectedProtocolVersion == 0 || !containsAttachedWorkerProtocol(challenge.WorkerProtocolVersions, challenge.SelectedProtocolVersion) ||
		!containsAttachedWorkerProtocol(challenge.PlatformProtocolVersions, challenge.SelectedProtocolVersion) {
		return ValidationError{Field: "attached_worker_challenge.protocol", Reason: "must contain an exact compatible version"}
	}
	if err := validateAttachedWorkerTransportDigest("attached_worker_challenge.worker_nonce_digest", string(challenge.WorkerNonceDigest)); err != nil {
		return err
	}
	if err := validateAttachedWorkerTransportDigest("attached_worker_challenge.platform_nonce_digest", string(challenge.PlatformNonceDigest)); err != nil {
		return err
	}
	if challenge.CreatedAt.IsZero() || !challenge.ExpiresAt.After(challenge.CreatedAt) || !challenge.RetainUntil.After(challenge.ExpiresAt) || challenge.Revision == 0 {
		return ValidationError{Field: "attached_worker_challenge.lifetime", Reason: "must have ordered timestamps and a positive revision"}
	}
	if !challenge.ConsumedAt.IsZero() && (challenge.ConsumedAt.Before(challenge.CreatedAt) || !challenge.ConsumedAt.Before(challenge.ExpiresAt)) {
		return ValidationError{Field: "attached_worker_challenge.consumed_at", Reason: "must be within the challenge lifetime"}
	}
	return nil
}

func validAttachedWorkerProtocolOffer(minimum, maximum uint32, versions []uint32) bool {
	if minimum == 0 || maximum < minimum || len(versions) == 0 || len(versions) > 8 {
		return false
	}
	var previous uint32
	for _, version := range versions {
		if version < minimum || version > maximum || version <= previous {
			return false
		}
		previous = version
	}
	return true
}

func containsAttachedWorkerProtocol(versions []uint32, selected uint32) bool {
	for _, version := range versions {
		if version == selected {
			return true
		}
	}
	return false
}

type AttachedWorkerCapabilityManifest struct {
	Version              uint32                          `json:"version"`
	TenantID             TenantID                        `json:"tenant_id"`
	OwnerUserID          UserID                          `json:"owner_user_id"`
	WorkerID             AttachedWorkerID                `json:"worker_id"`
	EnrollmentGeneration uint64                          `json:"enrollment_generation"`
	ManifestRevision     uint64                          `json:"manifest_revision"`
	Digest               AttachedWorkerCapabilityDigest  `json:"digest"`
	ProtocolVersion      uint32                          `json:"protocol_version"`
	IdentityKeyDigest    AttachedWorkerIdentityKeyDigest `json:"identity_key_digest"`
	ManifestPayload      []byte                          `json:"manifest_payload"`
}

func (manifest AttachedWorkerCapabilityManifest) Validate() error {
	if manifest.Version != AttachedWorkerCapabilityManifestVersionV1 {
		return ValidationError{Field: "attached_worker_capability.version", Reason: "is unsupported"}
	}
	if err := manifest.TenantID.Validate(); err != nil {
		return err
	}
	if err := manifest.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := manifest.WorkerID.Validate(); err != nil {
		return err
	}
	if manifest.EnrollmentGeneration == 0 || manifest.ManifestRevision == 0 || manifest.ProtocolVersion == 0 {
		return ValidationError{Field: "attached_worker_capability.generations", Reason: "must be positive"}
	}
	if err := validateAttachedWorkerTransportDigest("attached_worker_capability.digest", string(manifest.Digest)); err != nil {
		return err
	}
	if err := validateAttachedWorkerTransportDigest("attached_worker_capability.identity_key_digest", string(manifest.IdentityKeyDigest)); err != nil {
		return err
	}
	if len(manifest.ManifestPayload) == 0 || len(manifest.ManifestPayload) > 32<<10 {
		return ValidationError{Field: "attached_worker_capability.payload", Reason: "must contain a bounded immutable manifest"}
	}
	return nil
}

type AttachedWorkerConnection struct {
	TenantID              TenantID                             `json:"tenant_id"`
	OwnerUserID           UserID                               `json:"owner_user_id"`
	WorkerID              AttachedWorkerID                     `json:"worker_id"`
	ID                    AttachedWorkerConnectionID           `json:"connection_id"`
	ActivationChallengeID AttachedWorkerChallengeID            `json:"activation_challenge_id"`
	EnrollmentGeneration  uint64                               `json:"enrollment_generation"`
	ConnectionGeneration  uint64                               `json:"connection_generation"`
	ProtocolVersion       uint32                               `json:"protocol_version"`
	CapabilityDigest      AttachedWorkerCapabilityDigest       `json:"capability_digest"`
	SecretDigest          AttachedWorkerConnectionSecretDigest `json:"secret_digest"`
	ChannelBinding        AttachedWorkerChannelBinding         `json:"channel_binding"`
	ManifestRevision      uint64                               `json:"manifest_revision"`
	ManifestIdentityKey   AttachedWorkerIdentityKeyDigest      `json:"manifest_identity_key_digest"`
	ManifestSignature     []byte                               `json:"manifest_signature"`
	ManifestObservedAt    time.Time                            `json:"manifest_observed_at"`
	State                 AttachedWorkerConnectionState        `json:"state"`
	PlatformSequence      uint64                               `json:"platform_sequence"`
	WorkerSequence        uint64                               `json:"worker_sequence"`
	PlatformAck           uint64                               `json:"platform_ack"`
	WorkerAck             uint64                               `json:"worker_ack"`
	// ProtocolSnapshot is the canonical strict encoding of the sole durable
	// MachineSnapshotV1 for this connection. Scalar watermarks are projections,
	// not sufficient authority to reconstruct replay fingerprints.
	ProtocolSnapshot  []byte    `json:"protocol_snapshot"`
	ConnectedAt       time.Time `json:"connected_at"`
	LastCheckpointAt  time.Time `json:"last_checkpoint_at"`
	PresenceExpiresAt time.Time `json:"presence_expires_at"`
	AuthExpiresAt     time.Time `json:"auth_expires_at"`
	Revision          uint64    `json:"revision"`
}

func (connection AttachedWorkerConnection) Validate() error {
	if err := connection.TenantID.Validate(); err != nil {
		return err
	}
	if err := connection.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := connection.WorkerID.Validate(); err != nil {
		return err
	}
	if err := connection.ID.Validate(); err != nil {
		return err
	}
	if err := connection.ActivationChallengeID.Validate(); err != nil {
		return err
	}
	if connection.EnrollmentGeneration == 0 || connection.ConnectionGeneration == 0 || connection.ProtocolVersion == 0 || connection.Revision == 0 || !connection.State.Valid() {
		return ValidationError{Field: "attached_worker_connection", Reason: "has invalid generations, protocol, state, or revision"}
	}
	if err := validateAttachedWorkerTransportDigest("attached_worker_connection.capability_digest", string(connection.CapabilityDigest)); err != nil {
		return err
	}
	if err := connection.SecretDigest.Validate(); err != nil {
		return err
	}
	if err := connection.ChannelBinding.Validate(); err != nil {
		return err
	}
	if connection.ConnectedAt.IsZero() || !connection.AuthExpiresAt.After(connection.ConnectedAt) {
		return ValidationError{Field: "attached_worker_connection.lifetime", Reason: "must contain ordered timestamps"}
	}
	if connection.State == AttachedWorkerConnectionAttaching {
		if connection.ManifestRevision != 0 || connection.ManifestIdentityKey != "" || len(connection.ManifestSignature) != 0 ||
			!connection.ManifestObservedAt.IsZero() || !connection.LastCheckpointAt.IsZero() || !connection.PresenceExpiresAt.IsZero() {
			return ValidationError{Field: "attached_worker_connection.lifetime", Reason: "attaching connections must not hold a manifest observation or presence lease"}
		}
	} else {
		if connection.ManifestRevision == 0 || connection.ManifestIdentityKey == "" || len(connection.ManifestSignature) != ed25519.SignatureSize ||
			connection.ManifestObservedAt.Before(connection.ConnectedAt) || connection.LastCheckpointAt.Before(connection.ManifestObservedAt) ||
			!connection.PresenceExpiresAt.After(connection.LastCheckpointAt) {
			return ValidationError{Field: "attached_worker_connection.lifetime", Reason: "active connection manifest and presence timestamps must be ordered"}
		}
		if err := validateAttachedWorkerTransportDigest("attached_worker_connection.manifest_identity_key_digest", string(connection.ManifestIdentityKey)); err != nil {
			return err
		}
	}
	if connection.PlatformAck > connection.WorkerSequence || connection.WorkerAck > connection.PlatformSequence {
		return ValidationError{Field: "attached_worker_connection.watermarks", Reason: "acknowledgements must not exceed the opposite direction sequence"}
	}
	if len(connection.ProtocolSnapshot) == 0 || len(connection.ProtocolSnapshot) > maxAttachedWorkerProtocolSnapshotBytes {
		return ValidationError{Field: "attached_worker_connection.protocol_snapshot", Reason: "must contain a bounded canonical machine snapshot"}
	}
	return nil
}

type AttachedWorkerPresenceExpiry struct {
	Bucket               uint32                     `json:"bucket"`
	TenantID             TenantID                   `json:"tenant_id"`
	OwnerUserID          UserID                     `json:"owner_user_id"`
	WorkerID             AttachedWorkerID           `json:"worker_id"`
	ConnectionID         AttachedWorkerConnectionID `json:"connection_id"`
	ConnectionGeneration uint64                     `json:"connection_generation"`
	ConnectionRevision   uint64                     `json:"connection_revision"`
	PresenceExpiresAt    time.Time                  `json:"presence_expires_at"`
}

func (expiry AttachedWorkerPresenceExpiry) Validate() error {
	if expiry.Bucket >= 16 {
		return ValidationError{Field: "attached_worker_presence_expiry.bucket", Reason: "must be within the v1 bucket range"}
	}
	if err := expiry.TenantID.Validate(); err != nil {
		return err
	}
	if err := expiry.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := expiry.WorkerID.Validate(); err != nil {
		return err
	}
	if err := expiry.ConnectionID.Validate(); err != nil {
		return err
	}
	if expiry.ConnectionGeneration == 0 || expiry.ConnectionRevision == 0 || expiry.PresenceExpiresAt.IsZero() {
		return ValidationError{Field: "attached_worker_presence_expiry", Reason: "must contain positive generations and an expiry"}
	}
	return nil
}
