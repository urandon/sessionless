package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	AttachedWorkerMaxChallengeLifetime  = 15 * time.Minute
	AttachedWorkerMaxChallengeRetention = 7 * 24 * time.Hour
	AttachedWorkerMaxPresenceTTL        = 30 * time.Minute
	AttachedWorkerMaxAuthTTL            = 7 * 24 * time.Hour
	AttachedWorkerMaxCheckpointInterval = 15 * time.Minute
)

type AttachedWorkerConnectionStatus string

const (
	AttachedWorkerConnectionActivated  AttachedWorkerConnectionStatus = "activated"
	AttachedWorkerConnectionAuthorized AttachedWorkerConnectionStatus = "authorized"
	AttachedWorkerConnectionDenied     AttachedWorkerConnectionStatus = "denied"
	AttachedWorkerConnectionExpired    AttachedWorkerConnectionStatus = "expired"
	AttachedWorkerConnectionConsumed   AttachedWorkerConnectionStatus = "consumed"
	AttachedWorkerConnectionConflict   AttachedWorkerConnectionStatus = "conflict"
	AttachedWorkerConnectionRevoked    AttachedWorkerConnectionStatus = "revoked"
)

type AttachedWorkerChallengeCreate struct {
	TenantID                     domain.TenantID
	OwnerUserID                  domain.UserID
	WorkerID                     domain.AttachedWorkerID
	ChallengeID                  domain.AttachedWorkerChallengeID
	ConnectionID                 domain.AttachedWorkerConnectionID
	Purpose                      domain.AttachedWorkerAttachPurpose
	Audience                     string
	ExpectedWorkerRevision       uint64
	ExpectedEnrollmentGeneration uint64
	ExpectedConnectionGeneration uint64
	WorkerProtocolMinimum        uint32
	WorkerProtocolMaximum        uint32
	WorkerProtocolVersions       []uint32
	PlatformProtocolMinimum      uint32
	PlatformProtocolMaximum      uint32
	PlatformProtocolVersions     []uint32
	SelectedProtocolVersion      uint32
	WorkerNonceDigest            domain.AttachedWorkerChallengeDigest
	PlatformNonceDigest          domain.AttachedWorkerChallengeDigest
	Lifetime                     time.Duration
	Retention                    time.Duration
}

type AttachedWorkerCapabilityTarget struct {
	ManifestRevision  uint64
	Digest            domain.AttachedWorkerCapabilityDigest
	ProtocolVersion   uint32
	IdentityKeyDigest domain.AttachedWorkerIdentityKeyDigest
	CanonicalManifest []byte
	ManifestPayload   []byte
	Signature         []byte
}

type AttachedWorkerConnectionActivation struct {
	TenantID                     domain.TenantID
	OwnerUserID                  domain.UserID
	WorkerID                     domain.AttachedWorkerID
	ChallengeID                  domain.AttachedWorkerChallengeID
	ExpectedChallengeRevision    uint64
	ExpectedWorkerRevision       uint64
	ExpectedEnrollmentGeneration uint64
	ExpectedConnectionGeneration uint64
	PresentedWorkerNonceDigest   domain.AttachedWorkerChallengeDigest
	PresentedPlatformNonceDigest domain.AttachedWorkerChallengeDigest
	ConnectionSecretDigest       domain.AttachedWorkerConnectionSecretDigest
	ChannelBinding               domain.AttachedWorkerChannelBinding
	ExpectedCapabilityDigest     domain.AttachedWorkerCapabilityDigest
	AuthTTL                      time.Duration
}

type AttachedWorkerManifestAcceptance struct {
	TenantID                   domain.TenantID
	OwnerUserID                domain.UserID
	WorkerID                   domain.AttachedWorkerID
	ConnectionID               domain.AttachedWorkerConnectionID
	ConnectionGeneration       uint64
	ExpectedConnectionRevision uint64
	ExpectedWorkerRevision     uint64
	PresentedSecretDigest      domain.AttachedWorkerConnectionSecretDigest
	Capability                 AttachedWorkerCapabilityTarget
	PlatformSequence           uint64
	WorkerSequence             uint64
	PlatformAck                uint64
	WorkerAck                  uint64
	PresenceTTL                time.Duration
}

type AttachedWorkerConnectionResult struct {
	Status     AttachedWorkerConnectionStatus
	Connection domain.AttachedWorkerConnection
}

type AttachedWorkerExchangeAuthorization struct {
	TenantID                   domain.TenantID
	OwnerUserID                domain.UserID
	WorkerID                   domain.AttachedWorkerID
	ConnectionID               domain.AttachedWorkerConnectionID
	ConnectionGeneration       uint64
	PresentedSecretDigest      domain.AttachedWorkerConnectionSecretDigest
	ExpectedConnectionRevision uint64
	PlatformSequence           uint64
	WorkerSequence             uint64
	PlatformAck                uint64
	WorkerAck                  uint64
	CheckpointInterval         time.Duration
	PresenceTTL                time.Duration
}

type AttachedWorkerAuthorizationResult struct {
	Status       AttachedWorkerConnectionStatus
	Connection   domain.AttachedWorkerConnection
	Checkpointed bool
}

// AttachedWorkerPresenceCursor is the exclusive composite cursor for the
// presence-expiry primary key after shard_bucket. A timestamp-only cursor is
// lossy when several workers share YDB's microsecond timestamp precision.
type AttachedWorkerPresenceCursor struct {
	PresenceExpiresAt time.Time
	TenantID          domain.TenantID
	OwnerUserID       domain.UserID
	WorkerID          domain.AttachedWorkerID
}

// AttachedWorkerTransportStore is owner scoped. Every serving lookup starts
// with tenant_id and owner_user_id. Implementations use one authoritative YDB
// transaction timestamp for challenge creation/expiry, activation, presence
// checkpoints, and offline transitions. Raw nonces, bearer secrets and proofs
// never cross this port.
type AttachedWorkerTransportStore interface {
	AttachedWorkerStore
	CreateAttachedWorkerAttachChallenge(context.Context, AttachedWorkerChallengeCreate) (domain.AttachedWorkerAttachChallenge, error)
	LoadAttachedWorkerAttachChallenge(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID, domain.AttachedWorkerChallengeID) (domain.AttachedWorkerAttachChallenge, bool, error)
	ActivateAttachedWorkerConnection(context.Context, AttachedWorkerConnectionActivation) (AttachedWorkerConnectionResult, error)
	AcceptAttachedWorkerManifest(context.Context, AttachedWorkerManifestAcceptance) (AttachedWorkerAuthorizationResult, error)
	LoadAttachedWorkerConnection(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error)
	AuthorizeAttachedWorkerExchange(context.Context, AttachedWorkerExchangeAuthorization) (AttachedWorkerAuthorizationResult, error)
	ListExpiredAttachedWorkerPresence(context.Context, uint32, time.Time, AttachedWorkerPresenceCursor, uint64) ([]domain.AttachedWorkerPresenceExpiry, error)
	ExpireAttachedWorkerPresence(context.Context, domain.AttachedWorkerPresenceExpiry) (bool, error)
}
