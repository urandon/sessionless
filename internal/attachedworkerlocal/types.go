// Package attachedworkerlocal owns the durable, owner-local installation state
// used by the attached-worker operator surface. It deliberately does not own
// server identity, transport, attempt, or daemon state machines.
package attachedworkerlocal

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	ManifestVersionV1            = uint32(1)
	SecretVersionV1              = uint32(1)
	ReceiptVersionV1             = uint32(1)
	ReconnectCheckpointVersionV1 = uint32(1)

	ManifestFileName            = "manifest.json"
	SecretFileName              = "secret.json"
	LogoutIntentFileName        = "logout-intent.json"
	ObservationFileName         = "runtime-observation.json"
	ReconnectCheckpointFileName = "reconnect-checkpoint.json"
	StateLockFileName           = "state.lock"
	RuntimeLockFileName         = "runtime.lock"

	maxStateFileBytes = attachedworkerprotocol.MaxBatchBytes + 16<<10
)

var (
	ErrInvalidRoot      = errors.New("attached worker local state root is invalid")
	ErrInvalidState     = errors.New("attached worker local state is invalid")
	ErrStateMissing     = errors.New("attached worker local state is missing")
	ErrStateIncomplete  = errors.New("attached worker local state is incomplete")
	ErrStateConflict    = errors.New("attached worker local state conflicts with expected authority")
	ErrStateAmbiguous   = errors.New("attached worker local state durability is ambiguous")
	ErrStateBusy        = errors.New("attached worker local state is busy")
	ErrStateUnsupported = errors.New("attached worker local state is unsupported")
	ErrSecretRetired    = errors.New("attached worker local secret is retired")
	ErrLocalIO          = errors.New("attached worker local state operation failed")

	hexDigestPattern      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	failureCodePattern    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

type LifecycleState string

const (
	LifecycleActive    LifecycleState = "active"
	LifecycleLoggedOut LifecycleState = "logged_out"
)

type RuntimeBoundary string

const (
	BoundaryDarwinVM      RuntimeBoundary = "darwin-vm"
	BoundaryLinuxRootless RuntimeBoundary = "linux-rootless"
)

// OCIConfigV1 mirrors only explicit, operator-owned launcher inputs. It does
// not permit an ambient Docker context, PATH lookup, or credential helper.
type OCIConfigV1 struct {
	DockerPath          string          `json:"docker_path"`
	DockerSHA256        string          `json:"docker_sha256"`
	CLIConfigDir        string          `json:"cli_config_dir"`
	Host                string          `json:"host"`
	EngineID            string          `json:"engine_id"`
	InstallationID      string          `json:"installation_id"`
	Boundary            RuntimeBoundary `json:"boundary"`
	Image               string          `json:"image"`
	UserID              uint32          `json:"user_id"`
	GroupID             uint32          `json:"group_id"`
	DiskBytes           int64           `json:"disk_bytes"`
	CredentialFileBytes int64           `json:"credential_file_bytes"`
	MemoryBytes         int64           `json:"memory_bytes"`
	PIDsLimit           int64           `json:"pids_limit"`
	StopSeconds         int             `json:"stop_seconds"`
}

type HarnessConfigV1 struct {
	Executable string   `json:"executable"`
	SHA256     string   `json:"sha256"`
	Arguments  []string `json:"arguments"`
}

type LogoutReceiptV1 struct {
	Version          uint32    `json:"version"`
	RequestID        string    `json:"request_id"`
	RequestedAt      time.Time `json:"requested_at"`
	CommittedAt      time.Time `json:"committed_at"`
	SecretState      string    `json:"secret_state"`
	ServerRevocation string    `json:"server_revocation"`
	RemoteErasure    string    `json:"remote_erasure"`
}

// ManifestV1 is configuration and local lifecycle authority only. Server
// observations and protocol snapshots never become authoritative by being
// copied into this file.
type ManifestV1 struct {
	Version                uint32                  `json:"version"`
	Revision               uint64                  `json:"revision"`
	ControlPlaneOrigin     string                  `json:"control_plane_origin"`
	TenantID               domain.TenantID         `json:"tenant_id"`
	OwnerUserID            domain.UserID           `json:"owner_user_id"`
	WorkerID               domain.AttachedWorkerID `json:"worker_id"`
	EnrollmentGeneration   uint64                  `json:"enrollment_generation"`
	ConnectionGeneration   uint64                  `json:"connection_generation"`
	IdentityKeyFingerprint string                  `json:"identity_key_fingerprint"`
	OCI                    OCIConfigV1             `json:"oci"`
	Harness                HarnessConfigV1         `json:"harness"`
	Lifecycle              LifecycleState          `json:"lifecycle"`
	CreatedAt              time.Time               `json:"created_at"`
	UpdatedAt              time.Time               `json:"updated_at"`
	Logout                 *LogoutReceiptV1        `json:"logout,omitempty"`
}

// SecretRecordV1 is intentionally kept outside ManifestV1. String and GoString
// never expose its private material.
type SecretRecordV1 struct {
	Version              uint32                  `json:"version"`
	ManifestRevision     uint64                  `json:"manifest_revision"`
	TenantID             domain.TenantID         `json:"tenant_id"`
	OwnerUserID          domain.UserID           `json:"owner_user_id"`
	WorkerID             domain.AttachedWorkerID `json:"worker_id"`
	EnrollmentGeneration uint64                  `json:"enrollment_generation"`
	ConnectionGeneration uint64                  `json:"connection_generation"`
	IdentityPrivateKey   []byte                  `json:"identity_private_key"`
	ConnectionSecret     []byte                  `json:"connection_secret,omitempty"`
}

// RuntimeObservationV1 is content-free local daemon evidence. It is written
// only through the process that owns RuntimeLease and never represents server
// health, connection acceptance, or attempt authority.
type RuntimeObservationV1 struct {
	Version          uint32                           `json:"version"`
	Revision         uint64                           `json:"revision"`
	ManifestRevision uint64                           `json:"manifest_revision"`
	State            attachedworkerdaemon.DaemonState `json:"state"`
	Active           bool                             `json:"active"`
	Accepted         uint64                           `json:"accepted"`
	Completed        uint64                           `json:"completed"`
	Failed           uint64                           `json:"failed"`
	ObservedAt       time.Time                        `json:"observed_at"`
	LastFailureCode  string                           `json:"last_failure_code,omitempty"`
}

// ReconnectCheckpointV1 is secret-free local continuation evidence for one
// attached-worker connection, including its bounded active-attempt summary and
// optional terminal replay commitment. It is never server authority: the
// control plane compares its signed claim with the exact durable server head.
type ReconnectCheckpointV1 struct {
	Version               uint32                                   `json:"version"`
	Revision              uint64                                   `json:"revision"`
	ManifestRevision      uint64                                   `json:"manifest_revision"`
	TenantID              domain.TenantID                          `json:"tenant_id"`
	OwnerUserID           domain.UserID                            `json:"owner_user_id"`
	WorkerID              domain.AttachedWorkerID                  `json:"worker_id"`
	EnrollmentGeneration  uint64                                   `json:"enrollment_generation"`
	ConnectionGeneration  uint64                                   `json:"connection_generation"`
	ProtocolVersion       attachedworkerprotocol.ProtocolVersion   `json:"protocol_version"`
	ConnectionID          domain.AttachedWorkerConnectionID        `json:"connection_id"`
	CapabilityDigest      domain.AttachedWorkerCapabilityDigest    `json:"capability_digest"`
	AuthenticationExpires time.Time                                `json:"authentication_expires"`
	ChannelBinding        []byte                                   `json:"channel_binding"`
	WorkerOffer           attachedworkerprotocol.VersionOfferV1    `json:"worker_offer"`
	PlatformOffer         attachedworkerprotocol.VersionOfferV1    `json:"platform_offer"`
	MachineSnapshot       attachedworkerprotocol.MachineSnapshotV1 `json:"machine_snapshot"`
	CheckpointedAt        time.Time                                `json:"checkpointed_at"`
}

func (checkpoint ReconnectCheckpointV1) Validate(manifest ManifestV1) error {
	machine := checkpoint.MachineSnapshot
	if manifest.Validate() != nil || manifest.Lifecycle != LifecycleActive ||
		checkpoint.Version != ReconnectCheckpointVersionV1 || checkpoint.Revision == 0 ||
		checkpoint.ManifestRevision != manifest.Revision || checkpoint.TenantID != manifest.TenantID ||
		checkpoint.OwnerUserID != manifest.OwnerUserID || checkpoint.WorkerID != manifest.WorkerID ||
		checkpoint.EnrollmentGeneration != manifest.EnrollmentGeneration ||
		checkpoint.ConnectionGeneration != manifest.ConnectionGeneration ||
		checkpoint.ProtocolVersion != attachedworkerprotocol.ProtocolVersionV1 ||
		checkpoint.ConnectionID.Validate() != nil || checkpoint.CapabilityDigest.Validate() != nil ||
		checkpoint.WorkerOffer.Validate() != nil || checkpoint.PlatformOffer.Validate() != nil ||
		len(checkpoint.ChannelBinding) != 32 || machine.Validate() != nil ||
		(machine.Connection != attachedworkerprotocol.ConnectionReady && machine.Connection != attachedworkerprotocol.ConnectionDraining) ||
		machine.Reconnect != nil || machine.Manifest == nil ||
		checkpoint.AuthenticationExpires.IsZero() || checkpoint.AuthenticationExpires.Location() != time.UTC ||
		checkpoint.CheckpointedAt.IsZero() || checkpoint.CheckpointedAt.Location() != time.UTC ||
		checkpoint.CheckpointedAt.Before(manifest.UpdatedAt) || !checkpoint.AuthenticationExpires.After(checkpoint.CheckpointedAt) {
		return ErrInvalidState
	}
	if checkpoint.CapabilityDigest != domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", machine.CapabilityDigest)) {
		return ErrInvalidState
	}
	return nil
}

// SnapshotV1 is a point-in-time, secret-free view of the local installation.
// Observation remains local evidence only; it is never server or attempt
// authority.
type SnapshotV1 struct {
	Manifest           ManifestV1
	Observation        RuntimeObservationV1
	ObservationPresent bool
}

func (observation RuntimeObservationV1) Validate(manifest ManifestV1) error {
	if manifest.Validate() != nil || manifest.Lifecycle != LifecycleActive || observation.Version != 1 ||
		observation.Revision == 0 || observation.ManifestRevision != manifest.Revision ||
		observation.ObservedAt.IsZero() || observation.ObservedAt.Location() != time.UTC ||
		observation.ObservedAt.Before(manifest.UpdatedAt) ||
		observation.Completed > observation.Accepted || observation.Failed > observation.Accepted-observation.Completed ||
		(observation.LastFailureCode != "" && !failureCodePattern.MatchString(observation.LastFailureCode)) {
		return ErrInvalidState
	}
	switch observation.State {
	case attachedworkerdaemon.DaemonRunning, attachedworkerdaemon.DaemonDraining:
	case attachedworkerdaemon.DaemonStopped:
		if observation.Active {
			return ErrInvalidState
		}
	default:
		return ErrInvalidState
	}
	return nil
}

func (SecretRecordV1) String() string   { return "SecretRecordV1{[REDACTED]}" }
func (SecretRecordV1) GoString() string { return "SecretRecordV1{[REDACTED]}" }
func (SecretRecordV1) MarshalJSON() ([]byte, error) {
	return []byte(`{"version":1,"redacted":true}`), nil
}
func (SecretRecordV1) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "SecretRecordV1{[REDACTED]}")
}

type secretRecordDiskV1 struct {
	Version              uint32                  `json:"version"`
	ManifestRevision     uint64                  `json:"manifest_revision"`
	TenantID             domain.TenantID         `json:"tenant_id"`
	OwnerUserID          domain.UserID           `json:"owner_user_id"`
	WorkerID             domain.AttachedWorkerID `json:"worker_id"`
	EnrollmentGeneration uint64                  `json:"enrollment_generation"`
	ConnectionGeneration uint64                  `json:"connection_generation"`
	IdentityPrivateKey   []byte                  `json:"identity_private_key"`
	ConnectionSecret     []byte                  `json:"connection_secret,omitempty"`
}

func encodeSecretRecord(secret SecretRecordV1) ([]byte, error) {
	return encodeStrictJSON(secretRecordDiskV1{
		Version: secret.Version, ManifestRevision: secret.ManifestRevision,
		TenantID: secret.TenantID, OwnerUserID: secret.OwnerUserID, WorkerID: secret.WorkerID,
		EnrollmentGeneration: secret.EnrollmentGeneration, ConnectionGeneration: secret.ConnectionGeneration,
		IdentityPrivateKey: secret.IdentityPrivateKey, ConnectionSecret: secret.ConnectionSecret,
	})
}

func decodeSecretRecord(encoded []byte) (SecretRecordV1, error) {
	var disk secretRecordDiskV1
	if decodeStrictJSON(encoded, &disk) != nil {
		return SecretRecordV1{}, ErrInvalidState
	}
	return SecretRecordV1{
		Version: disk.Version, ManifestRevision: disk.ManifestRevision,
		TenantID: disk.TenantID, OwnerUserID: disk.OwnerUserID, WorkerID: disk.WorkerID,
		EnrollmentGeneration: disk.EnrollmentGeneration, ConnectionGeneration: disk.ConnectionGeneration,
		IdentityPrivateKey: append([]byte(nil), disk.IdentityPrivateKey...),
		ConnectionSecret:   append([]byte(nil), disk.ConnectionSecret...),
	}, nil
}

func (manifest ManifestV1) Validate() error {
	if manifest.Version != ManifestVersionV1 || manifest.Revision == 0 ||
		manifest.TenantID.Validate() != nil || manifest.OwnerUserID.Validate() != nil ||
		manifest.WorkerID.Validate() != nil || manifest.EnrollmentGeneration == 0 ||
		!hexDigestPattern.MatchString(manifest.IdentityKeyFingerprint) ||
		manifest.CreatedAt.IsZero() || manifest.UpdatedAt.IsZero() ||
		manifest.CreatedAt.Location() != time.UTC || manifest.UpdatedAt.Location() != time.UTC ||
		manifest.UpdatedAt.Before(manifest.CreatedAt) || validateOrigin(manifest.ControlPlaneOrigin) != nil ||
		validateOCI(manifest.OCI) != nil || validateHarness(manifest.Harness) != nil {
		return ErrInvalidState
	}
	switch manifest.Lifecycle {
	case LifecycleActive:
		if manifest.Logout != nil {
			return ErrInvalidState
		}
	case LifecycleLoggedOut:
		if validateLogoutReceipt(manifest.Logout) != nil || manifest.Logout.CommittedAt.After(manifest.UpdatedAt) {
			return ErrInvalidState
		}
	default:
		return ErrInvalidState
	}
	return nil
}

func (secret SecretRecordV1) Validate(manifest ManifestV1) error {
	if manifest.Validate() != nil || manifest.Lifecycle != LifecycleActive || secret.Version != SecretVersionV1 ||
		secret.ManifestRevision != manifest.Revision || secret.TenantID != manifest.TenantID ||
		secret.OwnerUserID != manifest.OwnerUserID || secret.WorkerID != manifest.WorkerID ||
		secret.EnrollmentGeneration != manifest.EnrollmentGeneration ||
		secret.ConnectionGeneration != manifest.ConnectionGeneration ||
		len(secret.IdentityPrivateKey) != ed25519.PrivateKeySize {
		return ErrInvalidState
	}
	if manifest.ConnectionGeneration == 0 {
		if len(secret.ConnectionSecret) != 0 {
			return ErrInvalidState
		}
	} else if len(secret.ConnectionSecret) != 32 {
		return ErrInvalidState
	}
	public := ed25519.PrivateKey(secret.IdentityPrivateKey).Public().(ed25519.PublicKey)
	digest := domain.DigestAttachedWorkerIdentityKey(public)
	if string(digest) != manifest.IdentityKeyFingerprint {
		return ErrInvalidState
	}
	return nil
}

func validateOrigin(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return ErrInvalidState
	}
	return nil
}

func validateOCI(config OCIConfigV1) error {
	if !hexDigestPattern.MatchString(config.DockerSHA256) || config.StopSeconds == 0 ||
		attachedworkeroci.ValidateDeclarativeConfig(toLauncherConfig(config)) != nil {
		return ErrInvalidState
	}
	return nil
}

func toLauncherConfig(config OCIConfigV1) attachedworkeroci.Config {
	return attachedworkeroci.Config{
		DockerPath: config.DockerPath, CLIConfigDir: config.CLIConfigDir, Host: config.Host,
		EngineID: config.EngineID, InstallationID: config.InstallationID,
		Boundary: attachedworkeroci.BoundaryKind(config.Boundary), Image: config.Image,
		UserID: config.UserID, GroupID: config.GroupID, DiskBytes: config.DiskBytes,
		CredentialFileBytes: config.CredentialFileBytes, MemoryBytes: config.MemoryBytes,
		PIDsLimit: config.PIDsLimit, StopSeconds: config.StopSeconds,
	}
}

func validateHarness(config HarnessConfigV1) error {
	if !filepath.IsAbs(config.Executable) || filepath.Clean(config.Executable) != config.Executable ||
		!hexDigestPattern.MatchString(config.SHA256) || config.Arguments == nil || len(config.Arguments) > 64 {
		return ErrInvalidState
	}
	for _, argument := range config.Arguments {
		if len(argument) > 4096 || strings.IndexByte(argument, 0) >= 0 {
			return ErrInvalidState
		}
	}
	return nil
}

func validateLogoutReceipt(receipt *LogoutReceiptV1) error {
	if receipt == nil || receipt.Version != ReceiptVersionV1 ||
		!idempotencyKeyPattern.MatchString(receipt.RequestID) || receipt.RequestedAt.IsZero() ||
		receipt.CommittedAt.IsZero() || receipt.RequestedAt.Location() != time.UTC ||
		receipt.CommittedAt.Location() != time.UTC || receipt.CommittedAt.Before(receipt.RequestedAt) ||
		receipt.SecretState != "retired" || receipt.ServerRevocation != "unknown" || receipt.RemoteErasure != "unknown" {
		return ErrInvalidState
	}
	return nil
}

func canonicalAbsolute(value string) bool {
	return value != "" && len(value) <= 4096 && filepath.IsAbs(value) && filepath.Clean(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n\t")
}
