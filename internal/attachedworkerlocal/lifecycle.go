package attachedworkerlocal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

type ResultCode string

const (
	CodeOK          ResultCode = "ok"
	CodeMissing     ResultCode = "state_missing"
	CodeInvalid     ResultCode = "state_invalid"
	CodeIncomplete  ResultCode = "state_incomplete"
	CodeConflict    ResultCode = "state_conflict"
	CodeAmbiguous   ResultCode = "state_ambiguous"
	CodeBusy        ResultCode = "state_busy"
	CodeUnsupported ResultCode = "platform_unsupported"
	CodeRetired     ResultCode = "secret_retired"
	CodeIO          ResultCode = "local_io_failed"
	CodeCancelled   ResultCode = "cancelled"
)

type CheckResultV1 struct {
	Version              uint32         `json:"version"`
	Code                 ResultCode     `json:"code"`
	Lifecycle            LifecycleState `json:"lifecycle,omitempty"`
	Revision             uint64         `json:"revision,omitempty"`
	EnrollmentGeneration uint64         `json:"enrollment_generation,omitempty"`
	ConnectionGeneration uint64         `json:"connection_generation,omitempty"`
	ManifestState        string         `json:"manifest_state"`
	SecretState          string         `json:"secret_state"`
}

type DoctorResultV1 struct {
	Version               uint32         `json:"version"`
	Code                  ResultCode     `json:"code"`
	Lifecycle             LifecycleState `json:"lifecycle,omitempty"`
	Platform              string         `json:"platform"`
	PlatformSupport       string         `json:"platform_support"`
	ConfigurationState    string         `json:"configuration_state"`
	DockerArtifactState   string         `json:"docker_artifact_state"`
	HarnessArtifactState  string         `json:"harness_artifact_state"`
	CLIConfigState        string         `json:"cli_config_state"`
	EngineObservation     string         `json:"engine_observation"`
	ExternalVerification  string         `json:"external_verification"`
	DaemonObservation     string         `json:"daemon_observation"`
	ServerConnectionState string         `json:"server_connection_state"`
}

type StatusResultV1 struct {
	Version              uint32         `json:"version"`
	Code                 ResultCode     `json:"code"`
	Lifecycle            LifecycleState `json:"lifecycle,omitempty"`
	Revision             uint64         `json:"revision,omitempty"`
	EnrollmentGeneration uint64         `json:"enrollment_generation,omitempty"`
	ConnectionGeneration uint64         `json:"connection_generation,omitempty"`
	DaemonObservation    string         `json:"daemon_observation"`
	DaemonState          string         `json:"daemon_state,omitempty"`
	DaemonActive         bool           `json:"daemon_active,omitempty"`
	ObservationRevision  uint64         `json:"observation_revision,omitempty"`
	ObservedAt           *time.Time     `json:"observed_at,omitempty"`
	ServerObservation    string         `json:"server_observation"`
	SecretState          string         `json:"secret_state"`
}

type LogoutInputV1 struct {
	ExpectedRevision uint64
	RequestID        string
}

type LogoutResultV1 struct {
	Version          uint32         `json:"version"`
	Code             ResultCode     `json:"code"`
	Lifecycle        LifecycleState `json:"lifecycle"`
	Revision         uint64         `json:"revision"`
	RequestID        string         `json:"request_id"`
	SecretState      string         `json:"secret_state"`
	ServerRevocation string         `json:"server_revocation"`
	RemoteErasure    string         `json:"remote_erasure"`
}

type PlanEntryV1 struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Present  bool   `json:"present"`
	Required bool   `json:"required"`
}

type UninstallPlanV1 struct {
	Version                 uint32         `json:"version"`
	Code                    ResultCode     `json:"code"`
	Lifecycle               LifecycleState `json:"lifecycle,omitempty"`
	RuntimeStopped          bool           `json:"runtime_stopped"`
	LocalLogoutCommitted    bool           `json:"local_logout_committed"`
	ServerRevocation        string         `json:"server_revocation"`
	RemoteErasure           string         `json:"remote_erasure"`
	DestructiveAction       string         `json:"destructive_action"`
	RequiresExplicitConsent bool           `json:"requires_explicit_consent"`
	Entries                 []PlanEntryV1  `json:"entries"`
}

type logoutIntentV1 struct {
	Version          uint32    `json:"version"`
	ExpectedRevision uint64    `json:"expected_revision"`
	NextRevision     uint64    `json:"next_revision"`
	RequestID        string    `json:"request_id"`
	RequestedAt      time.Time `json:"requested_at"`
	TenantID         string    `json:"tenant_id"`
	OwnerUserID      string    `json:"owner_user_id"`
	WorkerID         string    `json:"worker_id"`
}

func (store *Store) Check(ctx context.Context) (CheckResultV1, error) {
	manifest, _, _, err := store.loadSnapshot(ctx)
	if err != nil {
		return CheckResultV1{Version: 1, Code: Code(err), ManifestState: "unknown", SecretState: "unknown"}, err
	}
	if err := verifyPinnedArtifacts(manifest); err != nil {
		return CheckResultV1{Version: 1, Code: Code(err), Lifecycle: manifest.Lifecycle, Revision: manifest.Revision,
			EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: manifest.ConnectionGeneration,
			ManifestState: "valid", SecretState: "unknown"}, err
	}
	secretState := "ready"
	if manifest.Lifecycle == LifecycleLoggedOut {
		secretState = "retired"
	}
	return CheckResultV1{
		Version: 1, Code: CodeOK, Lifecycle: manifest.Lifecycle, Revision: manifest.Revision,
		EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: manifest.ConnectionGeneration,
		ManifestState: "valid", SecretState: secretState,
	}, nil
}

func (store *Store) Status(ctx context.Context) (StatusResultV1, error) {
	manifest, observation, present, err := store.loadSnapshot(ctx)
	if err != nil {
		return StatusResultV1{Version: 1, Code: Code(err), DaemonObservation: "unknown", ServerObservation: "unknown", SecretState: "unknown"}, err
	}
	secretState := "ready"
	if manifest.Lifecycle == LifecycleLoggedOut {
		secretState = "retired"
	}
	result := StatusResultV1{
		Version: 1, Code: CodeOK, Lifecycle: manifest.Lifecycle, Revision: manifest.Revision,
		EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: manifest.ConnectionGeneration,
		DaemonObservation: "unknown", ServerObservation: "unknown", SecretState: secretState,
	}
	if present {
		observedAt := observation.ObservedAt
		result.DaemonObservation = "observed_local"
		result.DaemonState = string(observation.State)
		result.DaemonActive = observation.Active
		result.ObservationRevision = observation.Revision
		result.ObservedAt = &observedAt
	}
	return result, nil
}

func (store *Store) Doctor(ctx context.Context) (DoctorResultV1, error) {
	result := DoctorResultV1{
		Version: 1, Platform: runtime.GOOS, PlatformSupport: "unsupported", ConfigurationState: "unknown",
		DockerArtifactState: "unknown", HarnessArtifactState: "unknown", CLIConfigState: "unknown", EngineObservation: "unknown",
		ExternalVerification: "unknown", DaemonObservation: "unknown", ServerConnectionState: "unknown",
	}
	manifest, _, observationPresent, err := store.loadSnapshot(ctx)
	if err != nil {
		result.Code = Code(err)
		return result, err
	}
	result.Lifecycle = manifest.Lifecycle
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		result.Code = CodeUnsupported
		return result, ErrStateUnsupported
	}
	result.PlatformSupport = "supported"
	if (runtime.GOOS == "darwin" && manifest.OCI.Boundary != BoundaryDarwinVM) ||
		(runtime.GOOS == "linux" && manifest.OCI.Boundary != BoundaryLinuxRootless) {
		result.Code = CodeInvalid
		result.ConfigurationState = "incompatible"
		return result, ErrInvalidState
	}
	result.ConfigurationState = "configured"
	if err := verifyRegularDigest(manifest.OCI.DockerPath, manifest.OCI.DockerSHA256); err != nil {
		result.Code = Code(err)
		result.DockerArtifactState = "mismatch"
		return result, err
	}
	result.DockerArtifactState = "verified"
	if err := verifyRegularDigest(manifest.Harness.Executable, manifest.Harness.SHA256); err != nil {
		result.Code = Code(err)
		result.HarnessArtifactState = "mismatch"
		return result, err
	}
	result.HarnessArtifactState = "verified"
	if err := verifyEmptyPrivateDirectory(manifest.OCI.CLIConfigDir); err != nil {
		result.Code = Code(err)
		result.CLIConfigState = "invalid"
		return result, err
	}
	result.CLIConfigState = "verified"
	if observationPresent {
		result.DaemonObservation = "observed_local"
	}
	// This slice intentionally has no live Docker/server probe or daemon IPC.
	// Unknown is preserved rather than upgraded from valid configuration.
	result.Code = CodeOK
	return result, nil
}

func verifyPinnedArtifacts(manifest ManifestV1) error {
	if err := verifyRegularDigest(manifest.OCI.DockerPath, manifest.OCI.DockerSHA256); err != nil {
		return err
	}
	return verifyRegularDigest(manifest.Harness.Executable, manifest.Harness.SHA256)
}

func (store *Store) Logout(ctx context.Context, input LogoutInputV1) (result LogoutResultV1, resultErr error) {
	if ctx == nil || ctx.Err() != nil || store == nil || input.ExpectedRevision == 0 ||
		!idempotencyKeyPattern.MatchString(input.RequestID) {
		return LogoutResultV1{Version: 1, Code: CodeInvalid}, ErrInvalidState
	}
	if err := store.ensureRoot(false); err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	runtimeLease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, runtimeLease.Close())
		if resultErr != nil {
			result.Code = Code(resultErr)
		}
	}()
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, lock.Close())
		if resultErr != nil {
			result.Code = Code(resultErr)
		}
	}()
	if err := store.validateInventory(); err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	manifest, err := store.loadManifestLocked()
	if err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	intent, intentPresent, err := store.loadLogoutIntentLocked()
	if err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	if manifest.Lifecycle == LifecycleLoggedOut {
		if manifest.Logout == nil || manifest.Logout.RequestID != input.RequestID ||
			manifest.Revision != input.ExpectedRevision+1 {
			return LogoutResultV1{Version: 1, Code: CodeConflict}, ErrStateConflict
		}
		if secretPresent, secretErr := store.filePresentSecure(SecretFileName); secretErr != nil {
			return LogoutResultV1{Version: 1, Code: Code(secretErr)}, secretErr
		} else if secretPresent {
			return LogoutResultV1{Version: 1, Code: CodeIncomplete}, ErrStateIncomplete
		}
		if observationPresent, observationErr := store.filePresentSecure(ObservationFileName); observationErr != nil {
			return LogoutResultV1{Version: 1, Code: Code(observationErr)}, observationErr
		} else if observationPresent {
			return LogoutResultV1{Version: 1, Code: CodeIncomplete}, ErrStateIncomplete
		}
		if intentPresent {
			if !intentMatches(intent, manifest, input) {
				return LogoutResultV1{Version: 1, Code: CodeConflict}, ErrStateConflict
			}
			if err := store.removeDurable(LogoutIntentFileName); err != nil {
				return LogoutResultV1{Version: 1, Code: Code(err)}, err
			}
		}
		return logoutResult(manifest), nil
	}
	if manifest.Revision != input.ExpectedRevision || input.ExpectedRevision == ^uint64(0) {
		return LogoutResultV1{Version: 1, Code: CodeConflict}, ErrStateConflict
	}
	if intentPresent {
		if !intentMatches(intent, manifest, input) {
			return LogoutResultV1{Version: 1, Code: CodeConflict}, ErrStateConflict
		}
	} else {
		consistent, consistentErr := store.loadConsistentLocked()
		if consistentErr != nil || consistent.Revision != manifest.Revision {
			if consistentErr == nil {
				consistentErr = ErrStateConflict
			}
			return LogoutResultV1{Version: 1, Code: Code(consistentErr)}, consistentErr
		}
		secret, secretErr := store.loadSecretLocked()
		if secretErr != nil || secret.Validate(manifest) != nil {
			return LogoutResultV1{Version: 1, Code: CodeIncomplete}, ErrStateIncomplete
		}
		intent = logoutIntentV1{
			Version: 1, ExpectedRevision: manifest.Revision, NextRevision: manifest.Revision + 1,
			RequestID: input.RequestID, RequestedAt: store.now().UTC(), TenantID: string(manifest.TenantID),
			OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID),
		}
		encoded, encodeErr := encodeStrictJSON(intent)
		if encodeErr != nil {
			return LogoutResultV1{Version: 1, Code: Code(encodeErr)}, encodeErr
		}
		if err := store.writeAtomic(LogoutIntentFileName, encoded); err != nil {
			return LogoutResultV1{Version: 1, Code: Code(err)}, err
		}
	}
	if secretPresent, secretErr := store.filePresentSecure(SecretFileName); secretErr != nil {
		return LogoutResultV1{Version: 1, Code: Code(secretErr)}, secretErr
	} else if secretPresent {
		secret, loadErr := store.loadSecretLocked()
		if loadErr != nil || secret.Validate(manifest) != nil {
			return LogoutResultV1{Version: 1, Code: CodeIncomplete}, ErrStateIncomplete
		}
	}
	if err := store.removeDurable(ObservationFileName); err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	if err := store.removeDurable(SecretFileName); err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	committedAt := store.now().UTC()
	if committedAt.Before(intent.RequestedAt) {
		return LogoutResultV1{Version: 1, Code: CodeInvalid}, ErrInvalidState
	}
	manifest.Revision = intent.NextRevision
	manifest.Lifecycle = LifecycleLoggedOut
	manifest.UpdatedAt = committedAt
	manifest.Logout = &LogoutReceiptV1{
		Version: 1, RequestID: intent.RequestID, RequestedAt: intent.RequestedAt, CommittedAt: committedAt,
		SecretState: "retired", ServerRevocation: "unknown", RemoteErasure: "unknown",
	}
	encodedManifest, err := encodeStrictJSON(manifest)
	if err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	if err := store.writeAtomic(ManifestFileName, encodedManifest); err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	if err := store.removeDurable(LogoutIntentFileName); err != nil {
		return LogoutResultV1{Version: 1, Code: Code(err)}, err
	}
	return logoutResult(manifest), nil
}

func (store *Store) UninstallPlan(ctx context.Context) (result UninstallPlanV1, resultErr error) {
	result = UninstallPlanV1{
		Version: 1, ServerRevocation: "unknown", RemoteErasure: "unknown", DestructiveAction: "not_performed",
		RequiresExplicitConsent: true,
	}
	if ctx == nil || ctx.Err() != nil || store == nil {
		result.Code = CodeInvalid
		return result, ErrInvalidState
	}
	runtimeLease, runtimeErr := store.AcquireRuntime(ctx)
	if runtimeErr == nil {
		result.RuntimeStopped = true
		defer func() {
			resultErr = errors.Join(resultErr, runtimeLease.Close())
			if resultErr != nil {
				result.Code = Code(resultErr)
			}
		}()
	} else if errors.Is(runtimeErr, ErrStateBusy) {
		result.RuntimeStopped = false
	} else {
		result.Code = Code(runtimeErr)
		return result, runtimeErr
	}
	stateLock, lockErr := store.acquireStateLock(false)
	if lockErr != nil {
		result.Code = Code(lockErr)
		return result, lockErr
	}
	defer func() {
		resultErr = errors.Join(resultErr, stateLock.Close())
		if resultErr != nil {
			result.Code = Code(resultErr)
		}
	}()
	manifest, loadErr := store.loadConsistentLocked()
	if loadErr == nil {
		result.Lifecycle = manifest.Lifecycle
		result.LocalLogoutCommitted = manifest.Lifecycle == LifecycleLoggedOut
	} else if !errors.Is(loadErr, ErrStateIncomplete) && !errors.Is(loadErr, ErrInvalidState) {
		result.Code = Code(loadErr)
		return result, loadErr
	}
	for _, entry := range []struct {
		kind, name string
		required   bool
	}{
		{"manifest", ManifestFileName, true}, {"secret", SecretFileName, false},
		{"logout_intent", LogoutIntentFileName, false}, {"state_lock", StateLockFileName, true},
		{"runtime_lock", RuntimeLockFileName, true}, {"runtime_observation", ObservationFileName, false},
	} {
		present, err := store.filePresentSecure(entry.name)
		if err != nil {
			result.Code = Code(err)
			return result, err
		}
		result.Entries = append(result.Entries, PlanEntryV1{Kind: entry.kind, Name: entry.name, Present: present, Required: entry.required})
	}
	sort.Slice(result.Entries, func(i, j int) bool { return result.Entries[i].Name < result.Entries[j].Name })
	if loadErr != nil {
		result.Code = Code(loadErr)
		return result, loadErr
	}
	result.Code = CodeOK
	return result, nil
}

func (store *Store) loadLogoutIntentLocked() (logoutIntentV1, bool, error) {
	present, err := store.filePresentSecure(LogoutIntentFileName)
	if err != nil || !present {
		return logoutIntentV1{}, present, err
	}
	encoded, err := store.readFileSecure(LogoutIntentFileName)
	if err != nil {
		return logoutIntentV1{}, false, err
	}
	var intent logoutIntentV1
	if decodeStrictJSON(encoded, &intent) != nil || intent.Version != 1 || intent.ExpectedRevision == 0 ||
		intent.NextRevision != intent.ExpectedRevision+1 || !idempotencyKeyPattern.MatchString(intent.RequestID) ||
		intent.RequestedAt.IsZero() || intent.RequestedAt.Location() != time.UTC || intent.TenantID == "" ||
		intent.OwnerUserID == "" || intent.WorkerID == "" {
		return logoutIntentV1{}, false, ErrInvalidState
	}
	return intent, true, nil
}

func intentMatches(intent logoutIntentV1, manifest ManifestV1, input LogoutInputV1) bool {
	return intent.ExpectedRevision == input.ExpectedRevision && intent.NextRevision == input.ExpectedRevision+1 &&
		intent.RequestID == input.RequestID && intent.TenantID == string(manifest.TenantID) &&
		intent.OwnerUserID == string(manifest.OwnerUserID) && intent.WorkerID == string(manifest.WorkerID)
}

func logoutResult(manifest ManifestV1) LogoutResultV1 {
	receipt := manifest.Logout
	return LogoutResultV1{
		Version: 1, Code: CodeOK, Lifecycle: manifest.Lifecycle, Revision: manifest.Revision,
		RequestID: receipt.RequestID, SecretState: receipt.SecretState,
		ServerRevocation: receipt.ServerRevocation, RemoteErasure: receipt.RemoteErasure,
	}
}

func verifyRegularDigest(path, want string) error {
	if !canonicalAbsolute(path) || !hexDigestPattern.MatchString(want) {
		return ErrInvalidState
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > 128<<20 ||
		info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 == 0 {
		return ErrInvalidState
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return ErrInvalidState
	}
	file, err := openReadNoFollow(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ErrLocalIO
	}
	if hex.EncodeToString(hash.Sum(nil)) != want {
		return ErrStateConflict
	}
	return nil
}

func verifyEmptyPrivateDirectory(path string) error {
	if !canonicalAbsolute(path) {
		return ErrInvalidState
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return ErrInvalidState
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return ErrInvalidState
	}
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) != 0 {
		return ErrInvalidState
	}
	return nil
}

func Code(err error) ResultCode {
	switch {
	case err == nil:
		return CodeOK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return CodeCancelled
	case errors.Is(err, ErrStateMissing):
		return CodeMissing
	case errors.Is(err, ErrStateIncomplete):
		return CodeIncomplete
	case errors.Is(err, ErrStateConflict):
		return CodeConflict
	case errors.Is(err, ErrStateAmbiguous):
		return CodeAmbiguous
	case errors.Is(err, ErrStateBusy):
		return CodeBusy
	case errors.Is(err, ErrStateUnsupported):
		return CodeUnsupported
	case errors.Is(err, ErrSecretRetired):
		return CodeRetired
	case errors.Is(err, ErrLocalIO):
		return CodeIO
	default:
		return CodeInvalid
	}
}
