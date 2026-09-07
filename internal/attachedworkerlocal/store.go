package attachedworkerlocal

import (
	"context"
	"crypto/subtle"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const temporaryFilePrefix = ".sessionless-tmp-"

type Store struct {
	root               string
	now                func() time.Time
	syncRootOverride   func() error
	syncParentOverride func() error
}

type RuntimeLease struct {
	file  *os.File
	store *Store
}

func NewStore(root string, now func() time.Time) (*Store, error) {
	if !canonicalAbsolute(root) || filepath.Dir(root) == root {
		return nil, ErrInvalidRoot
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Store{root: root, now: now}, nil
}

func (store *Store) Initialize(ctx context.Context, manifest ManifestV1, secret SecretRecordV1) (resultErr error) {
	if ctx == nil || ctx.Err() != nil || store == nil || manifest.Validate() != nil || secret.Validate(manifest) != nil ||
		manifest.Revision != 1 || manifest.Lifecycle != LifecycleActive || !manifest.CreatedAt.Equal(manifest.UpdatedAt) {
		return ErrInvalidState
	}
	if err := store.ensureRoot(true); err != nil {
		return err
	}
	lock, err := store.acquireStateLock(true)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if entries, err := os.ReadDir(store.root); err != nil {
		return ErrLocalIO
	} else {
		for _, entry := range entries {
			if entry.Name() != StateLockFileName && entry.Name() != RuntimeLockFileName {
				return ErrStateConflict
			}
		}
	}
	runtimeLock, err := openLockFile(store.path(RuntimeLockFileName), true)
	if err != nil {
		return err
	}
	if err := runtimeLock.Close(); err != nil {
		return ErrLocalIO
	}
	encodedSecret, err := encodeSecretRecord(secret)
	if err != nil {
		return err
	}
	encodedManifest, err := encodeStrictJSON(manifest)
	if err != nil {
		return err
	}
	if err := store.writeAtomic(SecretFileName, encodedSecret); err != nil {
		return err
	}
	if err := store.writeAtomic(ManifestFileName, encodedManifest); err != nil {
		return err
	}
	return nil
}

func (store *Store) Load(ctx context.Context) (manifest ManifestV1, resultErr error) {
	manifest, _, _, resultErr = store.loadSnapshot(ctx)
	return manifest, resultErr
}

func (store *Store) LoadSnapshot(ctx context.Context) (SnapshotV1, error) {
	manifest, observation, present, err := store.loadSnapshot(ctx)
	if err != nil {
		return SnapshotV1{}, err
	}
	return SnapshotV1{Manifest: manifest, Observation: observation, ObservationPresent: present}, nil
}

func (store *Store) loadSnapshot(ctx context.Context) (manifest ManifestV1, observation RuntimeObservationV1, observationPresent bool, resultErr error) {
	if ctx == nil || ctx.Err() != nil || store == nil {
		return ManifestV1{}, RuntimeObservationV1{}, false, ErrInvalidState
	}
	if err := store.ensureRoot(false); err != nil {
		return ManifestV1{}, RuntimeObservationV1{}, false, err
	}
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return ManifestV1{}, RuntimeObservationV1{}, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	if ctx.Err() != nil {
		return ManifestV1{}, RuntimeObservationV1{}, false, ctx.Err()
	}
	manifest, err = store.loadConsistentLocked()
	if err != nil {
		return ManifestV1{}, RuntimeObservationV1{}, false, err
	}
	observation, observationPresent, err = store.loadObservationLocked()
	if err != nil {
		return ManifestV1{}, RuntimeObservationV1{}, false, err
	}
	return manifest, observation, observationPresent, nil
}

func (store *Store) LoadSecret(ctx context.Context) (secretResult SecretRecordV1, resultErr error) {
	if ctx == nil || ctx.Err() != nil || store == nil {
		return SecretRecordV1{}, ErrInvalidState
	}
	if err := store.ensureRoot(false); err != nil {
		return SecretRecordV1{}, err
	}
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return SecretRecordV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	if ctx.Err() != nil {
		return SecretRecordV1{}, ctx.Err()
	}
	if err := store.validateInventory(); err != nil {
		return SecretRecordV1{}, err
	}
	if present, err := store.filePresentSecure(RuntimeLockFileName); err != nil {
		return SecretRecordV1{}, err
	} else if !present {
		return SecretRecordV1{}, ErrStateIncomplete
	}
	if present, err := store.filePresentSecure(LogoutIntentFileName); err != nil {
		return SecretRecordV1{}, err
	} else if present {
		return SecretRecordV1{}, ErrSecretRetired
	}
	manifest, err := store.loadManifestLocked()
	if err != nil {
		return SecretRecordV1{}, err
	}
	if manifest.Lifecycle != LifecycleActive {
		return SecretRecordV1{}, ErrSecretRetired
	}
	secret, err := store.loadSecretLocked()
	if err != nil || secret.Validate(manifest) != nil {
		return SecretRecordV1{}, ErrStateIncomplete
	}
	return cloneSecret(secret), nil
}

func (store *Store) Update(ctx context.Context, expectedRevision uint64, next ManifestV1, nextSecret SecretRecordV1) (resultErr error) {
	if ctx == nil || ctx.Err() != nil || store == nil || next.Validate() != nil || nextSecret.Validate(next) != nil {
		return ErrInvalidState
	}
	if err := store.ensureRoot(false); err != nil {
		return err
	}
	runtimeLease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, runtimeLease.Close()) }()
	return store.updateWithRuntimeLease(ctx, expectedRevision, next, nextSecret)
}

// LoadSnapshot reads the installation while this lease owns the local runtime
// boundary. It is the connection-session equivalent of Store.LoadSnapshot and
// does not treat the lease as server or protocol authority.
func (lease *RuntimeLease) LoadSnapshot(ctx context.Context) (SnapshotV1, error) {
	if lease == nil || lease.file == nil || lease.store == nil {
		return SnapshotV1{}, ErrInvalidState
	}
	return lease.store.LoadSnapshot(ctx)
}

// LoadSecret reads generation-bound local material while the same runtime
// owner remains active. The returned value is still a clone and remains the
// caller's responsibility to clear after use.
func (lease *RuntimeLease) LoadSecret(ctx context.Context) (SecretRecordV1, error) {
	if lease == nil || lease.file == nil || lease.store == nil {
		return SecretRecordV1{}, ErrInvalidState
	}
	return lease.store.LoadSecret(ctx)
}

// Update commits a generation change without attempting to acquire a second
// runtime lease. This is used by the one connection-session owner to durably
// fence an attach before any ambiguous network effect is possible.
func (lease *RuntimeLease) Update(ctx context.Context, expectedRevision uint64, next ManifestV1, nextSecret SecretRecordV1) error {
	if lease == nil || lease.file == nil || lease.store == nil {
		return ErrInvalidState
	}
	return lease.store.updateWithRuntimeLease(ctx, expectedRevision, next, nextSecret)
}

func (store *Store) updateWithRuntimeLease(ctx context.Context, expectedRevision uint64, next ManifestV1, nextSecret SecretRecordV1) (resultErr error) {
	if ctx == nil || ctx.Err() != nil || store == nil || next.Validate() != nil || nextSecret.Validate(next) != nil {
		return ErrInvalidState
	}
	if err := store.ensureRoot(false); err != nil {
		return err
	}
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	current, err := store.loadConsistentLocked()
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if current.Revision != expectedRevision || expectedRevision == ^uint64(0) || next.Revision != expectedRevision+1 ||
		!sameInstallation(current, next) || next.Lifecycle != LifecycleActive || !next.CreatedAt.Equal(current.CreatedAt) ||
		!next.UpdatedAt.After(current.UpdatedAt) || !sameOrNext(current.EnrollmentGeneration, next.EnrollmentGeneration) ||
		!sameOrNext(current.ConnectionGeneration, next.ConnectionGeneration) {
		return ErrStateConflict
	}
	currentSecret, err := store.loadSecretLocked()
	if err != nil || currentSecret.Validate(current) != nil ||
		!generationSecretChange(current.EnrollmentGeneration, next.EnrollmentGeneration, currentSecret.IdentityPrivateKey, nextSecret.IdentityPrivateKey) ||
		!generationSecretChange(current.ConnectionGeneration, next.ConnectionGeneration, currentSecret.ConnectionSecret, nextSecret.ConnectionSecret) {
		return ErrStateConflict
	}
	encodedSecret, err := encodeSecretRecord(nextSecret)
	if err != nil {
		return err
	}
	encodedManifest, err := encodeStrictJSON(next)
	if err != nil {
		return err
	}
	// Secret authority is replaced first. A crash before the manifest commit
	// leaves a detectable generation/revision mismatch and therefore fails closed.
	if err := store.removeDurable(ObservationFileName); err != nil {
		return err
	}
	if err := store.writeAtomic(SecretFileName, encodedSecret); err != nil {
		return err
	}
	if err := store.writeAtomic(ManifestFileName, encodedManifest); err != nil {
		return err
	}
	return nil
}

func (store *Store) AcquireRuntime(ctx context.Context) (*RuntimeLease, error) {
	if ctx == nil || ctx.Err() != nil || store == nil {
		return nil, ErrInvalidState
	}
	if err := store.ensureRoot(false); err != nil {
		return nil, err
	}
	file, err := openLockFile(store.path(RuntimeLockFileName), false)
	if err != nil {
		return nil, err
	}
	if err := acquireFileLock(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &RuntimeLease{file: file, store: store}, nil
}

func (lease *RuntimeLease) Close() error {
	if lease == nil || lease.file == nil {
		return nil
	}
	file := lease.file
	lease.file = nil
	lease.store = nil
	return releaseFileLock(file)
}

func (lease *RuntimeLease) PersistObservation(ctx context.Context, observation RuntimeObservationV1) (resultErr error) {
	if ctx == nil || ctx.Err() != nil || lease == nil || lease.file == nil || lease.store == nil {
		return ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	manifest, err := store.loadConsistentLocked()
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if observation.Validate(manifest) != nil {
		return ErrInvalidState
	}
	current, present, err := store.loadObservationLocked()
	if err != nil {
		return err
	}
	if !present {
		if observation.Revision != 1 {
			return ErrStateConflict
		}
	} else if current.Validate(manifest) != nil || current.Revision == ^uint64(0) ||
		observation.Revision != current.Revision+1 || observation.ObservedAt.Before(current.ObservedAt) ||
		observation.Accepted < current.Accepted || observation.Completed < current.Completed || observation.Failed < current.Failed {
		return ErrStateConflict
	}
	encoded, err := encodeStrictJSON(observation)
	if err != nil {
		return err
	}
	return store.writeAtomic(ObservationFileName, encoded)
}

// RetireObservation durably removes local runtime evidence while the caller
// still owns the kernel-backed runtime lease. Missing evidence is already
// retired and therefore succeeds.
func (lease *RuntimeLease) RetireObservation(ctx context.Context, expectedRevision uint64) (resultErr error) {
	if ctx == nil || ctx.Err() != nil || expectedRevision == 0 || lease == nil || lease.file == nil || lease.store == nil {
		return ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	if _, err := store.loadConsistentLocked(); err != nil {
		return err
	}
	observation, present, err := store.loadObservationLocked()
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if observation.Revision != expectedRevision {
		return ErrStateConflict
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return store.removeDurable(ObservationFileName)
}

type stateLock struct{ file *os.File }

func (store *Store) acquireStateLock(create bool) (*stateLock, error) {
	file, err := openLockFile(store.path(StateLockFileName), create)
	if err != nil {
		return nil, err
	}
	if err := acquireFileLock(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &stateLock{file: file}, nil
}

func (lock *stateLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	file := lock.file
	lock.file = nil
	return releaseFileLock(file)
}

func (store *Store) ensureRoot(create bool) error {
	if store == nil || !canonicalAbsolute(store.root) || filepath.Dir(store.root) == store.root {
		return ErrInvalidRoot
	}
	if !platformSupported() {
		return ErrStateUnsupported
	}
	parent := filepath.Dir(store.root)
	resolvedParent, err := filepath.EvalSymlinks(parent)
	if err != nil || resolvedParent != parent {
		return ErrInvalidRoot
	}
	if create {
		if err := os.Mkdir(store.root, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return ErrLocalIO
		}
	}
	info, err := os.Lstat(store.root)
	if errors.Is(err, fs.ErrNotExist) {
		return ErrStateMissing
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return ErrInvalidRoot
	}
	resolvedRoot, err := filepath.EvalSymlinks(store.root)
	if err != nil || resolvedRoot != store.root {
		return ErrInvalidRoot
	}
	if create {
		if err := store.syncParent(); err != nil {
			return ErrStateAmbiguous
		}
	}
	return nil
}

func (store *Store) loadConsistentLocked() (ManifestV1, error) {
	if err := store.validateInventory(); err != nil {
		return ManifestV1{}, err
	}
	if present, err := store.filePresentSecure(RuntimeLockFileName); err != nil {
		return ManifestV1{}, err
	} else if !present {
		return ManifestV1{}, ErrStateIncomplete
	}
	manifest, err := store.loadManifestLocked()
	if err != nil {
		if errors.Is(err, ErrStateMissing) {
			secretPresent, secretErr := store.filePresentSecure(SecretFileName)
			if secretErr != nil {
				return ManifestV1{}, secretErr
			}
			intentPresent, intentErr := store.filePresentSecure(LogoutIntentFileName)
			if intentErr != nil {
				return ManifestV1{}, intentErr
			}
			if secretPresent || intentPresent {
				return ManifestV1{}, ErrStateIncomplete
			}
		}
		return ManifestV1{}, err
	}
	intentPresent, err := store.filePresentSecure(LogoutIntentFileName)
	if err != nil {
		return ManifestV1{}, err
	}
	secretPresent, err := store.filePresentSecure(SecretFileName)
	if err != nil {
		return ManifestV1{}, err
	}
	if intentPresent {
		return ManifestV1{}, ErrStateIncomplete
	}
	if manifest.Lifecycle == LifecycleLoggedOut {
		observationPresent, observationErr := store.filePresentSecure(ObservationFileName)
		if observationErr != nil {
			return ManifestV1{}, observationErr
		}
		if secretPresent || observationPresent {
			return ManifestV1{}, ErrStateIncomplete
		}
		return manifest, nil
	}
	if !secretPresent {
		return ManifestV1{}, ErrStateIncomplete
	}
	secret, err := store.loadSecretLocked()
	if err != nil || secret.Validate(manifest) != nil {
		return ManifestV1{}, ErrStateIncomplete
	}
	observation, present, err := store.loadObservationLocked()
	if err != nil {
		return ManifestV1{}, err
	}
	if present && observation.Validate(manifest) != nil {
		return ManifestV1{}, ErrStateIncomplete
	}
	return manifest, nil
}

func (store *Store) loadManifestLocked() (ManifestV1, error) {
	encoded, err := store.readFileSecure(ManifestFileName)
	if err != nil {
		return ManifestV1{}, err
	}
	var manifest ManifestV1
	if decodeStrictJSON(encoded, &manifest) != nil || manifest.Validate() != nil {
		return ManifestV1{}, ErrInvalidState
	}
	return manifest, nil
}

func (store *Store) loadSecretLocked() (SecretRecordV1, error) {
	encoded, err := store.readFileSecure(SecretFileName)
	if err != nil {
		return SecretRecordV1{}, err
	}
	return decodeSecretRecord(encoded)
}

func (store *Store) loadObservationLocked() (RuntimeObservationV1, bool, error) {
	present, err := store.filePresentSecure(ObservationFileName)
	if err != nil || !present {
		return RuntimeObservationV1{}, present, err
	}
	encoded, err := store.readFileSecure(ObservationFileName)
	if err != nil {
		return RuntimeObservationV1{}, false, err
	}
	var observation RuntimeObservationV1
	if decodeStrictJSON(encoded, &observation) != nil {
		return RuntimeObservationV1{}, false, ErrInvalidState
	}
	return observation, true, nil
}

func (store *Store) readFileSecure(name string) ([]byte, error) {
	path := store.path(name)
	lstat, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrStateMissing
	}
	if err != nil || !lstat.Mode().IsRegular() || lstat.Mode().Perm() != 0o600 || !ownedByCurrentUser(lstat) ||
		lstat.Size() <= 0 || lstat.Size() > maxStateFileBytes {
		return nil, ErrInvalidState
	}
	file, err := openReadNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(lstat, opened) {
		return nil, ErrInvalidState
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxStateFileBytes+1))
	if err != nil || len(encoded) == 0 || len(encoded) > maxStateFileBytes {
		return nil, ErrInvalidState
	}
	return encoded, nil
}

func (store *Store) filePresentSecure(name string) (bool, error) {
	info, err := os.Lstat(store.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
		return false, ErrInvalidState
	}
	return true, nil
}

func (store *Store) validateInventory() error {
	entries, err := os.ReadDir(store.root)
	if err != nil {
		return ErrLocalIO
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), temporaryFilePrefix) {
			return ErrStateIncomplete
		}
		switch entry.Name() {
		case ManifestFileName, SecretFileName, LogoutIntentFileName, ObservationFileName,
			StateLockFileName, RuntimeLockFileName:
		default:
			return ErrInvalidState
		}
	}
	return nil
}

func (store *Store) writeAtomic(name string, encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > maxStateFileBytes {
		return ErrInvalidState
	}
	target := store.path(name)
	if info, err := os.Lstat(target); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
			return ErrInvalidState
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return ErrLocalIO
	}
	temporary, err := os.CreateTemp(store.root, temporaryFilePrefix)
	if err != nil {
		return ErrLocalIO
	}
	temporaryName := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			_ = temporary.Close()
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return ErrLocalIO
	}
	if _, err := temporary.Write(encoded); err != nil || temporary.Sync() != nil || temporary.Close() != nil {
		return ErrLocalIO
	}
	if err := os.Rename(temporaryName, target); err != nil {
		return ErrLocalIO
	}
	committed = true
	if err := store.syncRoot(); err != nil {
		return ErrStateAmbiguous
	}
	return nil
}

func (store *Store) removeDurable(name string) error {
	path := store.path(name)
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrLocalIO
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
		return ErrInvalidState
	}
	if err := os.Remove(path); err != nil {
		return ErrLocalIO
	}
	if err := store.syncRoot(); err != nil {
		return ErrStateAmbiguous
	}
	return nil
}

func (store *Store) syncRoot() error {
	if store.syncRootOverride != nil {
		return store.syncRootOverride()
	}
	return syncDirectory(store.root)
}

func (store *Store) syncParent() error {
	if store.syncParentOverride != nil {
		return store.syncParentOverride()
	}
	return syncDirectory(filepath.Dir(store.root))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return ErrLocalIO
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return ErrLocalIO
	}
	return nil
}

func (store *Store) path(name string) string { return filepath.Join(store.root, name) }

func sameInstallation(left, right ManifestV1) bool {
	return left.Version == right.Version && left.ControlPlaneOrigin == right.ControlPlaneOrigin &&
		left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID && left.WorkerID == right.WorkerID
}

func sameOrNext(current, next uint64) bool {
	return next == current || current != ^uint64(0) && next == current+1
}

func generationSecretChange(currentGeneration, nextGeneration uint64, current, next []byte) bool {
	same := len(current) == len(next) && subtle.ConstantTimeCompare(current, next) == 1
	if nextGeneration == currentGeneration {
		return same
	}
	return !same
}

func cloneSecret(secret SecretRecordV1) SecretRecordV1 {
	clone := secret
	clone.IdentityPrivateKey = append([]byte(nil), secret.IdentityPrivateKey...)
	clone.ConnectionSecret = append([]byte(nil), secret.ConnectionSecret...)
	return clone
}
