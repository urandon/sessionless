//go:build darwin || linux

package credentiallifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/domain"
	"golang.org/x/sys/unix"
)

type immutableNamespace struct {
	scratchPath   string
	scratch, root unix.Stat_t
}

type immutableSecretFile struct {
	file, directory unix.Stat_t
	fingerprint     domain.CredentialFingerprint
}

func (filesystem *PinnedSecretFileSystem) captureNamespace() error {
	identity := &immutableNamespace{scratchPath: filepath.Dir(filesystem.filesystem.rootPath)}
	// Snapshot here without tightening the original mutable constructor's
	// permission semantics. Exact private mode checks belong to the opt-in
	// immutable ancestry verifier, not subscription/router construction.
	if unix.Fstat(filesystem.scratchFD, &identity.scratch) != nil || unix.Fstat(filesystem.filesystem.rootFD, &identity.root) != nil {
		return ErrCredentialMaterialization
	}
	filesystem.identity = identity
	return nil
}

// CreateImmutable is an opt-in provider-file capability. Its private metadata
// records the original creator descriptor before any path is exposed; it does
// not retain plaintext or change mutable Create/subscription read semantics.
func (filesystem *PinnedSecretFileSystem) CreateImmutable(fileName string, secret []byte, maxBytes int64) (*PinnedSecretFile, error) {
	if filesystem == nil {
		return nil, ErrCredentialMaterialization
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if filesystem.filesystem == nil || filesystem.verifyAncestry() != nil {
		return nil, ErrCredentialMaterialization
	}
	pinned, err := filesystem.filesystem.createNamedMode(fileName, secret, maxBytes, true, filesystem.owned)
	if err != nil {
		return nil, err
	}
	if filesystem.verifyImmutableLocked(context.Background(), pinned, domain.FingerprintCredential(secret), maxBytes) != nil {
		if filesystem.filesystem.cleanup(pinned) == nil {
			delete(filesystem.owned, pinned)
		}
		return nil, ErrCredentialMaterialization
	}
	return &PinnedSecretFile{RootDir: pinned.public.RootDir, FilePath: pinned.public.AuthFile, pinned: pinned, owner: filesystem}, nil
}

// ImmutableFilePathBound reports the exact future path BYTE length for this
// live immutable namespace and filename before any credential is issued or
// file is created. The per-service random root already exists; only the fixed
// hex invocation suffix is still unknown. This is sizing DATA, not a current
// owner/Source/credential/file-integrity/native-fit capability. The real
// Materialize consumer must still validate its actual original locator and
// unchanged namespace, within the same prospectively funded Init reservation.
func (filesystem *PinnedSecretFileSystem) ImmutableFilePathBound(ctx context.Context, fileName string) (uint64, error) {
	if filesystem == nil || ctx == nil || ctx.Err() != nil || domain.ValidateOpaqueID("credential.materialization.file_name", fileName) != nil || filepath.Base(fileName) != fileName || fileName == "." || fileName == ".." {
		return 0, ErrCredentialMaterialization
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if filesystem.filesystem == nil || filesystem.verifyAncestry() != nil || ctx.Err() != nil {
		return 0, ErrCredentialMaterialization
	}
	// Exactly the SAME naming constants used by mkdirInvocation/createNamedMode.
	// No PATH_MAX guess, new random directory/file, key read or path placeholder.
	return uint64(len(filesystem.filesystem.rootPath)) + 1 + uint64(len(invocationPrefix)) + 2*invocationRandomBytes + 1 + uint64(len(fileName)), nil
}

func captureImmutable(pinned *pinnedMaterialization, creator *os.File, secret []byte, maxBytes int64) error {
	identity := &immutableSecretFile{fingerprint: domain.FingerprintCredential(secret)}
	if creator == nil || unix.Fstat(pinned.fileFD, &identity.file) != nil || unix.Fstat(pinned.dirFD, &identity.directory) != nil || !privateDirectory(identity.directory) || !privateRegular(identity.file) || identity.file.Size != int64(len(secret)) {
		return ErrCredentialMaterialization
	}
	pinned.immutable = identity
	return hashImmutable(context.Background(), pinned, identity.fingerprint, maxBytes)
}

// VerifyImmutable proves only bounded observed local file integrity. It grants
// no remote phase, Run/head, owner spending or publication authority. Lock order
// is wrapper mu -> pinned mu, with no lifecycle callbacks under either lock.
func (filesystem *PinnedSecretFileSystem) VerifyImmutable(ctx context.Context, file *PinnedSecretFile, expected domain.CredentialFingerprint, maxBytes int64) error {
	if filesystem == nil || ctx == nil || ctx.Err() != nil {
		return ErrCredentialMaterialization
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if filesystem.filesystem == nil || file == nil || file.owner != filesystem || file.pinned == nil || file.RootDir != file.pinned.public.RootDir || file.FilePath != file.pinned.public.AuthFile {
		return ErrCredentialMaterialization
	}
	if _, ok := filesystem.owned[file.pinned]; !ok {
		return ErrCredentialMaterialization
	}
	return filesystem.verifyImmutableLocked(ctx, file.pinned, expected, maxBytes)
}

func (filesystem *PinnedSecretFileSystem) verifyImmutableLocked(ctx context.Context, pinned *pinnedMaterialization, expected domain.CredentialFingerprint, maxBytes int64) error {
	pinned.mu.Lock()
	defer pinned.mu.Unlock()
	if ctx.Err() != nil || pinned.closed || pinned.immutable == nil || expected.Validate() != nil || expected != pinned.immutable.fingerprint {
		return ErrCredentialMaterialization
	}
	if filesystem.verifyNamespace(pinned) != nil || ctx.Err() != nil || hashImmutable(ctx, pinned, expected, maxBytes) != nil || ctx.Err() != nil || filesystem.verifyNamespace(pinned) != nil || ctx.Err() != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func privateDirectory(stat unix.Stat_t) bool {
	return stat.Mode&unix.S_IFMT == unix.S_IFDIR && stat.Mode&0o7777 == 0o700
}

func privateRegular(stat unix.Stat_t) bool {
	return stat.Mode&unix.S_IFMT == unix.S_IFREG && stat.Mode&0o7777 == 0o600 && stat.Nlink == 1 && stat.Size > 0
}

func sameIdentity(current, original unix.Stat_t) bool {
	return current.Dev == original.Dev && current.Ino == original.Ino && current.Uid == original.Uid && current.Gid == original.Gid
}

func verifyDirectory(fd int, original unix.Stat_t) error {
	var current unix.Stat_t
	if unix.Fstat(fd, &current) != nil || !privateDirectory(current) || !sameIdentity(current, original) {
		return ErrCredentialMaterialization
	}
	return nil
}

func verifyEntry(fd int, name string, original unix.Stat_t, directory bool) error {
	var current unix.Stat_t
	if unix.Fstatat(fd, name, &current, unix.AT_SYMLINK_NOFOLLOW) != nil || !sameIdentity(current, original) {
		return ErrCredentialMaterialization
	}
	if directory {
		if !privateDirectory(current) {
			return ErrCredentialMaterialization
		}
	} else if !privateRegular(current) || current.Size != original.Size {
		return ErrCredentialMaterialization
	}
	return nil
}

func (filesystem *PinnedSecretFileSystem) verifyAncestry() error {
	identity := filesystem.identity
	if identity == nil || verifyDirectory(filesystem.scratchFD, identity.scratch) != nil || verifyDirectory(filesystem.filesystem.rootFD, identity.root) != nil {
		return ErrCredentialMaterialization
	}
	canonical, err := filepath.EvalSymlinks(identity.scratchPath)
	if err != nil || canonical != identity.scratchPath {
		return ErrCredentialMaterialization
	}
	fd, err := unix.Open(identity.scratchPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ErrCredentialMaterialization
	}
	defer unix.Close(fd)
	if verifyDirectory(fd, identity.scratch) != nil || verifyEntry(filesystem.scratchFD, filepath.Base(filesystem.filesystem.rootPath), identity.root, true) != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func (filesystem *PinnedSecretFileSystem) verifyNamespace(pinned *pinnedMaterialization) error {
	if filesystem.verifyAncestry() != nil || verifyDirectory(pinned.dirFD, pinned.immutable.directory) != nil || verifyEntry(filesystem.filesystem.rootFD, pinned.name, pinned.immutable.directory, true) != nil || verifyEntry(pinned.dirFD, pinned.fileName, pinned.immutable.file, false) != nil {
		return ErrCredentialMaterialization
	}
	var fileStat unix.Stat_t
	if unix.Fstat(pinned.fileFD, &fileStat) != nil || !sameIdentity(fileStat, pinned.immutable.file) || !privateRegular(fileStat) || fileStat.Size != pinned.immutable.file.Size {
		return ErrCredentialMaterialization
	}
	files, err := boundedDirectoryNames(pinned.dirFD, pinned.immutable.directory, 2)
	if err != nil || len(files) != 1 || files[0] != pinned.fileName || len(filesystem.owned) == math.MaxInt {
		return ErrCredentialMaterialization
	}
	names, err := boundedDirectoryNames(filesystem.filesystem.rootFD, filesystem.identity.root, len(filesystem.owned)+1)
	if err != nil || len(names) != len(filesystem.owned) {
		return ErrCredentialMaterialization
	}
	known := make(map[string]struct{}, len(filesystem.owned))
	for owned := range filesystem.owned {
		known[owned.name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := known[name]; !ok {
			return ErrCredentialMaterialization
		}
		delete(known, name)
	}
	if len(known) != 0 {
		return ErrCredentialMaterialization
	}
	return nil
}

// Open a new description, not Dup: Dup would share and advance the retained
// directory offset and turn repeated bounded scans into false empty results.
func boundedDirectoryNames(fd int, original unix.Stat_t, limit int) ([]string, error) {
	if limit < 1 {
		return nil, ErrCredentialMaterialization
	}
	fresh, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrCredentialMaterialization
	}
	file := os.NewFile(uintptr(fresh), "credential-directory")
	if file == nil {
		_ = unix.Close(fresh)
		return nil, ErrCredentialMaterialization
	}
	defer file.Close()
	if verifyDirectory(fresh, original) != nil {
		return nil, ErrCredentialMaterialization
	}
	names, err := file.Readdirnames(limit)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, ErrCredentialMaterialization
	}
	return names, nil
}

func hashImmutable(ctx context.Context, pinned *pinnedMaterialization, expected domain.CredentialFingerprint, maxBytes int64) error {
	size := pinned.immutable.file.Size
	if maxBytes < 1 || size < 1 || size > maxBytes || size == math.MaxInt64 || ctx.Err() != nil {
		return ErrCredentialMaterialization
	}
	var buffer [4096]byte
	defer zero(buffer[:])
	digest := sha256.New()
	var offset int64
	for offset < size+1 {
		if ctx.Err() != nil {
			return ErrCredentialMaterialization
		}
		count := int64(len(buffer))
		if size+1-offset < count {
			count = size + 1 - offset
		}
		n, err := unix.Pread(pinned.fileFD, buffer[:int(count)], offset)
		if err != nil || ctx.Err() != nil {
			return ErrCredentialMaterialization
		}
		if n == 0 {
			break
		}
		_, _ = digest.Write(buffer[:n])
		zero(buffer[:])
		offset += int64(n)
	}
	if offset != size || domain.CredentialFingerprint(hex.EncodeToString(digest.Sum(nil))) != expected || ctx.Err() != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func closePinnedDescriptors(pinned *pinnedMaterialization) error {
	var result error
	if pinned.creator != nil {
		if pinned.creator.Close() != nil {
			result = ErrCredentialMaterialization
		}
		pinned.creator = nil
		pinned.fileFD = -1
	}
	if pinned.dirFD >= 0 {
		if unix.Close(pinned.dirFD) != nil {
			result = ErrCredentialMaterialization
		}
		pinned.dirFD = -1
	}
	return result
}

func (*PinnedSecretFileSystem) closedResult(pinned *pinnedMaterialization) error {
	pinned.mu.Lock()
	defer pinned.mu.Unlock()
	if !pinned.closed {
		return ErrCredentialMaterialization
	}
	return pinned.result
}

// Close drains descriptors only. Unreleased namespaces remain crash residue;
// closed ownership is not cleanup success or an immutable Finalize proof.
func (*PinnedSecretFileSystem) drain(pinned *pinnedMaterialization) error {
	pinned.mu.Lock()
	defer pinned.mu.Unlock()
	if pinned.closed {
		return nil
	}
	pinned.closed = true
	pinned.result = ErrCredentialMaterialization
	return closePinnedDescriptors(pinned)
}
