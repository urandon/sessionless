//go:build darwin || linux

package credentiallifecycle

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"golang.org/x/sys/unix"
)

const (
	invocationPrefix          = "invocation-"
	authFilename              = "auth.json"
	maxRecoveredProviderRoots = 256
)

type secureCredentialFS struct {
	rootPath string
	rootFD   int
}

type pinnedMaterialization struct {
	public   ports.CredentialMaterialization
	name     string
	fileName string
	dirFD    int

	mu     sync.Mutex
	closed bool
	result error
}

func newSecureCredentialFS(canonicalScratchRoot string) (*secureCredentialFS, error) {
	return newSecureCredentialFSWithPrefix(canonicalScratchRoot, "sessionless-credentials-")
}

func newSecureCredentialFSWithPrefix(canonicalScratchRoot, prefix string) (*secureCredentialFS, error) {
	rootPath, err := os.MkdirTemp(canonicalScratchRoot, prefix)
	if err != nil {
		return nil, ErrCredentialMaterialization
	}
	if err := os.Chmod(rootPath, 0o700); err != nil {
		_ = os.Remove(rootPath)
		return nil, ErrCredentialMaterialization
	}
	rootFD, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		_ = os.Remove(rootPath)
		return nil, ErrCredentialMaterialization
	}
	if err := validateDirectoryFD(rootFD); err != nil {
		_ = unix.Close(rootFD)
		_ = os.Remove(rootPath)
		return nil, err
	}
	return &secureCredentialFS{rootPath: rootPath, rootFD: rootFD}, nil
}

func acquireProviderScratchRoot(canonicalScratchRoot string) (int, error) {
	rootFD, err := unix.Open(canonicalScratchRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, ErrCredentialMaterialization
	}
	if err := validateDirectoryFD(rootFD); err != nil {
		_ = unix.Close(rootFD)
		return -1, err
	}
	if err := unix.Flock(rootFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = unix.Close(rootFD)
		return -1, ErrCredentialMaterialization
	}
	if err := recoverProviderScratchRoot(rootFD); err != nil {
		_ = unix.Flock(rootFD, unix.LOCK_UN)
		_ = unix.Close(rootFD)
		return -1, err
	}
	return rootFD, nil
}

func recoverProviderScratchRoot(rootFD int) error {
	duplicate, err := unix.Dup(rootFD)
	if err != nil {
		return ErrCredentialMaterialization
	}
	root := os.NewFile(uintptr(duplicate), "provider-credential-scratch")
	if root == nil {
		_ = unix.Close(duplicate)
		return ErrCredentialMaterialization
	}
	defer root.Close()
	names, err := root.Readdirnames(maxRecoveredProviderRoots + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return ErrCredentialMaterialization
	}
	if len(names) > maxRecoveredProviderRoots {
		return ErrCredentialMaterialization
	}
	for _, name := range names {
		if !strings.HasPrefix(name, providerRootPrefix) || filepath.Base(name) != name {
			return ErrCredentialMaterialization
		}
		if err := recoverProviderServiceRoot(rootFD, name); err != nil {
			return err
		}
	}
	return nil
}

func recoverProviderServiceRoot(scratchFD int, name string) error {
	serviceFD, err := unix.Openat(scratchFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ErrCredentialMaterialization
	}
	defer unix.Close(serviceFD)
	if err := validateDirectoryFD(serviceFD); err != nil {
		return err
	}
	duplicate, err := unix.Dup(serviceFD)
	if err != nil {
		return ErrCredentialMaterialization
	}
	serviceRoot := os.NewFile(uintptr(duplicate), name)
	if serviceRoot == nil {
		_ = unix.Close(duplicate)
		return ErrCredentialMaterialization
	}
	invocations, readErr := serviceRoot.Readdirnames(maxRecoveredProviderRoots + 1)
	_ = serviceRoot.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || len(invocations) > maxRecoveredProviderRoots {
		return ErrCredentialMaterialization
	}
	for _, invocation := range invocations {
		if !strings.HasPrefix(invocation, invocationPrefix) || filepath.Base(invocation) != invocation {
			return ErrCredentialMaterialization
		}
		if err := recoverProviderInvocation(serviceFD, invocation); err != nil {
			return err
		}
	}
	var pinnedStat, entryStat unix.Stat_t
	if err := unix.Fstat(serviceFD, &pinnedStat); err != nil || unix.Fstatat(scratchFD, name, &entryStat, unix.AT_SYMLINK_NOFOLLOW) != nil || pinnedStat.Dev != entryStat.Dev || pinnedStat.Ino != entryStat.Ino || entryStat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return ErrCredentialMaterialization
	}
	if err := unix.Unlinkat(scratchFD, name, unix.AT_REMOVEDIR); err != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func recoverProviderInvocation(serviceFD int, name string) error {
	dirFD, err := unix.Openat(serviceFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ErrCredentialMaterialization
	}
	defer unix.Close(dirFD)
	if err := validateDirectoryFD(dirFD); err != nil {
		return err
	}
	duplicate, err := unix.Dup(dirFD)
	if err != nil {
		return ErrCredentialMaterialization
	}
	directory := os.NewFile(uintptr(duplicate), name)
	if directory == nil {
		_ = unix.Close(duplicate)
		return ErrCredentialMaterialization
	}
	files, readErr := directory.Readdirnames(2)
	_ = directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || len(files) > 1 {
		return ErrCredentialMaterialization
	}
	for _, fileName := range files {
		if err := domain.ValidateOpaqueID("credential.materialization.file_name", fileName); err != nil || filepath.Base(fileName) != fileName {
			return ErrCredentialMaterialization
		}
		var stat unix.Stat_t
		if err := unix.Fstatat(dirFD, fileName, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 {
			return ErrCredentialMaterialization
		}
		if err := unix.Unlinkat(dirFD, fileName, 0); err != nil {
			return ErrCredentialMaterialization
		}
	}
	var pinnedStat, entryStat unix.Stat_t
	if err := unix.Fstat(dirFD, &pinnedStat); err != nil || unix.Fstatat(serviceFD, name, &entryStat, unix.AT_SYMLINK_NOFOLLOW) != nil || pinnedStat.Dev != entryStat.Dev || pinnedStat.Ino != entryStat.Ino || entryStat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return ErrCredentialMaterialization
	}
	if err := unix.Unlinkat(serviceFD, name, unix.AT_REMOVEDIR); err != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func releaseProviderScratchRoot(rootFD int) error {
	if rootFD < 0 {
		return nil
	}
	unlockErr := unix.Flock(rootFD, unix.LOCK_UN)
	closeErr := unix.Close(rootFD)
	if unlockErr != nil || closeErr != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func (filesystem *secureCredentialFS) create(secret []byte, maxBytes int64) (*pinnedMaterialization, error) {
	return filesystem.createNamed(authFilename, secret, maxBytes)
}

func (filesystem *secureCredentialFS) createNamed(fileName string, secret []byte, maxBytes int64) (*pinnedMaterialization, error) {
	if filesystem == nil || filesystem.rootFD < 0 || len(secret) == 0 || int64(len(secret)) > maxBytes {
		return nil, ErrCredentialMaterialization
	}
	if err := domain.ValidateOpaqueID("credential.materialization.file_name", fileName); err != nil || filepath.Base(fileName) != fileName {
		return nil, ErrCredentialMaterialization
	}
	name, err := filesystem.mkdirInvocation()
	if err != nil {
		return nil, err
	}
	dirFD, err := unix.Openat(filesystem.rootFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		_ = unix.Unlinkat(filesystem.rootFD, name, unix.AT_REMOVEDIR)
		return nil, ErrCredentialMaterialization
	}
	pinned := &pinnedMaterialization{
		name:     name,
		fileName: fileName,
		dirFD:    dirFD,
		public: ports.CredentialMaterialization{
			RootDir:  filepath.Join(filesystem.rootPath, name),
			AuthFile: filepath.Join(filesystem.rootPath, name, fileName),
		},
	}
	if err := validateDirectoryFD(dirFD); err != nil {
		_ = filesystem.cleanup(pinned)
		return nil, err
	}
	authFD, err := unix.Openat(dirFD, fileName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		_ = filesystem.cleanup(pinned)
		return nil, ErrCredentialMaterialization
	}
	file := os.NewFile(uintptr(authFD), fileName)
	if file == nil {
		_ = unix.Close(authFD)
		_ = filesystem.cleanup(pinned)
		return nil, ErrCredentialMaterialization
	}
	written, writeErr := file.Write(secret)
	chmodErr := file.Chmod(0o600)
	closeErr := file.Close()
	if writeErr != nil || written != len(secret) || chmodErr != nil || closeErr != nil {
		_ = filesystem.cleanup(pinned)
		return nil, ErrCredentialMaterialization
	}
	verified, err := filesystem.read(pinned, maxBytes)
	if err != nil {
		_ = filesystem.cleanup(pinned)
		return nil, err
	}
	zero(verified)
	return pinned, nil
}

func (filesystem *secureCredentialFS) mkdirInvocation() (string, error) {
	for attempt := 0; attempt < 16; attempt++ {
		var random [16]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", ErrCredentialMaterialization
		}
		name := invocationPrefix + hex.EncodeToString(random[:])
		err := unix.Mkdirat(filesystem.rootFD, name, 0o700)
		if err == nil {
			return name, nil
		}
		if !errors.Is(err, unix.EEXIST) {
			return "", ErrCredentialMaterialization
		}
	}
	return "", ErrCredentialMaterialization
}

func (filesystem *secureCredentialFS) read(pinned *pinnedMaterialization, maxBytes int64) ([]byte, error) {
	if !filesystem.validPinned(pinned) || maxBytes < 1 {
		return nil, ErrCredentialMaterialization
	}
	pinned.mu.Lock()
	if pinned.closed {
		pinned.mu.Unlock()
		return nil, ErrCredentialMaterialization
	}
	dirFD := pinned.dirFD
	if err := validateDirectoryFD(dirFD); err != nil {
		pinned.mu.Unlock()
		return nil, err
	}
	authFD, err := unix.Openat(dirFD, pinned.fileName, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	pinned.mu.Unlock()
	if err != nil {
		return nil, ErrCredentialMaterialization
	}
	file := os.NewFile(uintptr(authFD), pinned.fileName)
	if file == nil {
		_ = unix.Close(authFD)
		return nil, ErrCredentialMaterialization
	}
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(authFD, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 {
		return nil, ErrCredentialMaterialization
	}
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(content)) > maxBytes || len(content) == 0 {
		zero(content)
		return nil, ErrCredentialMaterialization
	}
	return content, nil
}

func (filesystem *secureCredentialFS) cleanup(pinned *pinnedMaterialization) error {
	if !filesystem.validPinned(pinned) {
		return ErrCredentialMaterialization
	}
	pinned.mu.Lock()
	if pinned.closed {
		result := pinned.result
		pinned.mu.Unlock()
		return result
	}
	dirFD := pinned.dirFD
	pinned.closed = true
	pinned.mu.Unlock()
	defer unix.Close(dirFD)
	finish := func(err error) error {
		pinned.mu.Lock()
		pinned.result = err
		pinned.mu.Unlock()
		return err
	}
	if err := validateDirectoryFD(dirFD); err != nil {
		return finish(err)
	}
	if err := unix.Unlinkat(dirFD, pinned.fileName, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return finish(ErrCredentialMaterialization)
	}
	var pinnedStat unix.Stat_t
	var entryStat unix.Stat_t
	if err := unix.Fstat(dirFD, &pinnedStat); err != nil {
		return finish(ErrCredentialMaterialization)
	}
	if err := unix.Fstatat(filesystem.rootFD, pinned.name, &entryStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return finish(ErrCredentialMaterialization)
	}
	if pinnedStat.Dev != entryStat.Dev || pinnedStat.Ino != entryStat.Ino || entryStat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return finish(ErrCredentialMaterialization)
	}
	if err := unix.Unlinkat(filesystem.rootFD, pinned.name, unix.AT_REMOVEDIR); err != nil {
		return finish(ErrCredentialMaterialization)
	}
	return finish(nil)
}

func (filesystem *secureCredentialFS) validPinned(pinned *pinnedMaterialization) bool {
	return filesystem != nil && filesystem.rootFD >= 0 && pinned != nil && pinned.name != "" && pinned.fileName != "" &&
		filepath.Dir(pinned.public.RootDir) == filesystem.rootPath &&
		filepath.Base(pinned.public.RootDir) == pinned.name &&
		pinned.public.AuthFile == filepath.Join(pinned.public.RootDir, pinned.fileName)
}

func (filesystem *secureCredentialFS) close() error {
	if filesystem == nil || filesystem.rootFD < 0 {
		return nil
	}
	err := unix.Close(filesystem.rootFD)
	filesystem.rootFD = -1
	if err != nil {
		return ErrCredentialMaterialization
	}
	return nil
}

func validateDirectoryFD(fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o777 != 0o700 {
		return ErrCredentialMaterialization
	}
	return nil
}
