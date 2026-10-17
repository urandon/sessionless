//go:build darwin || linux

package localoperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// openCanonicalDir anchors every path component to an already-open directory.
// No component is resolved through a symlink, including the final directory.
func openCanonicalDir(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.IndexByte(path, 0) >= 0 {
		return -1, ErrDenied
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, ErrDenied
	}
	if path == "/" {
		return fd, nil
	}
	for _, component := range strings.Split(path[1:], "/") {
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		closeErr := unix.Close(fd)
		if err != nil || closeErr != nil {
			if next >= 0 {
				_ = unix.Close(next)
			}
			return -1, ErrDenied
		}
		fd = next
	}
	return fd, nil
}

func stableOperatorStat(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func privateOperatorRoot(s unix.Stat_t) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFDIR && s.Mode&0o7777 == 0o700 && s.Uid == uint32(os.Geteuid())
}

func privateOperatorFile(s unix.Stat_t, maximum int64, exact int64) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFREG && s.Mode&0o7777 == 0o600 && s.Uid == uint32(os.Geteuid()) && s.Nlink == 1 && s.Size > 0 && s.Size <= maximum && (exact == 0 || s.Size == exact)
}

func readPrivateOperatorFile(ctx context.Context, root int, name string, maximum, exact int64) ([]byte, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrDenied
	}
	fd, err := unix.Openat(root, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrDenied
	}
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		_ = unix.Close(fd)
		return nil, ErrDenied
	}
	defer f.Close()
	var before, after, entry unix.Stat_t
	if unix.Fstat(fd, &before) != nil || !privateOperatorFile(before, maximum, exact) || unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(before, entry) {
		return nil, ErrDenied
	}
	data := make([]byte, 0, int(before.Size))
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	valid := false
	defer func() {
		if !valid {
			clear(data)
		}
	}()
	for {
		if ctx.Err() != nil {
			return nil, ErrDenied
		}
		n, readErr := f.Read(buffer)
		if int64(len(data))+int64(n) > maximum {
			return nil, ErrDenied
		}
		data = append(data, buffer[:n]...)
		if readErr == io.EOF {
			break
		}
		if readErr != nil || n == 0 {
			return nil, ErrDenied
		}
	}
	if ctx.Err() != nil || int64(len(data)) != before.Size || unix.Fstat(fd, &after) != nil || !stableOperatorStat(before, after) || unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(before, entry) {
		return nil, ErrDenied
	}
	valid = true
	return data, nil
}

func readOperatorFiles(ctx context.Context, root string) (manifest []byte, key [32]byte, resultErr error) {
	if ctx == nil || ctx.Err() != nil || root == "/" {
		return nil, key, ErrDenied
	}
	fd, err := openCanonicalDir(root)
	if err != nil {
		return nil, key, ErrDenied
	}
	defer unix.Close(fd)
	var before, after, originalManifest, originalKey, currentManifest, currentKey unix.Stat_t
	if unix.Fstat(fd, &before) != nil || !privateOperatorRoot(before) {
		return nil, key, ErrDenied
	}
	if unix.Fstatat(fd, "manifest.json", &originalManifest, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateOperatorFile(originalManifest, int64(maxOperatorManifestBytes), 0) || unix.Fstatat(fd, "source.key", &originalKey, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateOperatorFile(originalKey, 32, 32) {
		return nil, key, ErrDenied
	}
	defer func() {
		if resultErr != nil {
			clear(key[:])
			clear(manifest)
			manifest = nil
		}
	}()
	manifest, err = readPrivateOperatorFile(ctx, fd, "manifest.json", int64(maxOperatorManifestBytes), 0)
	if err != nil {
		return nil, key, ErrDenied
	}
	secret, err := readPrivateOperatorFile(ctx, fd, "source.key", 32, 32)
	if err != nil {
		return nil, key, ErrDenied
	}
	defer clear(secret)
	copy(key[:], secret)
	if ctx.Err() != nil || unix.Fstat(fd, &after) != nil || !stableOperatorStat(before, after) || !privateOperatorRoot(after) || unix.Fstatat(fd, "manifest.json", &currentManifest, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(originalManifest, currentManifest) || unix.Fstatat(fd, "source.key", &currentKey, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(originalKey, currentKey) {
		return nil, key, ErrDenied
	}
	// Confirm that the named canonical root still denotes this exact retained
	// directory, rather than a replacement installed during the two reads.
	check, err := openCanonicalDir(root)
	if err != nil {
		return nil, key, ErrDenied
	}
	defer unix.Close(check)
	if unix.Fstat(check, &after) != nil || !stableOperatorStat(before, after) {
		return nil, key, ErrDenied
	}
	return manifest, key, nil
}

func inspectExecutable(ctx context.Context, path string) (string, error) {
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.IndexByte(path, 0) >= 0 {
		return "", ErrDenied
	}
	parent, err := openCanonicalDir(filepath.Dir(path))
	if err != nil {
		return "", ErrDenied
	}
	defer unix.Close(parent)
	name := filepath.Base(path)
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", ErrDenied
	}
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		_ = unix.Close(fd)
		return "", ErrDenied
	}
	defer f.Close()
	var before, after, entry unix.Stat_t
	if unix.Fstat(fd, &before) != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Uid != uint32(os.Geteuid()) || before.Mode&0o022 != 0 || before.Mode&0o7000 != 0 || before.Mode&0o100 == 0 || before.Size <= 0 || before.Size > 256<<20 || unix.Fstatat(parent, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(before, entry) {
		return "", ErrDenied
	}
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	var total int64
	for {
		if ctx.Err() != nil {
			return "", ErrDenied
		}
		n, readErr := f.Read(buffer)
		total += int64(n)
		if total > 256<<20 {
			return "", ErrDenied
		}
		_, _ = hash.Write(buffer[:n])
		if readErr == io.EOF {
			break
		}
		if readErr != nil || n == 0 {
			return "", ErrDenied
		}
	}
	if ctx.Err() != nil || total != before.Size || unix.Fstat(fd, &after) != nil || !stableOperatorStat(before, after) || unix.Fstatat(parent, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(before, entry) {
		return "", ErrDenied
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
