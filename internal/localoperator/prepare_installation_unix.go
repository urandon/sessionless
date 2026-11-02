//go:build darwin || linux

package localoperator

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// Directory timestamps/size legitimately change during creation. Keep the
// retained directory identity, ownership and permissions, not its old mtime.
func samePreparedDirectory(a, b unix.Stat_t) bool {
	return privateOperatorRoot(a) && privateOperatorRoot(b) && a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid
}

func checkPreparedDirectory(path string, fd int, original unix.Stat_t) error {
	var current, named unix.Stat_t
	if unix.Fstat(fd, &current) != nil || !samePreparedDirectory(original, current) {
		return ErrDenied
	}
	check, err := openCanonicalDir(path)
	if err != nil {
		return ErrDenied
	}
	statErr := unix.Fstat(check, &named)
	closeErr := unix.Close(check)
	if statErr != nil || closeErr != nil || !samePreparedDirectory(current, named) {
		return ErrDenied
	}
	return nil
}

func writePreparedInstallation(ctx context.Context, root string, manifest, key []byte) (result error) {
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" || strings.IndexByte(root, 0) >= 0 ||
		len(manifest) == 0 || len(manifest) > maxOperatorManifestBytes || len(key) != 32 {
		return ErrDenied
	}
	parentPath, name := filepath.Dir(root), filepath.Base(root)
	parentFD, err := openCanonicalDir(parentPath)
	if err != nil {
		return ErrDenied
	}
	parent := os.NewFile(uintptr(parentFD), parentPath)
	defer func() {
		if parent.Close() != nil {
			result = ErrDenied
		}
	}()
	var originalParent unix.Stat_t
	if unix.Fstat(parentFD, &originalParent) != nil || !privateOperatorRoot(originalParent) || checkPreparedDirectory(parentPath, parentFD, originalParent) != nil || ctx.Err() != nil {
		return ErrDenied
	}
	// mkdirat is the exclusive reservation. No cleanup is permitted from here,
	// even when the later open, writes, syncs or close fail.
	if unix.Mkdirat(parentFD, name, 0o700) != nil {
		return ErrDenied
	}
	var entry, originalRoot unix.Stat_t
	if unix.Fstatat(parentFD, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateOperatorRoot(entry) || parent.Sync() != nil {
		return ErrDenied
	}
	rootFD, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrDenied
	}
	directory := os.NewFile(uintptr(rootFD), root)
	defer func() {
		if directory.Close() != nil {
			result = ErrDenied
		}
	}()
	verify := func() error {
		var currentEntry unix.Stat_t
		if ctx.Err() != nil || checkPreparedDirectory(parentPath, parentFD, originalParent) != nil || checkPreparedDirectory(root, rootFD, originalRoot) != nil ||
			unix.Fstatat(parentFD, name, &currentEntry, unix.AT_SYMLINK_NOFOLLOW) != nil || !samePreparedDirectory(originalRoot, currentEntry) {
			return ErrDenied
		}
		return nil
	}
	if unix.Fstat(rootFD, &originalRoot) != nil || !stableOperatorStat(entry, originalRoot) || verify() != nil {
		return ErrDenied
	}
	outputs := []struct {
		name     string
		body     []byte
		identity unix.Stat_t
	}{{name: "manifest.json", body: manifest}, {name: "source.key", body: key}}
	for i := range outputs {
		output := &outputs[i]
		if verify() != nil || writePreparedFile(ctx, rootFD, output.name, output.body, &output.identity) != nil {
			return ErrDenied
		}
	}
	// Retain exact file entry identities through the final directory durability
	// and canonical-path checks; no path is reopened for writing.
	if directory.Sync() != nil || parent.Sync() != nil || verify() != nil {
		return ErrDenied
	}
	for _, output := range outputs {
		var after unix.Stat_t
		if unix.Fstatat(rootFD, output.name, &after, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(output.identity, after) {
			return ErrDenied
		}
	}
	return nil
}

func writePreparedFile(ctx context.Context, root int, name string, body []byte, identity *unix.Stat_t) (result error) {
	if ctx.Err() != nil {
		return ErrDenied
	}
	fd, err := unix.Openat(root, name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return ErrDenied
	}
	f := os.NewFile(uintptr(fd), name)
	defer func() {
		if f != nil && f.Close() != nil {
			result = ErrDenied
		}
	}()
	var original, current, entry unix.Stat_t
	if unix.Fstat(fd, &original) != nil || original.Mode&unix.S_IFMT != unix.S_IFREG || original.Mode&0o7777 != 0o600 || original.Uid != uint32(os.Geteuid()) || original.Nlink != 1 || original.Size != 0 ||
		unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(original, entry) || ctx.Err() != nil {
		return ErrDenied
	}
	n, err := f.Write(body)
	if err != nil || n != len(body) || ctx.Err() != nil || f.Sync() != nil || unix.Fstat(fd, &current) != nil ||
		current.Dev != original.Dev || current.Ino != original.Ino || !privateOperatorFile(current, int64(len(body)), int64(len(body))) ||
		unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(current, entry) {
		return ErrDenied
	}
	// Verify the bounded bytes through the same retained fd, including the key;
	// this temporary copy is never returned and is cleared on every exit.
	copyBody := make([]byte, len(body))
	defer clear(copyBody)
	n, err = f.ReadAt(copyBody, 0)
	if err != nil || n != len(body) || !bytes.Equal(body, copyBody) || ctx.Err() != nil ||
		unix.Fstat(fd, &entry) != nil || !stableOperatorStat(current, entry) {
		return ErrDenied
	}
	closeErr := f.Close()
	f = nil
	if closeErr != nil || ctx.Err() != nil || unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(current, entry) {
		return ErrDenied
	}
	*identity = current
	return nil
}
