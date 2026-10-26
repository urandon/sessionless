//go:build linux || darwin

package ownerlocalcredential

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type filesystem struct {
	root        string
	fd          int
	lock        int
	device      uint64
	inode       uint64
	lockDevice  uint64
	lockInode   uint64
	stateDevice uint64
	stateInode  uint64
	tempDevice  uint64
	tempInode   uint64
	fault       func(string) error // Private deterministic durability fault seam.
}

func canonicalDirectory(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.IndexByte(path, 0) >= 0 {
		return -1, ErrDenied
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, ErrUnavailable
	}
	for _, part := range strings.Split(path[1:], "/") {
		next, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(fd)
		if e != nil || closeErr != nil {
			if next >= 0 {
				_ = unix.Close(next)
			}
			return -1, ErrDenied
		}
		fd = next
	}
	return fd, nil
}

func privateRoot(v unix.Stat_t) bool {
	return v.Mode&unix.S_IFMT == unix.S_IFDIR && v.Mode&0o7777 == 0o700 && v.Uid == uint32(os.Geteuid())
}
func privateFile(v unix.Stat_t, limit int) bool {
	return v.Mode&unix.S_IFMT == unix.S_IFREG && v.Mode&0o7777 == 0o600 && v.Uid == uint32(os.Geteuid()) && v.Nlink == 1 && v.Size >= 0 && v.Size <= int64(limit)
}
func sameFile(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func openFilesystem(root string) (*filesystem, error) {
	fd, err := canonicalDirectory(root)
	if err != nil {
		return nil, err
	}
	f := &filesystem{root: root, fd: fd, lock: -1}
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || !privateRoot(stat) {
		_ = unix.Close(fd)
		return nil, ErrDenied
	}
	f.device, f.inode = uint64(stat.Dev), stat.Ino
	lock, e := unix.Openat(fd, lockName, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0o600)
	if e != nil {
		_ = unix.Close(fd)
		return nil, ErrDenied
	}
	f.lock = lock
	if unix.Fstat(lock, &stat) != nil || !privateFile(stat, 0) {
		_ = f.close()
		return nil, ErrDenied
	}
	f.lockDevice, f.lockInode = uint64(stat.Dev), stat.Ino
	if e := unix.Flock(lock, unix.LOCK_EX|unix.LOCK_NB); e != nil {
		_ = f.close()
		if errors.Is(e, unix.EWOULDBLOCK) || errors.Is(e, unix.EAGAIN) {
			return nil, ErrBusy
		}
		return nil, ErrDenied
	}
	if f.check() != nil {
		_ = f.close()
		return nil, ErrDenied
	}
	return f, nil
}

func (f *filesystem) check() error {
	var held, named unix.Stat_t
	if f == nil || f.fd < 0 || f.lock < 0 || unix.Fstat(f.fd, &held) != nil || !privateRoot(held) || uint64(held.Dev) != f.device || held.Ino != f.inode {
		return ErrDenied
	}
	other, err := canonicalDirectory(f.root)
	if err != nil {
		return err
	}
	err = unix.Fstat(other, &named)
	closeErr := unix.Close(other)
	if err != nil || closeErr != nil || named.Dev != held.Dev || named.Ino != held.Ino || !privateRoot(named) {
		return ErrDenied
	}
	if unix.Fstat(f.lock, &held) != nil || unix.Fstatat(f.fd, lockName, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateFile(held, 0) || !sameFile(held, named) || uint64(held.Dev) != f.lockDevice || held.Ino != f.lockInode {
		return ErrDenied
	}
	return nil
}

func (f *filesystem) read(ctx context.Context, name string, limit int) ([]byte, bool, error) {
	if ctx == nil || ctx.Err() != nil || name != filepath.Base(name) || f.check() != nil {
		return nil, false, ErrDenied
	}
	fd, err := unix.Openat(f.fd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, ErrDenied
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var before, after, named unix.Stat_t
	if unix.Fstat(fd, &before) != nil || !privateFile(before, limit) || before.Size == 0 || unix.Fstatat(f.fd, name, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || !sameFile(before, named) {
		return nil, false, ErrDenied
	}
	if name == metadataName && f.stateInode != 0 && (uint64(before.Dev) != f.stateDevice || before.Ino != f.stateInode) {
		return nil, false, ErrDenied
	}
	raw := make([]byte, int(before.Size))
	valid := false
	defer func() {
		if !valid {
			clear(raw)
		}
	}()
	for off := 0; off < len(raw); {
		if ctx.Err() != nil {
			return nil, false, ErrDenied
		}
		n, e := file.Read(raw[off:])
		off += n
		if e != nil || n == 0 {
			return nil, false, ErrUnavailable
		}
	}
	var sentinel [1]byte
	if n, e := file.Read(sentinel[:]); n != 0 || e != io.EOF || unix.Fstat(fd, &after) != nil || unix.Fstatat(f.fd, name, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || !sameFile(before, after) || !sameFile(before, named) || ctx.Err() != nil {
		return nil, false, ErrDenied
	}
	valid = true
	if name == metadataName {
		f.stateDevice, f.stateInode = uint64(before.Dev), before.Ino
	}
	if name == metadataTemporary {
		f.tempDevice, f.tempInode = uint64(before.Dev), before.Ino
	}
	return raw, true, nil
}

func (f *filesystem) point(name string) error {
	if f.fault != nil && f.fault(name) != nil {
		return ErrUnavailable
	}
	return nil
}
func (f *filesystem) syncDirectory() error {
	if f.point("directory-sync") != nil || unix.Fsync(f.fd) != nil {
		return ErrUnknown
	}
	return nil
}

func (f *filesystem) create(ctx context.Context, name string, raw []byte) error {
	if ctx == nil || ctx.Err() != nil || f.check() != nil || name != filepath.Base(name) || len(raw) == 0 || len(raw) > maxEnvelopeBytes {
		return ErrDenied
	}
	if f.space(uint64(len(raw))) != nil {
		return ErrUnavailable
	}
	fd, err := unix.Openat(f.fd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return ErrDenied
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || !privateFile(stat, maxEnvelopeBytes) {
		return ErrDenied
	}
	if name == metadataTemporary {
		f.tempDevice, f.tempInode = uint64(stat.Dev), stat.Ino
	}
	for off := 0; off < len(raw); {
		if ctx.Err() != nil {
			return ErrUnavailable
		}
		n, e := file.Write(raw[off:])
		off += n
		if e != nil || n == 0 {
			return ErrUnavailable
		}
	}
	if f.point("file-sync") != nil || file.Sync() != nil || file.Close() != nil {
		return ErrUnknown
	}
	return f.syncDirectory()
}

func (f *filesystem) writeAtomic(ctx context.Context, raw []byte) error {
	if len(raw) > maxMetadataBytes || f.check() != nil {
		return ErrDenied
	}
	// A previous owned temp has no authority. Reject it rather than guessing
	// whether an interrupted snapshot ought to become current.
	f.tempDevice, f.tempInode = 0, 0
	if err := f.create(ctx, metadataTemporary, raw); err != nil {
		if f.tempInode == 0 {
			return err
		}
		if f.remove(metadataTemporary) != nil {
			return ErrUnknown
		}
		return ErrUnavailable
	}
	if f.point("before-rename") != nil {
		if f.remove(metadataTemporary) != nil {
			return ErrUnknown
		}
		return ErrUnavailable
	}
	var temporary unix.Stat_t
	if unix.Fstatat(f.fd, metadataTemporary, &temporary, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateFile(temporary, maxMetadataBytes) || uint64(temporary.Dev) != f.tempDevice || temporary.Ino != f.tempInode {
		return ErrUnknown
	}
	if unix.Renameat(f.fd, metadataTemporary, f.fd, metadataName) != nil {
		if f.remove(metadataTemporary) != nil {
			return ErrUnknown
		}
		return ErrUnavailable
	}
	f.stateDevice, f.stateInode = f.tempDevice, f.tempInode
	f.tempDevice, f.tempInode = 0, 0
	if f.point("after-rename") != nil {
		return ErrUnknown
	}
	return f.syncDirectory()
}

func (f *filesystem) remove(name string) error {
	if f.check() != nil || name != filepath.Base(name) {
		return ErrDenied
	}
	var stat unix.Stat_t
	if err := unix.Fstatat(f.fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return f.syncDirectory()
		}
		return ErrDenied
	}
	if !privateFile(stat, maxEnvelopeBytes) {
		return ErrDenied
	}
	if name == metadataTemporary && (f.tempInode == 0 || uint64(stat.Dev) != f.tempDevice || stat.Ino != f.tempInode) {
		return ErrDenied
	}
	if unix.Unlinkat(f.fd, name, 0) != nil {
		return ErrUnavailable
	}
	return f.syncDirectory()
}

func (f *filesystem) entries() (map[string]int64, error) {
	if f.check() != nil {
		return nil, ErrDenied
	}
	fd, err := unix.Openat(f.fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrDenied
	}
	dir := os.NewFile(uintptr(fd), "owner-local-inventory")
	defer dir.Close()
	names, err := dir.Readdirnames(15)
	if err != nil && err != io.EOF || len(names) > 14 {
		return nil, ErrDenied
	}
	result := make(map[string]int64, len(names))
	var total int64
	for _, name := range names {
		var stat unix.Stat_t
		if unix.Fstatat(f.fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW) != nil || !privateFile(stat, maxEnvelopeBytes) {
			return nil, ErrDenied
		}
		total += stat.Size
		if total > maxDiskBytes {
			return nil, ErrDenied
		}
		result[name] = stat.Size
	}
	return result, nil
}

func (f *filesystem) onlyLock() error {
	entries, err := f.entries()
	if err != nil || len(entries) != 1 || entries[lockName] != 0 {
		return ErrDenied
	}
	return nil
}

func (f *filesystem) space(extra uint64) error {
	entries, err := f.entries()
	if err != nil {
		return err
	}
	used := uint64(0)
	for _, size := range entries {
		used += uint64(size)
	}
	if extra > maxDiskBytes || used > maxDiskBytes-extra {
		return ErrDenied
	}
	var stat unix.Statfs_t
	if unix.Fstatfs(f.fd, &stat) != nil || stat.Bsize <= 0 || uint64(stat.Bavail) < (extra+uint64(stat.Bsize)-1)/uint64(stat.Bsize) {
		return ErrUnavailable
	}
	return nil
}

func (f *filesystem) close() error {
	var result error
	if f.lock >= 0 {
		result = errors.Join(result, unix.Flock(f.lock, unix.LOCK_UN), unix.Close(f.lock))
		f.lock = -1
	}
	if f.fd >= 0 {
		result = errors.Join(result, unix.Close(f.fd))
		f.fd = -1
	}
	if result != nil {
		return ErrUnavailable
	}
	return nil
}
