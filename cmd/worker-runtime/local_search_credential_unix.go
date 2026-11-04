//go:build darwin || linux

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// This command-private owner separates acquisition and consumption so named
// replacement can be checked without a test hook or another key lifecycle.
type localSearchKeyFile struct {
	file       *os.File
	parent     int
	parentPath string
	name       string
	identity   unix.Stat_t
	directory  unix.Stat_t
}

func openLocalSearchDirectory(path string) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.IndexByte(path, 0) >= 0 {
		return -1, errWorkerLocalExecution
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, errWorkerLocalExecution
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
			return -1, errWorkerLocalExecution
		}
		fd = next
	}
	return fd, nil
}

func sameLocalSearchFile(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func validLocalSearchKeyStat(s unix.Stat_t) bool {
	return s.Mode&unix.S_IFMT == unix.S_IFREG && s.Mode&0o7777 == 0o600 && s.Uid == uint32(os.Geteuid()) && s.Nlink == 1 && s.Size == 32
}

func openLocalSearchKeyFile(ctx context.Context, path string) (*localSearchKeyFile, error) {
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.IndexByte(path, 0) >= 0 {
		return nil, errWorkerLocalExecution
	}
	parentPath, name := filepath.Dir(path), filepath.Base(path)
	parent, err := openLocalSearchDirectory(parentPath)
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		_ = unix.Close(parent)
		return nil, errWorkerLocalExecution
	}
	file := os.NewFile(uintptr(fd), "[private local search key]")
	if file == nil {
		_ = unix.Close(fd)
		_ = unix.Close(parent)
		return nil, errWorkerLocalExecution
	}
	owner := &localSearchKeyFile{file: file, parent: parent, parentPath: parentPath, name: name}
	if unix.Fstat(fd, &owner.identity) != nil || unix.Fstat(parent, &owner.directory) != nil || owner.verify(ctx) != nil {
		_ = owner.close()
		return nil, errWorkerLocalExecution
	}
	return owner, nil
}

func (f *localSearchKeyFile) verify(ctx context.Context) error {
	if f == nil || f.file == nil || f.parent < 0 || ctx == nil || ctx.Err() != nil {
		return errWorkerLocalExecution
	}
	var opened, named unix.Stat_t
	if unix.Fstat(int(f.file.Fd()), &opened) != nil || !validLocalSearchKeyStat(opened) || !sameLocalSearchFile(f.identity, opened) || unix.Fstatat(f.parent, f.name, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || !sameLocalSearchFile(opened, named) {
		return errWorkerLocalExecution
	}
	// Re-resolve the canonical parent without following any symlink. A pinned
	// old directory descriptor cannot authorize a different current pathname.
	current, err := openLocalSearchDirectory(f.parentPath)
	if err != nil {
		return errWorkerLocalExecution
	}
	var directory unix.Stat_t
	statErr, closeErr := unix.Fstat(current, &directory), unix.Close(current)
	if statErr != nil || closeErr != nil || directory.Dev != f.directory.Dev || directory.Ino != f.directory.Ino || directory.Mode != f.directory.Mode || directory.Uid != f.directory.Uid || directory.Gid != f.directory.Gid || ctx.Err() != nil {
		return errWorkerLocalExecution
	}
	return nil
}

func (f *localSearchKeyFile) read(ctx context.Context) ([]byte, error) {
	if f.verify(ctx) != nil {
		return nil, errWorkerLocalExecution
	}
	var raw [33]byte
	defer clear(raw[:])
	n, err := io.ReadFull(f.file, raw[:])
	if n != 32 || err != io.ErrUnexpectedEOF || f.verify(ctx) != nil {
		return nil, errWorkerLocalExecution
	}
	var nonzero byte
	for _, b := range raw[:32] {
		nonzero |= b
	}
	if nonzero == 0 {
		return nil, errWorkerLocalExecution
	}
	return append([]byte(nil), raw[:32]...), nil
}

func (f *localSearchKeyFile) close() error {
	if f == nil {
		return nil
	}
	var fileErr, parentErr error
	if f.file != nil {
		fileErr = f.file.Close()
		f.file = nil
	}
	if f.parent >= 0 {
		parentErr = unix.Close(f.parent)
		f.parent = -1
	}
	if fileErr != nil || parentErr != nil {
		return errWorkerLocalExecution
	}
	return nil
}

func readLocalSearchKey(ctx context.Context, path string) (key []byte, err error) {
	f, err := openLocalSearchKeyFile(ctx, path)
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	defer func() {
		if f.close() != nil {
			clear(key)
			key, err = nil, errWorkerLocalExecution
		}
	}()
	return f.read(ctx)
}
