//go:build darwin || linux

package attachedworkerlocal

import (
	"errors"
	"io/fs"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func platformSupported() bool { return true }

func openLockFile(path string, create bool) (*os.File, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if create {
		flags |= unix.O_CREAT
	}
	fd, err := unix.Open(path, flags, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrStateMissing
		}
		if errors.Is(err, unix.ELOOP) {
			return nil, ErrInvalidState
		}
		return nil, ErrLocalIO
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, ErrLocalIO
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
		_ = file.Close()
		return nil, ErrInvalidState
	}
	return file, nil
}

func acquireFileLock(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return ErrStateBusy
		}
		return ErrLocalIO
	}
	return nil
}

func releaseFileLock(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil || closeErr != nil {
		return ErrLocalIO
	}
	return nil
}

func openReadNoFollow(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrStateMissing
		}
		if errors.Is(err, unix.ELOOP) {
			return nil, ErrInvalidState
		}
		return nil, ErrLocalIO
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, ErrLocalIO
	}
	return file, nil
}

func ownedByCurrentUser(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
