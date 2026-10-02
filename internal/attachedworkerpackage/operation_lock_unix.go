//go:build darwin || linux

package attachedworkerpackage

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// operationLease serializes package mutations and native starts across the
// runtime-lock handoff to the child service. Always acquire it before the
// runtime lease; unlike the runtime lease it remains held during start.
type operationLease struct{ file *os.File }

func acquireOperationLease(installDir string) (*operationLease, error) {
	if !canonicalPath(installDir) {
		return nil, ErrInvalid
	}
	if err := ensurePrivateDir(installDir, true); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	path := filepath.Join(installDir, ".package-operations.lock")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, errors.Join(ErrIO, err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
		_ = file.Close()
		return nil, errors.Join(ErrConflict, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, errors.Join(ErrConflict, err)
	}
	return &operationLease{file: file}, nil
}

func (lease *operationLease) Close() error {
	if lease == nil || lease.file == nil {
		return nil
	}
	err := syscall.Flock(int(lease.file.Fd()), syscall.LOCK_UN)
	return errors.Join(err, lease.file.Close())
}
