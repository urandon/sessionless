//go:build !darwin && !linux

package attachedworkerlocal

import (
	"io/fs"
	"os"
)

func platformSupported() bool { return false }

func openLockFile(string, bool) (*os.File, error) { return nil, ErrStateUnsupported }
func acquireFileLock(*os.File) error              { return ErrStateUnsupported }
func releaseFileLock(*os.File) error              { return ErrStateUnsupported }
func openReadNoFollow(string) (*os.File, error)   { return nil, ErrStateUnsupported }
func ownedByCurrentUser(fs.FileInfo) bool         { return false }
