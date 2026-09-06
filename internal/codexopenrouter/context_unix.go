//go:build darwin || linux

package codexopenrouter

import (
	"os"

	"golang.org/x/sys/unix"
)

// openContextHistory pins each path component and the final regular file to
// descriptors. This keeps an attacker with access to the work directory from
// swapping a checked path for a symlink before the provider prompt is read.
func openContextHistory(workDir string, maxBytes int64) (*os.File, error) {
	workFD, err := unix.Open(workDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrContract
	}
	defer unix.Close(workFD)

	contextFD, err := unix.Openat(workFD, "context", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrContract
	}
	defer unix.Close(contextFD)

	historyFD, err := unix.Openat(contextFD, "history.jsonl", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrContract
	}
	var stat unix.Stat_t
	if err := unix.Fstat(historyFD, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size <= 0 || stat.Size > maxBytes {
		_ = unix.Close(historyFD)
		return nil, ErrContract
	}
	file := os.NewFile(uintptr(historyFD), "history.jsonl")
	if file == nil {
		_ = unix.Close(historyFD)
		return nil, ErrContract
	}
	return file, nil
}
