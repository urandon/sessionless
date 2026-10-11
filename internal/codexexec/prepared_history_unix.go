//go:build darwin || linux

package codexexec

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// preparedHistory owns exactly one fresh directory, one child and one file.
// Cleanup uses held directory FDs and never recursively removes caller paths.
type preparedHistory struct {
	root, work, context                            int
	name, path                                     string
	workIdentity, contextIdentity, historyIdentity *unix.Stat_t
}

func stagePreparedHistory(root string, body []byte) (*preparedHistory, error) {
	if len(body) == 0 || len(body) > maxInstructionBytes || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, ErrContract
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return nil, ErrContract
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrContract
	}
	var stat unix.Stat_t
	if unix.Fstat(rootFD, &stat) != nil || stat.Mode&0o777 != 0o700 || stat.Uid != uint32(os.Geteuid()) {
		_ = unix.Close(rootFD)
		return nil, ErrContract
	}
	stage := &preparedHistory{root: rootFD, work: -1, context: -1, name: "codex-history-" + rand.Text()}
	stage.path = filepath.Join(root, stage.name)
	if unix.Mkdirat(rootFD, stage.name, 0o700) != nil {
		_ = unix.Close(rootFD)
		return nil, ErrContract
	}
	failed := func() (*preparedHistory, error) { return nil, errors.Join(ErrContract, stage.remove()) }
	stage.work, err = unix.Openat(rootFD, stage.name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return failed()
	}
	stage.workIdentity = &unix.Stat_t{}
	if unix.Fstat(stage.work, stage.workIdentity) != nil {
		return failed()
	}
	if unix.Mkdirat(stage.work, "context", 0o700) != nil {
		return failed()
	}
	stage.context, err = unix.Openat(stage.work, "context", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return failed()
	}
	stage.contextIdentity = &unix.Stat_t{}
	if unix.Fstat(stage.context, stage.contextIdentity) != nil {
		return failed()
	}
	fd, err := unix.Openat(stage.context, "history.jsonl", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return failed()
	}
	stage.historyIdentity = &unix.Stat_t{}
	if unix.Fstat(fd, stage.historyIdentity) != nil {
		_ = unix.Close(fd)
		return failed()
	}
	file := os.NewFile(uintptr(fd), "history.jsonl")
	n, writeErr := file.Write(body)
	closeErr := file.Close()
	if n != len(body) || writeErr != nil || closeErr != nil {
		return failed()
	}
	return stage, nil
}

func (stage *preparedHistory) remove() error {
	if stage == nil || stage.root < 0 {
		return nil
	}
	var result error
	remove := func(fd int, name string, flags int, original *unix.Stat_t) {
		var current unix.Stat_t
		if original == nil || unix.Fstatat(fd, name, &current, unix.AT_SYMLINK_NOFOLLOW) != nil ||
			current.Dev != original.Dev || current.Ino != original.Ino || current.Mode&unix.S_IFMT != original.Mode&unix.S_IFMT {
			result = errors.Join(result, ErrContract)
			return
		}
		if err := unix.Unlinkat(fd, name, flags); err != nil {
			result = errors.Join(result, ErrContract)
		}
	}
	if stage.context >= 0 {
		remove(stage.context, "history.jsonl", 0, stage.historyIdentity)
		_ = unix.Close(stage.context)
		stage.context = -1
	}
	if stage.work >= 0 {
		remove(stage.work, "context", unix.AT_REMOVEDIR, stage.contextIdentity)
		_ = unix.Close(stage.work)
		stage.work = -1
	}
	remove(stage.root, stage.name, unix.AT_REMOVEDIR, stage.workIdentity)
	_ = unix.Close(stage.root)
	stage.root = -1
	stage.path = ""
	return result
}
