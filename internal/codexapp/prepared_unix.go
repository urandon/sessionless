//go:build darwin || linux

package codexapp

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type preparedGuard struct {
	paths     PreparedPaths
	codexFD   int
	workFD    int
	codexStat unix.Stat_t
	workStat  unix.Stat_t
	authStat  unix.Stat_t
}

func openPreparedPaths(paths PreparedPaths, maxAuthBytes int64) (*preparedGuard, error) {
	if paths.AuthFile != filepath.Join(paths.CodexHome, "auth.json") ||
		validatePrivateDirectory(paths.CodexHome) != nil ||
		validatePrivateDirectory(paths.WorkDir) != nil ||
		!disjointRoots(paths.CodexHome, paths.WorkDir) {
		return nil, ErrProcessUnavailable
	}
	codexFD, err := unix.Open(paths.CodexHome, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	workFD, err := unix.Open(paths.WorkDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		_ = unix.Close(codexFD)
		return nil, err
	}
	guard := &preparedGuard{paths: paths, codexFD: codexFD, workFD: workFD}
	if unix.Fstat(codexFD, &guard.codexStat) != nil || unix.Fstat(workFD, &guard.workStat) != nil ||
		!privateDirStat(guard.codexStat) || !privateDirStat(guard.workStat) {
		guard.close()
		return nil, ErrProcessUnavailable
	}
	authFD, err := unix.Openat(codexFD, "auth.json", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		guard.close()
		return nil, err
	}
	err = unix.Fstat(authFD, &guard.authStat)
	_ = unix.Close(authFD)
	if err != nil || !privateAuthStat(guard.authStat, maxAuthBytes) {
		guard.close()
		return nil, ErrProcessUnavailable
	}
	if guard.recheck(true) != nil {
		guard.close()
		return nil, ErrProcessUnavailable
	}
	return guard, nil
}

func (guard *preparedGuard) recheck(includeAuth bool) error {
	var codexPath, workPath unix.Stat_t
	if unix.Fstatat(unix.AT_FDCWD, guard.paths.CodexHome, &codexPath, unix.AT_SYMLINK_NOFOLLOW) != nil ||
		unix.Fstatat(unix.AT_FDCWD, guard.paths.WorkDir, &workPath, unix.AT_SYMLINK_NOFOLLOW) != nil ||
		!sameFileStat(codexPath, guard.codexStat) || !sameFileStat(workPath, guard.workStat) ||
		!privateDirStat(codexPath) || !privateDirStat(workPath) {
		return ErrProcessUnavailable
	}
	if includeAuth {
		var authPath unix.Stat_t
		if unix.Fstatat(guard.codexFD, "auth.json", &authPath, unix.AT_SYMLINK_NOFOLLOW) != nil ||
			!sameFileStat(authPath, guard.authStat) || !privateAuthStat(authPath, guard.authStat.Size) {
			return ErrProcessUnavailable
		}
	}
	return nil
}

func (guard *preparedGuard) close() {
	if guard == nil {
		return
	}
	if guard.codexFD >= 0 {
		_ = unix.Close(guard.codexFD)
		guard.codexFD = -1
	}
	if guard.workFD >= 0 {
		_ = unix.Close(guard.workFD)
		guard.workFD = -1
	}
}

func privateDirStat(stat unix.Stat_t) bool {
	return stat.Mode&unix.S_IFMT == unix.S_IFDIR && stat.Mode&0o777 == 0o700 && stat.Uid == uint32(os.Geteuid())
}

func privateAuthStat(stat unix.Stat_t, maxBytes int64) bool {
	return stat.Mode&unix.S_IFMT == unix.S_IFREG && stat.Mode&0o777 == 0o600 &&
		stat.Uid == uint32(os.Geteuid()) && stat.Nlink == 1 && stat.Size > 0 && stat.Size <= maxBytes
}

func sameFileStat(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino && left.Uid == right.Uid &&
		left.Mode == right.Mode && left.Nlink == right.Nlink && left.Size == right.Size
}
