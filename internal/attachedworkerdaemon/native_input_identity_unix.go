//go:build darwin || linux

package attachedworkerdaemon

import (
	"os"
	"syscall"
)

func nativeInputIdentityV3(info os.FileInfo) (uint64, uint64, bool) {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0400 {
		return 0, 0, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return 0, 0, false
	}
	return uint64(stat.Dev), uint64(stat.Ino), true
}
