package localoperator

import (
	"context"
	"golang.org/x/sys/unix"
)

func inspectNetworkIdentity(ctx context.Context, host string) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", ErrDenied
	}
	// /proc/self/ns/net is the kernel namespace handle, intentionally followed
	// here; unlike operator paths it is not a caller-selected file source.
	fd, err := unix.Open("/proc/self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", ErrDenied
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	var fs unix.Statfs_t
	if unix.Fstat(fd, &stat) != nil || unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.NSFS_MAGIC || ctx.Err() != nil {
		return "", ErrDenied
	}
	return runtimeIdentityDigest("sessionless.linux-netns.v1", struct {
		Host          string
		Device, Inode uint64
	}{host, uint64(stat.Dev), stat.Ino}), nil
}
