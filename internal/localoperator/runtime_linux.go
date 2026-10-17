package localoperator

import (
	"context"
	"golang.org/x/sys/unix"
	"io"
	"os"
)

func inspectBootIdentity(ctx context.Context) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", ErrDenied
	}
	fd, err := unix.Open("/proc/sys/kernel/random/boot_id", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", ErrDenied
	}
	f := os.NewFile(uintptr(fd), "kernel-boot-id")
	if f == nil {
		_ = unix.Close(fd)
		return "", ErrDenied
	}
	defer f.Close()
	var fs unix.Statfs_t
	if unix.Fstatfs(fd, &fs) != nil || fs.Type != unix.PROC_SUPER_MAGIC {
		return "", ErrDenied
	}
	// The kernel UUID is 36 bytes and a newline. Read one extra byte to reject
	// truncation/extra data; proc reports size zero, so do not use st_size.
	raw, err := io.ReadAll(io.LimitReader(f, 38))
	if err != nil || ctx.Err() != nil || len(raw) != 37 || raw[36] != '\n' {
		return "", ErrDenied
	}
	return string(raw[:36]), nil
}

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
