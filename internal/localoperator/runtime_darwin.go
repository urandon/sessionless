package localoperator

import (
	"context"
	"golang.org/x/sys/unix"
)

func inspectBootIdentity(ctx context.Context) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", ErrDenied
	}
	// XNU's read-only boot-session UUID, not a hostname or disk-volume UUID.
	// An unavailable/restricted kernel interface denies; there is no fallback.
	value, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil || ctx.Err() != nil || len(value) != 36 {
		return "", ErrDenied
	}
	return value, nil
}

func inspectNetworkIdentity(ctx context.Context, host string) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", ErrDenied
	}
	// Darwin's supported local profile uses the observed host network. This
	// closed identity explicitly does NOT claim a Linux-style isolated netns.
	return runtimeIdentityDigest("sessionless.darwin-host-network.v1", struct{ Host string }{host}), nil
}
