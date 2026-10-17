package localoperator

import "context"

func inspectNetworkIdentity(ctx context.Context, host string) (string, error) {
	if ctx == nil || ctx.Err() != nil {
		return "", ErrDenied
	}
	// Darwin's supported local profile uses the observed host network. This
	// closed identity explicitly does NOT claim a Linux-style isolated netns.
	return runtimeIdentityDigest("sessionless.darwin-host-network.v1", struct{ Host string }{host}), nil
}
