package localoperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
	"strings"
)

// RuntimeIdentity identifies observed local process placement. It is not a
// cloud/cgroup/container attestation or evidence of model qualification.
type RuntimeIdentity struct {
	WorkerExecutableDigest              string
	LocalHostIdentityDigest             string
	LocalNetworkNamespaceIdentityDigest string
}

// InspectRuntime reads one explicitly named installed worker executable and
// kernel/host identity only. It starts no process and performs no network I/O.
// No ambient executable discovery, endpoint probe or model inspection occurs.
func InspectRuntime(ctx context.Context, executablePath string) (RuntimeIdentity, error) {
	if ctx == nil || ctx.Err() != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	executable, err := inspectExecutable(ctx, executablePath)
	if err != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" || len(hostname) > 255 || ctx.Err() != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	boot, err := inspectBootIdentity(ctx)
	if err != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	host, err := localHostIdentity(hostname, os.Geteuid(), runtime.GOOS, boot)
	if err != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	network, err := inspectNetworkIdentity(ctx, host)
	if err != nil || ctx.Err() != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	return RuntimeIdentity{WorkerExecutableDigest: executable, LocalHostIdentityDigest: host, LocalNetworkNamespaceIdentityDigest: network}, nil
}

// Identity is intentionally boot-scoped. A same-boot process restart can resume
// its installation; reboot or moving the files to another host must reconcile
// and requalify it, not silently reuse the old host/namespace authorization.
func localHostIdentity(hostname string, uid int, platform, boot string) (string, error) {
	if hostname == "" || len(hostname) > 255 || uid < 0 || (platform != "darwin" && platform != "linux") || len(boot) != 36 {
		return "", ErrDenied
	}
	for _, index := range []int{8, 13, 18, 23} {
		if boot[index] != '-' {
			return "", ErrDenied
		}
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(boot, "-", ""))
	if err != nil || len(raw) != 16 {
		return "", ErrDenied
	}
	var nonzero byte
	for _, b := range raw {
		nonzero |= b
	}
	if nonzero == 0 {
		return "", ErrDenied
	}
	return runtimeIdentityDigest("sessionless.local-host-boot.v1", struct {
		Hostname     string
		UID          int
		Platform     string
		BootIdentity string
	}{hostname, uid, platform, strings.ToLower(boot)}), nil
}

func runtimeIdentityDigest(kind string, facts any) string {
	raw, _ := json.Marshal(struct {
		Kind  string
		Facts any
	}{kind, facts})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
