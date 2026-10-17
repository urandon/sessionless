package localoperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
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
	host := runtimeIdentityDigest("sessionless.local-host.v1", struct {
		Hostname string
		UID      int
		Platform string
	}{hostname, os.Geteuid(), runtime.GOOS})
	network, err := inspectNetworkIdentity(ctx, host)
	if err != nil || ctx.Err() != nil {
		return RuntimeIdentity{}, ErrDenied
	}
	return RuntimeIdentity{WorkerExecutableDigest: executable, LocalHostIdentityDigest: host, LocalNetworkNamespaceIdentityDigest: network}, nil
}

func runtimeIdentityDigest(kind string, facts any) string {
	raw, _ := json.Marshal(struct {
		Kind  string
		Facts any
	}{kind, facts})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
