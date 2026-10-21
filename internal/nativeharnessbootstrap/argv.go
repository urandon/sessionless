// Package nativeharnessbootstrap lowers the existing native install envelope
// into one bounded helper invocation. It grants no Session or provider authority.
package nativeharnessbootstrap

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeconnect"
	"gitcode.com/urandon/sessionless/internal/nativeharnessrelay"
)

var (
	ErrConfig    = errors.New("invalid native helper invocation")
	ErrLifecycle = errors.New("native helper lifecycle failed")
	ErrNotJoined = errors.New("native helper children have not joined")
)

const deadlineName = "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS"

type program struct {
	path   string
	digest attachedworkerdaemon.ExecutableDigest
	args   []string
}

type relayPlan struct {
	socket  string
	limits  nativeharnessrelay.Limits
	cleanup time.Duration
}

type bootstrapPlan struct {
	relay, native program
	network       relayPlan
}

// Arguments derives both complete argv vectors, never templates or a second
// editable manifest. The profile builder seals these existing fields before
// InstallPolicyDigestV2; the runtime launcher must ValidateArguments again.
func Arguments(workload attachedworkerdaemon.LaunchSpec, bundle attachedworkerdaemon.NativeLaunchV2) ([]string, []string, error) {
	l := bundle.Limits
	if bundle.Version != 2 || nativeconnect.ValidateHosts(bundle.ProviderHosts) != nil ||
		!pathValid(bundle.SocketDestination) || len(bundle.SocketDestination) > 100 || l.Connections == 0 || l.Connections > 8 ||
		l.InboundBytes == 0 || l.InboundBytes > 64<<20 || l.OutboundBytes == 0 || l.OutboundBytes > 64<<20 ||
		l.Active <= 0 || l.Active > time.Minute || l.Cleanup <= 0 || l.Cleanup > 30*time.Second ||
		l.CompanionMemoryBytes == 0 || l.CompanionGoroutines == 0 || !argsValid(workload.Arguments) || !argsValid(bundle.CompanionArguments) {
		return nil, nil, ErrConfig
	}
	for _, a := range []attachedworkerdaemon.NativeArtifactV2{bundle.Bootstrap, bundle.Relay, bundle.Companion, {Executable: workload.Executable, Digest: workload.ExecutableDigest}} {
		if !pathValid(a.Executable) || a.Digest == (attachedworkerdaemon.ExecutableDigest{}) {
			return nil, nil, ErrConfig
		}
	}
	for _, b := range []byte(bundle.SocketDestination) {
		if b <= ' ' || b > '~' {
			return nil, nil, ErrConfig
		}
	}
	relay := []string{"relay", "--socket", bundle.SocketDestination, "--connections", strconv.FormatUint(l.Connections, 10),
		"--to-unix-bytes", strconv.FormatUint(l.OutboundBytes, 10), "--from-unix-bytes", strconv.FormatUint(l.InboundBytes, 10),
		"--active-ns", strconv.FormatInt(int64(l.Active), 10), "--cleanup-ns", strconv.FormatInt(int64(l.Cleanup), 10), "--ready-fd", "3"}
	boot := []string{"bootstrap", "--active-ns", strconv.FormatInt(int64(l.Active), 10), "--cleanup-ns", strconv.FormatInt(int64(l.Cleanup), 10),
		"--relay", bundle.Relay.Executable, "--relay-sha256", hex.EncodeToString(bundle.Relay.Digest[:]), "--relay-argc", strconv.Itoa(len(relay))}
	boot = append(boot, relay...)
	boot = append(boot, "--native", workload.Executable, "--native-sha256", hex.EncodeToString(workload.ExecutableDigest[:]), "--native-argc", strconv.Itoa(len(workload.Arguments)))
	boot = append(boot, workload.Arguments...)
	if !argsValid(boot) || !argsValid(relay) {
		return nil, nil, ErrConfig
	}
	return boot, relay, nil
}

func ValidateArguments(workload attachedworkerdaemon.LaunchSpec, bundle attachedworkerdaemon.NativeLaunchV2) error {
	boot, relay, err := Arguments(workload, bundle)
	if err != nil || !slices.Equal(boot, bundle.BootstrapArguments) || !slices.Equal(relay, bundle.RelayArguments) {
		return ErrConfig
	}
	return nil
}

func pathValid(path string) bool {
	return filepath.IsAbs(path) && path != "/" && filepath.Clean(path) == path && !strings.ContainsRune(path, 0)
}
func argsValid(args []string) bool {
	if len(args) == 0 || len(args) > 128 {
		return false
	}
	for _, arg := range args {
		if len(arg) > 4096 || strings.ContainsRune(arg, 0) {
			return false
		}
	}
	return true
}

type argvReader struct {
	args  []string
	index int
}

func (r *argvReader) value(flag string) (string, error) {
	if r.index+1 >= len(r.args) || r.args[r.index] != flag {
		return "", ErrConfig
	}
	v := r.args[r.index+1]
	r.index += 2
	return v, nil
}
func (r *argvReader) number(flag string) (uint64, error) {
	v, e := r.value(flag)
	if e != nil {
		return 0, e
	}
	n, e := strconv.ParseUint(v, 10, 64)
	if e != nil || strconv.FormatUint(n, 10) != v {
		return 0, ErrConfig
	}
	return n, nil
}
func (r *argvReader) program(kind string) (program, error) {
	var p program
	var err error
	p.path, err = r.value("--" + kind)
	if err != nil || !pathValid(p.path) {
		return p, ErrConfig
	}
	digest, err := r.value("--" + kind + "-sha256")
	if err != nil {
		return p, ErrConfig
	}
	raw, err := hex.DecodeString(digest)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != digest {
		return p, ErrConfig
	}
	copy(p.digest[:], raw)
	if p.digest == (attachedworkerdaemon.ExecutableDigest{}) {
		return p, ErrConfig
	}
	count, err := r.number("--" + kind + "-argc")
	if err != nil || count == 0 || count > 128 || count > uint64(len(r.args)-r.index) {
		return p, ErrConfig
	}
	p.args = slices.Clone(r.args[r.index : r.index+int(count)])
	r.index += int(count)
	return p, nil
}

func parseRelay(args []string) (relayPlan, error) {
	var p relayPlan
	if !argsValid(args) || args[0] != "relay" {
		return p, ErrConfig
	}
	r := argvReader{args: args, index: 1}
	var err error
	p.socket, err = r.value("--socket")
	if err != nil || !pathValid(p.socket) || len(p.socket) > 100 {
		return p, ErrConfig
	}
	for _, b := range []byte(p.socket) {
		if b <= ' ' || b > '~' {
			return p, ErrConfig
		}
	}
	connections, err := r.number("--connections")
	if err != nil || connections == 0 || connections > 8 {
		return p, ErrConfig
	}
	p.limits.Connections = uint32(connections)
	p.limits.BytesToUnix, err = r.number("--to-unix-bytes")
	if err != nil || p.limits.BytesToUnix == 0 || p.limits.BytesToUnix > 64<<20 {
		return p, ErrConfig
	}
	p.limits.BytesFromUnix, err = r.number("--from-unix-bytes")
	if err != nil || p.limits.BytesFromUnix == 0 || p.limits.BytesFromUnix > 64<<20 {
		return p, ErrConfig
	}
	active, err := r.number("--active-ns")
	if err != nil || active == 0 || active > uint64(time.Minute) {
		return p, ErrConfig
	}
	p.limits.Active = time.Duration(active)
	cleanup, err := r.number("--cleanup-ns")
	if err != nil || cleanup == 0 || cleanup > uint64(30*time.Second) {
		return p, ErrConfig
	}
	p.cleanup = time.Duration(cleanup)
	fd, err := r.value("--ready-fd")
	if err != nil || fd != "3" || r.index != len(args) {
		return p, ErrConfig
	}
	return p, nil
}

func parseBootstrap(args []string) (bootstrapPlan, error) {
	var p bootstrapPlan
	if !argsValid(args) || args[0] != "bootstrap" {
		return p, ErrConfig
	}
	r := argvReader{args: args, index: 1}
	active, err := r.number("--active-ns")
	if err != nil || active == 0 || active > uint64(time.Minute) {
		return p, ErrConfig
	}
	cleanup, err := r.number("--cleanup-ns")
	if err != nil || cleanup == 0 || cleanup > uint64(30*time.Second) {
		return p, ErrConfig
	}
	p.relay, err = r.program("relay")
	if err != nil {
		return p, err
	}
	p.native, err = r.program("native")
	if err != nil || r.index != len(args) {
		return p, ErrConfig
	}
	p.network, err = parseRelay(p.relay.args)
	if err != nil || p.network.limits.Active != time.Duration(active) || p.network.cleanup != time.Duration(cleanup) {
		return p, ErrConfig
	}
	return p, nil
}

var nativeNames = []string{"HOME", "TMPDIR", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "PATH", "LANG", "LC_ALL", "NO_COLOR", "CODEX_HOME"}

func environment(values []string) ([]string, string, error) {
	if len(values) > 64 {
		return nil, "", ErrConfig
	}
	known := make(map[string]string, len(nativeNames)+1)
	for _, value := range values {
		if len(value) > 4096 || strings.ContainsRune(value, 0) {
			return nil, "", ErrConfig
		}
		name, v, ok := strings.Cut(value, "=")
		if !ok {
			return nil, "", ErrConfig
		}
		if name != deadlineName && !slices.Contains(nativeNames, name) {
			continue
		}
		if _, exists := known[name]; exists {
			return nil, "", ErrConfig
		}
		known[name] = v
	}
	deadline, ok := known[deadlineName]
	if !ok {
		return nil, "", ErrConfig
	}
	base := make([]string, 0, len(nativeNames))
	for _, name := range nativeNames {
		value, ok := known[name]
		if !ok {
			return nil, "", ErrConfig
		}
		base = append(base, name+"="+value)
	}
	return base, deadline, nil
}

func activeContext(ctx context.Context, value string, limit time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrConfig
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != value {
		return nil, nil, ErrConfig
	}
	deadline := time.Unix(0, n)
	now := time.Now()
	if !deadline.After(now) || deadline.After(now.Add(limit)) {
		return nil, nil, ErrConfig
	}
	active, cancel := context.WithDeadline(ctx, deadline)
	if active.Err() != nil {
		cancel()
		return nil, nil, active.Err()
	}
	return active, cancel, nil
}
