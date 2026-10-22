package attachedworkerdaemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/nativeconnect"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

// NativeLaunchV3 is the immutable installation for the seven fixed native
// input vectors. Runtime descriptors and source mappings are distinct evidence;
// neither this value nor a matching digest creates Session/attempt authority.
// The V2 envelope and its digest semantics are deliberately unchanged.
type NativeLaunchV3 struct {
	Version                               uint32
	Bootstrap, Relay, Companion, Workload NativeArtifactV2
	SocketDestination                     string
	ProviderHosts                         []string
	Limits                                NativeConduitLimitsV2
	NativeArguments                       [7][]string
	BootstrapArguments                    [7][]string
	RelayArguments                        []string
	Catalog, Schema                       nativeharnessinput.FileV3
	ScopeDigest, PolicyDigest             ExecutableDigest
	// The full native profile producer derives this from actual config/env,
	// model, platform and bounds before computing InstallPolicyDigestV3.
	// A zero value is only an unqualified lower-level launch contract.
	StaticProfileDigest ExecutableDigest
}

func (v NativeLaunchV3) Clone() NativeLaunchV3 {
	v.ProviderHosts = slices.Clone(v.ProviderHosts)
	v.RelayArguments = slices.Clone(v.RelayArguments)
	for i := range v.NativeArguments {
		v.NativeArguments[i] = slices.Clone(v.NativeArguments[i])
		v.BootstrapArguments[i] = slices.Clone(v.BootstrapArguments[i])
	}
	return v
}

func (v NativeLaunchV3) Validate() error {
	if v.ScopeDigest == (ExecutableDigest{}) || v.PolicyDigest == (ExecutableDigest{}) {
		return ErrSupervisorConfig
	}
	return v.validateInstallV3()
}

// InstallPolicyDigestV3 seals every static member and all seven exact commands.
// Only per-execution scope and the derived policy value itself are excluded.
// This method never computes or accepts a V2 policy as the V3 installation.
func (v NativeLaunchV3) InstallPolicyDigestV3() (ExecutableDigest, error) {
	if v.validateInstallV3() != nil {
		return ExecutableDigest{}, ErrSupervisorConfig
	}
	v.ScopeDigest, v.PolicyDigest = ExecutableDigest{}, ExecutableDigest{}
	raw, err := json.Marshal(v)
	if err != nil {
		return ExecutableDigest{}, ErrSupervisorConfig
	}
	return ExecutableDigest(sha256.Sum256(append([]byte("sessionless.native-connect.install.v3\x00"), raw...))), nil
}

func (v NativeLaunchV3) validateInstallV3() error {
	if v.Version != 3 || nativeconnect.ValidateHosts(v.ProviderHosts) != nil ||
		!filepath.IsAbs(v.SocketDestination) || filepath.Clean(v.SocketDestination) != v.SocketDestination ||
		v.SocketDestination == "/" || len(v.SocketDestination) > 100 {
		return ErrSupervisorConfig
	}
	for _, b := range []byte(v.SocketDestination) {
		if b <= ' ' || b > '~' {
			return ErrSupervisorConfig
		}
	}
	l := v.Limits
	if l.Connections == 0 || l.Connections > 8 || l.InboundBytes == 0 || l.InboundBytes > 64<<20 ||
		l.OutboundBytes == 0 || l.OutboundBytes > 64<<20 || l.CompanionMemoryBytes == 0 || l.CompanionGoroutines == 0 ||
		l.Active <= 0 || l.Active > time.Minute || l.Cleanup <= 0 || l.Cleanup > maxSupervisorGrace {
		return ErrSupervisorConfig
	}
	for _, artifact := range []NativeArtifactV2{v.Bootstrap, v.Relay, v.Companion, v.Workload} {
		path, err := validateRegularPath(artifact.Executable)
		if err != nil || path != artifact.Executable || !utf8.ValidString(path) || artifact.Digest == (ExecutableDigest{}) {
			return ErrSupervisorConfig
		}
	}
	if v.Workload.Executable == v.Bootstrap.Executable || v.Workload.Executable == v.Relay.Executable {
		return ErrSupervisorConfig
	}
	// Reuse the shared scalar schema validation, not another hash/size dialect.
	profile := nativeharnessinput.NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: strings.Repeat("1", 64), InstallDigest: strings.Repeat("2", 64), Images: []nativeharnessinput.ImageV3{}, Catalog: v.Catalog, Schema: v.Schema}
	if nativeharnessinput.Validate(profile) != nil || !nativeArgsV3(v.RelayArguments, 128) || v.RelayArguments[0] != "relay" {
		return ErrSupervisorConfig
	}
	base := v.NativeArguments[0]
	if !nativeArgsV3(base, 128) || base[len(base)-1] != "-" {
		return ErrSupervisorConfig
	}
	for _, arg := range base[:len(base)-1] {
		if arg == "--" || arg == "-" || arg == "--image" || strings.HasPrefix(arg, "-i") || strings.HasPrefix(arg, "--image=") || arg == "--execution-descriptor-json" {
			return ErrSupervisorConfig
		}
	}
	for i, actual := range v.NativeArguments {
		paths, _ := nativeharnessinput.ImagePaths(uint32(i))
		expected := slices.Clone(base[:len(base)-1])
		for _, path := range paths {
			expected = append(expected, "--image", path)
		}
		expected = append(expected, "-")
		if !nativeArgsV3(actual, 128) || !slices.Equal(actual, expected) {
			return ErrSupervisorConfig
		}
		boot := v.BootstrapArguments[i]
		if !nativeArgsV3(boot, 126) || boot[0] != "bootstrap-v3" {
			return ErrSupervisorConfig
		}
		for _, arg := range boot {
			if arg == "--execution-descriptor-json" {
				return ErrSupervisorConfig
			}
		}
		tail := []string{"--native", v.Workload.Executable, "--native-sha256", hex.EncodeToString(v.Workload.Digest[:]), "--native-argc", strconv.Itoa(len(actual))}
		tail = append(tail, actual...)
		if len(boot) < len(tail)+1 || !slices.Equal(boot[len(boot)-len(tail):], tail) {
			return ErrSupervisorConfig
		}
	}
	// Full helper/relay syntax is regenerated and checked by the shared
	// bootstrap builder at the launcher seam; do not add a second parser here.
	return nil
}

func nativeArgsV3(args []string, limit int) bool {
	if len(args) == 0 || len(args) > limit {
		return false
	}
	for _, arg := range args {
		if len(arg) > 4096 || strings.ContainsRune(arg, 0) || !utf8.ValidString(arg) {
			return false
		}
	}
	return true
}
