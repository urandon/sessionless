package attachedworkerdaemon

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

func installV3Fixture(t *testing.T) NativeLaunchV3 {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	artifact := func(name string) NativeArtifactV2 {
		path := filepath.Join(root, name)
		body := []byte("fake executable, never executed: " + name)
		if err := os.WriteFile(path, body, 0o500); err != nil {
			t.Fatal(err)
		}
		return NativeArtifactV2{Executable: path, Digest: ExecutableDigest(sha256.Sum256(body))}
	}
	file := func(body string) nativeharnessinput.FileV3 {
		hash := sha256.Sum256([]byte(body))
		return nativeharnessinput.FileV3{Size: uint64(len(body)), SHA256: hex.EncodeToString(hash[:])}
	}
	v := NativeLaunchV3{Version: 3, Bootstrap: artifact("bootstrap"), Relay: artifact("relay"), Companion: artifact("companion"), Workload: artifact("codex"),
		SocketDestination: "/run/sessionless/connection.sock", ProviderHosts: []string{"chatgpt.com"},
		Limits:  NativeConduitLimitsV2{Connections: 2, InboundBytes: 1 << 20, OutboundBytes: 1 << 20, CompanionMemoryBytes: 1 << 20, CompanionGoroutines: 16, Active: time.Second, Cleanup: time.Second},
		Catalog: file("catalog"), Schema: file("schema"), ScopeDigest: ExecutableDigest{1}, PolicyDigest: ExecutableDigest{2}}
	installV3Vectors(&v, []string{"exec", "--json", "--ephemeral", "-"})
	return v
}

// Fixture construction is explicit data only; no helper process or real native
// artifact is run. The launcher separately checks full shared-builder syntax.
func installV3Vectors(v *NativeLaunchV3, base []string) {
	l := v.Limits
	v.RelayArguments = []string{"relay", "--socket", v.SocketDestination, "--connections", strconv.FormatUint(l.Connections, 10),
		"--to-unix-bytes", strconv.FormatUint(l.OutboundBytes, 10), "--from-unix-bytes", strconv.FormatUint(l.InboundBytes, 10),
		"--active-ns", strconv.FormatInt(int64(l.Active), 10), "--cleanup-ns", strconv.FormatInt(int64(l.Cleanup), 10), "--ready-fd", "3"}
	for i := range v.NativeArguments {
		args := slices.Clone(base[:len(base)-1])
		paths, _ := nativeharnessinput.ImagePaths(uint32(i))
		for _, path := range paths {
			args = append(args, "--image", path)
		}
		args = append(args, "-")
		v.NativeArguments[i] = args
		boot := []string{"bootstrap-v3", "--active-ns", strconv.FormatInt(int64(l.Active), 10), "--cleanup-ns", strconv.FormatInt(int64(l.Cleanup), 10),
			"--relay", v.Relay.Executable, "--relay-sha256", hex.EncodeToString(v.Relay.Digest[:]), "--relay-argc", strconv.Itoa(len(v.RelayArguments))}
		boot = append(boot, v.RelayArguments...)
		boot = append(boot, "--native", v.Workload.Executable, "--native-sha256", hex.EncodeToString(v.Workload.Digest[:]), "--native-argc", strconv.Itoa(len(args)))
		v.BootstrapArguments[i] = append(boot, args...)
	}
}

func TestNativeInstallV3IsDistinctStableAndScopeIndependent(t *testing.T) {
	v := installV3Fixture(t)
	if err := v.Validate(); err != nil {
		t.Fatalf("valid installation: %v", err)
	}
	want, err := v.InstallPolicyDigestV3()
	if err != nil || want == (ExecutableDigest{}) {
		t.Fatalf("digest=%x error=%v", want, err)
	}
	w := v.Clone()
	w.ScopeDigest = ExecutableDigest{99}
	w.PolicyDigest = ExecutableDigest{100}
	got, err := w.InstallPolicyDigestV3()
	if err != nil || got != want {
		t.Fatalf("runtime association changed install: got%x want%x err%v", got, want, err)
	}
	w.ScopeDigest, w.PolicyDigest = ExecutableDigest{}, ExecutableDigest{}
	got, err = w.InstallPolicyDigestV3()
	if err != nil || got != want {
		t.Fatalf("zero runtime fields changed install: got%x want%x err%v", got, want, err)
	}
	if err = w.Validate(); !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("runtime validation accepted zero authority associations: %v", err)
	}
	legacy := NativeLaunchV2{Version: 2, Bootstrap: v.Bootstrap, Relay: v.Relay, Companion: v.Companion, BootstrapArguments: v.BootstrapArguments[0], RelayArguments: v.RelayArguments, SocketDestination: v.SocketDestination, ProviderHosts: v.ProviderHosts, Limits: v.Limits}
	old, err := legacy.InstallPolicyDigestV2()
	if err != nil {
		t.Fatal(err)
	}
	if old == want {
		t.Fatal("V2 and V3 install domains aliased")
	}
	for i := range v.NativeArguments {
		paths, _ := nativeharnessinput.ImagePaths(uint32(i))
		wantArgs := slices.Clone(v.NativeArguments[0][:len(v.NativeArguments[0])-1])
		for _, p := range paths {
			wantArgs = append(wantArgs, "--image", p)
		}
		wantArgs = append(wantArgs, "-")
		if !slices.Equal(wantArgs, v.NativeArguments[i]) {
			t.Fatalf("variant%d wrong exact native arguments", i)
		}
	}
}

func TestNativeInstallV3CloneDoesNotAlias(t *testing.T) {
	v := installV3Fixture(t)
	clone := v.Clone()
	clone.ProviderHosts[0] = "foreign.example"
	clone.RelayArguments[0] = "different"
	for i := range clone.NativeArguments {
		clone.NativeArguments[i][0] = "different"
		clone.BootstrapArguments[i][0] = "different"
	}
	if v.ProviderHosts[0] != "chatgpt.com" || v.RelayArguments[0] != "relay" {
		t.Fatal("clone aliases origin/relay")
	}
	for i := range v.NativeArguments {
		if v.NativeArguments[i][0] != "exec" || v.BootstrapArguments[i][0] != "bootstrap-v3" {
			t.Fatalf("clone aliases command%d", i)
		}
	}
}

func TestNativeInstallV3SealsEveryStaticMember(t *testing.T) {
	v := installV3Fixture(t)
	want, err := v.InstallPolicyDigestV3()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*NativeLaunchV3)
	}{
		{"bootstrap", func(v *NativeLaunchV3) { v.Bootstrap.Digest[0] ^= 1 }},
		{"relay", func(v *NativeLaunchV3) { v.Relay.Digest[0] ^= 1; installV3Vectors(v, v.NativeArguments[0]) }},
		{"companion", func(v *NativeLaunchV3) { v.Companion.Digest[0] ^= 1 }},
		{"workload", func(v *NativeLaunchV3) { v.Workload.Digest[0] ^= 1; installV3Vectors(v, v.NativeArguments[0]) }},
		{"hosts", func(v *NativeLaunchV3) { v.ProviderHosts = []string{"other.example"} }},
		{"socket", func(v *NativeLaunchV3) {
			v.SocketDestination = "/run/sessionless/other.sock"
			installV3Vectors(v, v.NativeArguments[0])
		}},
		{"connections", func(v *NativeLaunchV3) { v.Limits.Connections++; installV3Vectors(v, v.NativeArguments[0]) }},
		{"inbound", func(v *NativeLaunchV3) { v.Limits.InboundBytes++; installV3Vectors(v, v.NativeArguments[0]) }},
		{"outbound", func(v *NativeLaunchV3) { v.Limits.OutboundBytes++; installV3Vectors(v, v.NativeArguments[0]) }},
		{"memory", func(v *NativeLaunchV3) { v.Limits.CompanionMemoryBytes++ }},
		{"goroutines", func(v *NativeLaunchV3) { v.Limits.CompanionGoroutines++ }},
		{"active", func(v *NativeLaunchV3) { v.Limits.Active++; installV3Vectors(v, v.NativeArguments[0]) }},
		{"cleanup", func(v *NativeLaunchV3) { v.Limits.Cleanup++; installV3Vectors(v, v.NativeArguments[0]) }},
		{"native-commands", func(v *NativeLaunchV3) {
			installV3Vectors(v, []string{"exec", "--json", "--ephemeral", "--color", "never", "-"})
		}},
		{"bootstrap-static", func(v *NativeLaunchV3) { v.BootstrapArguments[6][2] = "999" }},
		{"relay-static", func(v *NativeLaunchV3) { v.RelayArguments[2] = "/run/sessionless/new.sock" }},
		{"catalog-size", func(v *NativeLaunchV3) { v.Catalog.Size++ }},
		{"catalog-hash", func(v *NativeLaunchV3) { v.Catalog.SHA256 = strings.Repeat("a", 64) }},
		{"schema-size", func(v *NativeLaunchV3) { v.Schema.Size++ }},
		{"schema-hash", func(v *NativeLaunchV3) { v.Schema.SHA256 = strings.Repeat("b", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := v.Clone()
			tc.mutate(&w)
			got, err := w.InstallPolicyDigestV3()
			if err != nil || got == want {
				t.Fatalf("static mutation not sealed: got%x want-not%x err%v", got, want, err)
			}
		})
	}
}

func TestNativeInstallV3RejectsMalformedContract(t *testing.T) {
	v := installV3Fixture(t)
	cases := []struct {
		name   string
		mutate func(*NativeLaunchV3)
	}{
		{"version", func(v *NativeLaunchV3) { v.Version = 2 }},
		{"hosts-empty", func(v *NativeLaunchV3) { v.ProviderHosts = nil }},
		{"hosts-unsorted", func(v *NativeLaunchV3) { v.ProviderHosts = []string{"z.example", "a.example"} }},
		{"socket-relative", func(v *NativeLaunchV3) { v.SocketDestination = "relative" }},
		{"socket-root", func(v *NativeLaunchV3) { v.SocketDestination = "/" }},
		{"socket-space", func(v *NativeLaunchV3) { v.SocketDestination = "/run/space socket" }},
		{"socket-overlong", func(v *NativeLaunchV3) { v.SocketDestination = "/" + strings.Repeat("a", 100) }},
		{"connections-zero", func(v *NativeLaunchV3) { v.Limits.Connections = 0 }},
		{"connections-over", func(v *NativeLaunchV3) { v.Limits.Connections = 9 }},
		{"inbound-zero", func(v *NativeLaunchV3) { v.Limits.InboundBytes = 0 }},
		{"inbound-over", func(v *NativeLaunchV3) { v.Limits.InboundBytes = 64<<20 + 1 }},
		{"outbound-over", func(v *NativeLaunchV3) { v.Limits.OutboundBytes = 64<<20 + 1 }},
		{"memory-zero", func(v *NativeLaunchV3) { v.Limits.CompanionMemoryBytes = 0 }},
		{"goroutines-zero", func(v *NativeLaunchV3) { v.Limits.CompanionGoroutines = 0 }},
		{"active-zero", func(v *NativeLaunchV3) { v.Limits.Active = 0 }},
		{"active-over", func(v *NativeLaunchV3) { v.Limits.Active = time.Minute + 1 }},
		{"cleanup-over", func(v *NativeLaunchV3) { v.Limits.Cleanup = maxSupervisorGrace + 1 }},
		{"zero-artifact", func(v *NativeLaunchV3) { v.Companion.Digest = ExecutableDigest{} }},
		{"artifact-relative", func(v *NativeLaunchV3) { v.Workload.Executable = "relative" }},
		{"artifact-missing", func(v *NativeLaunchV3) { v.Bootstrap.Executable += "-missing" }},
		{"workload-is-helper", func(v *NativeLaunchV3) { v.Workload = v.Relay }},
		{"catalog-zero", func(v *NativeLaunchV3) { v.Catalog.Size = 0 }},
		{"catalog-over", func(v *NativeLaunchV3) { v.Catalog.Size = nativeharnessinput.MaxFileBytes + 1 }},
		{"catalog-zero-hash", func(v *NativeLaunchV3) { v.Catalog.SHA256 = strings.Repeat("0", 64) }},
		{"schema-over", func(v *NativeLaunchV3) { v.Schema.Size = nativeharnessinput.MaxFileBytes + 1 }},
		{"schema-upper-hash", func(v *NativeLaunchV3) { v.Schema.SHA256 = strings.ToUpper(v.Schema.SHA256) }},
		{"native-missing", func(v *NativeLaunchV3) { v.NativeArguments[6] = nil }},
		{"native-nul", func(v *NativeLaunchV3) { v.NativeArguments[6][0] += "\x00" }},
		{"native-overlong", func(v *NativeLaunchV3) { v.NativeArguments[6][0] = strings.Repeat("a", 4097) }},
		{"native-nonutf8", func(v *NativeLaunchV3) { v.NativeArguments[6][0] = "\xff" }},
		{"native-overcount", func(v *NativeLaunchV3) { v.NativeArguments[6] = slices.Repeat([]string{"x"}, 129) }},
		{"native-extra-image", func(v *NativeLaunchV3) {
			v.NativeArguments[0] = append(v.NativeArguments[0][:len(v.NativeArguments[0])-1], "--image", "/foreign", "-")
		}},
		{"base-image-alias", func(v *NativeLaunchV3) { installV3Vectors(v, []string{"exec", "-i/foreign", "-"}) }},
		{"base-image-equals", func(v *NativeLaunchV3) { installV3Vectors(v, []string{"exec", "--image=/foreign", "-"}) }},
		{"base-option-terminator", func(v *NativeLaunchV3) { installV3Vectors(v, []string{"exec", "--", "-"}) }},
		{"base-wrong-terminal", func(v *NativeLaunchV3) { v.NativeArguments[0][len(v.NativeArguments[0])-1] = "prompt" }},
		{"image-suffix-drift", func(v *NativeLaunchV3) {
			v.NativeArguments[1][len(v.NativeArguments[1])-2] = "/sessionless/input/image-01.jpg"
		}},
		{"image-order", func(v *NativeLaunchV3) {
			a := v.NativeArguments[4]
			a[len(a)-4], a[len(a)-2] = a[len(a)-2], a[len(a)-4]
		}},
		{"bootstrap-missing", func(v *NativeLaunchV3) { v.BootstrapArguments[3] = nil }},
		{"bootstrap-v2-mode", func(v *NativeLaunchV3) { v.BootstrapArguments[1][0] = "bootstrap" }},
		{"bootstrap-native-path", func(v *NativeLaunchV3) {
			a := v.BootstrapArguments[2]
			a[len(a)-len(v.NativeArguments[2])-5] = "/foreign/native"
		}},
		{"bootstrap-native-digest", func(v *NativeLaunchV3) {
			a := v.BootstrapArguments[2]
			a[len(a)-len(v.NativeArguments[2])-3] = strings.Repeat("a", 64)
		}},
		{"bootstrap-native-argc", func(v *NativeLaunchV3) { a := v.BootstrapArguments[2]; a[len(a)-len(v.NativeArguments[2])-1] = "999" }},
		{"bootstrap-carrier", func(v *NativeLaunchV3) {
			v.BootstrapArguments[3] = append(v.BootstrapArguments[3], "--execution-descriptor-json", "{}")
		}},
		{"bootstrap-no-carrier-reserve", func(v *NativeLaunchV3) {
			a := v.BootstrapArguments[3]
			prefix := slices.Repeat([]string{"x"}, 127-len(a))
			v.BootstrapArguments[3] = append(append(slices.Clone(a[:1]), prefix...), a[1:]...)
		}},
		{"relay-nul", func(v *NativeLaunchV3) { v.RelayArguments[0] += "\x00" }},
		{"relay-overlong", func(v *NativeLaunchV3) { v.RelayArguments[1] = strings.Repeat("x", 4097) }},
		{"relay-overcount", func(v *NativeLaunchV3) { v.RelayArguments = slices.Repeat([]string{"relay"}, 129) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := v.Clone()
			tc.mutate(&w)
			if err := w.Validate(); !errors.Is(err, ErrSupervisorConfig) {
				t.Fatalf("malformed V3 accepted: %v", err)
			}
			if _, err := w.InstallPolicyDigestV3(); !errors.Is(err, ErrSupervisorConfig) {
				t.Fatalf("malformed install hashed: %v", err)
			}
		})
	}
}

func TestNativeInstallV3RejectsArtifactSymlink(t *testing.T) {
	v := installV3Fixture(t)
	alias := v.Companion.Executable + "-alias"
	if err := os.Symlink(v.Companion.Executable, alias); err != nil {
		t.Fatal(err)
	}
	v.Companion.Executable = alias
	if _, err := v.InstallPolicyDigestV3(); !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("symlink artifact accepted: %v", err)
	}
}
