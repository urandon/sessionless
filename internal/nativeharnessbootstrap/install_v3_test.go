package nativeharnessbootstrap

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

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

var installationV3Images = [7][]string{{}, {"image-01.png"}, {"image-01.jpg"}, {"image-01.png", "image-02.png"}, {"image-01.png", "image-02.jpg"}, {"image-01.jpg", "image-02.png"}, {"image-01.jpg", "image-02.jpg"}}

func installationV3Fixture(t *testing.T, base []string) attachedworkerdaemon.NativeLaunchV3 {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	artifact := func(name string) attachedworkerdaemon.NativeArtifactV2 {
		path := filepath.Join(root, name)
		body := []byte("inert static artifact " + name)
		if err := os.WriteFile(path, body, 0500); err != nil {
			t.Fatal(err)
		}
		return attachedworkerdaemon.NativeArtifactV2{Executable: path, Digest: attachedworkerdaemon.ExecutableDigest(sha256.Sum256(body))}
	}
	file := func(body string) nativeharnessinput.FileV3 {
		sum := sha256.Sum256([]byte(body))
		return nativeharnessinput.FileV3{Size: uint64(len(body)), SHA256: hex.EncodeToString(sum[:])}
	}
	v := attachedworkerdaemon.NativeLaunchV3{Version: 3, Bootstrap: artifact("bootstrap"), Relay: artifact("relay"), Companion: artifact("companion"), Workload: artifact("native"),
		SocketDestination: "/run/sessionless/input.sock", ProviderHosts: []string{"api.openai.com"}, ScopeDigest: attachedworkerdaemon.ExecutableDigest{7},
		Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 128, OutboundBytes: 129, Active: 3 * time.Second, Cleanup: time.Second, CompanionMemoryBytes: 1, CompanionGoroutines: 1}, Catalog: file("catalog"), Schema: file("schema")}
	if base == nil {
		base = []string{"exec", "--json", "literal argument", "$not-a-shell", "-"}
	}
	installationV3SetNative(&v, base)
	v.BootstrapArguments, v.RelayArguments, err = InstallationArgumentsV3(v)
	if err != nil {
		t.Fatal(err)
	}
	v.PolicyDigest, err = v.InstallPolicyDigestV3()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func installationV3SetNative(v *attachedworkerdaemon.NativeLaunchV3, base []string) {
	for i, names := range installationV3Images {
		args := slices.Clone(base[:len(base)-1])
		for _, name := range names {
			args = append(args, "--image", "/sessionless/input/"+name)
		}
		v.NativeArguments[i] = append(args, "-")
	}
}

func installationV3Descriptor(v attachedworkerdaemon.NativeLaunchV3, variant uint32) nativeharnessinput.NativeExecutionDescriptorV3 {
	d := nativeharnessinput.NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: hex.EncodeToString(v.ScopeDigest[:]), InstallDigest: hex.EncodeToString(v.PolicyDigest[:]), Variant: variant, Images: []nativeharnessinput.ImageV3{}, Catalog: v.Catalog, Schema: v.Schema}
	for i := range installationV3Images[variant] {
		sum := sha256.Sum256([]byte("image" + strconv.Itoa(i)))
		d.Images = append(d.Images, nativeharnessinput.ImageV3{Ordinal: uint32(i + 1), Size: 6, SHA256: hex.EncodeToString(sum[:])})
	}
	return d
}

func TestInstallationV3SevenLiteralStaticCommandsAndCarrier(t *testing.T) {
	v := installationV3Fixture(t, nil)
	if err := ValidateInstallationV3(v); err != nil {
		t.Fatal(err)
	}
	wantRelay := []string{"relay", "--socket", "/run/sessionless/input.sock", "--connections", "2", "--to-unix-bytes", "129", "--from-unix-bytes", "128", "--active-ns", "3000000000", "--cleanup-ns", "1000000000", "--ready-fd", "3"}
	if !slices.Equal(v.RelayArguments, wantRelay) {
		t.Fatalf("relay ABI got%q want%q", v.RelayArguments, wantRelay)
	}
	for variant, names := range installationV3Images {
		t.Run(strconv.Itoa(variant), func(t *testing.T) {
			wantNative := []string{"exec", "--json", "literal argument", "$not-a-shell"}
			for _, name := range names {
				wantNative = append(wantNative, "--image", "/sessionless/input/"+name)
			}
			wantNative = append(wantNative, "-")
			wantBoot := []string{"bootstrap-v3", "--active-ns", "3000000000", "--cleanup-ns", "1000000000", "--relay", v.Relay.Executable, "--relay-sha256", hex.EncodeToString(v.Relay.Digest[:]), "--relay-argc", "15"}
			wantBoot = append(wantBoot, wantRelay...)
			wantBoot = append(wantBoot, "--native", v.Workload.Executable, "--native-sha256", hex.EncodeToString(v.Workload.Digest[:]), "--native-argc", strconv.Itoa(len(wantNative)))
			wantBoot = append(wantBoot, wantNative...)
			if !slices.Equal(v.NativeArguments[variant], wantNative) || !slices.Equal(v.BootstrapArguments[variant], wantBoot) {
				t.Fatalf("variant%d literal lowering differs\nnative%q\nbootstrap%q\nwant%q", variant, v.NativeArguments[variant], v.BootstrapArguments[variant], wantBoot)
			}
			if slices.Contains(wantBoot, "--execution-descriptor-json") {
				t.Fatal("runtime carrier leaked into installation")
			}
			d := installationV3Descriptor(v, uint32(variant))
			raw, err := nativeharnessinput.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			actual := append(append(slices.Clone(wantBoot), "--execution-descriptor-json"), string(raw))
			parsed, decoded, err := parseBootstrapV3(actual)
			if err != nil || decoded.Variant != uint32(variant) || !slices.Equal(parsed.native.args, wantNative) || !slices.Equal(parsed.relay.args, wantRelay) {
				t.Fatalf("actual carrier handoff variant%d: %+v %v", variant, decoded, err)
			}
			if slices.Contains(parsed.native.args, "--execution-descriptor-json") || slices.Contains(parsed.relay.args, "--execution-descriptor-json") {
				t.Fatal("bootstrap forwarded carrier to a child")
			}
			helpers := attachedworkerdaemon.NativeLaunchV2{Version: 2, Bootstrap: v.Bootstrap, Relay: v.Relay, Companion: v.Companion, SocketDestination: v.SocketDestination, ProviderHosts: slices.Clone(v.ProviderHosts), Limits: v.Limits}
			workload := attachedworkerdaemon.LaunchSpec{Executable: v.Workload.Executable, ExecutableDigest: v.Workload.Digest, Arguments: slices.Clone(wantNative)}
			built, relay, err := ArgumentsV3(workload, helpers, d)
			if err != nil || !slices.Equal(built, actual) || !slices.Equal(relay, wantRelay) {
				t.Fatalf("runtime builder incompatible with literal static command: %v", err)
			}
			if _, _, err := parseBootstrapV3(wantBoot); !errors.Is(err, ErrConfig) {
				t.Fatalf("static command without runtime carrier accepted: %v", err)
			}
		})
	}
}

func TestInstallationV3ReservesExactlyTwoRuntimeArguments(t *testing.T) {
	base := slices.Repeat([]string{"literal"}, 90)
	base[0] = "exec"
	base[len(base)-1] = "-"
	v := installationV3Fixture(t, base)
	if len(v.BootstrapArguments[6]) != 126 {
		t.Fatalf("static maximum=%d want126", len(v.BootstrapArguments[6]))
	}
	if err := ValidateInstallationV3(v); err != nil {
		t.Fatal(err)
	}
	d := installationV3Descriptor(v, 6)
	raw, err := nativeharnessinput.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	args := append(append(slices.Clone(v.BootstrapArguments[6]), "--execution-descriptor-json"), string(raw))
	if len(args) != 128 {
		t.Fatalf("complete maximum=%d want128", len(args))
	}
	if _, _, err := parseBootstrapV3(args); err != nil {
		t.Fatalf("inclusive full128 argv boundary denied: %v", err)
	}
	if _, _, err := parseBootstrapV3(append(args, "extra")); !errors.Is(err, ErrConfig) {
		t.Fatalf("129 arguments accepted: %v", err)
	}
	tooLarge := v.Clone()
	base = append(slices.Clone(base[:len(base)-1]), "extra", "-")
	installationV3SetNative(&tooLarge, base)
	if _, _, err := InstallationArgumentsV3(tooLarge); !errors.Is(err, ErrConfig) {
		t.Fatalf("static127 arguments left no room for carrier: %v", err)
	}
}

func TestInstallationV3RejectsRehashedStaticPrefixDrift(t *testing.T) {
	v := installationV3Fixture(t, nil)
	cases := []struct {
		name   string
		change func(*attachedworkerdaemon.NativeLaunchV3)
	}{
		{"active", func(v *attachedworkerdaemon.NativeLaunchV3) { v.BootstrapArguments[6][2] = "3000000001" }},
		{"cleanup", func(v *attachedworkerdaemon.NativeLaunchV3) { v.BootstrapArguments[5][4] = "1000000001" }},
		{"relay-path", func(v *attachedworkerdaemon.NativeLaunchV3) { v.BootstrapArguments[4][6] = "/foreign/relay" }},
		{"relay-hash", func(v *attachedworkerdaemon.NativeLaunchV3) { v.BootstrapArguments[3][8] = strings.Repeat("a", 64) }},
		{"relay-count", func(v *attachedworkerdaemon.NativeLaunchV3) { v.BootstrapArguments[2][10] = "14" }},
		{"noncanonical-count", func(v *attachedworkerdaemon.NativeLaunchV3) { v.BootstrapArguments[1][10] = "015" }},
		{"extra-prefix", func(v *attachedworkerdaemon.NativeLaunchV3) {
			v.BootstrapArguments[0] = append([]string{"bootstrap-v3", "unsealed"}, v.BootstrapArguments[0][1:]...)
		}},
		{"relay-readiness-fd", func(v *attachedworkerdaemon.NativeLaunchV3) { v.RelayArguments[len(v.RelayArguments)-1] = "4" }},
		{"relay-socket", func(v *attachedworkerdaemon.NativeLaunchV3) { v.RelayArguments[2] = "/foreign/socket" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := v.Clone()
			tc.change(&changed)
			// The envelope checks exact native tails and bounds, deliberately not a
			// second helper parser. Rehashing cannot make a malformed prefix valid.
			var err error
			changed.PolicyDigest, err = changed.InstallPolicyDigestV3()
			if err != nil {
				t.Fatalf("case did not reach shared-prefix validation: %v", err)
			}
			if err := changed.Validate(); err != nil {
				t.Fatalf("case rejected only by coarse envelope, not exact shared builder: %v", err)
			}
			if err := ValidateInstallationV3(changed); !errors.Is(err, ErrConfig) {
				t.Fatalf("rehashed static prefix drift accepted: %v", err)
			}
		})
	}
}

func TestInstallationV3RejectsMalformedHelpersAndConfiguration(t *testing.T) {
	v := installationV3Fixture(t, nil)
	cases := []struct {
		name   string
		change func(*attachedworkerdaemon.NativeLaunchV3)
	}{
		{"v2-version", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Version = 2 }},
		{"relative-helper", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Relay.Executable = "relative" }},
		{"zero-helper-hash", func(v *attachedworkerdaemon.NativeLaunchV3) {
			v.Companion.Digest = attachedworkerdaemon.ExecutableDigest{}
		}},
		{"empty-native-vector", func(v *attachedworkerdaemon.NativeLaunchV3) { v.NativeArguments[6] = nil }},
		{"native-nul", func(v *attachedworkerdaemon.NativeLaunchV3) { v.NativeArguments[4][0] += "\x00" }},
		{"native-overlong", func(v *attachedworkerdaemon.NativeLaunchV3) { v.NativeArguments[2][0] = strings.Repeat("x", 4097) }},
		{"provider-origin", func(v *attachedworkerdaemon.NativeLaunchV3) { v.ProviderHosts = []string{"127.0.0.1"} }},
		{"socket-space", func(v *attachedworkerdaemon.NativeLaunchV3) { v.SocketDestination = "/run/bad socket" }},
		{"connections-over", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Limits.Connections = 9 }},
		{"active-over", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Limits.Active = time.Minute + 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := v.Clone()
			tc.change(&changed)
			if _, _, err := InstallationArgumentsV3(changed); !errors.Is(err, ErrConfig) {
				t.Fatalf("malformed static builder accepted: %v", err)
			}
			if err := ValidateInstallationV3(changed); !errors.Is(err, ErrConfig) {
				t.Fatalf("malformed installation accepted: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*attachedworkerdaemon.NativeLaunchV3)
	}{
		{"catalog-empty", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Catalog.Size = 0 }},
		{"schema-over", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Schema.Size = nativeharnessinput.MaxFileBytes + 1 }},
		{"catalog-upperhash", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Catalog.SHA256 = strings.ToUpper(v.Catalog.SHA256) }},
		{"schema-zerohash", func(v *attachedworkerdaemon.NativeLaunchV3) { v.Schema.SHA256 = strings.Repeat("0", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := v.Clone()
			tc.change(&changed)
			if err := ValidateInstallationV3(changed); !errors.Is(err, ErrConfig) {
				t.Fatalf("malformed frozen profile accepted: %v", err)
			}
		})
	}
}
