//go:build darwin || linux

package codexexec

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
)

func nativeProfileFixtureV2(t *testing.T) NativeProfileV2 {
	t.Helper()
	p := nativeGeneratedFixtureV2()
	root := privatePreparedScratch(t)
	for i, artifact := range []*attachedworkerdaemon.NativeArtifactV2{&p.Launch.Bootstrap, &p.Launch.Relay, &p.Launch.Companion, &p.Launch.Workload} {
		body := []byte(fmt.Sprintf("inert profile fixture %d\n", i))
		artifact.Executable = filepath.Join(root, fmt.Sprintf("helper-%d", i))
		artifact.Digest = attachedworkerdaemon.ExecutableDigest(sha256.Sum256(body))
		if err := os.WriteFile(artifact.Executable, body, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	p.Backend.Executable, p.Backend.ExecutableDigest = p.Launch.Workload.Executable, p.Launch.Workload.Digest
	p.Platform = attachedworkerlocal.OCIConfigV1{
		DockerPath: "/owned/docker", DockerSHA256: strings.Repeat("a", 64), CLIConfigDir: "/owned/docker-config",
		Host: "unix:///run/user/1000/docker.sock", EngineID: "fixture-engine-0001", InstallationID: "fixture-installation-0001",
		Boundary: attachedworkerlocal.BoundaryLinuxRootlessNativeV3,
		Image:    "registry.example/native@sha256:" + strings.Repeat("b", 64),
		UserID:   0, GroupID: 0, DiskBytes: 32 << 20, CredentialFileBytes: 1 << 20, MemoryBytes: 128 << 20, PIDsLimit: 32, StopSeconds: 5,
	}
	return p
}

func TestNativeProfileV2DerivesDistinctStaticAndInstallIdentity(t *testing.T) {
	p := nativeProfileFixtureV2(t)
	frozen, err := freezeNativeProfileV2(p)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.launch.StaticProfileDigest == (attachedworkerdaemon.ExecutableDigest{}) || frozen.launch.PolicyDigest == frozen.launch.StaticProfileDigest ||
		frozen.launch.ScopeDigest != (attachedworkerdaemon.ExecutableDigest{}) || frozen.launch.PolicyDigest == (attachedworkerdaemon.ExecutableDigest{}) {
		t.Fatal("static/install identity missing or execution scope invented")
	}
	args, err := p.ArgumentsV2()
	if err != nil || !reflect.DeepEqual(args, frozen.generated.arguments[0]) {
		t.Fatalf("zero-image installed command differs: %v", err)
	}
	args[0] = "mutated"
	if frozen.launch.NativeArguments[0][0] != "exec" {
		t.Fatal("public arguments alias frozen installation")
	}
	platform, err := p.NativeLauncherConfigV3()
	if err != nil || platform.Boundary != attachedworkeroci.BoundaryLinuxRootless || platform.UserID != 0 || platform.GroupID != 0 ||
		p.Platform.Boundary != attachedworkerlocal.BoundaryLinuxRootlessNativeV3 || attachedworkeroci.ValidateDeclarativeConfig(platform) == nil {
		t.Fatalf("explicit native lowering changed original discriminant or weakened V1: %v", err)
	}
	for _, change := range []func(*NativeProfileV2){
		func(p *NativeProfileV2) { p.Backend.Enabled = true },
		func(p *NativeProfileV2) { p.Backend.Now = func() time.Time { return time.Unix(1, 0) } },
	} {
		other := p.Clone()
		change(&other)
		got, err := other.DescriptorV2()
		if err != nil || got != frozen.descriptor {
			t.Fatalf("activation or clock changed immutable identity: %v", err)
		}
	}
}

func TestNativeProfileV2PolicyDigestIsCanonicalMetadata(t *testing.T) {
	p := nativeProfileFixtureV2(t)
	frozen, err := freezeNativeProfileV2(p)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", frozen.launch.PolicyDigest)
	got, err := p.PolicyDigestV2()
	if err != nil || got.Validate() != nil || string(got) != want {
		t.Fatalf("canonical policy digest=%s want=%s err=%v", got, want, err)
	}
	if p.Launch.PolicyDigest != (attachedworkerdaemon.ExecutableDigest{}) || p.Launch.ScopeDigest != (attachedworkerdaemon.ExecutableDigest{}) || len(p.Launch.NativeArguments[0]) != 0 {
		t.Fatal("metadata accessor modified original profile or invented runtime scope")
	}
	other := p.Clone()
	other.Backend.Enabled = !other.Backend.Enabled
	other.Backend.Now = func() time.Time { return time.Unix(1, 0) }
	if next, err := other.PolicyDigestV2(); err != nil || next != got {
		t.Fatalf("activation or clock changed installation identity: %v", err)
	}
	other = p.Clone()
	other.Backend.Model = "other-model"
	if next, err := other.PolicyDigestV2(); err != nil || next == got {
		t.Fatalf("changed model did not change policy identity: %v", err)
	}
	other.Launch.ScopeDigest[0] = 1
	if next, err := other.PolicyDigestV2(); !errors.Is(err, ErrContract) || next != "" {
		t.Fatalf("caller runtime scope accepted: digest=%s err=%v", next, err)
	}
	if next, err := (NativeProfileV2{}).PolicyDigestV2(); !errors.Is(err, ErrContract) || next != "" {
		t.Fatalf("invalid installation emitted policy identity: digest=%s err=%v", next, err)
	}
}

func TestNativeProfileV2SealsEffectivePlatformAndBounds(t *testing.T) {
	p := nativeProfileFixtureV2(t)
	want, err := freezeNativeProfileV2(p)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		change func(*NativeProfileV2)
	}{
		{"model", func(p *NativeProfileV2) { p.Backend.Model = "other-model" }},
		{"architecture", func(p *NativeProfileV2) { p.Architecture = "arm64" }},
		{"context", func(p *NativeProfileV2) { p.DeclaredContextWindow++ }},
		{"reserve", func(p *NativeProfileV2) { p.OutputReserveTokens++ }},
		{"idle", func(p *NativeProfileV2) { p.StreamIdleTimeoutMS-- }},
		{"installation", func(p *NativeProfileV2) { p.Platform.InstallationID = "other-installation-0001" }},
		{"engine", func(p *NativeProfileV2) { p.Platform.EngineID = "other-engine-0001" }},
		{"engine-path", func(p *NativeProfileV2) { p.Platform.DockerPath = "/other/docker" }},
		{"socket", func(p *NativeProfileV2) { p.Platform.Host = "unix:///run/user/1000/other.sock" }},
		{"image", func(p *NativeProfileV2) {
			p.Platform.Image = "registry.example/native@sha256:" + strings.Repeat("c", 64)
		}},
		{"memory", func(p *NativeProfileV2) { p.Platform.MemoryBytes += 1 << 20 }},
		{"pids", func(p *NativeProfileV2) { p.Platform.PIDsLimit++ }},
		{"scratch", func(p *NativeProfileV2) { p.Platform.DiskBytes += 1 << 20 }},
		{"conduit", func(p *NativeProfileV2) { p.Launch.Limits.InboundBytes-- }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			other := p.Clone()
			tc.change(&other)
			got, err := freezeNativeProfileV2(other)
			if err != nil || got.descriptor == want.descriptor || got.launch.StaticProfileDigest == want.launch.StaticProfileDigest || got.launch.PolicyDigest == want.launch.PolicyDigest {
				t.Fatalf("changed effective profile must derive distinct identities: %v", err)
			}
		})
	}
}

func TestNativeProfileV2RejectsUnqualifiedCarrierAndRawOverrides(t *testing.T) {
	cases := []struct {
		name   string
		change func(*NativeProfileV2)
	}{
		{"old-boundary", func(p *NativeProfileV2) { p.Platform.Boundary = attachedworkerlocal.BoundaryLinuxRootless }},
		{"darwin", func(p *NativeProfileV2) { p.OperatingSystem = "darwin" }},
		{"unknown-arch", func(p *NativeProfileV2) { p.Architecture = "other" }},
		{"uid", func(p *NativeProfileV2) { p.Platform.UserID = 1000 }},
		{"gid", func(p *NativeProfileV2) { p.Platform.GroupID = 1000 }},
		{"raw-command", func(p *NativeProfileV2) { p.Launch.NativeArguments[0] = []string{"exec", "-"} }},
		{"raw-relay", func(p *NativeProfileV2) { p.Launch.RelayArguments = []string{"relay"} }},
		{"runtime-scope", func(p *NativeProfileV2) { p.Launch.ScopeDigest[0] = 1 }},
		{"caller-install-digest", func(p *NativeProfileV2) { p.Launch.PolicyDigest[0] = 1 }},
		{"caller-static-digest", func(p *NativeProfileV2) { p.Launch.StaticProfileDigest[0] = 1 }},
		{"missing-origin", func(p *NativeProfileV2) { p.Launch.ProviderHosts = []string{"chatgpt.com"} }},
		{"oversized-scratch", func(p *NativeProfileV2) { p.Platform.DiskBytes = 65 << 20 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := nativeProfileFixtureV2(t)
			tc.change(&p)
			if _, err := freezeNativeProfileV2(p); !errors.Is(err, ErrContract) {
				t.Fatalf("unqualified profile accepted: %v", err)
			}
		})
	}
}
