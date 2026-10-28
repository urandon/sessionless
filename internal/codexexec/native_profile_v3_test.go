//go:build darwin || linux

package codexexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
)

// The existing fixture creates inert local files only. No artifact is executed
// or read as a credential; static preparation itself has no filesystem effects.
func nativeProfileFixtureV3(t *testing.T) NativeProfileV3 {
	t.Helper()
	p := nativeProfileFixtureV2(t)
	return NativeProfileV3{Backend: p.Backend, Launch: p.Launch.Clone(), Platform: p.Platform,
		OperatingSystem: p.OperatingSystem, Architecture: p.Architecture,
		DeclaredContextWindow: p.DeclaredContextWindow, OutputReserveTokens: p.OutputReserveTokens,
		StreamIdleTimeoutMS: p.StreamIdleTimeoutMS}
}

func TestNativeProfileV3DistinctRecomputedIdentityAndUnchangedGeneratedBounds(t *testing.T) {
	p := nativeProfileFixtureV3(t)
	frozen, err := freezeNativeProfileV3(p)
	if err != nil {
		t.Fatal(err)
	}
	old, err := freezeNativeProfileV2(p.declarativeInputV2())
	if err != nil {
		t.Fatal(err)
	}
	if NativeProfileVersionV3 != "sessionless.codex-native-envelope.v3" || frozen.launch.Version != 3 || frozen.launch.ScopeDigest != (attachedworkerdaemon.ExecutableDigest{}) ||
		frozen.launch.StaticProfileDigest == (attachedworkerdaemon.ExecutableDigest{}) || frozen.launch.PolicyDigest == (attachedworkerdaemon.ExecutableDigest{}) ||
		frozen.launch.StaticProfileDigest == old.launch.StaticProfileDigest || frozen.launch.PolicyDigest == old.launch.PolicyDigest || frozen.descriptor == old.descriptor {
		t.Fatal("V3 invented scope, reused V2 qualification or failed to derive new identities")
	}
	actual, err := frozen.launch.InstallPolicyDigestV3()
	if err != nil || actual != frozen.launch.PolicyDigest {
		t.Fatalf("static installation policy not recomputed from V3 launch: %v", err)
	}
	want := sha256.Sum256(append(append([]byte(NativeProfileVersionV3+"\x00"), frozen.launch.StaticProfileDigest[:]...), actual[:]...))
	if frozen.descriptor.BackendProfileDigest != hex.EncodeToString(want[:]) {
		t.Fatal("backend descriptor does not commit V3 static and installation identities")
	}
	if !bytes.Equal(frozen.generated.configuration, old.generated.configuration) || !bytes.Equal(frozen.generated.catalog, old.generated.catalog) || !bytes.Equal(frozen.generated.schema, old.generated.schema) ||
		!reflect.DeepEqual(frozen.generated.arguments, old.generated.arguments) || !reflect.DeepEqual(frozen.generated.environment, old.generated.environment) ||
		frozen.launch.Limits != old.launch.Limits || frozen.launch.SocketDestination != old.launch.SocketDestination || frozen.launch.Catalog != old.launch.Catalog || frozen.launch.Schema != old.launch.Schema {
		t.Fatal("static successor changed generated commands/config/catalog/schema/environment or wire bounds")
	}
	for _, name := range []string{"DescriptorV2", "PolicyDigestV2", "ArgumentsV2"} {
		if _, exists := reflect.TypeOf(p).MethodByName(name); exists {
			t.Errorf("V3 exposes legacy qualification alias %s", name)
		}
	}
	if descriptor, err := p.DescriptorV3(); err != nil || descriptor != frozen.descriptor {
		t.Errorf("DescriptorV3 differs from private frozen identity: %v", err)
	}
	if digest, err := p.InstallationPolicyDigestV3(); err != nil || string(digest) != hex.EncodeToString(actual[:]) {
		t.Errorf("installation metadata accessor differs from canonical static digest: %v", err)
	}
	platform, err := p.NativeLauncherConfigV3()
	if err != nil || platform.Boundary != attachedworkeroci.BoundaryLinuxRootless || p.Platform.Boundary != attachedworkerlocal.BoundaryLinuxRootlessNativeV3 || attachedworkeroci.ValidateDeclarativeConfig(platform) == nil {
		t.Errorf("native platform lowering became a V1 compatibility interpretation: %v", err)
	}
	// Reusing declarative validation did not change the V2 API or its identity.
	after, err := p.declarativeInputV2().DescriptorV2()
	if err != nil || after != old.descriptor {
		t.Errorf("V3 preparation modified V2 identity: %v", err)
	}
}

func TestNativeProfileV3OwnsFrozenAndPublicCopies(t *testing.T) {
	p := nativeProfileFixtureV3(t)
	original := p.Clone()
	frozen, err := freezeNativeProfileV3(p)
	if err != nil {
		t.Fatal(err)
	}
	args, err := p.ArgumentsV3()
	if err != nil || !slices.Equal(args, frozen.launch.NativeArguments[0]) {
		t.Fatalf("arguments metadata differs: %v", err)
	}
	args[0] = "public-mutation"
	p.Launch.ProviderHosts[0] = "caller.example"
	if frozen.profile.Launch.ProviderHosts[0] != "auth.openai.com" || frozen.launch.ProviderHosts[0] != "auth.openai.com" || frozen.launch.NativeArguments[0][0] != "exec" || frozen.generated.arguments[0][0] != "exec" || original.Launch.ProviderHosts[0] != "auth.openai.com" {
		t.Fatal("caller/accessor/Clone aliases immutable frozen V3 values")
	}
	// Generated data and the lowered launch own independent copies as well.
	frozen.generated.arguments[0][0] = "private-generated-mutation"
	if frozen.launch.NativeArguments[0][0] != "exec" {
		t.Fatal("generated argument mutation changed frozen launch")
	}
	for _, change := range []func(*NativeProfileV3){
		func(p *NativeProfileV3) { p.Backend.Enabled = true },
		func(p *NativeProfileV3) { p.Backend.Now = func() time.Time { return time.Unix(1, 0) } },
	} {
		other := original.Clone()
		change(&other)
		got, err := other.DescriptorV3()
		if err != nil || got != frozen.descriptor {
			t.Errorf("activation/clock changed immutable identity: %v", err)
		}
	}
}

func TestNativeProfileV3EveryImmutableDeclarationChangesIdentity(t *testing.T) {
	p := nativeProfileFixtureV3(t)
	want, err := freezeNativeProfileV3(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*NativeProfileV3)
	}{
		{name: "model", change: func(p *NativeProfileV3) { p.Backend.Model = "another-model" }},
		{name: "architecture", change: func(p *NativeProfileV3) { p.Architecture = "arm64" }},
		{name: "context", change: func(p *NativeProfileV3) { p.DeclaredContextWindow++ }},
		{name: "reserve", change: func(p *NativeProfileV3) { p.OutputReserveTokens++ }},
		{name: "idle", change: func(p *NativeProfileV3) { p.StreamIdleTimeoutMS-- }},
		{name: "installation", change: func(p *NativeProfileV3) { p.Platform.InstallationID = "other-installation-0001" }},
		{name: "engine", change: func(p *NativeProfileV3) { p.Platform.EngineID = "other-engine-0001" }},
		{name: "engine-path", change: func(p *NativeProfileV3) { p.Platform.DockerPath = "/other/docker" }},
		{name: "socket", change: func(p *NativeProfileV3) { p.Platform.Host = "unix:///run/user/1000/other.sock" }},
		{name: "image", change: func(p *NativeProfileV3) {
			p.Platform.Image = "registry.example/native@sha256:" + strings.Repeat("c", 64)
		}},
		{name: "memory", change: func(p *NativeProfileV3) { p.Platform.MemoryBytes += 1 << 20 }},
		{name: "pids", change: func(p *NativeProfileV3) { p.Platform.PIDsLimit++ }},
		{name: "disk", change: func(p *NativeProfileV3) { p.Platform.DiskBytes += 1 << 20 }},
		{name: "credential-file-bound", change: func(p *NativeProfileV3) { p.Platform.CredentialFileBytes-- }},
		{name: "conduit", change: func(p *NativeProfileV3) { p.Launch.Limits.InboundBytes-- }},
		{name: "socket-destination", change: func(p *NativeProfileV3) { p.Launch.SocketDestination = "/conduit/other.sock" }},
		{name: "bootstrap-digest", change: func(p *NativeProfileV3) { p.Launch.Bootstrap.Digest[0]++ }},
		{name: "workload-digest", change: func(p *NativeProfileV3) {
			p.Launch.Workload.Digest[0]++
			p.Backend.ExecutableDigest = p.Launch.Workload.Digest
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := p.Clone()
			test.change(&other)
			got, err := freezeNativeProfileV3(other)
			if err != nil || got.descriptor == want.descriptor || got.launch.StaticProfileDigest == want.launch.StaticProfileDigest || got.launch.PolicyDigest == want.launch.PolicyDigest {
				t.Fatalf("changed %s failed to derive all V3 identities: %v", test.name, err)
			}
		})
	}
}

func TestNativeProfileV3RejectsUnqualifiedInputsBeforeMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*NativeProfileV3)
	}{
		{name: "old-boundary", change: func(p *NativeProfileV3) { p.Platform.Boundary = attachedworkerlocal.BoundaryLinuxRootless }},
		{name: "darwin", change: func(p *NativeProfileV3) { p.OperatingSystem = "darwin" }},
		{name: "unknown-architecture", change: func(p *NativeProfileV3) { p.Architecture = "other" }},
		{name: "uid", change: func(p *NativeProfileV3) { p.Platform.UserID = 1000 }},
		{name: "raw-command", change: func(p *NativeProfileV3) { p.Launch.NativeArguments[0] = []string{"exec", "-"} }},
		{name: "raw-bootstrap", change: func(p *NativeProfileV3) { p.Launch.BootstrapArguments[0] = []string{"bootstrap"} }},
		{name: "raw-relay", change: func(p *NativeProfileV3) { p.Launch.RelayArguments = []string{"relay"} }},
		{name: "scope", change: func(p *NativeProfileV3) { p.Launch.ScopeDigest[0] = 1 }},
		{name: "foreign-install-digest", change: func(p *NativeProfileV3) { p.Launch.PolicyDigest[0] = 1 }},
		{name: "foreign-static-digest", change: func(p *NativeProfileV3) { p.Launch.StaticProfileDigest[0] = 1 }},
		{name: "provider-host", change: func(p *NativeProfileV3) { p.Launch.ProviderHosts = []string{"chatgpt.com"} }},
		{name: "memory-bound", change: func(p *NativeProfileV3) { p.Platform.MemoryBytes = 1<<30 + 1 }},
		{name: "pids-bound", change: func(p *NativeProfileV3) { p.Platform.PIDsLimit = 65 }},
		{name: "disk-bound", change: func(p *NativeProfileV3) { p.Platform.DiskBytes = 65 << 20 }},
		{name: "cleanup-bound", change: func(p *NativeProfileV3) { p.Launch.Limits.Cleanup = 11 * time.Second }},
		{name: "active-bound", change: func(p *NativeProfileV3) { p.Launch.Limits.Active = 61 * time.Second }},
		{name: "context-zero", change: func(p *NativeProfileV3) { p.DeclaredContextWindow = 0 }},
		{name: "reserve-bound", change: func(p *NativeProfileV3) { p.OutputReserveTokens = p.DeclaredContextWindow }},
		{name: "idle-bound", change: func(p *NativeProfileV3) { p.StreamIdleTimeoutMS = 60001 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := nativeProfileFixtureV3(t)
			test.change(&p)
			if got, err := p.DescriptorV3(); !errors.Is(err, ErrContract) || got.BackendProfileDigest != "" {
				t.Errorf("invalid %s emitted descriptor: %+v err=%v", test.name, got, err)
			}
			if got, err := p.InstallationPolicyDigestV3(); !errors.Is(err, ErrContract) || got != "" {
				t.Errorf("invalid %s emitted installation digest: %s err=%v", test.name, got, err)
			}
			if got, err := p.ArgumentsV3(); !errors.Is(err, ErrContract) || got != nil {
				t.Errorf("invalid %s emitted arguments: err=%v", test.name, err)
			}
		})
	}
}
