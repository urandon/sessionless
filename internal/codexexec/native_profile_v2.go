package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

const NativeProfileVersionV2 = "sessionless.codex-native-envelope.v2"

// NativeProfileV2 is an explicit installation, not an admission or activation
// grant. No raw config, arbitrary argv or environment override is accepted.
// DeclaredContextWindow and OutputReserveTokens are declarations, not an upper
// bound on native context fit or an applied native max-output-token setting.
type NativeProfileV2 struct {
	Backend               Config
	Launch                attachedworkerdaemon.NativeLaunchV3
	Platform              attachedworkerlocal.OCIConfigV1
	OperatingSystem       string
	Architecture          string
	DeclaredContextWindow uint64
	OutputReserveTokens   uint64
	StreamIdleTimeoutMS   uint64
}

func (p NativeProfileV2) Clone() NativeProfileV2 {
	p.Launch = p.Launch.Clone()
	return p
}

type nativeGeneratedV2 struct {
	configuration []byte
	catalog       []byte
	schema        []byte
	arguments     [7][]string
	environment   []attachedworkerdaemon.EnvironmentVariable
}

type nativeFrozenProfileV2 struct {
	profile    NativeProfileV2
	generated  nativeGeneratedV2
	launch     attachedworkerdaemon.NativeLaunchV3
	descriptor domain.HarnessBackendDescriptorV1
}

// NativeLauncherConfigV3 lowers only the explicit installed native boundary.
// The original discriminant remains sealed in DescriptorV2. Ordinary manifest
// readers/launchers must not globally normalize it into the V1 contract.
func (p NativeProfileV2) NativeLauncherConfigV3() (attachedworkeroci.Config, error) {
	v := p.Platform
	if v.Boundary != attachedworkerlocal.BoundaryLinuxRootlessNativeV3 {
		return attachedworkeroci.Config{}, ErrContract
	}
	c := attachedworkeroci.Config{DockerPath: v.DockerPath, DockerSHA256: v.DockerSHA256, CLIConfigDir: v.CLIConfigDir, Host: v.Host,
		EngineID: v.EngineID, InstallationID: v.InstallationID, Boundary: attachedworkeroci.BoundaryLinuxRootless, Image: v.Image,
		UserID: v.UserID, GroupID: v.GroupID, DiskBytes: v.DiskBytes, CredentialFileBytes: v.CredentialFileBytes,
		MemoryBytes: v.MemoryBytes, PIDsLimit: v.PIDsLimit, StopSeconds: v.StopSeconds}
	if attachedworkeroci.ValidateNativeDeclarativeConfigV3(c) != nil {
		return attachedworkeroci.Config{}, ErrContract
	}
	return c, nil
}

func (p NativeProfileV2) DescriptorV2() (domain.HarnessBackendDescriptorV1, error) {
	v, err := freezeNativeProfileV2(p)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, err
	}
	return v.descriptor, nil
}

// ArgumentsV2 is the zero-image installed command. The original consumed owner
// alone selects one of its seven sealed variants from verified canonical media.
func (p NativeProfileV2) ArgumentsV2() ([]string, error) {
	v, err := freezeNativeProfileV2(p)
	if err != nil {
		return nil, err
	}
	return slices.Clone(v.launch.NativeArguments[0]), nil
}

func freezeNativeProfileV2(p NativeProfileV2) (*nativeFrozenProfileV2, error) {
	base, err := prepareConfig(p.Backend)
	if err != nil || p.Backend.ExecutableVersion != "0.162.1" || p.OperatingSystem != "linux" ||
		(p.Architecture != "amd64" && p.Architecture != "arm64") || p.DeclaredContextWindow == 0 || p.DeclaredContextWindow > math.MaxInt64 ||
		p.OutputReserveTokens == 0 || p.OutputReserveTokens >= p.DeclaredContextWindow || p.StreamIdleTimeoutMS == 0 || p.StreamIdleTimeoutMS > 60_000 {
		return nil, ErrContract
	}
	platform, err := p.NativeLauncherConfigV3()
	if err != nil || platform.MemoryBytes > 1<<30 || platform.PIDsLimit > 64 || platform.DiskBytes > 64<<20 || platform.StopSeconds > 10 ||
		p.Launch.Version != 3 || p.Launch.Workload.Executable != p.Backend.Executable || p.Launch.Workload.Digest != p.Backend.ExecutableDigest ||
		p.Launch.ScopeDigest != (attachedworkerdaemon.ExecutableDigest{}) || p.Launch.PolicyDigest != (attachedworkerdaemon.ExecutableDigest{}) ||
		p.Launch.StaticProfileDigest != (attachedworkerdaemon.ExecutableDigest{}) ||
		p.Launch.Limits.Active > time.Minute || p.Launch.Limits.Cleanup > 10*time.Second ||
		!slices.Equal(p.Launch.ProviderHosts, []string{"auth.openai.com", "chatgpt.com"}) {
		return nil, ErrContract
	}
	// Caller-supplied commands or file digests are not a generated installation.
	if len(p.Launch.RelayArguments) != 0 || p.Launch.Catalog != (nativeharnessinput.FileV3{}) || p.Launch.Schema != (nativeharnessinput.FileV3{}) {
		return nil, ErrContract
	}
	for i := range p.Launch.NativeArguments {
		if len(p.Launch.NativeArguments[i]) != 0 || len(p.Launch.BootstrapArguments[i]) != 0 {
			return nil, ErrContract
		}
	}
	p = p.Clone()
	g, err := generateNativeProfileV2(p)
	if err != nil || len(g.configuration) == 0 || len(g.configuration) > 128<<10 || len(g.catalog) == 0 || uint64(len(g.catalog)) > nativeharnessinput.MaxFileBytes || len(g.schema) == 0 || uint64(len(g.schema)) > nativeharnessinput.MaxFileBytes {
		return nil, ErrContract
	}
	file := func(body []byte) nativeharnessinput.FileV3 {
		h := sha256.Sum256(body)
		return nativeharnessinput.FileV3{Size: uint64(len(body)), SHA256: hex.EncodeToString(h[:])}
	}
	launch := p.Launch.Clone()
	launch.NativeArguments, launch.Catalog, launch.Schema = g.arguments, file(g.catalog), file(g.schema)
	launch.BootstrapArguments, launch.RelayArguments, err = nativeharnessbootstrap.InstallationArgumentsV3(launch)
	if err != nil {
		return nil, ErrContract
	}
	raw, err := json.Marshal(struct {
		Version, ExecutableVersion, Model, OS, Architecture, PolicySemantics, EnvironmentABI string
		ContextWindow, OutputReserve, StreamIdleMS                                           uint64
		Platform                                                                             attachedworkerlocal.OCIConfigV1
		Launch                                                                               attachedworkerdaemon.NativeLaunchV3
		Configuration, Catalog, Schema                                                       []byte
		Environment                                                                          []attachedworkerdaemon.EnvironmentVariable
		MediaBounds                                                                          [9]int
		ProtocolBounds                                                                       [7]int
		OutputBounds                                                                         [5]int
	}{NativeProfileVersionV2, p.Backend.ExecutableVersion, p.Backend.Model, p.OperatingSystem, p.Architecture, nativePolicySemanticsV2, nativeEnvironmentABIV2,
		p.DeclaredContextWindow, p.OutputReserveTokens, p.StreamIdleTimeoutMS, p.Platform, launch, g.configuration, g.catalog, g.schema, g.environment,
		[9]int{2, mediaTextBytes, mediaTextTotal, 2, mediaImageBytes, mediaImageTotal, mediaPixels, mediaRGBATotal, mediaValidationMemory},
		[7]int{maxInstructionBytes, maxJSONLLineBytes, maxJSONLOutputBytes, maxJSONLEvents, maxFinalBytes, maxJSONDepth, maxProcessStderrBytesV1},
		[5]int{nativeFinalBytesV2, nativeAnswerBytesV2, nativeArtifactBytesV2, nativeArtifactTotalV2, nativeArtifactCountV2}})
	if err != nil {
		return nil, ErrContract
	}
	launch.StaticProfileDigest = attachedworkerdaemon.ExecutableDigest(sha256.Sum256(append([]byte(NativeProfileVersionV2+"/static\x00"), raw...)))
	launch.PolicyDigest, err = launch.InstallPolicyDigestV3()
	if err != nil {
		return nil, ErrContract
	}
	// A local syntax sentinel only, never returned as runtime scope authority.
	check := launch.Clone()
	check.ScopeDigest = attachedworkerdaemon.ExecutableDigest{1}
	if nativeharnessbootstrap.ValidateInstallationV3(check) != nil {
		return nil, ErrContract
	}
	hash := sha256.Sum256(append(append([]byte(NativeProfileVersionV2+"\x00"), launch.StaticProfileDigest[:]...), launch.PolicyDigest[:]...))
	base.BackendProfileDigest = hex.EncodeToString(hash[:])
	if base.Validate() != nil {
		return nil, ErrContract
	}
	return &nativeFrozenProfileV2{profile: p, generated: g, launch: launch, descriptor: base}, nil
}
