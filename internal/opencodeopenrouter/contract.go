// Package opencodeopenrouter implements the closed, feature-disabled OpenCode/OpenRouter
// backend below the Sessionless-owned harness registry.
package opencodeopenrouter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	HarnessVersionV1         = "1"
	NativeProtocolVersionV1  = "opencode-jsonl.v1"
	OpenCodeSourceRevisionV1 = "3a31c4ea801915c0b050df4b3842997ea62b6e93"
	OpenRouterBaseURLV1      = "https://openrouter.ai/api/v1"
	ProviderIDV1             = "openrouter"
	ModelVendorIDV1          = "stealth"
	ModelIDV1                = "stealth/ox-alpha"
	CredentialEnvironmentV1  = "OPENROUTER_API_KEY"

	maxPromptBytes = 1 << 20
	maxOutputBytes = 16 << 20
	maxStderrBytes = 1 << 20
	maxLineBytes   = 1 << 20
	maxEvents      = 4096
	maxFinalBytes  = 256 << 10
	maxJSONDepth   = 32
)

var (
	ErrDisabled = errors.New("opencode openrouter backend is disabled")
	ErrContract = errors.New("opencode openrouter backend contract is invalid")
)

// ProfileV1 is an immutable process/profile pin. Enabled is a reversible local
// feature gate; it is excluded from the backend profile digest.
type ProfileV1 struct {
	Enabled           bool
	Executable        string
	ExecutableVersion string
	ExecutableDigest  attachedworkerdaemon.ExecutableDigest
	SourceRevision    string
	ProviderTimeoutMS uint64
}

func (profile ProfileV1) validate() error {
	if !filepath.IsAbs(profile.Executable) || filepath.Clean(profile.Executable) != profile.Executable ||
		profile.ExecutableVersion == "" || len(profile.ExecutableVersion) > 128 ||
		profile.ExecutableDigest == (attachedworkerdaemon.ExecutableDigest{}) ||
		profile.SourceRevision != OpenCodeSourceRevisionV1 || profile.ProviderTimeoutMS == 0 || profile.ProviderTimeoutMS > 300_000 {
		return ErrContract
	}
	for _, character := range profile.ExecutableVersion {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '.' && character != '-' && character != '_' {
			return ErrContract
		}
	}
	return nil
}

func (profile ProfileV1) descriptor() (domain.HarnessBackendDescriptorV1, error) {
	if err := profile.validate(); err != nil {
		return domain.HarnessBackendDescriptorV1{}, err
	}
	files, err := generatedFiles(profile)
	if err != nil || len(files) != 1 {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	configDigest := sha256.Sum256(files[0].Content)
	material := struct {
		Schema                string               `json:"schema"`
		SourceRevision        string               `json:"source_revision"`
		ExecutableVersion     string               `json:"executable_version"`
		ExecutableDigest      string               `json:"executable_digest"`
		Provider              string               `json:"provider"`
		BaseURL               string               `json:"base_url"`
		ModelVendor           string               `json:"model_vendor"`
		Model                 string               `json:"model"`
		RouteProviders        []string             `json:"route_providers"`
		AllowFallbacks        bool                 `json:"allow_fallbacks"`
		RequireParameters     bool                 `json:"require_parameters"`
		MaxProviderEffects    uint64               `json:"max_provider_effects"`
		ProviderTimeoutMS     uint64               `json:"provider_timeout_ms"`
		Arguments             []string             `json:"arguments"`
		Environment           []EnvironmentV1      `json:"environment"`
		PrivateDirectories    []PrivateDirectoryV1 `json:"private_directories"`
		GeneratedConfigDigest string               `json:"generated_config_digest"`
	}{
		Schema: "sessionless.opencode-openrouter-profile.v1", SourceRevision: profile.SourceRevision,
		ExecutableVersion: profile.ExecutableVersion, ExecutableDigest: hex.EncodeToString(profile.ExecutableDigest[:]),
		Provider: ProviderIDV1, BaseURL: OpenRouterBaseURLV1, ModelVendor: ModelVendorIDV1, Model: ModelIDV1,
		RouteProviders: []string{"stealth"}, AllowFallbacks: false, RequireParameters: true, MaxProviderEffects: 1,
		ProviderTimeoutMS: profile.ProviderTimeoutMS, Arguments: processArguments(), Environment: processEnvironment(),
		PrivateDirectories: privateDirectories(), GeneratedConfigDigest: hex.EncodeToString(configDigest[:]),
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	profileDigest := sha256.Sum256(encoded)
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: HarnessVersionV1,
		BackendKind: domain.HarnessBackendOpenCodeV1, ArtifactKind: domain.HarnessArtifactExecutableV1,
		ArtifactDigest: hex.EncodeToString(profile.ExecutableDigest[:]), NativeProtocolVersion: NativeProtocolVersionV1,
		BackendProfileDigest: hex.EncodeToString(profileDigest[:]), ProviderContractKind: domain.ProviderContractInvocationV1,
		CredentialDeliveryKind: domain.ProviderCredentialDeliveryEnvironmentV1,
	}
	if err := descriptor.Validate(); err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	return descriptor, nil
}

type EnvironmentV1 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type PrivateDirectoryV1 struct {
	EnvironmentName              string `json:"environment_name"`
	Purpose                      string `json:"purpose"`
	ReadOnlyAfterMaterialization bool   `json:"read_only_after_materialization"`
}

type GeneratedFileV1 struct {
	DirectoryEnvironmentName string
	RelativeDirectory        string
	Name                     string
	Content                  []byte
}

// ProcessInvocationV1 contains authority and an invocation-only credential
// handle, never secret bytes. The isolated boundary creates the private OpenCode
// directory and injects the named credential environment variable.
type ProcessInvocationV1 struct {
	Identity                       ports.ExecutionIdentity                   `json:"-"`
	Credential                     ports.ProviderInvocationCredentialV1      `json:"-"`
	CredentialMaterialization      ports.ProviderCredentialMaterializationV1 `json:"-"`
	Executable                     string
	ExecutableDigest               attachedworkerdaemon.ExecutableDigest
	Arguments                      []string
	Environment                    []EnvironmentV1
	PrivateDirectories             []PrivateDirectoryV1
	GeneratedFiles                 []GeneratedFileV1
	RequirePrivateWorkingDirectory bool
	RequireSanitizedEnvironment    bool
	RequireNoAmbientHome           bool
	RequireProviderEffectFence     bool
	MaxProviderEffects             uint64
	Stdin                          []byte `json:"-"`
}

func (invocation ProcessInvocationV1) String() string {
	return fmt.Sprintf("OpenCodeOpenRouterInvocation{tenant:%s owner:%s run:%s attempt:%s executable:[redacted] args:%d env:%d private_dirs:%d files:%d stdin:[redacted:%d] credential:[redacted]}",
		invocation.Identity.TenantID, invocation.Identity.OwnerUserID, invocation.Identity.RunID,
		invocation.Identity.AttemptID, len(invocation.Arguments),
		len(invocation.Environment), len(invocation.PrivateDirectories), len(invocation.GeneratedFiles), len(invocation.Stdin))
}

func (invocation ProcessInvocationV1) GoString() string { return invocation.String() }

type ProcessResultV1 struct {
	ExitCode                     int
	StdoutBytes                  int
	StderrBytes                  int
	OutputLimitExceeded          bool
	Cancelled                    bool
	Deadline                     bool
	ProcessStopped               bool
	DescendantsStopped           bool
	CleanupSucceeded             bool
	PrivateStateRemoved          bool
	CredentialFinalized          bool
	ProviderEffectFenceSatisfied bool
	ProviderEffects              uint64
	FailureCode                  string
	Stdout                       []byte `json:"-"`
}

func (result ProcessResultV1) String() string {
	return fmt.Sprintf("OpenCodeOpenRouterProcessResult{exit:%d stdout_bytes:%d stderr_bytes:%d output_limit:%t cancelled:%t deadline:%t stopped:%t descendants_stopped:%t cleanup:%t private_state_removed:%t credential_finalized:%t effect_fence:%t provider_effects:%d failure_present:%t stdout:[redacted:%d]}",
		result.ExitCode, result.StdoutBytes, result.StderrBytes, result.OutputLimitExceeded, result.Cancelled, result.Deadline, result.ProcessStopped,
		result.DescendantsStopped, result.CleanupSucceeded, result.PrivateStateRemoved, result.CredentialFinalized,
		result.ProviderEffectFenceSatisfied, result.ProviderEffects, result.FailureCode != "", len(result.Stdout))
}

func (result ProcessResultV1) GoString() string { return result.String() }

type ProcessBoundaryV1 interface {
	Run(context.Context, ProcessInvocationV1) (ProcessResultV1, error)
	Cancel(context.Context, ports.ExecutionIdentity) error
}
