// Package codexopenrouter implements the closed, feature-disabled
// Codex/OpenRouter backend below the Sessionless-owned harness registry.
package codexopenrouter

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
	NativeProtocolVersionV1  = "codex-jsonl.v1"
	OpenRouterBaseURLV1      = "https://openrouter.ai/api/v1"
	ProviderIDV1             = "openrouter"
	ModelVendorIDV1          = "stealth"
	ModelIDV1                = "stealth/ox-alpha"
	CredentialEnvironmentV1  = "OPENROUTER_API_KEY"
	ConfigEnvironmentV1      = "CODEX_HOME"
	ConfigMountPathV1        = "/sessionless/invocation/codex-home"
	ModelCatalogPathV1       = ConfigMountPathV1 + "/models.json"
	maxPromptBytes           = 1 << 20
	maxOutputBytes           = 16 << 20
	maxStderrBytes           = 1 << 20
	maxProviderEffects       = 1
	defaultContextWindowV1   = 65_536
	defaultMaxOutputTokensV1 = 8_192
)

var (
	ErrDisabled = errors.New("codex openrouter backend is disabled")
	ErrContract = errors.New("codex openrouter backend contract is invalid")
)

// ProfileV1 is an immutable Codex binary and provider configuration pin.
// Enabled is a reversible local gate and is excluded from the profile digest.
type ProfileV1 struct {
	Enabled           bool
	Executable        string
	ExecutableVersion string
	ExecutableDigest  attachedworkerdaemon.ExecutableDigest
	ProviderTimeoutMS uint64
	ContextWindow     uint64
	MaxOutputTokens   uint64
}

func (profile ProfileV1) validate() error {
	if !filepath.IsAbs(profile.Executable) || filepath.Clean(profile.Executable) != profile.Executable ||
		profile.ExecutableVersion == "" || len(profile.ExecutableVersion) > 128 ||
		profile.ExecutableDigest == (attachedworkerdaemon.ExecutableDigest{}) ||
		profile.ProviderTimeoutMS == 0 || profile.ProviderTimeoutMS > 300_000 ||
		profile.ContextWindow == 0 || profile.ContextWindow > 1_000_000 ||
		profile.MaxOutputTokens == 0 || profile.MaxOutputTokens >= profile.ContextWindow {
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
	if err != nil || len(files) != 2 {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	configDigest := sha256.Sum256(files[0].Content)
	catalogDigest := sha256.Sum256(files[1].Content)
	material := struct {
		Schema                string          `json:"schema"`
		ExecutableVersion     string          `json:"executable_version"`
		ExecutableDigest      string          `json:"executable_digest"`
		Provider              string          `json:"provider"`
		BaseURL               string          `json:"base_url"`
		ModelVendor           string          `json:"model_vendor"`
		Model                 string          `json:"model"`
		WireAPI               string          `json:"wire_api"`
		CredentialEnvironment string          `json:"credential_environment"`
		RequestMaxRetries     uint64          `json:"request_max_retries"`
		StreamMaxRetries      uint64          `json:"stream_max_retries"`
		ProviderTimeoutMS     uint64          `json:"provider_timeout_ms"`
		MaxProviderEffects    uint64          `json:"max_provider_effects"`
		Arguments             []string        `json:"arguments"`
		Environment           []EnvironmentV1 `json:"environment"`
		ConfigDigest          string          `json:"config_digest"`
		ModelCatalogDigest    string          `json:"model_catalog_digest"`
	}{
		Schema: "sessionless.codex-openrouter-profile.v1", ExecutableVersion: profile.ExecutableVersion,
		ExecutableDigest: hex.EncodeToString(profile.ExecutableDigest[:]), Provider: ProviderIDV1,
		BaseURL: OpenRouterBaseURLV1, ModelVendor: ModelVendorIDV1, Model: ModelIDV1, WireAPI: "responses",
		CredentialEnvironment: CredentialEnvironmentV1, RequestMaxRetries: 0, StreamMaxRetries: 0,
		ProviderTimeoutMS: profile.ProviderTimeoutMS, MaxProviderEffects: maxProviderEffects,
		Arguments: processArguments(), Environment: processEnvironment(),
		ConfigDigest: hex.EncodeToString(configDigest[:]), ModelCatalogDigest: hex.EncodeToString(catalogDigest[:]),
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	profileDigest := sha256.Sum256(encoded)
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: HarnessVersionV1,
		BackendKind: domain.HarnessBackendCodexOpenRouterV1, ArtifactKind: domain.HarnessArtifactExecutableV1,
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

type GeneratedFileV1 struct {
	Name    string
	Content []byte
}

// ProcessInvocationV1 is consumed by the reviewed isolated process boundary.
// It carries credential authority and generated config, never secret bytes.
type ProcessInvocationV1 struct {
	Identity                       ports.ExecutionIdentity                   `json:"-"`
	Credential                     ports.ProviderInvocationCredentialV1      `json:"-"`
	CredentialMaterialization      ports.ProviderCredentialMaterializationV1 `json:"-"`
	Executable                     string
	ExecutableDigest               attachedworkerdaemon.ExecutableDigest
	Arguments                      []string
	Environment                    []EnvironmentV1
	GeneratedFiles                 []GeneratedFileV1
	ConfigDirectoryEnvironmentName string
	ConfigDirectoryMountPath       string
	RequirePrivateWorkingDirectory bool
	RequireSanitizedEnvironment    bool
	RequireNoAmbientHome           bool
	RequireProviderEffectFence     bool
	MaxProviderEffects             uint64
	Stdin                          []byte `json:"-"`
}

func (invocation ProcessInvocationV1) String() string {
	return fmt.Sprintf("CodexOpenRouterInvocation{tenant:%s owner:%s run:%s attempt:%s executable:[redacted] args:%d env:%d files:%d stdin:[redacted:%d] credential:[redacted]}",
		invocation.Identity.TenantID, invocation.Identity.OwnerUserID, invocation.Identity.RunID,
		invocation.Identity.AttemptID, len(invocation.Arguments), len(invocation.Environment),
		len(invocation.GeneratedFiles), len(invocation.Stdin))
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
	return fmt.Sprintf("CodexOpenRouterProcessResult{exit:%d stdout_bytes:%d stderr_bytes:%d output_limit:%t cancelled:%t deadline:%t stopped:%t descendants_stopped:%t cleanup:%t private_state_removed:%t credential_finalized:%t effect_fence:%t provider_effects:%d failure_present:%t stdout:[redacted:%d]}",
		result.ExitCode, result.StdoutBytes, result.StderrBytes, result.OutputLimitExceeded,
		result.Cancelled, result.Deadline, result.ProcessStopped, result.DescendantsStopped,
		result.CleanupSucceeded, result.PrivateStateRemoved, result.CredentialFinalized,
		result.ProviderEffectFenceSatisfied, result.ProviderEffects, result.FailureCode != "", len(result.Stdout))
}

func (result ProcessResultV1) GoString() string { return result.String() }

type ProcessBoundaryV1 interface {
	Run(context.Context, ProcessInvocationV1) (ProcessResultV1, error)
	Cancel(context.Context, ports.ExecutionIdentity) error
}
