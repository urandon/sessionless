// Package piopenrouter implements the closed, feature-disabled Pi/OpenRouter
// backend below the Sessionless-owned harness registry.
package piopenrouter

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
	HarnessVersionV1        = "1"
	NativeProtocolVersionV1 = "pi-rpc-jsonl.v1"
	PiSourceRevisionV1      = "6c4f360264397c59801f6da2bdac13e3b1fcbe91"
	OpenRouterBaseURLV1     = "https://openrouter.ai/api/v1"
	ProviderIDV1            = "sessionless-openrouter"
	ModelVendorIDV1         = "stealth"
	ModelIDV1               = "stealth/ox-alpha"
	CredentialEnvironmentV1 = "OPENROUTER_API_KEY"

	maxPromptBytes = 1 << 20
	maxOutputBytes = 16 << 20
	maxStderrBytes = 1 << 20
	maxLineBytes   = 1 << 20
	maxEvents      = 4096
	maxFinalBytes  = 256 << 10
	maxJSONDepth   = 32
)

var (
	ErrDisabled = errors.New("pi openrouter backend is disabled")
	ErrContract = errors.New("pi openrouter backend contract is invalid")
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
		profile.SourceRevision != PiSourceRevisionV1 || profile.ProviderTimeoutMS == 0 || profile.ProviderTimeoutMS > 300_000 {
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
	material := struct {
		Schema            string   `json:"schema"`
		SourceRevision    string   `json:"source_revision"`
		ExecutableVersion string   `json:"executable_version"`
		ExecutableDigest  string   `json:"executable_digest"`
		Provider          string   `json:"provider"`
		BaseURL           string   `json:"base_url"`
		ModelVendor       string   `json:"model_vendor"`
		Model             string   `json:"model"`
		RouteProviders    []string `json:"route_providers"`
		AllowFallbacks    bool     `json:"allow_fallbacks"`
		RequireParameters bool     `json:"require_parameters"`
		ProviderTimeoutMS uint64   `json:"provider_timeout_ms"`
	}{
		Schema: "sessionless.pi-openrouter-profile.v1", SourceRevision: profile.SourceRevision,
		ExecutableVersion: profile.ExecutableVersion, ExecutableDigest: hex.EncodeToString(profile.ExecutableDigest[:]),
		Provider: ProviderIDV1, BaseURL: OpenRouterBaseURLV1, ModelVendor: ModelVendorIDV1, Model: ModelIDV1,
		RouteProviders: []string{"stealth"}, AllowFallbacks: false, RequireParameters: true,
		ProviderTimeoutMS: profile.ProviderTimeoutMS,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	profileDigest := sha256.Sum256(encoded)
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: HarnessVersionV1,
		BackendKind: domain.HarnessBackendPiV1, ArtifactKind: domain.HarnessArtifactExecutableV1,
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
	Name  string
	Value string
}

type GeneratedFileV1 struct {
	Name    string
	Content []byte
}

// ProcessInvocationV1 contains authority and an invocation-only credential
// handle, never secret bytes. The isolated boundary creates the private Pi
// directory and injects the named credential environment variable.
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
	RequirePrivateWorkingDirectory bool
	RequireSanitizedEnvironment    bool
	Stdin                          []byte `json:"-"`
}

func (invocation ProcessInvocationV1) String() string {
	return fmt.Sprintf("PiOpenRouterInvocation{tenant:%s owner:%s run:%s attempt:%s executable:%s args:%d env:%d files:%d stdin:[redacted:%d] credential:[redacted]}",
		invocation.Identity.TenantID, invocation.Identity.OwnerUserID, invocation.Identity.RunID,
		invocation.Identity.AttemptID, invocation.Executable, len(invocation.Arguments),
		len(invocation.Environment), len(invocation.GeneratedFiles), len(invocation.Stdin))
}

func (invocation ProcessInvocationV1) GoString() string { return invocation.String() }

type ProcessResultV1 struct {
	ExitCode            int
	StdoutBytes         int
	StderrBytes         int
	OutputLimitExceeded bool
	Cancelled           bool
	Deadline            bool
	ProcessStopped      bool
	CleanupSucceeded    bool
	CredentialFinalized bool
	FailureCode         string
	Stdout              []byte `json:"-"`
}

func (result ProcessResultV1) String() string {
	return fmt.Sprintf("PiOpenRouterProcessResult{exit:%d stdout_bytes:%d stderr_bytes:%d output_limit:%t cancelled:%t deadline:%t stopped:%t cleanup:%t credential_finalized:%t failure:%s stdout:[redacted:%d]}",
		result.ExitCode, result.StdoutBytes, result.StderrBytes, result.OutputLimitExceeded, result.Cancelled, result.Deadline, result.ProcessStopped,
		result.CleanupSucceeded, result.CredentialFinalized, result.FailureCode, len(result.Stdout))
}

func (result ProcessResultV1) GoString() string { return result.String() }

type ProcessBoundaryV1 interface {
	Run(context.Context, ProcessInvocationV1) (ProcessResultV1, error)
	Cancel(context.Context, ports.ExecutionIdentity) error
}
