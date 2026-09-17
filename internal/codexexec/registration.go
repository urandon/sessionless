package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

const (
	HarnessVersionV1            = "1"
	NativeProtocolVersionV1     = "codex.exec-jsonl.v1"
	ModelVendorIDV1             = "openai"
	CredentialHomeEnvironmentV1 = "CODEX_HOME"
)

// DescriptorV1 identifies the exact subscription-backed Codex exec profile.
// Enabled is deliberately excluded: disabling a profile must not change the
// identity needed to route bounded cancellation for previously admitted work.
func (adapter *Adapter) DescriptorV1() domain.HarnessBackendDescriptorV1 {
	if adapter == nil {
		return domain.HarnessBackendDescriptorV1{}
	}
	return adapter.descriptor
}

// DisabledRegistrationV1 adds the production-shaped driver to the closed
// registry without authorizing provider execution. Exact cancellation still
// reaches its boundary so a previously admitted attempt can be torn down.
func DisabledRegistrationV1(driver *Driver) (sessionlessharness.Registration, error) {
	return registrationV1(driver, false)
}

func registrationV1(driver *Driver, enabled bool) (sessionlessharness.Registration, error) {
	if driver == nil || driver.config.Enabled != enabled {
		return sessionlessharness.Registration{}, ErrContract
	}
	return sessionlessharness.Registration{
		Descriptor: driver.descriptor, Enabled: enabled, Driver: driver,
		ValidateBinding: driver.validateHarnessBinding,
	}, nil
}

func descriptorV1(config Config) (domain.HarnessBackendDescriptorV1, error) {
	artifactDigest := hex.EncodeToString(config.ExecutableDigest[:])
	profile := sha256.New()
	for _, field := range append([]string{
		"sessionless.codex-exec.subscription-profile.v1",
		config.ExecutableVersion,
		artifactDigest,
		config.Model,
		fmt.Sprint(maxInstructionBytes),
		fmt.Sprint(maxJSONLLineBytes),
		fmt.Sprint(maxJSONLOutputBytes),
		fmt.Sprint(maxProcessStderrBytesV1),
		fmt.Sprint(maxJSONLEvents),
		fmt.Sprint(maxFinalBytes),
		fmt.Sprint(maxJSONDepth),
		"credential_home=" + CredentialHomeEnvironmentV1,
		"billing_route=" + string(ObservationUnknown),
		"quota=" + string(ObservationUnknown),
		"cancel=exact_attached_worker_boundary_v1",
	}, processArguments(config.Model)...) {
		_, _ = fmt.Fprintf(profile, "%d:%s", len(field), field)
	}
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: HarnessVersionV1,
		BackendKind:            domain.HarnessBackendCodexExecV1,
		ArtifactKind:           domain.HarnessArtifactExecutableV1,
		ArtifactDigest:         artifactDigest,
		NativeProtocolVersion:  NativeProtocolVersionV1,
		BackendProfileDigest:   hex.EncodeToString(profile.Sum(nil)),
		ProviderContractKind:   domain.ProviderContractInvocationV1,
		CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1,
	}
	if err := descriptor.Validate(); err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	return descriptor, nil
}

func processArguments(model string) []string {
	return []string{
		"exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules",
		"--strict-config", "--sandbox", "read-only", "--skip-git-repo-check",
		"--color", "never", "--model", model, "-",
	}
}

func (driver *Driver) validateHarnessBinding(binding domain.HarnessBindingV1) sessionlessharness.FailureCode {
	if driver == nil || binding.Validate() != nil {
		return sessionlessharness.FailureHarnessBindingInvalid
	}
	if binding.Backend != driver.descriptor {
		return sessionlessharness.FailureHarnessBackendMismatch
	}
	if binding.Resource.Kind != domain.ProviderResourceSubscriptionV1 ||
		binding.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 ||
		domain.SubscriptionConnectionID(binding.Resource.ResourceID).Validate() != nil {
		return sessionlessharness.FailureProviderResourceMismatch
	}
	if binding.ModelVendorID != ModelVendorIDV1 || binding.ModelID != driver.config.Model {
		return sessionlessharness.FailureProviderCatalogExpired
	}
	return ""
}

func disabledHarnessError() error {
	return &domain.ClassifiedError{
		Kind: domain.ErrorTerminal, Code: string(sessionlessharness.FailureHarnessBackendDisabled),
		Operation: "codex_exec.subscription_registry",
	}
}

func (*Adapter) BackendProtocolState() harnessconformance.BackendProtocolStateV1 {
	// The legacy invocation adapter remains outside the canonical registry.
	return harnessconformance.BackendProtocolUnsupportedV1
}

var _ harnessconformance.BackendProtocolObserver = (*Adapter)(nil)
