package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
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

// DisabledRegistrationV1 adds the exact subscription profile to the closed
// Sessionless registry without authorizing provider execution. The bridge is
// intentionally cancellation-only: a disabled registration cannot have
// started work through this registry, so exact cancellation is a bounded no-op.
func DisabledRegistrationV1(adapter *Adapter) (sessionlessharness.Registration, error) {
	if adapter == nil || adapter.config.Enabled {
		return sessionlessharness.Registration{}, ErrContract
	}
	driver := &disabledRegistryDriver{adapter: adapter}
	return sessionlessharness.Registration{
		Descriptor: adapter.descriptor, Enabled: false, Driver: driver,
		ValidateBinding: adapter.validateHarnessBinding,
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
		fmt.Sprint(maxJSONLEvents),
		fmt.Sprint(maxFinalBytes),
		fmt.Sprint(maxJSONDepth),
		"credential_home=" + CredentialHomeEnvironmentV1,
		"billing_route=" + string(ObservationUnknown),
		"quota=" + string(ObservationUnknown),
		"disabled_cancel=bounded_noop_v1",
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

func (adapter *Adapter) validateHarnessBinding(binding domain.HarnessBindingV1) sessionlessharness.FailureCode {
	if adapter == nil || binding.Validate() != nil {
		return sessionlessharness.FailureHarnessBindingInvalid
	}
	if binding.Backend != adapter.descriptor {
		return sessionlessharness.FailureHarnessBackendMismatch
	}
	if binding.Resource.Kind != domain.ProviderResourceSubscriptionV1 ||
		binding.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 ||
		domain.SubscriptionConnectionID(binding.Resource.ResourceID).Validate() != nil {
		return sessionlessharness.FailureProviderResourceMismatch
	}
	if binding.ModelVendorID != ModelVendorIDV1 || binding.ModelID != adapter.config.Model {
		return sessionlessharness.FailureProviderCatalogExpired
	}
	return ""
}

type disabledRegistryDriver struct {
	adapter *Adapter
}

func (*disabledRegistryDriver) Preflight(context.Context, ports.ExecutionIdentity) error {
	return disabledHarnessError()
}

func (*disabledRegistryDriver) Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	return ports.ExecutionResult{}, disabledHarnessError()
}

func (driver *disabledRegistryDriver) Cancel(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || driver.adapter == nil || ctx == nil || ctx.Err() != nil || identity.Validate() != nil ||
		driver.adapter.validateHarnessBinding(identity.HarnessBinding) != "" {
		return disabledHarnessError()
	}
	return nil
}

func disabledHarnessError() error {
	return &domain.ClassifiedError{
		Kind: domain.ErrorTerminal, Code: string(sessionlessharness.FailureHarnessBackendDisabled),
		Operation: "codex_exec.subscription_registry",
	}
}

func (*Adapter) BackendProtocolState() harnessconformance.BackendProtocolStateV1 {
	// The legacy invocation adapter has not yet been bridged to the canonical
	// HarnessDriver request/evidence contract. Local process enablement alone
	// must never be reported as registry protocol support.
	return harnessconformance.BackendProtocolUnsupportedV1
}

var _ ports.HarnessDriver = (*disabledRegistryDriver)(nil)
var _ harnessconformance.BackendProtocolObserver = (*Adapter)(nil)
