package opencodeopenrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type Driver struct {
	profile    ProfileV1
	descriptor domain.HarnessBackendDescriptorV1
	boundary   ProcessBoundaryV1
}

func NewDriver(profile ProfileV1, boundary ProcessBoundaryV1) (*Driver, error) {
	if boundary == nil {
		return nil, ErrContract
	}
	descriptor, err := profile.descriptor()
	if err != nil {
		return nil, err
	}
	return &Driver{profile: profile, descriptor: descriptor, boundary: boundary}, nil
}

func (driver *Driver) DescriptorV1() domain.HarnessBackendDescriptorV1 {
	if driver == nil {
		return domain.HarnessBackendDescriptorV1{}
	}
	return driver.descriptor
}

// DisabledRegistrationV1 is the only production-facing registration builder.
// Tests in this package may use registrationV1 with an enabled gate to prove
// the native protocol without making ordinary worker selection possible.
func DisabledRegistrationV1(driver *Driver) (sessionlessharness.Registration, error) {
	return registrationV1(driver, false)
}

func registrationV1(driver *Driver, enabled bool) (sessionlessharness.Registration, error) {
	if driver == nil || enabled != driver.profile.Enabled {
		return sessionlessharness.Registration{}, ErrContract
	}
	return sessionlessharness.Registration{
		Descriptor: driver.descriptor, Enabled: enabled, Driver: driver,
		ValidateBinding: driver.validateBinding,
	}, nil
}

func (driver *Driver) validateBinding(binding domain.HarnessBindingV1) sessionlessharness.FailureCode {
	if driver == nil || binding.Validate() != nil {
		return sessionlessharness.FailureHarnessBindingInvalid
	}
	if binding.Backend != driver.descriptor {
		return sessionlessharness.FailureHarnessBackendMismatch
	}
	if binding.Resource.Kind != domain.ProviderResourceRouterAccountV1 ||
		binding.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 {
		return sessionlessharness.FailureProviderResourceMismatch
	}
	if binding.ModelVendorID != ModelVendorIDV1 || binding.ModelID != ModelIDV1 {
		return sessionlessharness.FailureProviderCatalogExpired
	}
	if binding.InputDataClass != domain.ProviderDataExternallyShareableV1 {
		return sessionlessharness.FailureEffectivePolicyMismatch
	}
	return ""
}

func (driver *Driver) Preflight(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || !driver.profile.Enabled {
		return ErrDisabled
	}
	if ctx == nil || ctx.Err() != nil || identity.Validate() != nil || driver.validateBinding(identity.HarnessBinding) != "" {
		return backendError(domain.ErrorTerminal)
	}
	return nil
}

func (driver *Driver) Execute(ctx context.Context, request ports.ExecutionRequest, sink ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	if driver == nil || !driver.profile.Enabled {
		return ports.ExecutionResult{}, ErrDisabled
	}
	if ctx == nil || ctx.Err() != nil || sink == nil || request.Validate() != nil ||
		driver.validateBinding(request.HarnessBinding) != "" || len(request.InputArtifacts) != 0 ||
		request.ContextWindow == nil || request.ResumeCheckpoint != nil || len(request.AllowedMCPServers) != 0 ||
		request.CredentialMaterialization.Kind != domain.ProviderCredentialDeliveryEnvironmentV1 ||
		request.CredentialMaterialization.EnvironmentName != CredentialEnvironmentV1 ||
		request.HarnessBinding.EvidenceExpiresAt == nil ||
		request.Credential.ExpiresAt.After(request.HarnessBinding.EvidenceExpiresAt.UTC()) {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	prompt, err := compilePrompt(request)
	if err != nil {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	if len(prompt) > maxPromptBytes {
		zero(prompt)
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	defer zero(prompt)
	files, err := generatedFiles(driver.profile)
	if err != nil {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	identity := ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2, HarnessBinding: request.HarnessBinding.Clone(),
		SubstrateBinding: request.SubstrateBinding, AdmissionCostCeiling: request.AdmissionCostCeiling,
	}
	invocation := ProcessInvocationV1{
		Identity: identity, Credential: request.Credential,
		CredentialMaterialization: request.CredentialMaterialization,
		Executable:                driver.profile.Executable, ExecutableDigest: driver.profile.ExecutableDigest,
		Arguments: processArguments(), Environment: processEnvironment(), GeneratedFiles: files,
		PrivateDirectories: privateDirectories(), RequirePrivateWorkingDirectory: true,
		RequireSanitizedEnvironment: true, RequireNoAmbientHome: true,
		// Pinned OpenCode has an application-level retry loop that cannot be
		// disabled by AI SDK maxRetries. The trusted egress/process boundary must
		// therefore reject every provider attempt after the first admitted effect.
		RequireProviderEffectFence: true, MaxProviderEffects: 1,
		Stdin: append([]byte(nil), prompt...),
	}
	process, runErr := driver.boundary.Run(ctx, invocation)
	zero(invocation.Stdin)
	parsed := parseOpenCodeJSONL(process.Stdout)
	zero(process.Stdout)
	evidence, terminalErr := reduceEvidence(request.HarnessBinding, parsed, process, runErr)
	if terminalErr != nil {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	result := ports.ExecutionResult{ProviderEvidence: &evidence}
	if evidence.FinishClass != domain.ProviderFinishCompletedV1 {
		return result, backendError(domain.ErrorTerminal)
	}
	result.Summary = string(parsed.final)
	if err := sink.Emit(ctx, ports.ExecutionEvent{
		Sequence: 1, Boundary: "opencode.step_finished", InputTokens: cloneUint64(parsed.inputTokens), OutputTokens: cloneUint64(parsed.outputTokens),
	}); err != nil {
		result.Summary = ""
		return result, backendError(domain.ErrorTerminal)
	}
	return result, nil
}

func (driver *Driver) Cancel(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || ctx == nil || identity.Validate() != nil || driver.validateBinding(identity.HarnessBinding) != "" {
		return backendError(domain.ErrorTerminal)
	}
	if err := driver.boundary.Cancel(ctx, identity); err != nil {
		return backendError(domain.ErrorRetryable)
	}
	return nil
}

func (driver *Driver) BackendProtocolState() harnessconformance.BackendProtocolStateV1 {
	if driver != nil && driver.profile.Enabled {
		return harnessconformance.BackendProtocolSupportedV1
	}
	return harnessconformance.BackendProtocolUnsupportedV1
}

func reduceEvidence(binding domain.HarnessBindingV1, parsed rpcResult, process ProcessResultV1, runErr error) (domain.ProviderExecutionEvidenceV1, error) {
	evidence := domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceUnknownV1, FinishClass: domain.ProviderFinishFailedV1,
		RouteState: domain.ProviderEvidenceUnknownV1, PolicyVerdict: domain.ProviderPolicyConditionalV1,
		UsageProvenance: domain.ProviderUsageUnknownV1, FailureCode: domain.ProviderExecutionFailureBackendV1,
	}
	if parsed.accepted {
		evidence.AcceptanceClass = domain.ProviderAcceptanceAcceptedV1
	}
	if parsed.inputTokens != nil && parsed.outputTokens != nil {
		evidence.UsageProvenance = domain.ProviderUsageProviderReportedV1
		evidence.InputTokens, evidence.OutputTokens = cloneUint64(parsed.inputTokens), cloneUint64(parsed.outputTokens)
	}
	switch {
	case process.Cancelled || process.Deadline:
		evidence.FinishClass = domain.ProviderFinishCancelledV1
		evidence.FailureCode = domain.ProviderExecutionFailureCancelledV1
	case parsed.protocolDrift:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProtocolDriftV1
	case parsed.accepted && !parsed.terminal:
		evidence.FinishClass = domain.ProviderFinishUnknownV1
		evidence.FailureCode = domain.ProviderExecutionFailureAcceptedUnknownV1
	case !parsed.accepted:
		evidence.AcceptanceClass = domain.ProviderAcceptancePreAcceptanceV1
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailurePreAcceptanceV1
	case parsed.providerFailed:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProviderFailedV1
	case parsed.terminal && runErr == nil && process.ExitCode == 0 && process.StdoutBytes == len(process.Stdout) &&
		process.StderrBytes >= 0 && process.StderrBytes <= maxStderrBytes && !process.OutputLimitExceeded &&
		process.ProcessStopped && process.DescendantsStopped && process.CleanupSucceeded && process.PrivateStateRemoved &&
		process.CredentialFinalized && process.ProviderEffectFenceSatisfied && process.ProviderEffects == 1 && process.FailureCode == "":
		evidence.FinishClass = domain.ProviderFinishCompletedV1
		evidence.FailureCode = ""
	case !process.CredentialFinalized:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureCredentialFinalizeV1
	case !process.ProcessStopped || !process.DescendantsStopped || !process.CleanupSucceeded || !process.PrivateStateRemoved:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureTeardownV1
	default:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureBackendV1
	}
	return evidence.SealForBinding(binding)
}

func compilePrompt(request ports.ExecutionRequest) ([]byte, error) {
	path := filepath.Join(request.WorkDir, "context", "history.jsonl")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxPromptBytes {
		return nil, ErrContract
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrContract
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxPromptBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxPromptBytes {
		zero(raw)
		return nil, ErrContract
	}
	defer zero(raw)
	records, err := sessioncontext.DecodeJSONL(raw, request.TenantID, request.SessionID)
	if err != nil || len(records) == 0 || records[len(records)-1].Event.ID != request.TriggerEventID {
		return nil, ErrContract
	}
	if request.ContextWindow != nil && records[len(records)-1].Event.Sequence != request.ContextWindow.ThroughSequence {
		return nil, ErrContract
	}
	var output bytes.Buffer
	output.WriteString("Sessionless canonical transcript v1\n")
	for _, record := range records {
		var role, text string
		switch record.Event.Kind {
		case domain.SessionEventUserMessage:
			var payload struct {
				Version     uint32            `json:"version"`
				Origin      json.RawMessage   `json:"origin"`
				Text        string            `json:"text"`
				Metadata    map[string]string `json:"metadata"`
				Attachments []json.RawMessage `json:"attachments"`
			}
			if decodeExact(record.Payload, &payload) != nil || payload.Version != 1 || len(payload.Attachments) != 0 {
				return nil, ErrContract
			}
			role, text = "user", payload.Text
		case domain.SessionEventAssistantMessage:
			var payload struct {
				Schema             string `json:"schema"`
				Summary            string `json:"summary"`
				ArtifactManifestID string `json:"artifact_manifest_id"`
			}
			if decodeExact(record.Payload, &payload) != nil || payload.Schema != "sessionless.assistant-message.v1" {
				return nil, ErrContract
			}
			role, text = "assistant", payload.Summary
		default:
			return nil, ErrContract
		}
		if !validText(text, maxPromptBytes) {
			return nil, ErrContract
		}
		fmt.Fprintf(&output, "[%s]\n%s\n", role, text)
		if output.Len() > maxPromptBytes {
			return nil, ErrContract
		}
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func decodeExact(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrContract
	}
	return nil
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func validText(value string, max int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= max && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func backendError(kind domain.ErrorKind) error {
	return &domain.ClassifiedError{Kind: kind, Code: string(sessionlessharness.FailureHarnessBackendFailed), Operation: "opencode_openrouter.adapter"}
}

var _ ports.HarnessDriver = (*Driver)(nil)
var _ harnessconformance.BackendProtocolObserver = (*Driver)(nil)
