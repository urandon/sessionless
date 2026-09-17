package codexexec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

const (
	maxProviderEffectsV1    uint64 = 1
	maxProcessStderrBytesV1        = 1 << 20
)

var completedCheckpointV1 = []byte(`{"schema":"sessionless.codex-exec-checkpoint.v1","terminal":true}`)

// Driver bridges the canonical HarnessDriver contract to one exact
// owner-attached Codex exec profile. It consumes an already materialized
// invocation credential and cannot issue a second credential behind the
// canonical worker's back.
type Driver struct {
	config     Config
	descriptor domain.HarnessBackendDescriptorV1
	authority  AuthorityResolverV1
	boundary   ProcessBoundaryV1
	now        func() time.Time
}

func NewDriver(config Config, authority AuthorityResolverV1, boundary ProcessBoundaryV1) (*Driver, error) {
	if authority == nil || boundary == nil {
		return nil, ErrContract
	}
	descriptor, err := prepareConfig(config)
	if err != nil {
		return nil, err
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Driver{
		config: config, descriptor: descriptor, authority: authority, boundary: boundary,
		now: now,
	}, nil
}

func (driver *Driver) DescriptorV1() domain.HarnessBackendDescriptorV1 {
	if driver == nil {
		return domain.HarnessBackendDescriptorV1{}
	}
	return driver.descriptor
}

func (driver *Driver) Preflight(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || !driver.config.Enabled {
		return disabledHarnessError()
	}
	if ctx == nil || ctx.Err() != nil || identity.Validate() != nil ||
		driver.validateHarnessBinding(identity.HarnessBinding) != "" ||
		identity.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker {
		return driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBindingInvalid)
	}
	return nil
}

func (driver *Driver) Execute(
	ctx context.Context,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	if driver == nil || !driver.config.Enabled {
		return ports.ExecutionResult{}, disabledHarnessError()
	}
	if ctx == nil || ctx.Err() != nil || sink == nil || request.Validate() != nil ||
		driver.validateHarnessBinding(request.HarnessBinding) != "" ||
		request.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
		len(request.InputArtifacts) != 0 || request.ContextWindow == nil ||
		request.ResumeCheckpoint != nil || len(request.AllowedMCPServers) != 0 ||
		request.CredentialMaterialization.Kind != domain.ProviderCredentialDeliveryFileV1 ||
		filepath.Base(request.CredentialMaterialization.FilePath) != "auth.json" ||
		request.HarnessBinding.EvidenceExpiresAt == nil {
		return ports.ExecutionResult{}, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBindingInvalid)
	}
	identity := executionIdentity(request)
	authority, err := driver.authority.Resolve(ctx, identity)
	if err != nil || driver.validateExecutionAuthority(request, authority) != nil {
		return ports.ExecutionResult{}, driverError(domain.ErrorPolicyDenied, sessionlessharness.FailureEffectivePolicyMismatch)
	}
	prompt, err := compileCanonicalPrompt(request)
	if err != nil {
		zeroBytes(prompt)
		return ports.ExecutionResult{}, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBindingInvalid)
	}
	defer zeroBytes(prompt)
	invocation := ProcessInvocationV1{
		Authority: authority, Identity: identity, Credential: request.Credential,
		CredentialMaterialization: request.CredentialMaterialization,
		WorkDir:                   request.WorkDir,
		Executable:                driver.config.Executable, ExecutableDigest: driver.config.ExecutableDigest,
		Arguments:                      processArguments(driver.config.Model),
		CredentialHomeEnvironmentName:  CredentialHomeEnvironmentV1,
		RequirePrivateWorkingDirectory: true, RequireSanitizedEnvironment: true,
		RequireNoAmbientHome: true, RequireNoAmbientAPIKey: true,
		RequireProviderEffectFence: true, MaxProviderEffects: maxProviderEffectsV1,
		MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1,
		Stdin: append([]byte(nil), prompt...),
	}
	process, runErr := driver.boundary.Run(ctx, invocation)
	zeroBytes(invocation.Stdin)
	parsed := ParseProtocolV1(process.Stdout)
	zeroBytes(process.Stdout)
	defer zeroBytes(parsed.Final)
	evidence, terminalErr := reduceDriverEvidence(request.HarnessBinding, parsed, process, runErr)
	if terminalErr != nil {
		return ports.ExecutionResult{}, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
	}
	result := ports.ExecutionResult{ProviderEvidence: &evidence}
	if evidence.FinishClass != domain.ProviderFinishCompletedV1 {
		return result, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
	}
	result.Summary = string(parsed.Final)
	if err := sink.Emit(ctx, ports.ExecutionEvent{
		Sequence: 1, Boundary: "codex.turn_completed",
		CheckpointState: append([]byte(nil), completedCheckpointV1...),
	}); err != nil {
		result.Summary = ""
		return result, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
	}
	return result, nil
}

func (driver *Driver) Cancel(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || ctx == nil || ctx.Err() != nil || identity.Validate() != nil ||
		driver.validateHarnessBinding(identity.HarnessBinding) != "" {
		return driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBindingInvalid)
	}
	authority, err := driver.authority.Resolve(ctx, identity)
	if err != nil || driver.validateAuthority(identity, authority, false) != nil {
		return driverError(domain.ErrorPolicyDenied, sessionlessharness.FailureEffectivePolicyMismatch)
	}
	if err := driver.boundary.Cancel(ctx, authority); err != nil {
		return driverError(domain.ErrorRetryable, sessionlessharness.FailureHarnessBackendFailed)
	}
	return nil
}

func (driver *Driver) BackendProtocolState() harnessconformance.BackendProtocolStateV1 {
	if driver != nil && driver.config.Enabled {
		return harnessconformance.BackendProtocolSupportedV1
	}
	return harnessconformance.BackendProtocolUnsupportedV1
}

func (driver *Driver) validateAuthority(
	identity ports.ExecutionIdentity,
	authority AuthorityV1,
	requireLiveAdmission bool,
) error {
	if authority.Validate() != nil || identity.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker {
		return ErrContract
	}
	placement := identity.ExecutionPlacementV2
	if authority.TenantID != identity.TenantID || authority.OwnerUserID != identity.OwnerUserID ||
		authority.RunID != identity.RunID || authority.AttemptID != identity.AttemptID ||
		authority.OwnerUserID != placement.OwnerUserID || authority.WorkerID != placement.WorkerID ||
		authority.CapabilityDigest != placement.CapabilityDigest ||
		authority.PolicyDigest != placement.PolicyDigest ||
		authority.ProviderResource != identity.HarnessBinding.Resource {
		return ErrContract
	}
	if requireLiveAdmission {
		now := driver.now().UTC()
		if !authority.LeaseExpiresAt.After(now) || identity.HarnessBinding.EvidenceExpiresAt == nil ||
			!identity.HarnessBinding.EvidenceExpiresAt.UTC().After(now) {
			return ErrContract
		}
	}
	return nil
}

func (driver *Driver) validateExecutionAuthority(request ports.ExecutionRequest, authority AuthorityV1) error {
	now := driver.now().UTC()
	if driver.validateAuthority(executionIdentity(request), authority, true) != nil ||
		request.Credential.Validate() != nil ||
		request.Credential.TenantID != authority.TenantID ||
		request.Credential.OwnerUserID != authority.OwnerUserID ||
		domain.AttachedWorkerID(request.Credential.WorkerID) != authority.WorkerID ||
		request.Credential.RunID != authority.RunID ||
		request.Credential.AttemptID != authority.AttemptID ||
		request.Credential.LeaseID != authority.LeaseID ||
		request.Credential.LeaseFence != authority.LeaseGeneration ||
		request.Credential.ProviderResource != authority.ProviderResource ||
		!request.Credential.ExpiresAt.UTC().After(now) ||
		request.Credential.ExpiresAt.After(authority.LeaseExpiresAt) ||
		request.Credential.ExpiresAt.After(request.HarnessBinding.EvidenceExpiresAt.UTC()) {
		return ErrContract
	}
	return nil
}

func executionIdentity(request ports.ExecutionRequest) ports.ExecutionIdentity {
	return ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID,
		RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2,
		HarnessBinding:       request.HarnessBinding.Clone(),
		SubstrateBinding:     request.SubstrateBinding,
		AdmissionCostCeiling: request.AdmissionCostCeiling,
	}
}

func reduceDriverEvidence(
	binding domain.HarnessBindingV1,
	parsed ProtocolResultV1,
	process ProcessResultV1,
	runErr error,
) (domain.ProviderExecutionEvidenceV1, error) {
	evidence := domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceUnknownV1,
		FinishClass:     domain.ProviderFinishFailedV1,
		RouteState:      domain.ProviderEvidenceUnknownV1,
		PolicyVerdict:   domain.ProviderPolicyConditionalV1,
		UsageProvenance: domain.ProviderUsageUnknownV1,
		FailureCode:     domain.ProviderExecutionFailureBackendV1,
	}
	if parsed.Accepted {
		evidence.AcceptanceClass = domain.ProviderAcceptanceAcceptedV1
	}
	switch {
	case process.Cancelled || process.Deadline:
		evidence.FinishClass = domain.ProviderFinishCancelledV1
		evidence.FailureCode = domain.ProviderExecutionFailureCancelledV1
	case parsed.ProtocolDrift || parsed.TerminalDrift:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProtocolDriftV1
	case parsed.Accepted && parsed.FailureCode != "":
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProviderFailedV1
	case parsed.Accepted && !parsed.Terminal:
		evidence.FinishClass = domain.ProviderFinishUnknownV1
		evidence.FailureCode = domain.ProviderExecutionFailureAcceptedUnknownV1
	case !parsed.Accepted:
		evidence.AcceptanceClass = domain.ProviderAcceptancePreAcceptanceV1
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailurePreAcceptanceV1
	case parsed.Terminal && runErr == nil && process.ExitCode == 0 &&
		process.StdoutBytes == len(process.Stdout) && process.StdoutBytes <= maxJSONLOutputBytes &&
		process.StderrBytes >= 0 && process.StderrBytes <= maxProcessStderrBytesV1 &&
		!process.OutputLimitExceeded && process.ProcessStopped && process.DescendantsStopped &&
		process.CleanupSucceeded && process.PrivateStateRemoved && process.CredentialStateQuiesced &&
		process.ProviderEffectFenceSatisfied && process.ProviderEffects == maxProviderEffectsV1 &&
		process.FailureCode == "":
		evidence.FinishClass = domain.ProviderFinishCompletedV1
		evidence.FailureCode = ""
	case !process.CredentialStateQuiesced:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureCredentialFinalizeV1
	case !process.ProcessStopped || !process.DescendantsStopped ||
		!process.CleanupSucceeded || !process.PrivateStateRemoved:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureTeardownV1
	default:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureBackendV1
	}
	return evidence.SealForBinding(binding)
}

func compileCanonicalPrompt(request ports.ExecutionRequest) ([]byte, error) {
	file, err := openContextHistory(request.WorkDir, maxInstructionBytes)
	if err != nil {
		return nil, ErrContract
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxInstructionBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxInstructionBytes {
		zeroBytes(raw)
		return nil, ErrContract
	}
	defer zeroBytes(raw)
	records, err := sessioncontext.DecodeJSONL(raw, request.TenantID, request.SessionID)
	if err != nil || len(records) == 0 ||
		records[len(records)-1].Event.ID != request.TriggerEventID ||
		records[len(records)-1].Event.Sequence != request.ContextWindow.ThroughSequence {
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
			if decodeExactJSON(record.Payload, &payload) != nil ||
				payload.Version != 1 || len(payload.Attachments) != 0 {
				return nil, ErrContract
			}
			role, text = "user", payload.Text
		case domain.SessionEventAssistantMessage:
			var payload struct {
				Schema             string `json:"schema"`
				Summary            string `json:"summary"`
				ArtifactManifestID string `json:"artifact_manifest_id"`
			}
			if decodeExactJSON(record.Payload, &payload) != nil ||
				payload.Schema != "sessionless.assistant-message.v1" {
				return nil, ErrContract
			}
			role, text = "assistant", payload.Summary
		default:
			return nil, ErrContract
		}
		if !validPromptText(text) {
			return nil, ErrContract
		}
		_, _ = fmt.Fprintf(&output, "[%s]\n%s\n", role, text)
		if output.Len() > maxInstructionBytes {
			return nil, ErrContract
		}
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func decodeExactJSON(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return ErrContract
	}
	return nil
}

func validPromptText(value string) bool {
	return value != "" && len(value) <= maxInstructionBytes &&
		utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func driverError(kind domain.ErrorKind, code sessionlessharness.FailureCode) error {
	return &domain.ClassifiedError{
		Kind: kind, Code: string(code), Operation: "codex_exec.harness_driver",
	}
}

var _ ports.HarnessDriver = (*Driver)(nil)
var _ harnessconformance.BackendProtocolObserver = (*Driver)(nil)
