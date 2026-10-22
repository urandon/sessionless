package codexexec

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicalprompt"
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
	config               Config
	descriptor           domain.HarnessBackendDescriptorV1
	authority            AuthorityResolverV1
	boundary             ProcessBoundaryV1
	now                  func() time.Time
	mediaConfig          *MediaConfigV1
	media                *mediaInput
	mediaStage           *preparedHistory
	nativeProfileV2      *nativeFrozenProfileV2
	nativeInlineOutputV2 bool
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
	return driver.execute(ctx, request, sink, nil)
}

// execute's private witness output is available only to the concrete prepared
// composition. The public HarnessDriver contract remains unchanged.
func (driver *Driver) execute(ctx context.Context, request ports.ExecutionRequest, sink ports.ExecutionEventSink, original **preparedObservation) (ports.ExecutionResult, error) {
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
	authority, err := driver.authority.ResolveExecution(ctx, identity)
	if err != nil || driver.validateExecutionAuthority(request, authority) != nil {
		return ports.ExecutionResult{}, driverError(domain.ErrorPolicyDenied, sessionlessharness.FailureEffectivePolicyMismatch)
	}
	prompt, err := driver.compilePrompt(request)
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
	if driver.media != nil {
		invocation.media = driver.media
		invocation.mediaStage = driver.mediaStage
	}
	if driver.nativeProfileV2 != nil {
		invocation.Arguments = append([]string(nil), driver.nativeProfileV2.launch.NativeArguments[0]...)
		if driver.mediaStage != nil {
			invocation.nativeStageV2 = driver.mediaStage.nativeV2
		}
	}
	process, runErr := driver.boundary.Run(ctx, invocation)
	if original != nil {
		*original = process.original
	}
	zeroBytes(invocation.Stdin)
	var parsed ProtocolResultV1
	if boundary, concrete := driver.boundary.(*PreparedProcessBoundaryV1); concrete && process.original != nil &&
		process.original.self == process.original && process.original.boundary == boundary && process.original.authority == authority {
		parsed = protocolResultV1(process.original.protocol)
	} else {
		parsed = ParseProtocolV1(process.Stdout)
	}
	if process.original != nil {
		defer zeroBytes(process.original.protocol.final)
	}
	zeroBytes(process.Stdout)
	defer zeroBytes(parsed.Final)
	var evidence domain.ProviderExecutionEvidenceV1
	var terminalErr error
	var nativeFinal *nativeStructuredFinalV2
	if driver.nativeProfileV2 != nil {
		boundary, concrete := driver.boundary.(*PreparedProcessBoundaryV1)
		if !concrete || boundary.native == nil || boundary.native.profileV2 != driver.nativeProfileV2 {
			return ports.ExecutionResult{}, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
		}
		var reduced nativeReduction
		reduced, terminalErr = boundary.reduceNativeResult(request.HarnessBinding, process)
		defer zeroBytes(reduced.final)
		evidence = reduced.evidence
		if terminalErr == nil {
			terminalErr = boundary.native.evidenceV2.ValidateExecutionEvidence(request.HarnessBinding, evidence)
		}
		if terminalErr == nil && evidence.FinishClass == domain.ProviderFinishCompletedV1 {
			nativeFinal, terminalErr = boundary.structuredNativeFinalV2(process.original, reduced.final)
			if terminalErr == nil {
				process.original.nativeFinalV2 = nativeFinal
			}
		}
	} else {
		evidence, terminalErr = reduceDriverEvidence(request.HarnessBinding, parsed, process, runErr)
	}
	if terminalErr != nil {
		return ports.ExecutionResult{}, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
	}
	result := ports.ExecutionResult{ProviderEvidence: &evidence}
	if evidence.FinishClass != domain.ProviderFinishCompletedV1 {
		return result, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
	}
	result.Summary = string(parsed.Final)
	if nativeFinal != nil {
		result.Summary = nativeFinal.answer
		if len(nativeFinal.artifacts) != 0 && !driver.nativeInlineOutputV2 {
			// Typed bytes remain in the original private result. Until #200's
			// explicitly selected output transport joins this owner, never publish an
			// answer-only success that silently discards requested artifacts.
			return result, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
		}
	}
	if err := sink.Emit(ctx, ports.ExecutionEvent{
		Sequence: 1, Boundary: "codex.turn_completed",
		CheckpointState: append([]byte(nil), completedCheckpointV1...),
	}); err != nil {
		result.Summary = ""
		return result, driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBackendFailed)
	}
	return result, nil
}

func (driver *Driver) compilePrompt(request ports.ExecutionRequest) ([]byte, error) {
	if driver.nativeProfileV2 != nil {
		if driver.media == nil || driver.mediaStage == nil || driver.mediaStage.path != request.WorkDir || driver.mediaStage.nativeV2 == nil ||
			driver.mediaStage.nativeV2.verify(context.Background(), driver.nativeProfileV2, driver.media) != nil {
			return nil, ErrContract
		}
		return append([]byte(nil), driver.media.prompt...), nil
	}
	if driver.mediaConfig == nil {
		return compileCanonicalPrompt(request)
	}
	if driver.mediaConfig.eligibility() != nil || driver.media == nil || driver.mediaStage == nil || driver.mediaStage.path != request.WorkDir || driver.mediaStage.verifyImages(driver.media) != nil {
		return nil, ErrContract
	}
	return append([]byte(nil), driver.media.prompt...), nil
}

func (driver *Driver) Cancel(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || ctx == nil || ctx.Err() != nil || identity.Validate() != nil ||
		driver.validateHarnessBinding(identity.HarnessBinding) != "" {
		return driverError(domain.ErrorTerminal, sessionlessharness.FailureHarnessBindingInvalid)
	}
	authority, err := driver.authority.ResolveCancellation(ctx, identity)
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
	binding domain.HarnessBindingV2,
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
	defer func() { zeroBytes(output.Bytes()) }()
	output.WriteString(canonicalprompt.HeaderV1)
	for _, record := range records {
		frame, err := canonicalprompt.RenderFrameV1(record.Event.Kind, record.Payload)
		if err != nil {
			return nil, ErrContract
		}
		if len(frame) > maxInstructionBytes-output.Len() {
			zeroBytes(frame)
			return nil, ErrContract
		}
		_, _ = output.Write(frame)
		zeroBytes(frame)
	}
	return append([]byte(nil), output.Bytes()...), nil
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
