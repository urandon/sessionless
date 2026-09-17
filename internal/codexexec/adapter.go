package codexexec

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type InvocationRunner interface {
	Run(context.Context, attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error)
}

type Config struct {
	// Enabled is a reversible local feature gate, not provider authorization.
	// No production binary wires it while registry, isolation, and egress gates are open.
	Enabled           bool
	Executable        string
	ExecutableVersion string
	ExecutableDigest  attachedworkerdaemon.ExecutableDigest
	Model             string
	Now               func() time.Time
}

type Adapter struct {
	config     Config
	descriptor domain.HarnessBackendDescriptorV1
	runner     InvocationRunner
}

func New(config Config, runner InvocationRunner) (*Adapter, error) {
	if runner == nil {
		return nil, ErrContract
	}
	descriptor, err := prepareConfig(config)
	if err != nil {
		return nil, err
	}
	return &Adapter{config: config, descriptor: descriptor, runner: runner}, nil
}

func prepareConfig(config Config) (domain.HarnessBackendDescriptorV1, error) {
	if config.Executable == "" || config.ExecutableVersion == "" ||
		config.ExecutableDigest == (attachedworkerdaemon.ExecutableDigest{}) ||
		!filepath.IsAbs(config.Executable) || filepath.Clean(config.Executable) != config.Executable ||
		domain.ValidateOpaqueID("codex_exec.executable_version", config.ExecutableVersion) != nil ||
		!validModelID(config.Model) {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	descriptor, err := descriptorV1(config)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, err
	}
	return descriptor, nil
}

func validModelID(value string) bool {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '.' && character != '_' &&
			character != ':' && character != '/' && character != '-' {
			return false
		}
	}
	return true
}

func (adapter *Adapter) Run(ctx context.Context, request RequestV1) (ResultV1, error) {
	if adapter == nil || !adapter.config.Enabled {
		return ResultV1{}, ErrDisabled
	}
	sealedInstruction := append([]byte(nil), request.Instruction...)
	request.Instruction = sealedInstruction
	defer func() {
		for index := range sealedInstruction {
			sealedInstruction[index] = 0
		}
	}()
	if ctx == nil || ctx.Err() != nil || request.Validate() != nil {
		return ResultV1{}, ErrContract
	}
	authority := request.Authority
	invocation := attachedworkerdaemon.Invocation{
		Identity: attachedworkerdaemon.InvocationIdentity{
			TenantID: authority.TenantID, OwnerUserID: authority.OwnerUserID,
			WorkerID: authority.WorkerID, RunID: authority.RunID, AttemptID: authority.AttemptID,
			LeaseID: authority.LeaseID, FenceToken: authority.LeaseGeneration,
		},
		Process: attachedworkerdaemon.AttemptSpec{
			Executable: adapter.config.Executable, ExecutableDigest: adapter.config.ExecutableDigest,
			Arguments: processArguments(adapter.config.Model),
			Stdin:     append([]byte(nil), request.Instruction...),
		},
		Credential: &attachedworkerdaemon.CredentialInvocation{
			IssueRequest: request.Credential, HomeEnvironment: CredentialHomeEnvironmentV1,
			ExpectedBindingGeneration: authority.ProviderResource.CredentialGeneration,
		},
	}
	invocationResult, runErr := adapter.runner.Run(ctx, invocation)
	for index := range invocation.Process.Stdin {
		invocation.Process.Stdin[index] = 0
	}
	process := invocationResult.Process
	parsed := parseJSONL(process.Stdout)
	// Raw provider frames and final text must not survive in the generic process result.
	for index := range process.Stdout {
		process.Stdout[index] = 0
	}
	processStopped := process.DescendantsReaped
	cleanupSucceeded := process.CleanupSucceeded && process.BoundaryReleased
	credentialFinalized := invocationResult.CredentialGeneration > 0 &&
		!strings.HasPrefix(invocationResult.FailureCode, "credential_")
	failureCode := parsed.failureCode
	if failureCode == "" {
		failureCode = process.FailureCode
	}
	if failureCode == "" {
		failureCode = invocationResult.FailureCode
	}
	if failureCode == "" && runErr != nil {
		failureCode = FailureLocalRunner
	}
	teardownFailure := runErr != nil || !processStopped || !cleanupSucceeded || !credentialFinalized ||
		process.FailureCode != "" || invocationResult.FailureCode != ""
	lifecycle := classify(parsed, process.Cancelled, process.Deadline, teardownFailure)
	result := ResultV1{
		Lifecycle: lifecycle, Accepted: parsed.accepted, NativeTerminal: parsed.terminal,
		ProcessStopped: processStopped, CleanupSucceeded: cleanupSucceeded,
		CredentialFinalized: credentialFinalized, FailureCode: failureCode,
		FinalCandidate: append([]byte(nil), parsed.final...), BillingRoute: ObservationUnknown,
		Quota: ObservationUnknown,
	}
	result.EvidenceDigest = evidenceDigest(
		authority, adapter.config.ExecutableVersion, adapter.config.ExecutableDigest,
		adapter.config.Model, request.Instruction, parsed, lifecycle, failureCode,
	)
	return result, nil
}

func classify(parsed parseResult, cancelled, deadline, teardownFailure bool) LifecycleClass {
	if parsed.terminalDrift {
		return LifecycleTerminalProtocolDrift
	}
	if parsed.protocolDrift {
		return LifecycleProtocolDrift
	}
	if parsed.terminal {
		if teardownFailure || cancelled || deadline {
			return LifecycleCompletedWithTeardownFailure
		}
		return LifecycleCompleted
	}
	if parsed.accepted {
		if cancelled || deadline {
			return LifecycleCancelledAmbiguous
		}
		return LifecycleAmbiguous
	}
	return LifecyclePreAcceptance
}
