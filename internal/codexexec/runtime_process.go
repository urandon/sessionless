package codexexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrProcessBoundaryUnavailable = errors.New("codex exec prepared process boundary is unavailable")
	ErrProcessAlreadyActive       = errors.New("codex exec prepared process is already active")
	ErrProcessNotActive           = errors.New("codex exec prepared process is not active")
)

// PreparedProcessBoundaryConfigV1 pins the exact workload accepted by the
// already-reviewed Go supervisor. Credential lifecycle ownership remains with
// the canonical worker; this boundary receives only its private materialization.
type PreparedProcessBoundaryConfigV1 struct {
	Supervisor       *attachedworkerdaemon.Supervisor
	Executable       string
	ExecutableDigest attachedworkerdaemon.ExecutableDigest
	Arguments        []string
}

type activePreparedProcess struct {
	authority AuthorityV1
	cancel    context.CancelFunc
	cancelled bool
}

// PreparedProcessBoundaryV1 runs a single exact prepared Codex process through
// the Go supervisor. It never issues, materializes, writes back, or releases a
// credential and it owns exact in-process cancellation only while Run is live.
type PreparedProcessBoundaryV1 struct {
	supervisor *attachedworkerdaemon.Supervisor
	executable string
	digest     attachedworkerdaemon.ExecutableDigest
	arguments  []string

	mu     sync.Mutex
	active *activePreparedProcess
}

func NewPreparedProcessBoundaryV1(config PreparedProcessBoundaryConfigV1) (*PreparedProcessBoundaryV1, error) {
	if config.Supervisor == nil || !filepath.IsAbs(config.Executable) ||
		filepath.Clean(config.Executable) != config.Executable ||
		config.ExecutableDigest == (attachedworkerdaemon.ExecutableDigest{}) || len(config.Arguments) == 0 {
		return nil, ErrContract
	}
	for _, argument := range config.Arguments {
		if strings.IndexByte(argument, 0) >= 0 {
			return nil, ErrContract
		}
	}
	actual, err := attachedworkerdaemon.DigestExecutable(config.Executable)
	if err != nil || actual != config.ExecutableDigest {
		return nil, ErrContract
	}
	return &PreparedProcessBoundaryV1{
		supervisor: config.Supervisor, executable: config.Executable,
		digest: config.ExecutableDigest, arguments: append([]string(nil), config.Arguments...),
	}, nil
}

func (boundary *PreparedProcessBoundaryV1) Run(ctx context.Context, invocation ProcessInvocationV1) (ProcessResultV1, error) {
	if boundary == nil || ctx == nil || ctx.Err() != nil || boundary.validateInvocation(invocation) != nil {
		return ProcessResultV1{}, ErrProcessBoundaryUnavailable
	}
	runCtx, cancel := context.WithCancel(ctx)
	active := &activePreparedProcess{authority: invocation.Authority, cancel: cancel}
	boundary.mu.Lock()
	if boundary.active != nil {
		boundary.mu.Unlock()
		cancel()
		return ProcessResultV1{}, ErrProcessAlreadyActive
	}
	boundary.active = active
	boundary.mu.Unlock()
	defer func() {
		cancel()
		boundary.mu.Lock()
		if boundary.active == active {
			boundary.active = nil
		}
		boundary.mu.Unlock()
	}()

	spec := attachedworkerdaemon.AttemptSpec{
		Executable: boundary.executable, ExecutableDigest: boundary.digest,
		Arguments: append([]string(nil), boundary.arguments...),
		Environment: []attachedworkerdaemon.EnvironmentVariable{{
			Name:  invocation.CredentialHomeEnvironmentName,
			Value: invocation.CredentialMaterialization.RootDir,
		}},
		Stdin: append([]byte(nil), invocation.Stdin...),
	}
	defer zeroBytes(spec.Stdin)
	prepared, err := attachedworkerdaemon.BindPreparedCredential(
		spec, invocation.CredentialMaterialization.RootDir,
		invocation.CredentialMaterialization.FilePath,
	)
	if err != nil {
		return ProcessResultV1{}, ErrProcessBoundaryUnavailable
	}
	observed, runErr := boundary.supervisor.Run(runCtx, prepared)
	result := mapPreparedProcessResult(invocation, observed)
	return result, runErr
}

func (boundary *PreparedProcessBoundaryV1) Cancel(ctx context.Context, authority AuthorityV1) error {
	if boundary == nil || ctx == nil || ctx.Err() != nil || authority.Validate() != nil {
		return ErrProcessBoundaryUnavailable
	}
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if boundary.active == nil {
		return ErrProcessNotActive
	}
	if boundary.active.authority != authority {
		return ErrProcessBoundaryUnavailable
	}
	if boundary.active.cancelled {
		return nil
	}
	boundary.active.cancelled = true
	boundary.active.cancel()
	return nil
}

func (boundary *PreparedProcessBoundaryV1) validateInvocation(invocation ProcessInvocationV1) error {
	if invocation.Authority.Validate() != nil || invocation.Identity.Validate() != nil ||
		invocation.Executable != boundary.executable || invocation.ExecutableDigest != boundary.digest ||
		!sameStrings(invocation.Arguments, boundary.arguments) || invocation.WorkDir == "" ||
		!filepath.IsAbs(invocation.WorkDir) || filepath.Clean(invocation.WorkDir) != invocation.WorkDir ||
		invocation.CredentialHomeEnvironmentName != CredentialHomeEnvironmentV1 ||
		!invocation.RequirePrivateWorkingDirectory || !invocation.RequireSanitizedEnvironment ||
		!invocation.RequireNoAmbientHome || !invocation.RequireNoAmbientAPIKey ||
		!invocation.RequireProviderEffectFence || invocation.MaxProviderEffects != maxProviderEffectsV1 ||
		invocation.MaxStdoutBytes != maxJSONLOutputBytes || invocation.MaxStderrBytes != maxProcessStderrBytesV1 ||
		len(invocation.Stdin) == 0 || len(invocation.Stdin) > maxInstructionBytes ||
		invocation.Credential.Validate() != nil || invocation.CredentialMaterialization.Validate() != nil ||
		invocation.CredentialMaterialization.Kind != domain.ProviderCredentialDeliveryFileV1 ||
		filepath.Base(invocation.CredentialMaterialization.FilePath) != "auth.json" ||
		!authorityMatchesProcessIdentity(invocation.Authority, invocation.Identity) ||
		!credentialMatchesProcessAuthority(invocation.Credential, invocation.Authority) {
		return ErrContract
	}
	info, err := os.Lstat(invocation.WorkDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrContract
	}
	return nil
}

func authorityMatchesProcessIdentity(authority AuthorityV1, identity ports.ExecutionIdentity) bool {
	placement := identity.ExecutionPlacementV2
	return identity.Validate() == nil && placement.Kind == domain.ExecutionPlacementAttachedWorker &&
		authority.TenantID == identity.TenantID && authority.OwnerUserID == identity.OwnerUserID &&
		authority.RunID == identity.RunID && authority.AttemptID == identity.AttemptID &&
		authority.OwnerUserID == placement.OwnerUserID && authority.WorkerID == placement.WorkerID &&
		authority.CapabilityDigest == placement.CapabilityDigest && authority.PolicyDigest == placement.PolicyDigest &&
		authority.ProviderResource == identity.HarnessBinding.Resource
}

func credentialMatchesProcessAuthority(credential ports.ProviderInvocationCredentialV1, authority AuthorityV1) bool {
	return credential.Validate() == nil && credential.TenantID == authority.TenantID &&
		credential.OwnerUserID == authority.OwnerUserID &&
		domain.AttachedWorkerID(credential.WorkerID) == authority.WorkerID &&
		credential.RunID == authority.RunID && credential.AttemptID == authority.AttemptID &&
		credential.LeaseID == authority.LeaseID && credential.LeaseFence == authority.LeaseGeneration &&
		credential.ProviderResource == authority.ProviderResource &&
		!credential.ExpiresAt.After(authority.LeaseExpiresAt)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func mapPreparedProcessResult(invocation ProcessInvocationV1, observed attachedworkerdaemon.AttemptResult) ProcessResultV1 {
	limitExceeded := observed.StdoutBytes > invocation.MaxStdoutBytes || observed.StderrBytes > invocation.MaxStderrBytes ||
		observed.FailureCode == "stdout_limit_exceeded" || observed.FailureCode == "stderr_limit_exceeded"
	stdout := append([]byte(nil), observed.Stdout...)
	providerEffects := uint64(0)
	if !limitExceeded {
		parsed := ParseProtocolV1(stdout)
		if parsed.Accepted {
			providerEffects = 1
		}
		zeroBytes(parsed.Final)
	} else {
		zeroBytes(stdout)
		stdout = nil
	}
	stopped := observed.DescendantsReaped
	return ProcessResultV1{
		ExitCode: observed.ExitCode, StdoutBytes: observed.StdoutBytes, StderrBytes: observed.StderrBytes,
		OutputLimitExceeded: limitExceeded, Cancelled: observed.Cancelled, Deadline: observed.Deadline,
		ProcessStopped: stopped, DescendantsStopped: stopped,
		CleanupSucceeded: observed.CleanupSucceeded, PrivateStateRemoved: observed.CleanupSucceeded,
		CredentialStateQuiesced: stopped && observed.BoundaryReleased,
		ProviderEffectFenceSatisfied: invocation.Authority.ReservationID.Validate() == nil &&
			invocation.RequireProviderEffectFence && invocation.MaxProviderEffects == maxProviderEffectsV1,
		ProviderEffects: providerEffects, FailureCode: observed.FailureCode, Stdout: stdout,
	}
}

var _ ProcessBoundaryV1 = (*PreparedProcessBoundaryV1)(nil)
