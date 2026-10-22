package codexexec

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrProcessBoundaryUnavailable = errors.New("codex exec prepared process boundary is unavailable")
	ErrProcessAlreadyActive       = errors.New("codex exec prepared process is already active")
	ErrProcessAlreadyConsumed     = errors.New("codex exec prepared process authority was already consumed")
	ErrProcessNotActive           = errors.New("codex exec prepared process is not active")
)

// PreparedProcessBoundaryConfigV1 pins the exact workload and one accepted
// attempt authority consumed by the already-reviewed Go supervisor. Credential
// lifecycle ownership remains with the canonical worker; this boundary receives
// only its private materialization.
type PreparedProcessBoundaryConfigV1 struct {
	Supervisor       *attachedworkerdaemon.Supervisor
	Authority        AuthorityV1
	Executable       string
	ExecutableDigest attachedworkerdaemon.ExecutableDigest
	Arguments        []string
	Now              func() time.Time
}

type activePreparedProcess struct {
	authority AuthorityV1
	context   context.Context
	cancel    context.CancelFunc
	cancelled bool
	finished  bool
}

type preparedObservation struct {
	self               *preparedObservation
	boundary           *PreparedProcessBoundaryV1
	authority          AuthorityV1
	process            attachedworkerdaemon.AttemptResult
	mediaAssociation   [32]byte
	mediaProfileDigest string
	native             *nativeConduitOwner
	binding            domain.HarnessBindingV2
	protocol           parseResult
	runErr             error
}

// PreparedProcessBoundaryV1 runs at most one exact prepared Codex process for
// its immutable accepted attempt. It never issues, materializes, writes back,
// or releases a credential. Exact cancellation and terminal observation are
// arbitrated under one lock so they cannot both report success.
type PreparedProcessBoundaryV1 struct {
	supervisor     *attachedworkerdaemon.Supervisor
	authority      AuthorityV1
	executable     string
	digest         attachedworkerdaemon.ExecutableDigest
	arguments      []string
	now            func() time.Time
	mediaConfig    *MediaConfigV1
	native         *nativeConduitOwner
	nativeIdentity *PreparedProcessBoundaryV1

	mu       sync.Mutex
	consumed bool
	active   *activePreparedProcess
}

func NewPreparedProcessBoundaryV1(config PreparedProcessBoundaryConfigV1) (*PreparedProcessBoundaryV1, error) {
	if config.Supervisor != nil && config.Supervisor.NativeV2() {
		return nil, ErrContract
	}
	return newPreparedProcessBoundary(config)
}

func newPreparedProcessBoundary(config PreparedProcessBoundaryConfigV1) (*PreparedProcessBoundaryV1, error) {
	if config.Supervisor == nil || config.Authority.Validate() != nil || !filepath.IsAbs(config.Executable) ||
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
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &PreparedProcessBoundaryV1{
		supervisor: config.Supervisor, authority: config.Authority,
		executable: config.Executable, digest: config.ExecutableDigest,
		arguments: append([]string(nil), config.Arguments...), now: now,
	}, nil
}

func (boundary *PreparedProcessBoundaryV1) Run(ctx context.Context, invocation ProcessInvocationV1) (result ProcessResultV1, runErr error) {
	if boundary == nil || (boundary.native != nil && boundary.nativeIdentity != boundary) ||
		ctx == nil || ctx.Err() != nil || boundary.validateInvocation(invocation) != nil {
		return ProcessResultV1{}, ErrProcessBoundaryUnavailable
	}
	runCtx, cancel := context.WithCancel(ctx)
	active := &activePreparedProcess{authority: invocation.Authority, context: runCtx, cancel: cancel}
	boundary.mu.Lock()
	switch {
	case boundary.active != nil:
		boundary.mu.Unlock()
		cancel()
		return ProcessResultV1{}, ErrProcessAlreadyActive
	case boundary.consumed:
		boundary.mu.Unlock()
		cancel()
		return ProcessResultV1{}, ErrProcessAlreadyConsumed
	default:
		boundary.consumed = true
		boundary.active = active
		boundary.mu.Unlock()
	}
	var observed *attachedworkerdaemon.AttemptResult
	var protocol parseResult
	defer func() {
		if boundary.finish(active) {
			result.Cancelled = true
			if observed != nil {
				observed.Cancelled = true
			}
			runErr = errors.Join(runErr, context.Canceled)
		}
		if observed != nil {
			observation := &preparedObservation{boundary: boundary, authority: invocation.Authority,
				binding: invocation.Identity.HarnessBinding.Clone(), protocol: protocol, runErr: runErr}
			observation.self = observation
			if boundary.native != nil {
				observation.native = boundary.native
			}
			// The witness keeps only the bounded parsed candidate, not raw frames.
			zeroBytes(observed.Stdout)
			observed.Stdout = nil
			observation.process = *observed
			result.original = observation
			if invocation.media != nil {
				result.original.mediaAssociation = invocation.media.associationDigest
				result.original.mediaProfileDigest = invocation.Identity.HarnessBinding.Backend.BackendProfileDigest
			}
		}
		if boundary.native != nil {
			// Retire this original child only after finish sampled its authority.
			// The consumed latch is deliberately retained on every outcome.
			_ = boundary.native.child.Close()
		}
		cancel()
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
	if invocation.media != nil {
		// Only the owner-created private original stage can expand native argv;
		// the installed static command, cwd, replacement environment and the
		// supervisor's one actual process stay unchanged.
		spec.Arguments = append([]string(nil), boundary.arguments[:len(boundary.arguments)-1]...)
		for _, path := range invocation.mediaStage.imagePaths() {
			spec.Arguments = append(spec.Arguments, "--image", path)
		}
		spec.Arguments = append(spec.Arguments, "-")
		spec.AdditionalReadRoots = []string{invocation.mediaStage.path}
	}
	defer zeroBytes(spec.Stdin)
	prepared, err := attachedworkerdaemon.BindPreparedCredential(
		spec, invocation.CredentialMaterialization.RootDir,
		invocation.CredentialMaterialization.FilePath,
	)
	if err != nil {
		return ProcessResultV1{}, ErrProcessBoundaryUnavailable
	}
	deadline, err := boundary.authorityDeadline(invocation)
	if err != nil {
		return ProcessResultV1{}, ErrProcessBoundaryUnavailable
	}
	expiryCtx, cancelExpiry := context.WithDeadline(runCtx, deadline)
	var actual attachedworkerdaemon.AttemptResult
	if boundary.native == nil {
		actual, runErr = boundary.supervisor.Run(expiryCtx, prepared)
	} else {
		actual, runErr = boundary.native.run(expiryCtx, prepared, invocation)
	}
	observed = &actual
	cancelExpiry()
	protocol = parseJSONL(actual.Stdout)
	result = mapPreparedProcessResult(invocation, actual, boundary.native != nil, protocol)
	return result, runErr
}

func (boundary *PreparedProcessBoundaryV1) Cancel(ctx context.Context, authority AuthorityV1) error {
	if boundary == nil || (boundary.native != nil && boundary.nativeIdentity != boundary) ||
		ctx == nil || ctx.Err() != nil || authority.Validate() != nil || authority != boundary.authority {
		return ErrProcessBoundaryUnavailable
	}
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if boundary.active == nil || boundary.active.finished {
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

// finish is the single terminal arbitration point. A nil Cancel result implies
// cancelled=true here; if finish wins first, a later Cancel observes not-active.
func (boundary *PreparedProcessBoundaryV1) finish(active *activePreparedProcess) bool {
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if active == nil {
		return false
	}
	if !active.finished {
		if boundary.native != nil && (boundary.native.child == nil || boundary.native.child.Current() != nil) {
			active.cancelled = true
		}
		// Normal daemon cancellation arrives through this original context,
		// including after natural process exit while real teardown is held.
		// It must settle under the same mutex as explicit Cancel and finish.
		if active.context != nil && errors.Is(active.context.Err(), context.Canceled) {
			active.cancelled = true
		}
		active.finished = true
		if boundary.active == active {
			boundary.active = nil
		}
	}
	return active.cancelled
}

func (boundary *PreparedProcessBoundaryV1) validateInvocation(invocation ProcessInvocationV1) error {
	if boundary.mediaConfig != nil {
		descriptor, err := boundary.mediaConfig.DescriptorV1()
		if err != nil || boundary.mediaConfig.eligibility() != nil || invocation.Identity.HarnessBinding.Backend != descriptor || invocation.media == nil || invocation.mediaStage == nil || invocation.mediaStage.path != invocation.WorkDir || !bytes.Equal(invocation.Stdin, invocation.media.prompt) || invocation.mediaStage.verifyImages(invocation.media) != nil {
			return ErrContract
		}
	} else if invocation.media != nil || invocation.mediaStage != nil {
		return ErrContract
	}
	if invocation.Authority.Validate() != nil || invocation.Authority != boundary.authority ||
		invocation.Identity.Validate() != nil || invocation.Executable != boundary.executable ||
		invocation.ExecutableDigest != boundary.digest || !sameStrings(invocation.Arguments, boundary.arguments) ||
		invocation.WorkDir == "" || !filepath.IsAbs(invocation.WorkDir) ||
		filepath.Clean(invocation.WorkDir) != invocation.WorkDir ||
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
	if _, err := boundary.authorityDeadline(invocation); err != nil {
		return err
	}
	info, err := os.Lstat(invocation.WorkDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrContract
	}
	return nil
}

func (boundary *PreparedProcessBoundaryV1) authorityDeadline(invocation ProcessInvocationV1) (time.Time, error) {
	if invocation.Identity.HarnessBinding.EvidenceExpiresAt == nil {
		return time.Time{}, ErrContract
	}
	notAfter := invocation.Authority.LeaseExpiresAt.UTC()
	if credentialExpiry := invocation.Credential.ExpiresAt.UTC(); credentialExpiry.Before(notAfter) {
		notAfter = credentialExpiry
	}
	if evidenceExpiry := invocation.Identity.HarnessBinding.EvidenceExpiresAt.UTC(); evidenceExpiry.Before(notAfter) {
		notAfter = evidenceExpiry
	}
	if !notAfter.After(boundary.now().UTC()) {
		return time.Time{}, ErrContract
	}
	return notAfter, nil
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

func mapPreparedProcessResult(invocation ProcessInvocationV1, observed attachedworkerdaemon.AttemptResult, native bool, parsed parseResult) ProcessResultV1 {
	limitExceeded := observed.StdoutBytes > invocation.MaxStdoutBytes || observed.StderrBytes > invocation.MaxStderrBytes ||
		observed.FailureCode == "stdout_limit_exceeded" || observed.FailureCode == "stderr_limit_exceeded"
	stdout := append([]byte(nil), observed.Stdout...)
	providerEffects := uint64(0)
	if !limitExceeded && !native {
		if parsed.accepted {
			providerEffects = 1
		}
	} else if limitExceeded {
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
		// Run's boundary-local one-shot latch consumed this exact immutable
		// reservation before supervisor preparation. The numeric limit is only
		// the matching invocation contract, not the fence by itself.
		ProviderEffectFenceSatisfied: !native && invocation.Authority.ReservationID.Validate() == nil &&
			invocation.RequireProviderEffectFence && invocation.MaxProviderEffects == maxProviderEffectsV1,
		ProviderEffects: providerEffects, FailureCode: observed.FailureCode, Stdout: stdout,
	}
}

var _ ProcessBoundaryV1 = (*PreparedProcessBoundaryV1)(nil)
