package serverlessisolation

import (
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type Clock func() time.Time

type SupervisorConfigV1 struct {
	ScratchRoot             string
	Launcher                AttestedLauncher
	Gate                    PreparedInvocationGate
	Outputs                 OutputFinalizer
	Credentials             CredentialFinalizer
	Clock                   Clock
	AllowedEnvironmentNames []string
}

type SupervisorV1 struct {
	config SupervisorConfigV1
}

func NewSupervisorV1(config SupervisorConfigV1) (*SupervisorV1, error) {
	if config.ScratchRoot == "" || config.Launcher == nil || config.Gate == nil ||
		config.Outputs == nil || config.Credentials == nil || config.Clock == nil {
		return nil, ErrConfig
	}
	// Construction through AW-05 validates the real launcher profile and the
	// canonical scratch root before a serverless attempt can be accepted.
	if _, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: config.ScratchRoot, Launcher: config.Launcher,
		AllowedEnvironmentNames: append([]string(nil), config.AllowedEnvironmentNames...),
	}); err != nil {
		return nil, ErrConfig
	}
	config.AllowedEnvironmentNames = append([]string(nil), config.AllowedEnvironmentNames...)
	return &SupervisorV1{config: config}, nil
}

func (supervisor *SupervisorV1) Preflight(
	ctx context.Context,
	authority domain.ServerlessInvocationAuthorityV1,
) (domain.PreparedAllocationV1, error) {
	if supervisor == nil || ctx == nil || ctx.Err() != nil || authority.ValidateAt(supervisor.config.Clock().UTC()) != nil {
		return domain.PreparedAllocationV1{}, ErrAuthority
	}
	allocation, err := supervisor.config.Launcher.Preflight(ctx, authority.Clone())
	if err != nil {
		return domain.PreparedAllocationV1{}, ErrAttestation
	}
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		return domain.PreparedAllocationV1{}, ErrAttestation
	}
	return allocation.Clone(), nil
}

func (supervisor *SupervisorV1) Run(ctx context.Context, spec RunSpecV1) (RunResultV1, error) {
	if supervisor == nil || ctx == nil || ctx.Err() != nil || supervisor.config.Gate.Validate(spec.Prepared) != nil {
		return RunResultV1{}, ErrAuthority
	}
	authority := spec.Prepared.Authority()
	allocation := spec.Prepared.Allocation()
	now := supervisor.config.Clock().UTC()
	if authority.ValidateAt(now) != nil || allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend) != nil ||
		!now.Before(spec.Prepared.ExecuteDeadline()) {
		return RunResultV1{}, ErrAuthority
	}
	if authority.SubstrateBinding.WorkloadMode != domain.SubstrateWorkloadChildProcessV1 || allocation.ChildProcess == nil {
		return RunResultV1{}, ErrAttestation
	}
	expectedDigest, err := executableDigest(allocation.ChildProcess.ExecutableDigest)
	actualDigest, digestErr := attachedworkerdaemon.DigestExecutable(spec.Executable)
	if err != nil || digestErr != nil || spec.Executable == "" || expectedDigest != actualDigest ||
		!equalStrings(spec.Arguments, allocation.ChildProcess.ExactArgv) {
		return RunResultV1{}, ErrAttestation
	}
	limits := authority.SubstrateBinding.Limits
	if uint64(len(spec.Stdin)) > limits.ArtifactBytes {
		return RunResultV1{}, ErrConfig
	}
	stdoutLimit, ok := boundedInt(limits.StdoutBytes)
	if !ok {
		return RunResultV1{}, ErrConfig
	}
	stderrLimit, ok := boundedInt(limits.StderrBytes)
	if !ok {
		return RunResultV1{}, ErrConfig
	}
	executionTimeout := limits.ExecutionTimeout
	if untilDeadline := spec.Prepared.ExecuteDeadline().Sub(now); untilDeadline < executionTimeout {
		executionTimeout = untilDeadline
	}
	if executionTimeout <= 0 {
		return RunResultV1{}, ErrAuthority
	}
	grace := limits.CleanupTimeout
	if grace > 30*time.Second {
		grace = 30 * time.Second
	}
	processSupervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: supervisor.config.ScratchRoot, Launcher: supervisor.config.Launcher,
		Timeout: executionTimeout, TerminationGrace: grace,
		MaxStdoutBytes: stdoutLimit, MaxStderrBytes: stderrLimit,
		AllowedEnvironmentNames: append([]string(nil), supervisor.config.AllowedEnvironmentNames...),
	})
	if err != nil {
		return RunResultV1{}, ErrConfig
	}
	// Preparing the boundary, spawning the child, or handing it credentials is
	// the first provider-side effect for a child-process profile. Burn the
	// process-local capability here, after every pure attestation/configuration
	// check and immediately before the AW-05 launcher can create state.
	if supervisor.config.Gate.Consume(spec.Prepared) != nil {
		return RunResultV1{}, ErrAuthority
	}
	sealedStdin := append([]byte(nil), spec.Stdin...)
	defer clear(sealedStdin)
	process, processErr := processSupervisor.Run(ctx, attachedworkerdaemon.AttemptSpec{
		Executable: spec.Executable, ExecutableDigest: expectedDigest,
		Arguments:   append([]string(nil), spec.Arguments...),
		Environment: append([]attachedworkerdaemon.EnvironmentVariable(nil), spec.Environment...),
		Stdin:       sealedStdin,
	})
	result := RunResultV1{
		Process: process,
		StopProof: StopProofV1{
			DescendantsReaped: process.DescendantsReaped,
			BoundaryReleased:  process.BoundaryReleased,
			WorkspaceRemoved:  process.CleanupSucceeded,
		},
		CredentialFinalization: domain.CredentialFinalizationUnknownV1,
		Cleanup:                domain.SubstrateCleanupUnknownV1,
	}
	request := FinalizationRequestV1{
		AuthorityDigest: authorityDigest(authority), PreparedDigest: spec.Prepared.Digest(),
		PhysicalClaimID: spec.Prepared.Reservation().PhysicalInvocationClaimID, Process: process,
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), limits.CleanupTimeout)
	defer cancelCleanup()
	output, outputErr := func() (OutputFinalizationProofV1, error) {
		defer clear(request.Process.Stdout)
		return supervisor.config.Outputs.FinalizeOutputs(cleanupCtx, request)
	}()
	process.Stdout = nil
	request.Process.Stdout = nil
	result.Process.Stdout = nil
	result.Output = output
	if output.NativeEventCount > limits.NativeEventCount || output.ArtifactBytes > limits.ArtifactBytes ||
		output.EvidenceBytes > authority.AdmissionCostCeiling.MaxEvidenceBytes {
		outputErr = ErrOutputFinalization
		result.Output.Finalized = false
	}
	credential, credentialErr := supervisor.config.Credentials.FinalizeCredentials(cleanupCtx, request)
	result.CredentialFinalization = credential
	residueRequest := ResidueRequestV1{
		AuthorityDigest: request.AuthorityDigest, PreparedDigest: request.PreparedDigest,
		PhysicalClaimID: request.PhysicalClaimID,
	}
	residue, residueErr := supervisor.config.Launcher.VerifyResidue(cleanupCtx, residueRequest)
	result.Residue = residue
	if (!result.StopProof.Verified() || process.FailureCode != "" || process.ExitCode != 0 || process.Cancelled || process.Deadline) && processErr == nil {
		processErr = ErrProcess
	}
	if !result.Output.Finalized && outputErr == nil {
		outputErr = ErrOutputFinalization
	}
	if credential != domain.CredentialFinalizationVerifiedV1 && credential != domain.CredentialFinalizationNotRequiredV1 && credentialErr == nil {
		credentialErr = ErrCredentialFinalize
	}
	if !result.Residue.Verified() && residueErr == nil {
		residueErr = ErrCleanup
	}
	reasons := taintReasons(result, credentialErr, residueErr)
	if len(reasons) == 0 {
		result.Cleanup = domain.SubstrateCleanupVerifiedV1
	} else {
		result.Cleanup = domain.SubstrateCleanupFailedV1
		result.TaintRequested = true
		sort.Slice(reasons, func(left, right int) bool { return reasons[left] < reasons[right] })
		// Taint/termination is the fail-closed escape path when the ordinary
		// finalization budget has already expired. Give it a separate bounded
		// emergency context instead of passing an inevitably cancelled one.
		taintCtx, cancelTaint := context.WithTimeout(context.Background(), grace)
		taintErr := supervisor.config.Launcher.Taint(taintCtx, TaintRequestV1{
			AuthorityDigest: request.AuthorityDigest, PreparedDigest: request.PreparedDigest,
			PhysicalClaimID: request.PhysicalClaimID, Reasons: reasons,
		})
		cancelTaint()
		result.TaintConfirmed = taintErr == nil
		if taintErr != nil {
			return result, errors.Join(finalizationError(processErr, outputErr, credentialErr, residueErr), ErrTaintNotConfirmed)
		}
	}
	return result, finalizationError(processErr, outputErr, credentialErr, residueErr)
}

func taintReasons(result RunResultV1, credentialErr, residueErr error) []TaintReasonV1 {
	reasons := make([]TaintReasonV1, 0, 3)
	if !result.StopProof.Verified() {
		reasons = append(reasons, TaintProcessStopUnknownV1)
	}
	if credentialErr != nil || (result.CredentialFinalization != domain.CredentialFinalizationVerifiedV1 &&
		result.CredentialFinalization != domain.CredentialFinalizationNotRequiredV1) {
		reasons = append(reasons, TaintCredentialFinalizeFailedV1)
	}
	if residueErr != nil || !result.Residue.Verified() {
		reasons = append(reasons, TaintResidueUnknownV1)
	}
	return reasons
}

func finalizationError(processErr, outputErr, credentialErr, residueErr error) error {
	var values []error
	if processErr != nil {
		values = append(values, ErrProcess)
	}
	if outputErr != nil {
		values = append(values, ErrOutputFinalization)
	}
	if credentialErr != nil {
		values = append(values, ErrCredentialFinalize)
	}
	if residueErr != nil {
		values = append(values, ErrCleanup)
	}
	return errors.Join(values...)
}

func executableDigest(value string) (attachedworkerdaemon.ExecutableDigest, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(attachedworkerdaemon.ExecutableDigest{}) {
		return attachedworkerdaemon.ExecutableDigest{}, ErrAttestation
	}
	var digest attachedworkerdaemon.ExecutableDigest
	copy(digest[:], decoded)
	return digest, nil
}

func boundedInt(value uint64) (int, bool) {
	converted := int(value)
	return converted, converted > 0 && uint64(converted) == value
}

func authorityDigest(authority domain.ServerlessInvocationAuthorityV1) domain.ServerlessInvocationAuthorityDigestV1 {
	digest, _ := authority.Digest()
	return digest
}

func equalStrings(left, right []string) bool {
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
