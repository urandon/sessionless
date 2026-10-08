package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrInvocationInvalid      = errors.New("attached worker invocation is invalid")
	ErrCredentialUnavailable  = errors.New("attached worker credential lifecycle is unavailable")
	ErrCredentialFinalization = errors.New("attached worker credential finalization failed")
)

type InvocationIdentity struct {
	TenantID    domain.TenantID
	OwnerUserID domain.UserID
	WorkerID    domain.AttachedWorkerID
	RunID       domain.RunID
	AttemptID   domain.AttemptID
	LeaseID     domain.LeaseID
	FenceToken  uint64
}

func (identity InvocationIdentity) Validate() error {
	if identity.TenantID.Validate() != nil || identity.OwnerUserID.Validate() != nil ||
		identity.WorkerID.Validate() != nil || identity.RunID.Validate() != nil ||
		identity.AttemptID.Validate() != nil || identity.LeaseID.Validate() != nil ||
		identity.FenceToken == 0 {
		return ErrInvocationInvalid
	}
	return nil
}

type CredentialInvocation struct {
	IssueRequest              ports.CredentialIssueRequest
	HomeEnvironment           string
	ExpectedBindingGeneration uint64
}

type Invocation struct {
	Identity   InvocationIdentity
	Process    AttemptSpec
	Credential *CredentialInvocation
}

func (invocation Invocation) Validate() error {
	if invocation.Identity.Validate() != nil || invocation.Process.Executable == "" ||
		invocation.Process.ExecutableDigest == (ExecutableDigest{}) {
		return ErrInvocationInvalid
	}
	if invocation.Credential == nil {
		return nil
	}
	return invocation.Credential.ValidateFor(invocation.Identity)
}

// ValidateFor checks that a credential request is bound to the exact local
// invocation. The sealed-input boundary uses the same predicate before a
// process or credential lifecycle receives the request.
func (credential CredentialInvocation) ValidateFor(identity InvocationIdentity) error {
	if identity.Validate() != nil {
		return ErrInvocationInvalid
	}
	request := credential.IssueRequest
	if !environmentNamePattern.MatchString(credential.HomeEnvironment) || reservedEnvironmentName(credential.HomeEnvironment) ||
		credential.ExpectedBindingGeneration == 0 ||
		request.OwnerUserID != identity.OwnerUserID ||
		request.Run.TenantID != identity.TenantID ||
		request.Run.ID != identity.RunID ||
		request.Attempt.ID != identity.AttemptID ||
		request.Lease.ID != identity.LeaseID ||
		request.Lease.WorkerID != string(identity.WorkerID) ||
		request.Lease.FenceToken != identity.FenceToken ||
		request.Attempt.ValidateForRun(request.Run) != nil ||
		request.Lease.ValidateForAttempt(request.Run, request.Attempt) != nil {
		return ErrInvocationInvalid
	}
	return nil
}

type InvocationResult struct {
	Process                   AttemptResult
	CredentialChanged         bool
	CredentialGeneration      uint64
	CredentialReleaseRequired bool `json:",omitempty"`
	CredentialReleased        bool `json:",omitempty"`
	FailureCode               string
	CommittedArtifactDigests  []CommittedEvidenceDigest
	CommittedEventDigests     []CommittedEvidenceDigest
}

// CleanupComplete is a necessary, not sufficient, condition for a terminal
// acknowledgement. The output receipt must independently prove canonical
// materialization before the server may commit a result.
func (result InvocationResult) CleanupComplete() bool {
	return result.Process.DescendantsReaped && result.Process.BoundaryReleased &&
		result.Process.CleanupSucceeded &&
		(!result.CredentialReleaseRequired || result.CredentialReleased)
}

// Succeeded reports the same process-level success predicate used by the
// terminal protocol. It says nothing about whether the terminal was committed;
// callers must separately require a successful terminal acknowledgement.
func (result InvocationResult) Succeeded(runErr error) bool {
	process := result.Process
	return runErr == nil && !process.Cancelled && !process.Deadline && process.ExitCode == 0 &&
		process.FailureCode == "" && result.FailureCode == "" && result.CleanupComplete()
}

// CommittedEvidenceDigest is content-addressed evidence already committed by
// the local invocation boundary. The daemon transport carries only the digest;
// it never embeds artifact, event, provider, prompt, or result content.
type CommittedEvidenceDigest [sha256.Size]byte

type ProcessRunner interface {
	Run(context.Context, AttemptSpec) (AttemptResult, error)
}

type InvocationRunnerConfig struct {
	CredentialFinalizeGrace time.Duration
}

type InvocationRunner struct {
	process     ProcessRunner
	credentials ports.CredentialLifecycle
	grace       time.Duration
}

func NewInvocationRunner(
	config InvocationRunnerConfig,
	process ProcessRunner,
	credentials ports.CredentialLifecycle,
) (*InvocationRunner, error) {
	if process == nil {
		return nil, ErrInvocationInvalid
	}
	if config.CredentialFinalizeGrace <= 0 {
		config.CredentialFinalizeGrace = 15 * time.Second
	}
	if config.CredentialFinalizeGrace > time.Minute {
		return nil, ErrInvocationInvalid
	}
	return &InvocationRunner{process: process, credentials: credentials, grace: config.CredentialFinalizeGrace}, nil
}

func (runner *InvocationRunner) Run(
	ctx context.Context,
	invocation Invocation,
) (InvocationResult, error) {
	if ctx == nil || ctx.Err() != nil || invocation.Validate() != nil {
		return InvocationResult{}, ErrInvocationInvalid
	}
	if invocation.Credential == nil {
		process, err := runner.process.Run(ctx, invocation.Process)
		return InvocationResult{Process: process}, err
	}
	if runner.credentials == nil {
		return InvocationResult{FailureCode: "credential_lifecycle_unavailable"}, ErrCredentialUnavailable
	}
	handle, err := runner.credentials.Issue(ctx, invocation.Credential.IssueRequest)
	if err != nil {
		return InvocationResult{FailureCode: "credential_issue_failed"}, ErrCredentialUnavailable
	}
	if handle.Validate() != nil || !credentialHandleMatchesInvocation(
		handle, invocation.Identity, invocation.Credential.IssueRequest.Run.SubscriptionConnectionID,
		invocation.Credential.ExpectedBindingGeneration,
	) {
		releaseErr := runner.releaseCredential(ctx, handle)
		if releaseErr != nil {
			return InvocationResult{CredentialReleaseRequired: true, FailureCode: "credential_release_failed"}, ErrCredentialFinalization
		}
		return InvocationResult{CredentialReleaseRequired: true, CredentialReleased: true, FailureCode: "credential_handle_mismatch"}, ErrCredentialUnavailable
	}
	materialization, err := runner.credentials.Materialize(ctx, handle)
	root, rootErr := validateDirectoryPath(materialization.RootDir)
	authFile, authErr := validateDataFilePath(materialization.AuthFile)
	if err != nil || materialization.Validate() != nil || rootErr != nil || authErr != nil ||
		root != materialization.RootDir || authFile != materialization.AuthFile {
		releaseErr := runner.releaseCredential(ctx, handle)
		if releaseErr != nil {
			return InvocationResult{CredentialReleaseRequired: true, FailureCode: "credential_release_failed"}, ErrCredentialFinalization
		}
		return InvocationResult{CredentialReleaseRequired: true, CredentialReleased: true, FailureCode: "credential_materialization_failed"}, ErrCredentialUnavailable
	}
	processSpec := cloneAttemptSpec(invocation.Process)
	processSpec.AdditionalReadRoots = append(processSpec.AdditionalReadRoots, materialization.RootDir)
	processSpec.credentialWriteFile = materialization.AuthFile
	processSpec.Environment = append(processSpec.Environment, EnvironmentVariable{
		Name: invocation.Credential.HomeEnvironment, Value: materialization.RootDir,
	})
	processResult, processErr := runner.process.Run(ctx, processSpec)
	writeBackCtx, cancelWriteBack := context.WithTimeout(context.WithoutCancel(ctx), runner.grace)
	writeBack, writeBackErr := runner.credentials.WriteBack(writeBackCtx, handle, materialization)
	cancelWriteBack()
	releaseErr := runner.releaseCredential(ctx, handle)
	result := InvocationResult{
		Process: processResult, CredentialChanged: writeBack.Changed,
		CredentialGeneration:      writeBack.Generation,
		CredentialReleaseRequired: true, CredentialReleased: releaseErr == nil,
	}
	if writeBackErr != nil {
		result.FailureCode = "credential_writeback_failed"
		return result, ErrCredentialFinalization
	}
	if releaseErr != nil {
		result.FailureCode = "credential_release_failed"
		return result, ErrCredentialFinalization
	}
	return result, processErr
}

func credentialHandleMatchesInvocation(
	handle ports.CredentialHandle,
	identity InvocationIdentity,
	connectionID domain.SubscriptionConnectionID,
	expectedGeneration uint64,
) bool {
	return handle.TenantID == identity.TenantID && handle.OwnerUserID == identity.OwnerUserID &&
		handle.SubscriptionConnectionID == connectionID &&
		handle.RunID == identity.RunID && handle.AttemptID == identity.AttemptID &&
		handle.WorkerID == string(identity.WorkerID) && handle.LeaseID == identity.LeaseID &&
		handle.LeaseFence == identity.FenceToken && handle.BindingGeneration == expectedGeneration
}

func (runner *InvocationRunner) releaseCredential(
	ctx context.Context,
	handle ports.CredentialHandle,
) error {
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runner.grace)
	defer cancel()
	return runner.credentials.Release(finalizeCtx, handle)
}

func cloneAttemptSpec(spec AttemptSpec) AttemptSpec {
	clone := spec
	clone.Arguments = append([]string(nil), spec.Arguments...)
	clone.Environment = append([]EnvironmentVariable(nil), spec.Environment...)
	clone.AdditionalReadRoots = append([]string(nil), spec.AdditionalReadRoots...)
	clone.Stdin = append([]byte(nil), spec.Stdin...)
	return clone
}

var _ ProcessRunner = (*Supervisor)(nil)
