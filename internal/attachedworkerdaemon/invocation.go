package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
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
	Identity      InvocationIdentity
	Process       AttemptSpec
	Credential    *CredentialInvocation
	VerifiedInput *VerifiedAttachedInputV1 `json:"-"`
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
	process        ProcessRunner
	prepared       PreparedProcessRunner
	credentials    ports.CredentialLifecycle
	grace          time.Duration
	apiCredentials ports.ProviderResourceCredentialLifecycleV1
	apiFinalizer   ports.ProviderCredentialImmutableFileFinalizerV1
	apiParent      PreparedAPIParentV1
}

func NewInvocationRunner(
	config InvocationRunnerConfig,
	process ProcessRunner,
	credentials ports.CredentialLifecycle,
) (*InvocationRunner, error) {
	return newInvocationRunner(config, process, nil, credentials)
}

func newInvocationRunner(config InvocationRunnerConfig, process ProcessRunner, prepared PreparedProcessRunner, credentials ports.CredentialLifecycle) (*InvocationRunner, error) {
	if (process == nil) == (prepared == nil) || prepared != nil && credentials == nil {
		return nil, ErrInvocationInvalid
	}
	if config.CredentialFinalizeGrace <= 0 {
		config.CredentialFinalizeGrace = 15 * time.Second
	}
	if config.CredentialFinalizeGrace > time.Minute {
		return nil, ErrInvocationInvalid
	}
	return &InvocationRunner{process: process, prepared: prepared, credentials: credentials, grace: config.CredentialFinalizeGrace}, nil
}

func (runner *InvocationRunner) Run(
	ctx context.Context,
	invocation Invocation,
) (InvocationResult, error) {
	if runner != nil && runner.apiCredentials != nil {
		return runner.runPreparedAPI(ctx, invocation)
	}
	if runner == nil || runner.process == nil && runner.prepared == nil ||
		ctx == nil || ctx.Err() != nil || invocation.Validate() != nil ||
		runner.prepared != nil && invocation.VerifiedInput.validateCredential(invocation.Identity, invocation.Credential) != nil {
		return InvocationResult{}, ErrInvocationInvalid
	}
	if invocation.Credential == nil {
		process, err := runner.process.Run(ctx, invocation.Process)
		return InvocationResult{Process: process}, err
	}
	if runner.credentials == nil {
		return InvocationResult{FailureCode: "credential_lifecycle_unavailable"}, ErrCredentialUnavailable
	}
	// Freeze an independent owned copy before acquiring a credential. Executor
	// mutations cannot affect the caller or the finalization owner's inputs.
	if runner.prepared != nil {
		owned, err := invocation.VerifiedInput.Clone()
		if err != nil {
			return InvocationResult{}, ErrInvocationInvalid
		}
		defer owned.Clear()
		invocation.VerifiedInput = owned
		if ctx.Err() != nil {
			return InvocationResult{}, ErrInvocationInvalid
		}
	}
	handle, err := runner.credentials.Issue(ctx, invocation.Credential.IssueRequest)
	if err != nil {
		return InvocationResult{FailureCode: "credential_issue_failed"}, ErrCredentialUnavailable
	}
	if handle.Validate() != nil || !credentialHandleMatchesInvocation(
		handle, invocation.Identity, invocation.Credential.IssueRequest.Run.SubscriptionConnectionID,
		invocation.Credential.ExpectedBindingGeneration, invocation.Credential.IssueRequest.ProviderResource,
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
	processResult, processErr := runner.runPreparedProcess(ctx, invocation, handle, materialization)
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

// runPreparedAPI owns the genuine typed local lifecycle. Its trusted parent
// dependency must supply actual admission/source provenance, not just DATA.
// No shipped constructor/materializer binds it; positive local test DI is NOT
// currentness/native fit/hard-F proof or permission to enable production.
func (runner *InvocationRunner) runPreparedAPI(ctx context.Context, invocation Invocation) (result InvocationResult, runErr error) {
	if ctx == nil || ctx.Err() != nil || invocation.Credential != nil || invocation.Validate() != nil ||
		nilPreparedAPIDependency(runner.prepared) || nilPreparedAPIDependency(runner.apiCredentials) || nilPreparedAPIDependency(runner.apiParent) || nilPreparedAPIDependency(runner.apiFinalizer) ||
		invocation.VerifiedInput == nil || !invocation.VerifiedInput.bounded() {
		return result, ErrInvocationInvalid
	}
	// Prepare funds StageA/B/C and transfers independent owned originals plus
	// child copies. The subscription Clone path is deliberately not used here.
	data, err := runner.apiParent.PrepareOriginalAPIInvocation(ctx, invocation.Identity, invocation.VerifiedInput, invocation.Process)
	callerIndependent, partitionsIndependent := apiMutableOwnership(data, invocation)
	if callerIndependent && !nilPreparedAPIDependency(data.Custody) {
		defer data.Custody.Clear()
	}
	if err != nil || data == nil || !partitionsIndependent || data.validateOriginal(invocation, ctx) != nil {
		return InvocationResult{FailureCode: "api_preparation_failed"}, ErrCredentialUnavailable
	}
	cutoff, err := runner.observeAPIPhase(ctx, data, pisdkphasewire.PreIssue, time.Time{})
	if err != nil || ctx.Err() != nil || !time.Now().Before(cutoff) {
		return InvocationResult{FailureCode: "api_pre_issue_failed"}, ErrCredentialUnavailable
	}
	// Tighten to the SAME already-derived original F, never a fresh duration.
	// This Go context does not implement the missing hard JS/native bridge.
	useCtx, cancelUse := context.WithDeadline(ctx, cutoff)
	defer cancelUse()
	send := &preparedAPIPhysicalSend{ctx: useCtx, body: data.ParentRequest.Body,
		profile: data.Issue.HarnessBinding.Backend.BackendProfileDigest,
		authorize: func() (time.Time, error) {
			return runner.observeAPIPhase(useCtx, data, pisdkphasewire.PhysicalSend, cutoff)
		}}
	defer send.retire()
	handle, issueErr := runner.apiCredentials.IssueProviderCredential(useCtx, data.Issue)
	// Even an error accompanied by an actual returned handle retains cleanup
	// ownership. A zero error handle is not fabricated release evidence.
	if handle != (ports.ProviderInvocationCredentialV1{}) {
		result.CredentialReleaseRequired = true
		defer func() {
			retirement, retirementErr := data.Retirement.Context()
			validRetirement := retirementErr == nil && retirement != nil && retirement.Done() != nil
			// This is the original owner's once-selected applicable cutoff, not
			// fresh runner.grace or model-use permission. Expiry still attempts
			// Release using the genuine original context and records its error.
			if validRetirement {
				deadline, ok := retirement.Deadline()
				validRetirement = ok && deadline != deadline.Round(0)
			}
			// A malformed trusted owner cannot convert an unbounded Background
			// context into cleanup. Still attempt the exact acquired handle with
			// nil (canonical invalid context), never invent a replacement timer.
			if !validRetirement {
				retirement = nil
			}
			releaseErr := runner.apiCredentials.ReleaseProviderCredential(retirement, handle)
			result.CredentialReleased = validRetirement && releaseErr == nil
			if releaseErr != nil || !validRetirement {
				result.FailureCode = "credential_release_failed"
				runErr = errors.Join(runErr, ErrCredentialFinalization)
			}
		}()
	}
	if issueErr != nil || handle.Validate() != nil || !apiHandleMatches(handle, data.Issue) {
		result.FailureCode = "credential_handle_mismatch"
		return result, ErrCredentialUnavailable
	}
	var plan ports.ProviderCredentialMaterializationV1
	callbacks := 0
	var processErr error
	var sendCutoff time.Time
	materializeErr := runner.apiCredentials.MaterializeProviderCredential(useCtx, handle, func(value ports.ProviderCredentialMaterializationV1, secret []byte) error {
		callbacks++
		if callbacks != 1 || len(secret) != 0 || useCtx.Err() != nil || !time.Now().Before(cutoff) || value.Validate() != nil ||
			value.Kind != domain.ProviderCredentialDeliveryFileV1 || data.ChildProcess.credentialWriteFile != "" {
			return ErrInvocationInvalid
		}
		root, rootErr := validateDirectoryPath(value.RootDir)
		file, fileErr := validateDataFilePath(value.FilePath)
		if rootErr != nil || fileErr != nil || root != value.RootDir || file != value.FilePath {
			return ErrInvocationInvalid
		}
		plan = value // independent parent value, never executor-mutated input
		defer func() { sendCutoff = send.retire() }()
		// The extra read-root slot was reserved/allocated by the original
		// parent BEFORE Issue; assignment adds no new independent copy pool.
		roots := data.ChildProcess.AdditionalReadRoots
		roots[len(roots)-1] = root
		result.Process, processErr = runner.prepared.RunPrepared(useCtx, PreparedInvocationV1{
			Identity: invocation.Identity, Process: data.ChildProcess, Credential: handle, Materialization: value,
			VerifiedInput: data.ChildInput, APIRequest: &data.ChildRequest,
			PhysicalSend: send,
		})
		return processErr
	})
	// Genuine synchronous Materialize has now returned/unlocked. Neither its
	// return nor a nil process error alone proves actual process quiescence.
	if materializeErr != nil || callbacks != 1 || processErr != nil || sendCutoff.IsZero() || !result.Process.DescendantsReaped ||
		!result.Process.BoundaryReleased || !result.Process.CleanupSucceeded || result.Process.Cancelled || result.Process.Deadline ||
		result.Process.ExitCode != 0 || result.Process.FailureCode != "" {
		result.FailureCode = "api_process_incomplete"
		return result, ErrCredentialFinalization
	}
	finalCutoff, err := runner.observeAPIPhase(useCtx, data, pisdkphasewire.ImmutableFinalize, sendCutoff)
	if err != nil || useCtx.Err() != nil || !time.Now().Before(finalCutoff) {
		result.FailureCode = "api_immutable_finalize_denied"
		return result, ErrCredentialFinalization
	}
	finalCtx, cancelFinal := context.WithDeadline(useCtx, finalCutoff)
	defer cancelFinal()
	if err := runner.apiFinalizer.FinalizeProviderCredentialFile(finalCtx, handle, plan); err != nil {
		result.FailureCode = "credential_finalize_failed"
		return result, ErrCredentialFinalization
	}
	return result, nil
}

func apiHandleMatches(handle ports.ProviderInvocationCredentialV1, request ports.ProviderCredentialIssueRequestV1) bool {
	b := request.HarnessBinding
	return handle.TenantID == b.TenantID && handle.OwnerUserID == b.OwnerUserID && handle.RunID == b.RunID &&
		handle.AttemptID == b.AttemptID && handle.WorkerID == request.WorkerID && handle.LeaseID == request.LeaseID &&
		handle.LeaseFence == request.LeaseFence && handle.ProviderResource == b.Resource && handle.ExpiresAt == request.ExpiresAt.UTC().Truncate(time.Microsecond)
}

func (runner *InvocationRunner) observeAPIPhase(ctx context.Context, data *PreparedAPIDataV1, phase pisdkphasewire.Phase, prior time.Time) (time.Time, error) {
	deadline := data.CallerDeadline
	for _, d := range []time.Time{data.InvocationDeadline, data.ProcessDeadline, prior} {
		if !d.IsZero() && d.Before(deadline) {
			deadline = d
		}
	}
	sent := time.Now()
	remaining := deadline.Sub(sent)
	if ctx.Err() != nil || remaining <= 0 || !sent.Add(remaining).Equal(deadline) {
		return time.Time{}, ErrInvocationInvalid
	}
	digest, err := data.Issue.HarnessBinding.Digest()
	if err != nil {
		return time.Time{}, ErrInvocationInvalid
	}
	request := pisdkphasewire.Request{Version: 1, Authorization: data.Authorization, ExpectedHarnessBindingDigest: string(digest),
		ExpectedAttachedPiAPIInvocationPolicyDigest: string(data.Issue.HarnessBinding.AttachedPiAPIInvocationPolicyDigest),
		CallerDeadline: pisdkphasewire.CallerDeadline{Version: 1, RemainingNanoseconds: uint64(remaining)}, Phase: phase}
	reply, err := runner.apiParent.ObserveOriginalAPIPhase(ctx, request)
	received := time.Now()
	if err != nil || ctx.Err() != nil || pisdkphasewire.CompareReply(request, reply) != nil ||
		reply.AcceptedAttempt != data.ParentInput.AcceptedAttempt {
		return time.Time{}, ErrCredentialUnavailable
	}
	// SAME-call measurements bound this dependency call only. The trusted
	// source implementation's authenticity/S/hard-JS bridge are NOT proven
	// by this arithmetic or the static metadata comparison.
	return attachedPiAPIUseDeadlineV1(reply.CheckedAt, reply.UseExpiresAt, sent, received, deadline, data.SafetySlack)
}

func (data *PreparedAPIDataV1) validateOriginal(invocation Invocation, ctx context.Context) error {
	if nilPreparedAPIDependency(data.Ledger) || nilPreparedAPIDependency(data.Custody) || nilPreparedAPIDependency(data.Retirement) ||
		!apiInputsIndependent(data.ParentInput, data.ChildInput) || !apiInputsIndependent(data.ParentInput, invocation.VerifiedInput) || !apiInputsIndependent(data.ChildInput, invocation.VerifiedInput) ||
		data.ParentInput.validateAPIRequest(invocation.Identity, data.Issue) != nil || data.ChildInput.validateAPIRequest(invocation.Identity, data.Issue) != nil ||
		!reflect.DeepEqual(data.ParentInput, invocation.VerifiedInput) || !reflect.DeepEqual(data.ChildInput, data.ParentInput) ||
		!reflect.DeepEqual(data.ParentProcess, invocation.Process) || !apiProcessesIndependent(data.ParentProcess, data.ChildProcess) ||
		!apiProcessesIndependent(data.ParentProcess, invocation.Process) || !apiProcessesIndependent(data.ChildProcess, invocation.Process) ||
		data.ParentProcess.credentialWriteFile != "" || data.ChildProcess.credentialWriteFile != "" {
		return ErrInvocationInvalid
	}
	child := data.ChildProcess
	if len(child.AdditionalReadRoots) != len(data.ParentProcess.AdditionalReadRoots)+1 || child.AdditionalReadRoots[len(child.AdditionalReadRoots)-1] != "" {
		return ErrInvocationInvalid
	}
	for i, root := range data.ParentProcess.AdditionalReadRoots {
		if child.AdditionalReadRoots[i] != root {
			return ErrInvocationInvalid
		}
	}
	child.AdditionalReadRoots = data.ParentProcess.AdditionalReadRoots
	if !reflect.DeepEqual(child, data.ParentProcess) {
		return ErrInvocationInvalid
	}
	p := data.ParentRequest.Profile
	artifact := hex.EncodeToString(data.ParentProcess.ExecutableDigest[:])
	if p.ValidateBindingIdentity(data.Issue.HarnessBinding, data.ParentInput.Job.ExecutionPlacementV2) != nil || p != data.ChildRequest.Profile ||
		artifact != p.Sources.NativeArtifactDigest || artifact != data.Issue.HarnessBinding.Backend.ArtifactDigest {
		return ErrInvocationInvalid
	}
	for _, item := range []struct {
		a, b []byte
		cap  uint64
	}{
		{data.ParentRequest.Body, data.ChildRequest.Body, p.Limits.NativeRequestBytes}, {data.ParentRequest.Model, data.ChildRequest.Model, p.Limits.NativeRequestBytes},
		{data.ParentRequest.Schema, data.ChildRequest.Schema, p.Limits.NativeRequestBytes}, {data.ParentRequest.Headers, data.ChildRequest.Headers, p.Limits.RequestHeaderBytes},
	} {
		if len(item.a) == 0 || len(item.b) == 0 || uint64(len(item.a)) > item.cap || uint64(len(item.b)) > item.cap || apiBytesOverlap(item.a, item.b) || !bytes.Equal(item.a, item.b) {
			return ErrInvocationInvalid
		}
	}
	hash := sha256.Sum256(data.ParentRequest.Schema)
	if hex.EncodeToString(hash[:]) != p.Sources.FinalSchemaDigest {
		return ErrInvocationInvalid
	}
	head := data.ParentInput.AcceptedAttempt
	if data.Authorization.ExpectedAttemptRevision != head.Revision || data.Authorization.PresentedSecretDigest != "" ||
		!(ports.AttachedWorkerSealedInputAuthorizationResult{Status: ports.AttachedWorkerExecutionApplied, AttemptRevision: head.Revision, Attempt: head}).Matches(data.Authorization) {
		return ErrInvocationInvalid
	}
	d, ok := ctx.Deadline()
	if !ok || data.Entry.IsZero() || data.Entry == data.Entry.Round(0) || data.Entry.After(time.Now()) ||
		data.SafetySlack <= 0 || data.CallerDeadline != d || d == d.Round(0) || data.InvocationDeadline == data.InvocationDeadline.Round(0) ||
		data.ProcessDeadline == data.ProcessDeadline.Round(0) || data.InvocationDeadline.After(data.Entry.Add(data.ParentInput.Job.Limits.MaxRuntime)) ||
		!time.Now().Before(data.InvocationDeadline) || !time.Now().Before(data.ProcessDeadline) {
		return ErrInvocationInvalid
	}
	return nil
}

func apiProcessesIndependent(a, b AttemptSpec) bool {
	if apiBytesOverlap(a.Stdin, b.Stdin) {
		return false
	}
	for _, pair := range []struct{ a, b any }{{a.Arguments, b.Arguments}, {a.Environment, b.Environment}, {a.AdditionalReadRoots, b.AdditionalReadRoots}} {
		x, y := reflect.ValueOf(pair.a), reflect.ValueOf(pair.b)
		if x.Len() == 0 || y.Len() == 0 {
			continue
		}
		p, q := x.Pointer(), y.Pointer()
		size := x.Type().Elem().Size()
		if p <= q && q-p < uintptr(x.Len())*size || q <= p && p-q < uintptr(y.Len())*size {
			return false
		}
	}
	return true
}

func credentialHandleMatchesInvocation(
	handle ports.CredentialHandle,
	identity InvocationIdentity,
	connectionID domain.SubscriptionConnectionID,
	expectedGeneration uint64,
	expectedResource domain.ProviderResourceBindingV1,
) bool {
	return handle.TenantID == identity.TenantID && handle.OwnerUserID == identity.OwnerUserID &&
		handle.SubscriptionConnectionID == connectionID &&
		handle.RunID == identity.RunID && handle.AttemptID == identity.AttemptID &&
		handle.WorkerID == string(identity.WorkerID) && handle.LeaseID == identity.LeaseID &&
		handle.LeaseFence == identity.FenceToken && handle.BindingGeneration == expectedGeneration &&
		handle.ProviderResource == expectedResource
}

func (runner *InvocationRunner) runPreparedProcess(ctx context.Context, invocation Invocation, handle ports.CredentialHandle, materialization ports.CredentialMaterialization) (AttemptResult, error) {
	processSpec := cloneAttemptSpec(invocation.Process)
	processSpec.AdditionalReadRoots = append(processSpec.AdditionalReadRoots, materialization.RootDir)
	processSpec.credentialWriteFile = materialization.AuthFile
	processSpec.Environment = append(processSpec.Environment, EnvironmentVariable{
		Name: invocation.Credential.HomeEnvironment, Value: materialization.RootDir,
	})
	if runner.prepared == nil {
		return runner.process.Run(ctx, processSpec)
	}
	// The prepared executor borrows this isolated input only for the call. It
	// receives no lifecycle port and cannot supply credential release evidence.
	defer clear(processSpec.Stdin)
	defer invocation.VerifiedInput.Clear()
	return runner.prepared.RunPrepared(ctx, PreparedInvocationV1{
		Identity: invocation.Identity, Process: processSpec,
		Credential: handle.ProviderInvocationCredential(), Materialization: materialization.ProviderMaterialization(),
		VerifiedInput: invocation.VerifiedInput,
	})
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
