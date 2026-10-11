package codexexec

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// PreparedAttachedWorkerRunnerV1 is a fixed text-only composition, not a spawn
// port or an issuer. Its sole InvocationRunner owns the credential lifecycle.
// One pending/finalized slot is kept until exact receipt retirement or Close.
type PreparedAttachedWorkerRunnerV1 struct {
	config     Config
	scratch    string
	supervisor *attachedworkerdaemon.Supervisor
	outer      *attachedworkerdaemon.InvocationRunner
	mu         sync.Mutex
	slot       *preparedResultSlot
	last       attachedworkerdaemon.InvocationIdentity
	closed     bool
}

type preparedResultSlot struct {
	identity                              attachedworkerdaemon.InvocationIdentity
	driver                                *Driver
	request                               ports.ExecutionRequest
	stage                                 *preparedHistory
	semantic                              ports.ExecutionResult
	candidate                             attachedworkeroutput.Candidate
	original                              *preparedObservation
	result                                attachedworkerdaemon.InvocationResult
	runErr                                error
	finalized, stageClean, candidateBuilt bool
	cancel                                context.CancelFunc
	done                                  chan struct{}
}

type preparedExecutor struct {
	owner *PreparedAttachedWorkerRunnerV1
}

func NewPreparedAttachedWorkerRunnerV1(config Config, scratch string, supervisor *attachedworkerdaemon.Supervisor,
	credentials ports.CredentialLifecycle, outerConfig attachedworkerdaemon.InvocationRunnerConfig) (*PreparedAttachedWorkerRunnerV1, error) {
	if !config.Enabled || supervisor == nil || credentials == nil {
		return nil, ErrDisabled
	}
	if _, err := prepareConfig(config); err != nil {
		return nil, err
	}
	owner := &PreparedAttachedWorkerRunnerV1{config: config, scratch: scratch, supervisor: supervisor}
	outer, err := attachedworkerdaemon.NewPreparedInvocationRunner(outerConfig, &preparedExecutor{owner: owner}, credentials)
	if err != nil {
		return nil, err
	}
	owner.outer = outer
	return owner, nil
}

func (owner *PreparedAttachedWorkerRunnerV1) String() string {
	return "PreparedAttachedWorkerRunnerV1{private:[redacted]}"
}
func (owner *PreparedAttachedWorkerRunnerV1) GoString() string { return owner.String() }

func (owner *PreparedAttachedWorkerRunnerV1) Run(ctx context.Context, invocation attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	if owner == nil || ctx == nil || ctx.Err() != nil || invocation.Validate() != nil || invocation.Credential == nil ||
		invocation.VerifiedInput.ValidateFor(invocation.Identity) != nil {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	input, err := invocation.VerifiedInput.Clone()
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	defer input.Clear()
	job := input.Job
	// Unsupported authority-bearing material is rejected, never stripped.
	if job.ContextWindow == nil || input.CanonicalContext == nil || !preparedTextEnvelope(input) ||
		job.WorkspaceSnapshot != nil || job.SkillBundle != nil || len(job.AllowedMCPServers) != 0 ||
		invocation.Process.Executable != owner.config.Executable || invocation.Process.ExecutableDigest != owner.config.ExecutableDigest ||
		!sameStrings(invocation.Process.Arguments, processArguments(owner.config.Model)) {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	driver, err := NewPreparedAttachedWorkerDriverV1(owner.config, input.AcceptedAttempt, job.HarnessBinding.Resource, owner.supervisor)
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, err
	}
	request := ports.ExecutionRequest{TenantID: job.TenantID, OwnerUserID: job.CredentialOwnerUserID, RunID: job.RunID,
		SessionID: job.SessionID, TriggerEventID: job.TriggerEventID, AttemptID: job.AttemptID,
		ContextSnapshot: job.ContextSnapshot, ContextWindow: job.ContextWindow, ExecutionPlacementV2: job.ExecutionPlacementV2,
		HarnessBinding: job.HarnessBinding.Clone(), SubstrateBinding: job.SubstrateBinding, AdmissionCostCeiling: job.AdmissionCostCeiling}
	if driver.Preflight(ctx, executionIdentity(request)) != nil {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	stage, err := stagePreparedHistory(owner.scratch, input.Context)
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, err
	}
	request.WorkDir = stage.path
	prompt, err := compileCanonicalPrompt(request)
	zeroBytes(prompt)
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, errors.Join(err, stage.remove())
	}
	runCtx, cancel := context.WithCancel(ctx)
	slot := &preparedResultSlot{identity: invocation.Identity, driver: driver, request: request, stage: stage, cancel: cancel, done: make(chan struct{})}
	owner.mu.Lock()
	if owner.closed || owner.slot != nil || owner.last == invocation.Identity {
		owner.mu.Unlock()
		cancel()
		return attachedworkerdaemon.InvocationResult{}, errors.Join(ErrProcessAlreadyConsumed, stage.remove())
	}
	owner.slot = slot
	owner.mu.Unlock()
	invocation.VerifiedInput = input
	result, runErr := owner.outer.Run(runCtx, invocation)
	cancel()
	stageErr := stage.remove()
	if stageErr != nil {
		runErr = errors.Join(runErr, ErrContract)
	}
	owner.mu.Lock()
	slot.result, slot.runErr, slot.stageClean, slot.finalized = result, runErr, stageErr == nil, true
	close(slot.done)
	if owner.closed {
		owner.clearSlotLocked()
	}
	owner.mu.Unlock()
	return result, runErr
}

// preparedTextEnvelope consumes only the canonical ingress envelope already
// represented in verified history. It is not an attachment staging shortcut.
// ValidateFor has reprojected and byte-compared the original proof before this
// predicate; decoding that projection also handles snapshot-contained triggers.
func preparedTextEnvelope(input *attachedworkerdaemon.VerifiedAttachedInputV1) bool {
	if input == nil || input.CanonicalContext == nil || input.Job.ContextWindow == nil || len(input.CanonicalContext.Attachments) != 0 {
		return false
	}
	if len(input.Artifacts) == 0 && len(input.Manifest.Artifacts) == 0 {
		return true // Existing admitted history-only library inputs.
	}
	if len(input.Artifacts) != 1 || len(input.Manifest.Artifacts) != 1 {
		return false
	}
	artifact, ref := input.Artifacts[0], input.Manifest.Artifacts[0]
	if artifact.Name != "message.json" || ref.Name != "message.json" || ref.MediaType != "application/json" {
		return false
	}
	records, err := sessioncontext.DecodeJSONL(input.Context, input.Job.TenantID, input.Job.SessionID)
	defer func() {
		for _, record := range records {
			clear(record.Payload)
		}
	}()
	if err != nil || len(records) == 0 {
		return false
	}
	trigger := records[len(records)-1]
	event, job := trigger.Event, input.Job
	return event.ID == job.TriggerEventID && event.Sequence == job.ContextWindow.ThroughSequence &&
		event.TenantID == job.TenantID && event.SessionID == job.SessionID && event.Kind == domain.SessionEventUserMessage &&
		event.AuthorUserID != nil && *event.AuthorUserID == job.CredentialOwnerUserID &&
		event.RunID != nil && *event.RunID == job.RunID && ref.Blob == event.Payload && bytes.Equal(artifact.Body, trigger.Payload)
}

func (executor *preparedExecutor) RunPrepared(ctx context.Context, prepared attachedworkerdaemon.PreparedInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	owner := executor.owner
	owner.mu.Lock()
	slot := owner.slot
	if slot == nil || slot.identity != prepared.Identity || slot.finalized {
		owner.mu.Unlock()
		return attachedworkerdaemon.AttemptResult{}, ErrContract
	}
	request, driver := slot.request, slot.driver
	owner.mu.Unlock()
	if prepared.VerifiedInput.ValidateFor(prepared.Identity) != nil {
		return attachedworkerdaemon.AttemptResult{}, ErrContract
	}
	request.Credential, request.CredentialMaterialization = prepared.Credential, prepared.Materialization
	var original *preparedObservation
	sink := &preparedCheckpointSink{}
	semantic, err := driver.execute(ctx, request, sink, &original)
	boundary, concrete := driver.boundary.(*PreparedProcessBoundaryV1)
	if original == nil || !concrete || original.boundary != boundary || original.authority != boundary.authority {
		return attachedworkerdaemon.AttemptResult{}, errors.Join(err, ErrContract)
	}
	owner.mu.Lock()
	slot.original, slot.semantic = original, semantic
	owner.mu.Unlock()
	return original.process, err
}

// ReceiptCandidate accepts only the exact result this wrapper received from
// its real outer runner. Caller-supplied cleanup/finalization flags cannot fill
// or replace the private slot. Retries return the same immutable candidate.
func (owner *PreparedAttachedWorkerRunnerV1) ReceiptCandidate(identity attachedworkerdaemon.InvocationIdentity, result attachedworkerdaemon.InvocationResult, runErr error,
	status domain.AttachedWorkerTerminalStatus) (attachedworkeroutput.Candidate, error) {
	if owner == nil {
		return attachedworkeroutput.Candidate{}, ErrContract
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	slot := owner.slot
	if owner.closed || slot == nil || !slot.finalized || !slot.stageClean || slot.original == nil || identity != slot.identity ||
		!reflect.DeepEqual(result, slot.result) || !reflect.DeepEqual(runErr, slot.runErr) || !result.CleanupComplete() {
		return attachedworkeroutput.Candidate{}, ErrContract
	}
	candidate := attachedworkeroutput.Candidate{Status: status}
	switch status {
	case domain.AttachedWorkerTerminalSucceeded:
		if !result.Succeeded(runErr) || slot.semantic.ProviderEvidence == nil || slot.semantic.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 ||
			!utf8.ValidString(slot.semantic.Summary) || slot.semantic.Summary == "" || utf8.RuneCountInString(slot.semantic.Summary) > 32_000 {
			return attachedworkeroutput.Candidate{}, ErrContract
		}
		candidate.Summary = slot.semantic.Summary
	case domain.AttachedWorkerTerminalCancelled:
		if !result.Process.Cancelled || runErr == nil {
			return attachedworkeroutput.Candidate{}, ErrContract
		}
		candidate.FailureCode = "codex_exec_cancelled"
	case domain.AttachedWorkerTerminalFailed:
		// A known completed provider turn may fail outer finalization. Unknown
		// provider outcomes remain reconciliation-only, never replay authority.
		if runErr == nil || slot.semantic.ProviderEvidence == nil || slot.semantic.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 {
			return attachedworkeroutput.Candidate{}, ErrContract
		}
		candidate.FailureCode = "codex_exec_finalization_failed"
	default:
		return attachedworkeroutput.Candidate{}, ErrContract
	}
	if slot.candidateBuilt && !reflect.DeepEqual(candidate, slot.candidate) {
		return attachedworkeroutput.Candidate{}, ErrContract
	}
	slot.candidate, slot.candidateBuilt = candidate, true
	return candidate, nil
}

// RetireReceipt relinquishes only the original finalized slot after the
// adapter has received the exact ACK and retired its durable journal entry.
func (owner *PreparedAttachedWorkerRunnerV1) RetireReceipt(identity attachedworkerdaemon.InvocationIdentity) error {
	if owner == nil {
		return ErrContract
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.slot == nil || owner.slot.identity != identity || !owner.slot.finalized || !owner.slot.candidateBuilt {
		return ErrContract
	}
	owner.last = identity
	owner.clearSlotLocked()
	return nil
}

func (owner *PreparedAttachedWorkerRunnerV1) clearSlotLocked() {
	if owner.slot != nil {
		owner.slot.semantic = ports.ExecutionResult{}
		owner.slot.candidate = attachedworkeroutput.Candidate{}
		owner.slot.original = nil
		owner.slot.request = ports.ExecutionRequest{}
		owner.slot.driver = nil
		owner.slot.cancel = nil
		owner.slot = nil
	}
}

func (owner *PreparedAttachedWorkerRunnerV1) Close(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return ErrContract
	}
	owner.mu.Lock()
	owner.closed = true
	slot := owner.slot
	if slot == nil {
		owner.mu.Unlock()
		return nil
	}
	if slot.finalized {
		owner.clearSlotLocked()
		owner.mu.Unlock()
		return nil
	}
	slot.cancel()
	done := slot.done
	owner.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type preparedCheckpointSink struct{ emitted bool }

func (sink *preparedCheckpointSink) Emit(ctx context.Context, event ports.ExecutionEvent) error {
	defer zeroBytes(event.CheckpointState)
	if ctx == nil || ctx.Err() != nil || sink.emitted || event.Sequence != 1 || event.Boundary != "codex.turn_completed" ||
		!reflect.DeepEqual(event.CheckpointState, completedCheckpointV1) {
		return ErrContract
	}
	sink.emitted = true
	return nil
}

var _ attachedworkerdaemon.Runner = (*PreparedAttachedWorkerRunnerV1)(nil)
