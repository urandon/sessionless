package codexexec

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// PreparedAttachedWorkerRunnerV1 is an explicitly selected fixed composition,
// not a spawn port or an issuer. The old constructor remains text-only. Its
// sole InvocationRunner owns the credential lifecycle.
// One pending/finalized slot is kept. For successful inline output only exact
// receipt retirement relinquishes it; Close fences work, not semantic custody.
type PreparedAttachedWorkerRunnerV1 struct {
	config           Config
	mediaConfig      *MediaConfigV1
	nativeConfigV2   *NativePreparedConfigV2
	nativeProfileV2  *nativeFrozenProfileV2
	nativeConfigV3   *NativePreparedConfigV3
	nativeProfileV3  *nativeFrozenProfileV3
	nativeSessionV2  *attachedworkersession.Session
	nativeIdentityV2 *PreparedAttachedWorkerRunnerV1
	scratch          string
	supervisor       *attachedworkerdaemon.Supervisor
	outer            *attachedworkerdaemon.InvocationRunner
	mu               sync.Mutex
	slot             *preparedResultSlot
	last             attachedworkerdaemon.InvocationIdentity
	closed           bool
	preparing        bool
	preparingDone    chan struct{}
}

type preparedResultSlot struct {
	identity                              attachedworkerdaemon.InvocationIdentity
	driver                                *Driver
	request                               ports.ExecutionRequest
	accepted                              domain.AttachedWorkerAttemptV1
	callerDeadline                        time.Time
	credentialDeadline                    time.Time
	stage                                 *preparedHistory
	media                                 *mediaInput
	mediaAssociation                      [32]byte
	semantic                              ports.ExecutionResult
	candidate                             attachedworkeroutput.Candidate
	inline                                *NativeInlineOutputV2
	nativeEvidenceV3                      *domain.ProviderEvidenceBundleV1
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
	if owner == nil || owner.nativeSelected() && owner.nativeIdentityV2 != owner || ctx == nil || ctx.Err() != nil {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	var nativeSession *attachedworkersession.Session
	if owner.mediaConfig != nil || owner.nativeSelected() {
		// One owner may hold only one media preflight/decoder/copy set. Reject
		// concurrent/replayed calls before source projection or deep allocation.
		owner.mu.Lock()
		if owner.closed || owner.preparing || owner.slot != nil || owner.last == invocation.Identity || owner.nativeSelected() && owner.nativeSessionV2 == nil {
			owner.mu.Unlock()
			return attachedworkerdaemon.InvocationResult{}, ErrProcessAlreadyConsumed
		}
		owner.preparing = true
		nativeSession = owner.nativeSessionV2
		owner.preparingDone = make(chan struct{})
		owner.mu.Unlock()
		defer func() {
			owner.mu.Lock()
			owner.preparing = false
			close(owner.preparingDone)
			owner.preparingDone = nil
			owner.mu.Unlock()
		}()
	}
	if invocation.Validate() != nil || invocation.Credential == nil ||
		invocation.VerifiedInput.ValidateFor(invocation.Identity) != nil {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	input, err := invocation.VerifiedInput.Clone()
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	defer input.Clear()
	job := input.Job
	if owner.nativeSelected() {
		deadline, err := acceptedNativeRuntimeDeadlineV2(input, owner.nativeNowV2())
		if err != nil {
			return attachedworkerdaemon.InvocationResult{}, err
		}
		var stopRuntime context.CancelFunc
		ctx, stopRuntime = context.WithDeadline(ctx, deadline)
		defer stopRuntime()
	}
	var media *mediaInput
	var generationEvidence *domain.ProviderEvidenceBundleV1
	if owner.mediaConfig != nil || owner.nativeSelected() {
		var descriptor domain.HarnessBackendDescriptorV1
		var descriptorErr error
		if owner.nativeConfigV3 != nil {
			bundle, qualifyErr := owner.qualifyNativeGenerationV3(ctx, input, nativeSession)
			if qualifyErr != nil {
				return attachedworkerdaemon.InvocationResult{}, qualifyErr
			}
			generationEvidence, descriptor = &bundle, owner.nativeProfileV3.descriptor
		} else if owner.nativeConfigV2 != nil {
			descriptor = owner.nativeProfileV2.descriptor
			if validateNativeEnvelopeEvidenceV2(owner.nativeProfileV2, owner.nativeConfigV2.Evidence, job.HarnessBinding, owner.nativeNowV2()) != nil || verifyNativeArtifactsV2(ctx, owner.nativeProfileV2) != nil {
				return attachedworkerdaemon.InvocationResult{}, ErrContract
			}
		} else {
			descriptor, descriptorErr = owner.mediaConfig.DescriptorV1()
		}
		if !owner.config.Enabled || descriptorErr != nil || descriptor != job.HarnessBinding.Backend || !preparedIngressEnvelope(input) {
			return attachedworkerdaemon.InvocationResult{}, ErrContract
		}
		media, err = prepareMediaInput(input)
		if err != nil {
			return attachedworkerdaemon.InvocationResult{}, err
		}
		defer media.clear()
		if owner.mediaConfig != nil {
			err = owner.mediaConfig.eligibility()
		}
		if err != nil {
			return attachedworkerdaemon.InvocationResult{}, err
		}
	}
	// Unsupported authority-bearing material is rejected, never stripped.
	arguments := processArguments(owner.config.Model)
	if owner.nativeSelected() {
		arguments = owner.nativeLaunch().NativeArguments[0]
	}
	if job.ContextWindow == nil || input.CanonicalContext == nil || (media == nil && !preparedTextEnvelope(input)) ||
		job.WorkspaceSnapshot != nil || job.SkillBundle != nil || len(job.AllowedMCPServers) != 0 ||
		invocation.Process.Executable != owner.config.Executable || invocation.Process.ExecutableDigest != owner.config.ExecutableDigest ||
		!sameStrings(invocation.Process.Arguments, arguments) {
		return attachedworkerdaemon.InvocationResult{}, ErrContract
	}
	var driver *Driver
	if owner.nativeConfigV3 != nil {
		driver, err = owner.nativeGenerationDriverV3(ctx, input, nativeSession, *generationEvidence)
	} else if owner.nativeConfigV2 != nil {
		driver, err = owner.nativeDriverV2(ctx, input, nativeSession)
	} else {
		driver, err = NewPreparedAttachedWorkerDriverV1(owner.config, input.AcceptedAttempt, job.HarnessBinding.Resource, owner.supervisor)
	}
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, err
	}
	if media != nil && owner.mediaConfig != nil {
		driver.mediaConfig = owner.mediaConfig
		driver.descriptor, err = owner.mediaConfig.DescriptorV1()
		if err != nil {
			return attachedworkerdaemon.InvocationResult{}, err
		}
		boundary, ok := driver.boundary.(*PreparedProcessBoundaryV1)
		if !ok {
			return attachedworkerdaemon.InvocationResult{}, ErrContract
		}
		boundary.mediaConfig = owner.mediaConfig
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
	if media != nil {
		if err = stage.stageImages(media); err != nil {
			return attachedworkerdaemon.InvocationResult{}, errors.Join(err, stage.remove())
		}
		driver.media, driver.mediaStage = media, stage
		if driver.nativeSelected() {
			var nativeStage *nativeInputStageV2
			var stageErr error
			if driver.nativeProfileV3 != nil {
				nativeStage, stageErr = stageNativeGenerationInputV3(stage, media, driver.nativeProfileV3)
			} else {
				nativeStage, stageErr = stageNativeInputV2(stage, media, driver.nativeProfileV2)
			}
			if stageErr != nil {
				return attachedworkerdaemon.InvocationResult{}, errors.Join(stageErr, stage.remove())
			}
			boundary := driver.boundary.(*PreparedProcessBoundaryV1)
			boundary.native.stageV2 = nativeStage
		}
	}
	prompt, err := driver.compilePrompt(request)
	zeroBytes(prompt)
	if err != nil {
		return attachedworkerdaemon.InvocationResult{}, errors.Join(err, stage.remove())
	}
	runCtx, cancel := context.WithCancel(ctx)
	slot := &preparedResultSlot{identity: invocation.Identity, driver: driver, request: request, accepted: input.AcceptedAttempt, stage: stage, media: media, cancel: cancel, done: make(chan struct{})}
	slot.nativeEvidenceV3 = generationEvidence
	slot.callerDeadline, _ = runCtx.Deadline()
	if media != nil {
		slot.mediaAssociation = media.associationDigest
	}
	owner.mu.Lock()
	if owner.closed || owner.slot != nil || owner.last == invocation.Identity {
		owner.mu.Unlock()
		cancel()
		return attachedworkerdaemon.InvocationResult{}, errors.Join(ErrProcessAlreadyConsumed, stage.remove())
	}
	owner.slot = slot
	owner.mu.Unlock()
	invocation.VerifiedInput = input
	// This is the final NEW-input fence immediately before the SAME outer
	// lifecycle's sole Issue. Failure still owns/cleans the original stage.
	var result attachedworkerdaemon.InvocationResult
	var runErr error
	if owner.nativeConfigV3 != nil {
		runErr = owner.checkNativeGenerationV3(runCtx, nativeSession, job.HarnessBinding.Resource)
	}
	if runErr == nil {
		result, runErr = owner.outer.Run(runCtx, invocation)
	}
	cancel()
	stageErr := stage.remove()
	if stageErr != nil {
		runErr = errors.Join(runErr, ErrContract)
	}
	owner.mu.Lock()
	slot.result, slot.runErr, slot.stageClean, slot.finalized = result, runErr, stageErr == nil, true
	close(slot.done)
	if owner.closed {
		owner.closeSlotLocked()
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
	return preparedIngressEnvelope(input)
}

func preparedIngressEnvelope(input *attachedworkerdaemon.VerifiedAttachedInputV1) bool {
	if input == nil || input.CanonicalContext == nil || input.Job.ContextWindow == nil {
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
	slot.credentialDeadline = prepared.Credential.ExpiresAt
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
	if (owner.mediaConfig != nil || owner.nativeSelected()) && (original.mediaAssociation != slot.mediaAssociation || original.mediaProfileDigest != request.HarnessBinding.Backend.BackendProfileDigest) {
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
	if owner.nativeSelected() && owner.nativeIdentityV2 != owner {
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
		if slot.original.nativeFinalV2 != nil && len(slot.original.nativeFinalV2.artifacts) != 0 {
			return attachedworkeroutput.Candidate{}, ErrContract
		}
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
		// Only the original private parse can distinguish a valid failed
		// terminal from an error notice or lost reply. Public evidence strings
		// are insufficient. Both paths still require the exact outer result
		// and successful cleanup above; neither reopens execution authority.
		if runErr == nil || slot.semantic.ProviderEvidence == nil {
			return attachedworkeroutput.Candidate{}, ErrContract
		}
		if slot.semantic.ProviderEvidence.FinishClass == domain.ProviderFinishCompletedV1 {
			candidate.FailureCode = "codex_exec_finalization_failed"
		} else if slot.original.self == slot.original && slot.original.protocol.failedTerminal &&
			!slot.original.protocol.protocolDrift && !slot.original.protocol.terminalDrift &&
			slot.semantic.ProviderEvidence.FinishClass == domain.ProviderFinishFailedV1 &&
			slot.semantic.ProviderEvidence.FailureCode == domain.ProviderExecutionFailureProviderFailedV1 {
			candidate.FailureCode = "codex_exec_provider_failed"
		} else {
			return attachedworkeroutput.Candidate{}, ErrContract
		}
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
	if owner == nil || owner.nativeSelected() && owner.nativeIdentityV2 != owner {
		return ErrContract
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.slot == nil || owner.slot.identity != identity || !owner.slot.finalized || !owner.slot.candidateBuilt {
		return ErrContract
	}
	if err := owner.clearSlotLocked(); err != nil {
		return err
	}
	owner.last = identity
	return nil
}

func (owner *PreparedAttachedWorkerRunnerV1) clearSlotLocked() error {
	if owner.slot != nil {
		if history := owner.slot.stage; history != nil && history.nativeV2 != nil {
			native := history.nativeV2
			// Failed handback cannot relinquish the only retained owner merely
			// because Close was requested. The consumed slot remains unavailable.
			if !owner.slot.stageClean || native.self != native || native.history != history || native.dispatched && !native.quiesced {
				return ErrContract
			}
		}
		if owner.slot.original != nil {
			owner.slot.original.nativeFinalV2.clear()
			zeroBytes(owner.slot.original.protocol.final)
		}
		owner.slot.semantic = ports.ExecutionResult{}
		owner.slot.candidate = attachedworkeroutput.Candidate{}
		owner.slot.original = nil
		owner.slot.request = ports.ExecutionRequest{}
		owner.slot.accepted = domain.AttachedWorkerAttemptV1{}
		owner.slot.nativeEvidenceV3 = nil
		if owner.slot.inline != nil {
			owner.slot.inline.candidate = attachedworkeroutput.InlineCandidateV2{}
			owner.slot.inline.producer = attachedworkeroutput.InlineProducerBindingV2{}
		}
		owner.slot.driver = nil
		owner.slot.cancel = nil
		owner.slot = nil
	}
	return nil
}

// Close is not an ACK or definitive disposition. Keep the actual completed
// inline owner even if Close won the race before its carrier was requested.
// Failure/non-inline slots retain their existing cleanup and custody checks.
func (owner *PreparedAttachedWorkerRunnerV1) closeSlotLocked() error {
	if owner.pendingInlineReceiptLockedV2() {
		return ErrInlineReceiptPendingV2
	}
	return owner.clearSlotLocked()
}

func (owner *PreparedAttachedWorkerRunnerV1) Close(ctx context.Context) error {
	if owner == nil || owner.nativeSelected() && owner.nativeIdentityV2 != owner || ctx == nil {
		return ErrContract
	}
	owner.mu.Lock()
	owner.closed = true
	slot := owner.slot
	mediaDone := owner.preparingDone
	if slot == nil {
		owner.mu.Unlock()
		if mediaDone != nil {
			select {
			case <-mediaDone:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	if slot.finalized {
		clearErr := owner.closeSlotLocked()
		owner.mu.Unlock()
		if clearErr != nil {
			return clearErr
		}
		if mediaDone != nil {
			select {
			case <-mediaDone:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	slot.cancel()
	done := slot.done
	if mediaDone != nil {
		done = mediaDone
	}
	owner.mu.Unlock()
	select {
	case <-done:
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return owner.closeSlotLocked()
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
