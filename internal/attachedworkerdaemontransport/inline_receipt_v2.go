package attachedworkerdaemontransport

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// InlineReceiptPublisherV2 is a one-exchange wire port, not provenance or a
// retry policy. Only the pinned original prepared owner supplies the body.
type InlineReceiptPublisherV2 interface {
	Prepare(context.Context, attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
	Submit(context.Context, attachedworkeroutput.AttachedWorkerInlineBodyV2, int64, io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
	Reconcile(context.Context, attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
}

type InlineReceiptJournalV2 interface {
	SealInlineReceiptSubmissionV2(context.Context, []byte) error
	AdvanceInlineReceiptSubmissionV2(context.Context, []byte, []byte) (bool, error)
	RetireInlineReceiptSubmissionV2(context.Context, []byte) error
}

// This belongs to the existing activeAttempt. It is never reconstructed from
// a public declaration or idempotent journal seal after loss/restart.
type inlineReceiptAttemptV2 struct {
	source     *codexexec.NativeInlineOutputV2
	checkpoint attachedworkeroutput.InlineReceiptCheckpointV2
	completion [32]byte
	blocked    bool
	retired    bool
}

func (adapter *Adapter) publishInlineReceiptV2(ctx context.Context, active *activeAttempt, result attachedworkerdaemon.InvocationResult, runErr error) ([]byte, error) {
	if !adapter.config.preparedInlineV2 || adapter.config.preparedReceipt == nil || adapter.config.InlineReceiptPublisher == nil || !result.Succeeded(runErr) || !result.CleanupComplete() {
		return nil, ErrTerminalEvidenceInvalid
	}
	fingerprint, err := completionFingerprint(active.identity, result, runErr)
	if err != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	state := active.inline
	if state != nil {
		if state.completion != fingerprint || state.blocked {
			return nil, ErrReconciliationRequired
		}
		if state.checkpoint.Progress.CanonicalDigest != "" {
			return hex.DecodeString(string(state.checkpoint.Progress.CanonicalDigest))
		}
		// A previous call consumed Prepare, even if its response was lost.
		// This path can spend only the one remaining metadata reconciliation.
		return adapter.reconcileInlineReceiptV2(ctx, state)
	}
	source, err := adapter.config.preparedReceipt.inlineReceiptCandidate(ctx, active.identity, result, runErr)
	if err != nil {
		return nil, errors.Join(ErrTerminalEvidenceInvalid, err)
	}
	candidate, producer, expiry, err := source.Declaration()
	if err != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	observation := receiptProcessObservation(result, runErr)
	observationDigest, err := observation.Digest()
	if err != nil || observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, result.CredentialReleaseRequired) != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	prepare := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineReceiptAuthorizationV2(active.request),
		Nonce: receiptNonce(active.identity), Candidate: candidate, Producer: producer, Observation: observation, ExpiresAt: expiry}
	planFingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(prepare)
	if err != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	plan := attachedworkeroutput.InlineReceiptPlanV2{Authorization: prepare.Authorization, Nonce: prepare.Nonce, Fingerprint: planFingerprint,
		Producer: producer, Artifacts: candidate.Artifacts, ObservationDigest: observationDigest, ExpiresAt: expiry,
		WorkerAttemptSequence: active.nextWorkerAttemptSequence, PlatformAttemptSequence: active.nextPlatformAttemptSequence, TerminalSequence: 1}
	planDigest, err := plan.Digest()
	if err != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	state = &inlineReceiptAttemptV2{source: source, completion: fingerprint, checkpoint: attachedworkeroutput.InlineReceiptCheckpointV2{
		Version: 2, Plan: plan, PlanSHA256: planDigest, Progress: attachedworkeroutput.InlineReceiptProgressV2{Revision: 1, PrepareBurned: true}}}
	// Install the private consumed identity BEFORE any durable or network call.
	// A failed/ambiguous seal cannot be retried to mint a fresh live token.
	active.inline = state
	sealed, err := attachedworkeroutput.EncodeInlineCheckpointV2(state.checkpoint)
	if err != nil {
		state.blocked = true
		return nil, ErrTerminalEvidenceInvalid
	}
	defer clear(sealed)
	if err := adapter.session.(InlineReceiptJournalV2).SealInlineReceiptSubmissionV2(ctx, sealed); err != nil {
		state.blocked = true
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	bound, cancel := context.WithDeadline(ctx, expiry)
	defer cancel()
	if err := source.CheckCurrent(bound); err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	response, err := adapter.config.InlineReceiptPublisher.Prepare(bound, prepare)
	if err != nil {
		return adapter.reconcileInlineReceiptV2(bound, state)
	}
	return adapter.resolveInlineReceiptV2(bound, state, response)
}

func receiptProcessObservation(result attachedworkerdaemon.InvocationResult, runErr error) attachedworkeroutput.ProcessObservationV1 {
	return attachedworkeroutput.ProcessObservationV1{Version: 1, ExitCode: result.Process.ExitCode, Cancelled: result.Process.Cancelled,
		Deadline: result.Process.Deadline, ProcessFailureCode: safeFailureCode(result.Process.FailureCode),
		InvocationFailureCode: safeFailureCode(result.FailureCode), RunnerFailed: runErr != nil,
		DescendantsReaped: result.Process.DescendantsReaped, BoundaryReleased: result.Process.BoundaryReleased,
		CleanupSucceeded: result.Process.CleanupSucceeded, CredentialReleaseRequired: result.CredentialReleaseRequired, CredentialReleased: result.CredentialReleased}
}

func inlineReceiptAuthorizationV2(request MaterializationRequestV1) ports.AttachedWorkerSealedInputAuthorization {
	b := request.Attempt
	return ports.AttachedWorkerSealedInputAuthorization{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID,
		WorkerID: request.WorkerID, ConnectionID: request.ConnectionID, EnrollmentGeneration: request.EnrollmentGeneration, ConnectionGeneration: request.ConnectionGeneration,
		RunID: domain.RunID(b.RunID), AttemptID: domain.AttemptID(b.AttemptID), AttemptSequence: request.AttemptSequence,
		LeaseID: domain.LeaseID(b.LeaseID), LeaseGeneration: b.LeaseGeneration, FenceToken: domain.AttachedWorkerFenceToken(b.FenceToken), LeaseExpiresAtUnixMicro: b.ExpiresAtUnixMicro,
		ContextDigest: domain.AttachedWorkerContextDigest(hex.EncodeToString(b.ContextDigest)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(b.CapabilityDigest)),
		PolicyDigest: domain.AttachedWorkerPolicyDigest(hex.EncodeToString(b.PolicyDigest))}
}

// Recovery authenticates the retained original result, not fresh execution.
// The real Session/protocol and server finalizer still decide whether an exact
// Terminal/replay is admissible. This does not change their lease predicates.
func (adapter *Adapter) validateInlineRecoveryAuthorityV2(active *activeAttempt, completion [32]byte) error {
	snapshot, err := adapter.readySnapshot()
	if err != nil {
		return errors.Join(ErrReconciliationRequired, err)
	}
	s := active.inline
	if s == nil || s.blocked || s.source == nil || s.completion != completion || s.checkpoint.Validate() != nil {
		return ErrReconciliationRequired
	}
	p := s.checkpoint.Plan
	r := active.request
	if p.Authorization != inlineReceiptAuthorizationV2(r) || p.WorkerAttemptSequence != active.nextWorkerAttemptSequence ||
		p.PlatformAttemptSequence != active.nextPlatformAttemptSequence || p.TerminalSequence != 1 ||
		r.TenantID != snapshot.TenantID || r.OwnerUserID != snapshot.OwnerUserID || r.WorkerID != snapshot.WorkerID ||
		r.ConnectionID != snapshot.ConnectionID || r.EnrollmentGeneration != snapshot.EnrollmentGeneration || r.ConnectionGeneration != snapshot.ConnectionGeneration ||
		r.Attempt.Validate() != nil || p.Authorization.CapabilityDigest != snapshot.CapabilityDigest {
		return ErrReconciliationRequired
	}
	candidate, producer, expiry, err := s.source.Declaration()
	if err != nil || producer != p.Producer || !expiry.Equal(p.ExpiresAt) || !slices.Equal(candidate.Artifacts, p.Artifacts) {
		return ErrReconciliationRequired
	}
	return nil
}

func (adapter *Adapter) advanceInlineReceiptV2(ctx context.Context, state *inlineReceiptAttemptV2, next attachedworkeroutput.InlineReceiptCheckpointV2) error {
	previousBytes, err := attachedworkeroutput.EncodeInlineCheckpointV2(state.checkpoint)
	if err != nil {
		return ErrTerminalEvidenceInvalid
	}
	defer clear(previousBytes)
	nextBytes, err := attachedworkeroutput.EncodeInlineCheckpointV2(next)
	if err != nil {
		return ErrTerminalEvidenceInvalid
	}
	defer clear(nextBytes)
	applied, err := adapter.session.(InlineReceiptJournalV2).AdvanceInlineReceiptSubmissionV2(ctx, previousBytes, nextBytes)
	if err != nil || !applied {
		state.blocked = true // an ambiguous or observed transition grants no token
		return errors.Join(ErrReconciliationRequired, err)
	}
	state.checkpoint = next
	return nil
}

func (adapter *Adapter) resolveInlineReceiptV2(ctx context.Context, state *inlineReceiptAttemptV2, response attachedworkeroutput.AttachedWorkerInlineResultV2) ([]byte, error) {
	p := state.checkpoint.Plan
	if response.Version != 2 || response.Nonce != p.Nonce || response.Fingerprint != p.Fingerprint || !response.ExpiresAt.Equal(p.ExpiresAt) {
		return nil, ErrTerminalEvidenceInvalid
	}
	if response.Stage == attachedworkeroutput.InlineReadyV2 {
		if response.CanonicalDigest.Validate() != nil || response.Capability != "" || len(p.Artifacts) != 0 && !state.checkpoint.Progress.BodyBurned {
			return nil, ErrTerminalEvidenceInvalid
		}
		next := state.checkpoint
		next.Progress.Revision++
		next.Progress.CanonicalDigest = response.CanonicalDigest
		if err := adapter.advanceInlineReceiptV2(ctx, state, next); err != nil {
			return nil, err
		}
		return hex.DecodeString(string(response.CanonicalDigest))
	}
	if !adapter.config.Now().Before(p.ExpiresAt) || response.Stage != attachedworkeroutput.InlinePreparedV2 || state.checkpoint.Progress.BodyBurned || len(p.Artifacts) == 0 ||
		domain.HarnessBindingDigestV2(response.Capability).Validate() != nil || response.CanonicalDigest != "" || state.source == nil {
		return nil, ErrReconciliationRequired
	}
	next := state.checkpoint
	next.Progress.Revision++
	next.Progress.BodyBurned = true
	if err := adapter.advanceInlineReceiptV2(ctx, state, next); err != nil {
		return nil, err
	}
	if err := state.source.CheckCurrent(ctx); err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	body, size, err := state.source.OpenBody(ctx)
	if err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	result, sendErr := adapter.config.InlineReceiptPublisher.Submit(ctx, attachedworkeroutput.AttachedWorkerInlineBodyV2{
		Version: 2, Authorization: p.Authorization, Nonce: p.Nonce, Fingerprint: p.Fingerprint, Capability: response.Capability, ExpiresAt: p.ExpiresAt}, size, body)
	closeErr := body.Close()
	if sendErr != nil || closeErr != nil || result.Stage != attachedworkeroutput.InlineReadyV2 {
		return adapter.reconcileInlineReceiptV2(ctx, state)
	}
	return adapter.resolveInlineReceiptV2(ctx, state, result)
}

func (adapter *Adapter) reconcileInlineReceiptV2(ctx context.Context, state *inlineReceiptAttemptV2) ([]byte, error) {
	if state.blocked || state.checkpoint.Progress.ReconcileBurned || ctx == nil || ctx.Err() != nil {
		return nil, ErrReconciliationRequired
	}
	deadline := state.checkpoint.Plan.ExpiresAt
	metadataOnly := !adapter.config.Now().Before(deadline)
	if metadataOnly {
		snapshot, err := adapter.readySnapshot()
		if err != nil {
			return nil, errors.Join(ErrReconciliationRequired, err)
		}
		deadline = adapter.config.Now().Add(15 * time.Second)
		if snapshot.AuthenticationExpires.Before(deadline) {
			deadline = *snapshot.AuthenticationExpires
		}
	}
	bound, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	next := state.checkpoint
	next.Progress.Revision++
	next.Progress.ReconcileBurned = true
	if err := adapter.advanceInlineReceiptV2(bound, state, next); err != nil {
		return nil, err
	}
	p := state.checkpoint.Plan
	response, err := adapter.config.InlineReceiptPublisher.Reconcile(bound, attachedworkeroutput.AttachedWorkerInlineReconcileV2{
		Version: 2, Authorization: p.Authorization, Nonce: p.Nonce, Fingerprint: p.Fingerprint, BodySendBurned: metadataOnly || state.checkpoint.Progress.BodyBurned})
	if err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	if metadataOnly && (response.Stage != attachedworkeroutput.InlineReadyV2 || response.Capability != "") {
		return nil, ErrReconciliationRequired
	}
	return adapter.resolveInlineReceiptV2(bound, state, response)
}

// Called only AFTER the existing completeOwned verified the full current
// TerminalAck binding/sequences/status/result/digest. The local ACK bit itself
// never substitutes for that protocol authority.
func (adapter *Adapter) ackInlineReceiptV2(ctx context.Context, active *activeAttempt) error {
	state := active.inline
	if state == nil || state.blocked || state.checkpoint.Progress.CanonicalDigest == "" {
		return ErrReconciliationRequired
	}
	if state.retired {
		return nil
	}
	if !state.checkpoint.Progress.TerminalAcked {
		next := state.checkpoint
		next.Progress.Revision++
		next.Progress.TerminalAcked = true
		if err := adapter.advanceInlineReceiptV2(ctx, state, next); err != nil {
			return err
		}
	}
	sealed, err := attachedworkeroutput.EncodeInlineCheckpointV2(state.checkpoint)
	if err != nil {
		return err
	}
	defer clear(sealed)
	if err := adapter.session.(InlineReceiptJournalV2).RetireInlineReceiptSubmissionV2(ctx, sealed); err != nil {
		return err
	}
	state.retired = true
	return nil
}
