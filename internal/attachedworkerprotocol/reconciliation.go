package attachedworkerprotocol

import (
	"bytes"
	"crypto/sha256"
)

const (
	attemptSummaryDomainV1    = "sessionless.attached-worker.attempt-summary.v1"
	reconnectSnapshotDomainV1 = "sessionless.attached-worker.reconnect-snapshot.v1"
)

type ReconnectSnapshotV1 struct {
	PreviousConnectionGeneration uint64                 `json:"previous_connection_generation"`
	Watermarks                   ConnectionWatermarksV1 `json:"watermarks"`
	Attempt                      AttemptSummaryV1       `json:"attempt"`
	Digest                       []byte                 `json:"digest"`
}

func (snapshot ReconnectSnapshotV1) Validate() error {
	if snapshot.PreviousConnectionGeneration == 0 || snapshot.Watermarks.Validate() != nil || snapshot.Attempt.Validate() != nil ||
		!validBytes(snapshot.Digest, sha256.Size) || !bytes.Equal(reconnectSnapshotDigestV1(snapshot), snapshot.Digest) {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func reconnectSnapshotDigestV1(snapshot ReconnectSnapshotV1) []byte {
	result := appendCanonicalField(nil, []byte(reconnectSnapshotDomainV1))
	result = appendCanonicalUint64(result, snapshot.PreviousConnectionGeneration)
	result = appendWatermarks(result, snapshot.Watermarks)
	result = appendCanonicalField(result, snapshot.Attempt.Digest)
	digest := sha256.Sum256(result)
	return append([]byte(nil), digest[:]...)
}

func sealReconnectSnapshot(snapshot ReconnectSnapshotV1) ReconnectSnapshotV1 {
	snapshot.Attempt = cloneAttemptSummary(snapshot.Attempt)
	snapshot.Digest = reconnectSnapshotDigestV1(snapshot)
	return snapshot
}

type ReconnectNegotiationV1 struct {
	WorkerOffer      VersionOfferV1
	PlatformOffer    VersionOfferV1
	SelectedVersion  ProtocolVersion
	WorkerNonce      []byte
	PlatformNonce    []byte
	CapabilityDigest []byte
}

func (input ReconnectNegotiationV1) validate() error {
	return validateNegotiationProof(input.WorkerOffer, input.PlatformOffer, input.SelectedVersion,
		input.WorkerNonce, input.PlatformNonce, input.CapabilityDigest)
}

type ReplayPlanV1 struct {
	// Envelope replay watermarks refer to the previous connection. Replayed
	// messages are wrapped in fresh envelopes on the new connection.
	PlatformEnvelopeAfter uint64 `json:"platform_envelope_after"`
	WorkerEnvelopeAfter   uint64 `json:"worker_envelope_after"`
	// Attempt replay watermarks retain their direction-local attempt sequence.
	PlatformAttemptAfter uint64 `json:"platform_attempt_after"`
	WorkerAttemptAfter   uint64 `json:"worker_attempt_after"`
	// TerminalDecision is an explicit control-plane decision: replay is not a
	// commit, and only committed permits worker-side erasure.
	TerminalDecision ReconnectTerminalDecision `json:"terminal_decision"`
}

type ReconnectTerminalDecision string

const (
	ReconnectTerminalNone      ReconnectTerminalDecision = "none"
	ReconnectTerminalReplay    ReconnectTerminalDecision = "replay"
	ReconnectTerminalCommitted ReconnectTerminalDecision = "committed"
)

func (plan ReplayPlanV1) Validate() error {
	if plan.PlatformAttemptAfter > MaxAttemptMessages || plan.WorkerAttemptAfter > MaxAttemptMessages {
		return protocolError(ErrorMalformedFrame)
	}
	switch plan.TerminalDecision {
	case ReconnectTerminalNone, ReconnectTerminalReplay, ReconnectTerminalCommitted:
		return nil
	default:
		return protocolError(ErrorMalformedFrame)
	}
}

func BuildReconnectV1(snapshot ReconnectSnapshotV1, negotiation ReconnectNegotiationV1) (ReconnectV1, error) {
	if snapshot.Validate() != nil || negotiation.validate() != nil {
		return ReconnectV1{}, protocolError(ErrorMalformedFrame)
	}
	return ReconnectV1{
		WorkerOffer: cloneVersionOffer(negotiation.WorkerOffer), PlatformOffer: cloneVersionOffer(negotiation.PlatformOffer),
		SelectedVersion: negotiation.SelectedVersion, PreviousConnectionGeneration: snapshot.PreviousConnectionGeneration,
		WorkerNonce: append([]byte(nil), negotiation.WorkerNonce...), PlatformNonce: append([]byte(nil), negotiation.PlatformNonce...),
		CapabilityDigest: append([]byte(nil), negotiation.CapabilityDigest...), PreviousWatermarks: snapshot.Watermarks,
		AttemptSummary: cloneAttemptSummary(snapshot.Attempt), Signature: []byte{},
	}, nil
}

func BuildReconnectAcceptedV1(
	authoritative ReconnectSnapshotV1,
	workerClaim ReconnectSnapshotV1,
	negotiation ReconnectNegotiationV1,
) (ReconnectAcceptedV1, error) {
	if authoritative.Validate() != nil || workerClaim.Validate() != nil || negotiation.validate() != nil ||
		authoritative.PreviousConnectionGeneration != workerClaim.PreviousConnectionGeneration ||
		!reconnectClaimsCompatible(workerClaim, authoritative) {
		return ReconnectAcceptedV1{}, protocolError(ErrorConflict)
	}
	decision := ReconnectTerminalNone
	if authoritative.Attempt.State == AttemptTerminalCommitted {
		decision = ReconnectTerminalCommitted
	} else if workerClaim.Attempt.TerminalSequence > authoritative.Attempt.TerminalSequence {
		decision = ReconnectTerminalReplay
	}
	return ReconnectAcceptedV1{
		WorkerOffer: cloneVersionOffer(negotiation.WorkerOffer), PlatformOffer: cloneVersionOffer(negotiation.PlatformOffer),
		SelectedVersion: negotiation.SelectedVersion, WorkerNonce: append([]byte(nil), negotiation.WorkerNonce...),
		PlatformNonce: append([]byte(nil), negotiation.PlatformNonce...), CapabilityDigest: append([]byte(nil), negotiation.CapabilityDigest...),
		AuthoritativeWatermarks: authoritative.Watermarks, AuthoritativeAttempt: cloneAttemptSummary(authoritative.Attempt),
		ReplayPlan: ReplayPlanV1{
			PlatformEnvelopeAfter: workerClaim.Watermarks.PlatformSequence,
			WorkerEnvelopeAfter:   authoritative.Watermarks.WorkerSequence,
			PlatformAttemptAfter:  workerClaim.Attempt.PlatformSequence,
			WorkerAttemptAfter:    authoritative.Attempt.WorkerSequence,
			TerminalDecision:      decision,
		},
	}, nil
}

type ConnectionWatermarksV1 struct {
	PlatformSequence uint64 `json:"platform_sequence"`
	WorkerSequence   uint64 `json:"worker_sequence"`
	PlatformAck      uint64 `json:"platform_ack"`
	WorkerAck        uint64 `json:"worker_ack"`
}

func (watermarks ConnectionWatermarksV1) Validate() error {
	if watermarks.PlatformAck > watermarks.WorkerSequence || watermarks.WorkerAck > watermarks.PlatformSequence {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

type AttemptSummaryV1 struct {
	State                  AttemptState     `json:"state"`
	Binding                AttemptBindingV1 `json:"binding"`
	PlatformSequence       uint64           `json:"platform_sequence"`
	WorkerSequence         uint64           `json:"worker_sequence"`
	ProgressSequence       uint64           `json:"progress_sequence"`
	CancelRevision         uint64           `json:"cancel_revision"`
	CancelCode             CancelCode       `json:"cancel_code"`
	TerminalSequence       uint64           `json:"terminal_sequence"`
	TerminalStatus         TerminalStatus   `json:"terminal_status"`
	TerminalResult         TerminalResult   `json:"terminal_result"`
	TerminalEvidenceDigest []byte           `json:"terminal_evidence_digest"`
	Digest                 []byte           `json:"digest"`
}

func (summary AttemptSummaryV1) Validate() error {
	if summary.PlatformSequence > MaxAttemptMessages || summary.WorkerSequence > MaxAttemptMessages ||
		summary.ProgressSequence > MaxProgressUpdates || !validBytes(summary.Digest, sha256.Size) {
		return protocolError(ErrorMalformedFrame)
	}
	if summary.State == AttemptIdle {
		if summary.PlatformSequence != 0 || summary.WorkerSequence != 0 || summary.ProgressSequence != 0 ||
			summary.CancelRevision != 0 || summary.CancelCode != "" || summary.TerminalSequence != 0 || summary.TerminalStatus != "" ||
			summary.TerminalResult != "" || len(summary.TerminalEvidenceDigest) != 0 {
			return protocolError(ErrorMalformedFrame)
		}
		if !attemptBindingIsZero(summary.Binding) {
			return protocolError(ErrorMalformedFrame)
		}
	} else if summary.Binding.Validate() != nil {
		return protocolError(ErrorMalformedFrame)
	}
	if summary.CancelRevision == 0 {
		if summary.CancelCode != "" {
			return protocolError(ErrorMalformedFrame)
		}
	} else if summary.CancelCode != CancelRequested && summary.CancelCode != CancelFenced {
		return protocolError(ErrorMalformedFrame)
	}
	if summary.TerminalSequence == 0 {
		if summary.TerminalStatus != "" || summary.TerminalResult != "" || len(summary.TerminalEvidenceDigest) != 0 {
			return protocolError(ErrorMalformedFrame)
		}
	} else if !validTerminalResult(summary.TerminalStatus, summary.TerminalResult) ||
		!validBytes(summary.TerminalEvidenceDigest, sha256.Size) {
		return protocolError(ErrorMalformedFrame)
	}
	if summary.CancelCode == CancelFenced && summary.TerminalSequence != 0 && summary.TerminalStatus != TerminalCancelled {
		return protocolError(ErrorMalformedFrame)
	}
	switch summary.State {
	case AttemptIdle:
	case AttemptOffered:
		if summary.PlatformSequence < 1 {
			return protocolError(ErrorMalformedFrame)
		}
	case AttemptClaimPending:
		if summary.PlatformSequence < 1 || summary.WorkerSequence < 1 {
			return protocolError(ErrorMalformedFrame)
		}
	case AttemptClaimed:
		if summary.PlatformSequence < 2 || summary.WorkerSequence < 1 {
			return protocolError(ErrorMalformedFrame)
		}
	case AttemptCancelRequested, AttemptCancelAcked, AttemptFenced:
		if summary.CancelRevision != 1 {
			return protocolError(ErrorMalformedFrame)
		}
	case AttemptTerminalPending, AttemptTerminalCommitted:
		if summary.TerminalSequence != 1 {
			return protocolError(ErrorMalformedFrame)
		}
	default:
		return protocolError(ErrorMalformedFrame)
	}
	digest, err := attemptSummaryDigestV1(summary)
	if err != nil || !bytes.Equal(digest, summary.Digest) {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func attemptSummaryDigestV1(summary AttemptSummaryV1) ([]byte, error) {
	result := appendCanonicalField(nil, []byte(attemptSummaryDomainV1))
	result = appendCanonicalField(result, []byte(summary.State))
	result = appendCanonicalField(result, []byte(summary.Binding.RunID))
	result = appendCanonicalField(result, []byte(summary.Binding.AttemptID))
	result = appendCanonicalField(result, []byte(summary.Binding.LeaseID))
	result = appendCanonicalUint64(result, summary.Binding.LeaseGeneration)
	result = appendCanonicalField(result, []byte(summary.Binding.FenceToken))
	result = appendCanonicalUint64(result, uint64(summary.Binding.ExpiresAtUnixMicro))
	result = appendCanonicalField(result, summary.Binding.ContextDigest)
	result = appendCanonicalField(result, summary.Binding.CapabilityDigest)
	result = appendCanonicalField(result, summary.Binding.PolicyDigest)
	result = appendCanonicalUint64(result, summary.PlatformSequence)
	result = appendCanonicalUint64(result, summary.WorkerSequence)
	result = appendCanonicalUint64(result, summary.ProgressSequence)
	result = appendCanonicalUint64(result, summary.CancelRevision)
	result = appendCanonicalField(result, []byte(summary.CancelCode))
	result = appendCanonicalUint64(result, summary.TerminalSequence)
	result = appendCanonicalField(result, []byte(summary.TerminalStatus))
	result = appendCanonicalField(result, []byte(summary.TerminalResult))
	result = appendCanonicalField(result, summary.TerminalEvidenceDigest)
	digest := sha256.Sum256(result)
	return append([]byte(nil), digest[:]...), nil
}

func sealAttemptSummary(summary AttemptSummaryV1) AttemptSummaryV1 {
	if summary.TerminalSequence == 0 {
		summary.TerminalEvidenceDigest = []byte{}
	}
	if summary.State == AttemptIdle {
		summary.Binding.ContextDigest = []byte{}
		summary.Binding.CapabilityDigest = []byte{}
		summary.Binding.PolicyDigest = []byte{}
	}
	summary.Digest, _ = attemptSummaryDigestV1(summary)
	return summary
}

func sameAttemptSummary(left, right AttemptSummaryV1) bool {
	return left.State == right.State && sameAttemptBinding(left.Binding, right.Binding) &&
		left.PlatformSequence == right.PlatformSequence && left.WorkerSequence == right.WorkerSequence &&
		left.ProgressSequence == right.ProgressSequence && left.CancelRevision == right.CancelRevision &&
		left.CancelCode == right.CancelCode && left.TerminalSequence == right.TerminalSequence &&
		left.TerminalStatus == right.TerminalStatus && left.TerminalResult == right.TerminalResult &&
		bytes.Equal(left.TerminalEvidenceDigest, right.TerminalEvidenceDigest) && bytes.Equal(left.Digest, right.Digest)
}

func summaryCanAdvance(from, to AttemptSummaryV1) bool {
	if from.Validate() != nil || to.Validate() != nil ||
		from.State != AttemptIdle && (to.State == AttemptIdle || !sameAttemptBinding(from.Binding, to.Binding)) ||
		from.State == AttemptIdle && to.State == AttemptIdle && !sameAttemptSummary(from, to) ||
		from.PlatformSequence > to.PlatformSequence || from.WorkerSequence > to.WorkerSequence ||
		from.ProgressSequence > to.ProgressSequence || from.CancelRevision > to.CancelRevision ||
		(from.TerminalSequence > to.TerminalSequence && !cancellationOverridesTerminal(to)) || !attemptStateCanAdvance(from, to) {
		return false
	}
	return true
}

func cancellationOverridesTerminal(summary AttemptSummaryV1) bool {
	return summary.CancelRevision > 0 && (summary.State == AttemptCancelRequested ||
		summary.State == AttemptCancelAcked || summary.State == AttemptFenced)
}

func attemptStateCanAdvance(from, to AttemptSummaryV1) bool {
	if from.State == AttemptTerminalCommitted {
		return to.State == AttemptTerminalCommitted
	}
	if to.CancelRevision > 0 &&
		(to.State == AttemptCancelRequested || to.State == AttemptCancelAcked || to.State == AttemptFenced) {
		return true
	}
	return attemptStateRank(from.State) <= attemptStateRank(to.State)
}

func attemptBindingIsZero(binding AttemptBindingV1) bool {
	return binding.RunID == "" && binding.AttemptID == "" && binding.LeaseID == "" && binding.LeaseGeneration == 0 &&
		binding.FenceToken == "" && binding.ExpiresAtUnixMicro == 0 && len(binding.ContextDigest) == 0 &&
		len(binding.CapabilityDigest) == 0 && len(binding.PolicyDigest) == 0
}

func attemptStateRank(state AttemptState) int {
	switch state {
	case AttemptIdle:
		return 0
	case AttemptOffered:
		return 1
	case AttemptClaimPending:
		return 2
	case AttemptClaimed:
		return 3
	case AttemptCancelRequested, AttemptCancelAcked, AttemptFenced:
		return 4
	case AttemptTerminalPending:
		return 5
	case AttemptTerminalCommitted:
		return 6
	default:
		return 100
	}
}

func watermarksCanAdvance(from, to ConnectionWatermarksV1) bool {
	return from.Validate() == nil && to.Validate() == nil && from.PlatformSequence <= to.PlatformSequence &&
		from.WorkerSequence <= to.WorkerSequence && from.PlatformAck <= to.PlatformAck && from.WorkerAck <= to.WorkerAck
}

func reconnectClaimsCompatible(workerClaim, authoritative ReconnectSnapshotV1) bool {
	if workerClaim.Validate() != nil || authoritative.Validate() != nil ||
		workerClaim.PreviousConnectionGeneration != authoritative.PreviousConnectionGeneration ||
		workerClaim.Watermarks.PlatformSequence > authoritative.Watermarks.PlatformSequence ||
		workerClaim.Watermarks.PlatformAck > authoritative.Watermarks.PlatformAck ||
		workerClaim.Attempt.PlatformSequence > authoritative.Attempt.PlatformSequence {
		return false
	}
	workerAttempt, platformAttempt := workerClaim.Attempt, authoritative.Attempt
	if workerAttempt.State == AttemptIdle || platformAttempt.State == AttemptIdle {
		return workerAttempt.State == platformAttempt.State
	}
	if !sameAttemptBinding(workerAttempt.Binding, platformAttempt.Binding) ||
		workerAttempt.CancelRevision > platformAttempt.CancelRevision ||
		!reconnectStatesCompatible(workerAttempt.State, platformAttempt.State) ||
		(workerAttempt.CancelRevision > 0 && platformAttempt.CancelRevision > 0 && workerAttempt.CancelCode != platformAttempt.CancelCode) ||
		(workerAttempt.TerminalSequence > 0 && platformAttempt.TerminalSequence > 0 &&
			(workerAttempt.TerminalStatus != platformAttempt.TerminalStatus || workerAttempt.TerminalResult != platformAttempt.TerminalResult ||
				!bytes.Equal(workerAttempt.TerminalEvidenceDigest, platformAttempt.TerminalEvidenceDigest))) {
		return false
	}
	return true
}

func reconnectStatesCompatible(worker, platform AttemptState) bool {
	switch platform {
	case AttemptOffered:
		return worker == AttemptOffered || worker == AttemptClaimPending
	case AttemptClaimPending:
		return worker == AttemptOffered || worker == AttemptClaimPending
	case AttemptClaimed:
		return worker == AttemptClaimPending || worker == AttemptClaimed || worker == AttemptTerminalPending
	case AttemptCancelRequested:
		return worker == AttemptClaimed || worker == AttemptCancelRequested || worker == AttemptCancelAcked || worker == AttemptTerminalPending
	case AttemptCancelAcked:
		return worker == AttemptCancelRequested || worker == AttemptCancelAcked || worker == AttemptTerminalPending
	case AttemptFenced:
		return worker == AttemptClaimed || worker == AttemptCancelRequested || worker == AttemptCancelAcked ||
			worker == AttemptFenced || worker == AttemptTerminalPending
	case AttemptTerminalPending:
		return worker == AttemptClaimed || worker == AttemptCancelRequested || worker == AttemptCancelAcked || worker == AttemptTerminalPending
	case AttemptTerminalCommitted:
		return worker == AttemptTerminalPending || worker == AttemptTerminalCommitted
	default:
		return false
	}
}

func cloneAttemptSummary(summary AttemptSummaryV1) AttemptSummaryV1 {
	summary.Binding = cloneBinding(summary.Binding)
	summary.TerminalEvidenceDigest = append([]byte(nil), summary.TerminalEvidenceDigest...)
	summary.Digest = append([]byte(nil), summary.Digest...)
	return summary
}

func cloneReconnectSnapshot(snapshot ReconnectSnapshotV1) ReconnectSnapshotV1 {
	snapshot.Attempt = cloneAttemptSummary(snapshot.Attempt)
	snapshot.Digest = append([]byte(nil), snapshot.Digest...)
	return snapshot
}
