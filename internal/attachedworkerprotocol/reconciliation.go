package attachedworkerprotocol

import (
	"bytes"
	"crypto/sha256"
)

const attemptSummaryDomainV1 = "sessionless.attached-worker.attempt-summary.v1"

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
