package attachedworkerreconnect

import (
	"bytes"
	"encoding/hex"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// AttemptMatchesSnapshot proves that the owner-scoped AW-04 attempt head and
// the exact AW-02 protocol snapshot describe the same semantic authority. It
// deliberately ignores transport-envelope fingerprints: those are rebound to
// the replacement connection while attempt sequences and effects stay fixed.
func AttemptMatchesSnapshot(
	snapshot attachedworkerprotocol.MachineSnapshotV1,
	attempt domain.AttachedWorkerAttemptV1,
	found bool,
	connection domain.AttachedWorkerConnection,
) bool {
	summary := snapshot.Attempt.Summary
	if summary.State == attachedworkerprotocol.AttemptIdle {
		return !found || attempt.Validate() == nil && attempt.State == domain.AttachedWorkerAttemptRetired &&
			attempt.TenantID == connection.TenantID && attempt.OwnerUserID == connection.OwnerUserID &&
			attempt.WorkerID == connection.WorkerID
	}
	if !found || attempt.Validate() != nil || summary.Validate() != nil ||
		attempt.TenantID != connection.TenantID || attempt.OwnerUserID != connection.OwnerUserID ||
		attempt.WorkerID != connection.WorkerID || attempt.ConnectionID != connection.ID ||
		attempt.EnrollmentGeneration != connection.EnrollmentGeneration ||
		attempt.ConnectionGeneration != connection.ConnectionGeneration ||
		attempt.CapabilityDigest != connection.CapabilityDigest ||
		!stateMatches(attempt.State, summary.State, summary.CancelCode) ||
		attempt.PlatformAttemptSequence != summary.PlatformSequence ||
		attempt.WorkerAttemptSequence != summary.WorkerSequence ||
		attempt.ProgressSequence != summary.ProgressSequence || attempt.CancelRevision != summary.CancelRevision ||
		attempt.TerminalSequence != summary.TerminalSequence ||
		attachedworkerprotocol.TerminalStatus(attempt.TerminalStatus) != summary.TerminalStatus ||
		terminalResult(attempt.TerminalStatus) != summary.TerminalResult {
		return false
	}
	contextDigest, contextErr := hex.DecodeString(string(attempt.ContextDigest))
	capabilityDigest, capabilityErr := hex.DecodeString(string(attempt.CapabilityDigest))
	policyDigest, policyErr := hex.DecodeString(string(attempt.PolicyDigest))
	evidenceDigest, evidenceErr := decodeOptionalDigest(string(attempt.TerminalEvidenceDigest))
	return contextErr == nil && capabilityErr == nil && policyErr == nil && evidenceErr == nil &&
		string(attempt.RunID) == summary.Binding.RunID && string(attempt.AttemptID) == summary.Binding.AttemptID &&
		string(attempt.LeaseID) == summary.Binding.LeaseID && attempt.LeaseGeneration == summary.Binding.LeaseGeneration &&
		string(attempt.FenceToken) == summary.Binding.FenceToken &&
		attempt.LeaseExpiresAt.UnixMicro() == summary.Binding.ExpiresAtUnixMicro &&
		bytes.Equal(contextDigest, summary.Binding.ContextDigest) &&
		bytes.Equal(capabilityDigest, summary.Binding.CapabilityDigest) &&
		bytes.Equal(policyDigest, summary.Binding.PolicyDigest) &&
		bytes.Equal(evidenceDigest, summary.TerminalEvidenceDigest)
}

func stateMatches(state domain.AttachedWorkerAttemptState, protocolState attachedworkerprotocol.AttemptState, cancelCode attachedworkerprotocol.CancelCode) bool {
	switch state {
	case domain.AttachedWorkerAttemptOffered:
		return protocolState == attachedworkerprotocol.AttemptOffered
	case domain.AttachedWorkerAttemptClaimed:
		return protocolState == attachedworkerprotocol.AttemptClaimed
	case domain.AttachedWorkerAttemptCancelRequested:
		return protocolState == attachedworkerprotocol.AttemptCancelRequested && cancelCode == attachedworkerprotocol.CancelRequested
	case domain.AttachedWorkerAttemptCancelAcknowledged:
		return protocolState == attachedworkerprotocol.AttemptCancelAcked && cancelCode == attachedworkerprotocol.CancelRequested
	case domain.AttachedWorkerAttemptCancelledBeforeClaim:
		return protocolState == attachedworkerprotocol.AttemptCancelRequested && cancelCode == attachedworkerprotocol.CancelRequested ||
			protocolState == attachedworkerprotocol.AttemptFenced && cancelCode == attachedworkerprotocol.CancelFenced
	case domain.AttachedWorkerAttemptFencedUnknown:
		return protocolState == attachedworkerprotocol.AttemptFenced && cancelCode == attachedworkerprotocol.CancelFenced
	case domain.AttachedWorkerAttemptTerminalPending:
		return protocolState == attachedworkerprotocol.AttemptTerminalPending
	case domain.AttachedWorkerAttemptTerminalCommitted:
		return protocolState == attachedworkerprotocol.AttemptTerminalCommitted
	default:
		return false
	}
}

func terminalResult(status domain.AttachedWorkerTerminalStatus) attachedworkerprotocol.TerminalResult {
	switch status {
	case domain.AttachedWorkerTerminalSucceeded:
		return attachedworkerprotocol.TerminalResultCompleted
	case domain.AttachedWorkerTerminalFailed:
		return attachedworkerprotocol.TerminalResultFailed
	case domain.AttachedWorkerTerminalCancelled:
		return attachedworkerprotocol.TerminalResultCancelled
	default:
		return ""
	}
}

func decodeOptionalDigest(value string) ([]byte, error) {
	if value == "" {
		return []byte{}, nil
	}
	return hex.DecodeString(value)
}
