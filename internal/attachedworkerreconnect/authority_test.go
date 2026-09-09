package attachedworkerreconnect

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestAttemptMatchesSnapshotCoversEveryDurableActiveHead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		durableState  domain.AttachedWorkerAttemptState
		protocolState attachedworkerprotocol.AttemptState
		platform      uint64
		worker        uint64
		progress      uint64
		cancel        uint64
		cancelCode    attachedworkerprotocol.CancelCode
		terminal      uint64
		terminalState domain.AttachedWorkerTerminalStatus
	}{
		{name: "offered", durableState: domain.AttachedWorkerAttemptOffered, protocolState: attachedworkerprotocol.AttemptOffered, platform: 1},
		{name: "claimed", durableState: domain.AttachedWorkerAttemptClaimed, protocolState: attachedworkerprotocol.AttemptClaimed, platform: 2, worker: 1},
		{name: "running progress", durableState: domain.AttachedWorkerAttemptClaimed, protocolState: attachedworkerprotocol.AttemptClaimed, platform: 2, worker: 2, progress: 1},
		{name: "cancel pending", durableState: domain.AttachedWorkerAttemptCancelRequested, protocolState: attachedworkerprotocol.AttemptCancelRequested, platform: 3, worker: 1, cancel: 1, cancelCode: attachedworkerprotocol.CancelRequested},
		{name: "cancel acknowledged", durableState: domain.AttachedWorkerAttemptCancelAcknowledged, protocolState: attachedworkerprotocol.AttemptCancelAcked, platform: 3, worker: 3, cancel: 1, cancelCode: attachedworkerprotocol.CancelRequested},
		{name: "cancelled before claim", durableState: domain.AttachedWorkerAttemptCancelledBeforeClaim, protocolState: attachedworkerprotocol.AttemptCancelRequested, platform: 2, cancel: 1, cancelCode: attachedworkerprotocol.CancelRequested},
		{name: "fenced unknown", durableState: domain.AttachedWorkerAttemptFencedUnknown, protocolState: attachedworkerprotocol.AttemptFenced, platform: 3, worker: 1, cancel: 1, cancelCode: attachedworkerprotocol.CancelFenced},
		{name: "terminal pending", durableState: domain.AttachedWorkerAttemptTerminalPending, protocolState: attachedworkerprotocol.AttemptTerminalPending, platform: 2, worker: 2, terminal: 1, terminalState: domain.AttachedWorkerTerminalSucceeded},
		{name: "terminal committed", durableState: domain.AttachedWorkerAttemptTerminalCommitted, protocolState: attachedworkerprotocol.AttemptTerminalCommitted, platform: 3, worker: 2, terminal: 1, terminalState: domain.AttachedWorkerTerminalSucceeded},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connection, attempt, snapshot := matchingAuthorityFixture(t, test.durableState, test.protocolState,
				test.platform, test.worker, test.progress, test.cancel, test.cancelCode, test.terminal, test.terminalState)
			if !AttemptMatchesSnapshot(snapshot, attempt, true, connection) {
				t.Fatal("exact active head did not match its protocol snapshot")
			}

			stale := attempt
			stale.ConnectionGeneration++
			if AttemptMatchesSnapshot(snapshot, stale, true, connection) {
				t.Fatal("replacement connection generation matched stale protocol authority")
			}
		})
	}
}

func TestAttemptMatchesSnapshotRejectsDisagreementBeforeReconnectAuthority(t *testing.T) {
	t.Parallel()
	connection, attempt, snapshot := matchingAuthorityFixture(t,
		domain.AttachedWorkerAttemptClaimed, attachedworkerprotocol.AttemptClaimed, 2, 2, 1, 0, "", 0, "")

	tests := []struct {
		name   string
		mutate func(*domain.AttachedWorkerAttemptV1, *attachedworkerprotocol.MachineSnapshotV1, *domain.AttachedWorkerConnection)
	}{
		{name: "missing durable head", mutate: func(*domain.AttachedWorkerAttemptV1, *attachedworkerprotocol.MachineSnapshotV1, *domain.AttachedWorkerConnection) {
		}},
		{name: "wrong owner", mutate: func(a *domain.AttachedWorkerAttemptV1, _ *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			a.OwnerUserID = "owner-b"
		}},
		{name: "wrong worker", mutate: func(a *domain.AttachedWorkerAttemptV1, _ *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			a.WorkerID = "worker-b"
		}},
		{name: "wrong connection", mutate: func(a *domain.AttachedWorkerAttemptV1, _ *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			a.ConnectionID = "wcn_other"
		}},
		{name: "wrong capability", mutate: func(a *domain.AttachedWorkerAttemptV1, _ *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			a.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(domain.DigestAttachedWorkerCapability([]byte("other")))
		}},
		{name: "wrong revision sequence", mutate: func(a *domain.AttachedWorkerAttemptV1, _ *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			a.ProgressSequence = 0
		}},
		{name: "wrong protocol state", mutate: func(_ *domain.AttachedWorkerAttemptV1, s *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			s.Attempt.Summary.State = attachedworkerprotocol.AttemptOffered
		}},
		{name: "wrong attempt binding", mutate: func(a *domain.AttachedWorkerAttemptV1, _ *attachedworkerprotocol.MachineSnapshotV1, _ *domain.AttachedWorkerConnection) {
			a.AttemptID = "attempt-2"
		}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			candidateAttempt := attempt
			candidateSnapshot := snapshot.Clone()
			candidateConnection := connection
			test.mutate(&candidateAttempt, &candidateSnapshot, &candidateConnection)
			found := test.name != "missing durable head"
			if AttemptMatchesSnapshot(candidateSnapshot, candidateAttempt, found, candidateConnection) {
				t.Fatal("divergent authority was accepted")
			}
		})
	}
}

func matchingAuthorityFixture(
	t *testing.T,
	durableState domain.AttachedWorkerAttemptState,
	protocolState attachedworkerprotocol.AttemptState,
	platform, worker, progress, cancel uint64,
	cancelCode attachedworkerprotocol.CancelCode,
	terminal uint64,
	terminalState domain.AttachedWorkerTerminalStatus,
) (domain.AttachedWorkerConnection, domain.AttachedWorkerAttemptV1, attachedworkerprotocol.MachineSnapshotV1) {
	t.Helper()
	created := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
	leaseExpiry := created.Add(time.Hour)
	contextDigest := domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context")))
	capabilityDigest := domain.AttachedWorkerCapabilityDigest(domain.DigestAttachedWorkerCapability([]byte("capability")))
	policyDigest := domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy")))
	fence, err := domain.NewAttachedWorkerFenceTokenV1("tenant-a", "owner-a", "worker-a", "run-1", "attempt-1", "lease-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	connection := domain.AttachedWorkerConnection{
		TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "worker-a", ID: "wcn_current",
		EnrollmentGeneration: 2, ConnectionGeneration: 3, CapabilityDigest: capabilityDigest,
	}
	attempt := domain.AttachedWorkerAttemptV1{
		Version: 1, TenantID: connection.TenantID, OwnerUserID: connection.OwnerUserID, WorkerID: connection.WorkerID,
		ConnectionID: connection.ID, RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1",
		LeaseID: "lease-1", LeaseGeneration: 7, FenceToken: fence,
		EnrollmentGeneration: connection.EnrollmentGeneration, ConnectionGeneration: connection.ConnectionGeneration,
		ContextDigest: contextDigest, CapabilityDigest: capabilityDigest, PolicyDigest: policyDigest,
		State: durableState, PlatformAttemptSequence: platform, WorkerAttemptSequence: worker, ProgressSequence: progress,
		CancelRevision: cancel, LeaseExpiresAt: leaseExpiry, CreatedAt: created, UpdatedAt: created, Revision: 1,
	}
	if cancel > 0 {
		attempt.CancelDeadline = created.Add(30 * time.Minute)
	}
	var evidence []byte
	var terminalStatus attachedworkerprotocol.TerminalStatus
	var terminalResult attachedworkerprotocol.TerminalResult
	if terminal > 0 {
		evidence = digestBytes(t, string(domain.DigestAttachedWorkerCapability([]byte("terminal evidence"))))
		attempt.TerminalSequence = terminal
		attempt.TerminalStatus = terminalState
		attempt.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(evidence))
		terminalStatus = attachedworkerprotocol.TerminalStatus(terminalState)
		switch terminalState {
		case domain.AttachedWorkerTerminalSucceeded:
			terminalResult = attachedworkerprotocol.TerminalResultCompleted
		case domain.AttachedWorkerTerminalFailed:
			terminalResult = attachedworkerprotocol.TerminalResultFailed
		case domain.AttachedWorkerTerminalCancelled:
			terminalResult = attachedworkerprotocol.TerminalResultCancelled
		}
	}
	if err := attempt.Validate(); err != nil {
		t.Fatalf("invalid durable fixture: %v", err)
	}
	summary := attachedworkerprotocol.AttemptSummaryV1{
		State: protocolState,
		Binding: attachedworkerprotocol.AttemptBindingV1{
			RunID: string(attempt.RunID), AttemptID: string(attempt.AttemptID), LeaseID: string(attempt.LeaseID),
			LeaseGeneration: attempt.LeaseGeneration, FenceToken: string(attempt.FenceToken), ExpiresAtUnixMicro: leaseExpiry.UnixMicro(),
			ContextDigest: digestBytes(t, string(contextDigest)), CapabilityDigest: digestBytes(t, string(capabilityDigest)),
			PolicyDigest: digestBytes(t, string(policyDigest)),
		},
		PlatformSequence: platform, WorkerSequence: worker, ProgressSequence: progress,
		CancelRevision: cancel, CancelCode: cancelCode, TerminalSequence: terminal,
		TerminalStatus: terminalStatus, TerminalResult: terminalResult, TerminalEvidenceDigest: evidence,
	}
	summary.Digest = testAttemptSummaryDigest(summary)
	return connection, attempt, attachedworkerprotocol.MachineSnapshotV1{Attempt: attachedworkerprotocol.MachineAttemptSnapshotV1{Summary: summary}}
}

func digestBytes(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func testAttemptSummaryDigest(summary attachedworkerprotocol.AttemptSummaryV1) []byte {
	result := testCanonicalField(nil, []byte("sessionless.attached-worker.attempt-summary.v1"))
	result = testCanonicalField(result, []byte(summary.State))
	result = testCanonicalField(result, []byte(summary.Binding.RunID))
	result = testCanonicalField(result, []byte(summary.Binding.AttemptID))
	result = testCanonicalField(result, []byte(summary.Binding.LeaseID))
	result = testCanonicalUint64(result, summary.Binding.LeaseGeneration)
	result = testCanonicalField(result, []byte(summary.Binding.FenceToken))
	result = testCanonicalUint64(result, uint64(summary.Binding.ExpiresAtUnixMicro))
	result = testCanonicalField(result, summary.Binding.ContextDigest)
	result = testCanonicalField(result, summary.Binding.CapabilityDigest)
	result = testCanonicalField(result, summary.Binding.PolicyDigest)
	result = testCanonicalUint64(result, summary.PlatformSequence)
	result = testCanonicalUint64(result, summary.WorkerSequence)
	result = testCanonicalUint64(result, summary.ProgressSequence)
	result = testCanonicalUint64(result, summary.CancelRevision)
	result = testCanonicalField(result, []byte(summary.CancelCode))
	result = testCanonicalUint64(result, summary.TerminalSequence)
	result = testCanonicalField(result, []byte(summary.TerminalStatus))
	result = testCanonicalField(result, []byte(summary.TerminalResult))
	result = testCanonicalField(result, summary.TerminalEvidenceDigest)
	digest := sha256.Sum256(result)
	return digest[:]
}

func testCanonicalField(destination, value []byte) []byte {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	destination = append(destination, size[:]...)
	return append(destination, value...)
}

func testCanonicalUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}
