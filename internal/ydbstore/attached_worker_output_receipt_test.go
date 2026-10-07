package ydbstore

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestAttachedWorkerOutputReceiptStatusAdmissible(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	attempt := domain.AttachedWorkerAttemptV1{
		State:          domain.AttachedWorkerAttemptClaimed,
		LeaseExpiresAt: base.Add(time.Minute),
	}
	for _, tc := range []struct {
		name           string
		state          domain.AttachedWorkerAttemptState
		status         domain.AttachedWorkerTerminalStatus
		cancelRevision uint64
		cancelDeadline time.Time
		at             time.Time
		want           bool
	}{
		{name: "claimed success", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, at: base, want: true},
		{name: "claimed failure", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalFailed, at: base, want: true},
		{name: "claimed cancelled", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalCancelled, at: base},
		{name: "claimed invalid status", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalStatus("unknown"), at: base},
		{name: "claimed at zero time", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded},
		{name: "claimed at lease expiry", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, at: base.Add(time.Minute)},
		{name: "claimed with cancel revision", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, at: base},
		{name: "claimed with cancel deadline", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, cancelDeadline: base.Add(10 * time.Second), at: base},
		{name: "cancel requested before ack deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second), want: true},
		{name: "cancel requested at ack deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(10 * time.Second)},
		{name: "cancel requested without revision", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second)},
		{name: "cancel requested without deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, at: base.Add(5 * time.Second)},
		{name: "cancel requested cannot succeed", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second)},
		{name: "cancel acknowledged after ack deadline", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second), want: true},
		{name: "cancel acknowledged cannot succeed", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second)},
		{name: "cancel acknowledged without revision", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second)},
		{name: "cancel acknowledged without deadline", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, at: base.Add(30 * time.Second)},
		{name: "expired lease", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(time.Minute)},
		{name: "fenced", state: domain.AttachedWorkerAttemptFencedUnknown, status: domain.AttachedWorkerTerminalFailed, at: base},
		{name: "terminal pending", state: domain.AttachedWorkerAttemptTerminalPending, status: domain.AttachedWorkerTerminalSucceeded, at: base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := attempt
			candidate.State = tc.state
			candidate.CancelRevision = tc.cancelRevision
			candidate.CancelDeadline = tc.cancelDeadline
			if got := attachedWorkerOutputReceiptStatusAdmissible(candidate, tc.status, tc.at); got != tc.want {
				t.Errorf("receipt admissible state=%s status=%s at=%s: got %t, want %t", tc.state, tc.status, tc.at.Format(time.RFC3339), got, tc.want)
			}
		})
	}
}
