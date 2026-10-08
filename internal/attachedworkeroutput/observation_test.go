package attachedworkeroutput

import (
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestProcessObservationRequiresServerPinnedCredentialRelease(t *testing.T) {
	t.Parallel()
	base := ProcessObservationV1{
		Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
		CredentialReleaseRequired: true, CredentialReleased: true,
	}
	if err := base.ValidateFor(domain.AttachedWorkerTerminalSucceeded, true); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*ProcessObservationV1)
	}{
		{name: "missing requirement", change: func(o *ProcessObservationV1) { o.CredentialReleaseRequired = false; o.CredentialReleased = false }},
		{name: "release failed", change: func(o *ProcessObservationV1) { o.CredentialReleased = false }},
		{name: "writeback failed", change: func(o *ProcessObservationV1) { o.InvocationFailureCode = "credential_writeback_failed" }},
		{name: "root cleanup failed", change: func(o *ProcessObservationV1) { o.CleanupSucceeded = false }},
		{name: "boundary retained", change: func(o *ProcessObservationV1) { o.BoundaryReleased = false }},
		{name: "descendants left", change: func(o *ProcessObservationV1) { o.DescendantsReaped = false }},
		{name: "runner error", change: func(o *ProcessObservationV1) { o.RunnerFailed = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value := base
			tc.change(&value)
			if err := value.ValidateFor(domain.AttachedWorkerTerminalSucceeded, true); !errors.Is(err, ErrCandidateInvalid) {
				t.Errorf("invalid process observation accepted: %+v err=%v", value, err)
			}
		})
	}
	withoutRelease := base
	withoutRelease.CredentialReleased = false
	first, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := withoutRelease.Digest()
	if err != nil || first == second {
		t.Errorf("credential release must change observation digest: first=%s second=%s err=%v", first, second, err)
	}
}

func TestProcessObservationCancellationRejectsCredentialFinalizationFailure(t *testing.T) {
	t.Parallel()
	base := ProcessObservationV1{
		Version: 1, Cancelled: true, DescendantsReaped: true, BoundaryReleased: true,
		CleanupSucceeded: true, CredentialReleaseRequired: true, CredentialReleased: true,
	}
	if err := base.ValidateFor(domain.AttachedWorkerTerminalCancelled, true); err != nil {
		t.Fatal(err)
	}
	base.InvocationFailureCode = "credential_writeback_failed"
	if err := base.ValidateFor(domain.AttachedWorkerTerminalCancelled, true); !errors.Is(err, ErrCandidateInvalid) {
		t.Errorf("credential finalization failure was acknowledged: err=%v", err)
	}
}
