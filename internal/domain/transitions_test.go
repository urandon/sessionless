package domain_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRunTransitionTable(t *testing.T) {
	t.Parallel()

	statuses := []domain.RunStatus{
		domain.RunCreated,
		domain.RunAdmitted,
		domain.RunQueued,
		domain.RunRunning,
		domain.RunSucceeded,
		domain.RunFailed,
		domain.RunCancelled,
		domain.RunQuotaBlocked,
	}
	allowed := map[[2]domain.RunStatus]bool{
		{domain.RunCreated, domain.RunAdmitted}:       true,
		{domain.RunCreated, domain.RunQuotaBlocked}:   true,
		{domain.RunCreated, domain.RunCancelled}:      true,
		{domain.RunCreated, domain.RunFailed}:         true,
		{domain.RunAdmitted, domain.RunQueued}:        true,
		{domain.RunAdmitted, domain.RunQuotaBlocked}:  true,
		{domain.RunAdmitted, domain.RunCancelled}:     true,
		{domain.RunAdmitted, domain.RunFailed}:        true,
		{domain.RunQueued, domain.RunRunning}:         true,
		{domain.RunQueued, domain.RunQuotaBlocked}:    true,
		{domain.RunQueued, domain.RunCancelled}:       true,
		{domain.RunQueued, domain.RunFailed}:          true,
		{domain.RunRunning, domain.RunSucceeded}:      true,
		{domain.RunRunning, domain.RunQuotaBlocked}:   true,
		{domain.RunRunning, domain.RunCancelled}:      true,
		{domain.RunRunning, domain.RunFailed}:         true,
		{domain.RunQuotaBlocked, domain.RunAdmitted}:  true,
		{domain.RunQuotaBlocked, domain.RunCancelled}: true,
		{domain.RunQuotaBlocked, domain.RunFailed}:    true,
	}

	for _, from := range statuses {
		for _, to := range statuses {
			from, to := from, to
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				t.Parallel()
				want := allowed[[2]domain.RunStatus{from, to}]
				if got := domain.CanTransitionRun(from, to); got != want {
					t.Fatalf("CanTransitionRun(%q, %q) = %v, want %v", from, to, got, want)
				}

				run := validRun()
				run.Status = from
				if from.Terminal() {
					finishedAt := run.UpdatedAt
					run.FinishedAt = &finishedAt
				}
				err := run.Transition(to, run.UpdatedAt.Add(time.Second))
				if (err == nil) != want {
					t.Fatalf("Transition(%q -> %q) error = %v, allowed = %v", from, to, err, want)
				}
			})
		}
	}
}

func TestDeliveryTransitionTable(t *testing.T) {
	t.Parallel()

	statuses := []domain.DeliveryStatus{
		domain.DeliveryPending,
		domain.DeliverySending,
		domain.DeliveryRetryWait,
		domain.DeliverySent,
		domain.DeliveryFailed,
		domain.DeliveryCancelled,
	}
	allowed := map[[2]domain.DeliveryStatus]bool{
		{domain.DeliveryPending, domain.DeliverySending}:     true,
		{domain.DeliveryPending, domain.DeliveryCancelled}:   true,
		{domain.DeliverySending, domain.DeliverySent}:        true,
		{domain.DeliverySending, domain.DeliveryRetryWait}:   true,
		{domain.DeliverySending, domain.DeliveryFailed}:      true,
		{domain.DeliverySending, domain.DeliveryCancelled}:   true,
		{domain.DeliveryRetryWait, domain.DeliverySending}:   true,
		{domain.DeliveryRetryWait, domain.DeliveryFailed}:    true,
		{domain.DeliveryRetryWait, domain.DeliveryCancelled}: true,
	}

	for _, from := range statuses {
		for _, to := range statuses {
			want := allowed[[2]domain.DeliveryStatus{from, to}]
			if got := domain.CanTransitionDelivery(from, to); got != want {
				t.Errorf("CanTransitionDelivery(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestAttemptTransitionTable(t *testing.T) {
	t.Parallel()

	statuses := []domain.AttemptStatus{
		domain.AttemptCreated,
		domain.AttemptRunning,
		domain.AttemptSucceeded,
		domain.AttemptFailed,
		domain.AttemptCancelled,
	}
	allowed := map[[2]domain.AttemptStatus]bool{
		{domain.AttemptCreated, domain.AttemptRunning}:   true,
		{domain.AttemptCreated, domain.AttemptFailed}:    true,
		{domain.AttemptCreated, domain.AttemptCancelled}: true,
		{domain.AttemptRunning, domain.AttemptSucceeded}: true,
		{domain.AttemptRunning, domain.AttemptFailed}:    true,
		{domain.AttemptRunning, domain.AttemptCancelled}: true,
	}
	for _, from := range statuses {
		for _, to := range statuses {
			want := allowed[[2]domain.AttemptStatus{from, to}]
			if got := domain.CanTransitionAttempt(from, to); got != want {
				t.Errorf("CanTransitionAttempt(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestDispatchTransitionTable(t *testing.T) {
	t.Parallel()

	statuses := []domain.DispatchStatus{
		domain.DispatchPending,
		domain.DispatchPublished,
		domain.DispatchCancelled,
	}
	for _, from := range statuses {
		for _, to := range statuses {
			want := from == domain.DispatchPending &&
				(to == domain.DispatchPublished || to == domain.DispatchCancelled)
			if got := domain.CanTransitionDispatch(from, to); got != want {
				t.Errorf("CanTransitionDispatch(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestProviderQuotaTransitionTable(t *testing.T) {
	t.Parallel()

	states := []domain.ProviderQuotaState{
		domain.ProviderQuotaUnknown,
		domain.ProviderQuotaAvailable,
		domain.ProviderQuotaLimited,
		domain.ProviderQuotaExhausted,
	}
	for _, from := range states {
		for _, to := range states {
			want := from != to
			if got := domain.CanTransitionProviderQuota(from, to); got != want {
				t.Errorf("CanTransitionProviderQuota(%q, %q) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestReservationTransitionTable(t *testing.T) {
	t.Parallel()

	for _, terminal := range []domain.ReservationStatus{
		domain.ReservationCommitted,
		domain.ReservationReleased,
		domain.ReservationExpired,
	} {
		if !domain.CanTransitionReservation(domain.ReservationHeld, terminal) {
			t.Errorf("held -> %s should be allowed", terminal)
		}
		if domain.CanTransitionReservation(terminal, domain.ReservationHeld) {
			t.Errorf("%s -> held should not be allowed", terminal)
		}
	}
}
