package attachedworkerdaemon

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

func TestPreparedAPIPhysicalSendOneShot(t *testing.T) {
	for _, name := range []string{"valid", "wrong body", "wrong profile", "source error", "cancelled", "wall cutoff", "expired cutoff"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			cutoff := time.Now().Add(time.Hour)
			send := &preparedAPIPhysicalSend{ctx: ctx, body: []byte("whole original request"), profile: "original-profile", authorize: func() (time.Time, error) {
				calls++
				if name == "source error" {
					return time.Time{}, errors.New("private backend body/key diagnostic")
				}
				if name == "wall cutoff" {
					return cutoff.Round(0), nil
				}
				if name == "expired cutoff" {
					return time.Now(), nil
				}
				return cutoff, nil
			}}
			body, profile := []byte("whole original request"), "original-profile"
			switch name {
			case "wrong body":
				body[0] = '!'
			case "wrong profile":
				profile = "foreign-profile"
			case "cancelled":
				cancel()
			}
			got, err := send.Authorize(body, profile)
			if name == "valid" {
				if err != nil || got != cutoff {
					t.Fatalf("legitimate original got=%v err=%v", got, err)
				}
			} else if !got.IsZero() || !errors.Is(err, ErrCredentialUnavailable) || strings.Contains(err.Error(), "private") {
				t.Fatalf("%s got usable/private result: cutoff=%v err=%v", name, got, err)
			}
			wantCalls := 1
			if name == "wrong body" || name == "wrong profile" || name == "cancelled" {
				wantCalls = 0
			}
			if got, err := send.Authorize(send.body, send.profile); err == nil || !got.IsZero() || calls != wantCalls {
				t.Fatalf("%s replenished permit: calls=%d want=%d err=%v", name, calls, wantCalls, err)
			}
			if !send.retire().IsZero() {
				t.Error("second entry did not poison successful completion")
			}
		})
	}
}

func TestPreparedAPIPhysicalSendRetirementJoinsAndDeniesHeldReply(t *testing.T) {
	entered, resume, done, retired := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	var got time.Time
	var err error
	send := &preparedAPIPhysicalSend{ctx: context.Background(), body: []byte("original"), profile: "original", authorize: func() (time.Time, error) {
		close(entered)
		<-resume
		return time.Now().Add(time.Hour), nil
	}}
	go func() { defer close(done); got, err = send.Authorize([]byte("original"), "original") }()
	t.Cleanup(func() { unblock(); apiWait(t, done) })
	apiWait(t, entered)
	go func() { defer close(retired); send.retire() }()
	t.Cleanup(func() { unblock(); apiWait(t, retired) })
	// Mutex waits are not durably blocked in a synctest bubble. Observe
	// the actual revocation condition under a bounded poll instead; the
	// timer is only a failure bound, never evidence that retirement began.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !send.retired.Load() {
		select {
		case <-poll.C:
		case <-deadline.C:
			if !send.retired.Load() {
				t.Fatal("retirement did not revoke before joining held Source")
			}
		}
	}
	select {
	case <-retired:
		t.Fatal("retirement failed to join in-flight borrowed data")
	default:
	}
	unblock()
	apiWait(t, done)
	apiWait(t, retired)
	if err == nil || !got.IsZero() || send.body != nil || send.authorize != nil {
		t.Fatalf("late reply escaped retirement: got=%v err=%v", got, err)
	}
	if _, err := send.Authorize([]byte("original"), "original"); err == nil {
		t.Error("saved callback usable after retirement")
	}
}

func TestPreparedAPIPhysicalSendConcurrentEntry(t *testing.T) {
	calls := 0
	send := &preparedAPIPhysicalSend{ctx: context.Background(), body: []byte("original"), profile: "original", authorize: func() (time.Time, error) {
		calls++
		return time.Now().Add(time.Hour), nil
	}}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, _ = send.Authorize([]byte("original"), "original") })
	}
	done := make(chan struct{})
	go func() { defer close(done); wg.Wait() }()
	apiWait(t, done)
	if calls != 1 || !send.retire().IsZero() {
		t.Fatalf("competing entries: Source calls=%d, want1 and failed completion", calls)
	}
}

func TestAPIOwnerPhysicalSendRequiredAndScoped(t *testing.T) {
	for _, name := range []string{"valid", "missing", "wrong body", "wrong profile", "denied", "wrong mapping", "reentry"} {
		t.Run(name, func(t *testing.T) {
			inv, parent, life := longLivedAPIOwnerFixture(t)
			if name == "denied" {
				parent.phaseErr = pisdkphasewire.PhysicalSend
			}
			if name == "wrong mapping" {
				parent.phaseMutate = func(request pisdkphasewire.Request, reply *pisdkphasewire.Reply) {
					if request.Phase == pisdkphasewire.PhysicalSend {
						reply.AttachedPiAPIInvocationPolicyDigest = strings.Repeat("f", 64)
					}
				}
			}
			var saved PreparedAPIPhysicalSendV1
			var body []byte
			var profile string
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
				saved, body, profile = input.PhysicalSend, input.APIRequest.Body, input.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest
				if name != "missing" {
					if name == "wrong body" {
						body[0] = '!'
					}
					if name == "wrong profile" {
						profile = strings.Repeat("f", 64)
					}
					_, _ = saved.Authorize(body, profile)
					if name == "reentry" {
						_, _ = saved.Authorize(body, profile)
					}
				}
				// Intentionally swallow guard errors: child-reported success must
				// not upgrade missing/denied/repeated parent phase to Finalize.
				return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			result, runErr := runner.Run(ctx, inv)
			if (name == "valid") != result.Succeeded(runErr) || life.releaseCount != 1 || !result.CredentialReleased {
				t.Fatalf("%s result=%+v err=%v order=%v", name, result, runErr, parent.order)
			}
			physicalCalls := 0
			for _, phase := range parent.order {
				if phase == "physical_send" {
					physicalCalls++
				}
			}
			wantCalls := 1
			if name == "missing" || name == "wrong body" || name == "wrong profile" {
				wantCalls = 0
			}
			if physicalCalls != wantCalls || (name != "valid" && life.finalizeCount != 0) {
				t.Errorf("%s calls=%d want=%d finalize=%d", name, physicalCalls, wantCalls, life.finalizeCount)
			}
			if got, err := saved.Authorize(body, profile); err == nil || !got.IsZero() {
				t.Error("completed executor retained usable callback")
			}
		})
	}
}

func TestAPIOwnerPhysicalCutoffCannotRenewAtFinalize(t *testing.T) {
	inv, parent, life := longLivedAPIOwnerFixture(t)
	const physicalWindow = 30 * time.Minute
	var sentCutoff time.Time
	var finalizeRemaining uint64
	parent.phaseMutate = func(request pisdkphasewire.Request, reply *pisdkphasewire.Reply) {
		switch request.Phase {
		case pisdkphasewire.PreIssue:
			reply.UseExpiresAt = reply.CheckedAt.Add(time.Hour)
		case pisdkphasewire.PhysicalSend:
			// A shorter genuine server interval is represented only by this
			// synthetic source. No expiry/sleep decides this test's outcome.
			reply.UseExpiresAt = reply.CheckedAt.Add(physicalWindow)
		case pisdkphasewire.ImmutableFinalize:
			finalizeRemaining = request.CallerDeadline.RemainingNanoseconds
			reply.UseExpiresAt = reply.CheckedAt.Add(physicalWindow)
		}
		if remaining := time.Duration(request.CallerDeadline.RemainingNanoseconds); reply.UseExpiresAt.Sub(reply.CheckedAt) > remaining {
			reply.UseExpiresAt = reply.CheckedAt.Add(remaining)
		}
	}
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		var err error
		sentCutoff, err = input.PhysicalSend.Authorize(input.APIRequest.Body, input.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest)
		if err != nil {
			return AttemptResult{}, err
		}
		if prior, ok := ctx.Deadline(); !ok || !sentCutoff.Before(prior) {
			t.Error("physical phase did not tighten original pre-issue cutoff")
		}
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	result, err := runner.Run(ctx, inv)
	if !result.Succeeded(err) || sentCutoff.IsZero() || finalizeRemaining == 0 || finalizeRemaining > uint64(physicalWindow) {
		t.Fatalf("physical window renewed: remaining=%d result=%+v err=%v", finalizeRemaining, result, err)
	}
}

func longLivedAPIOwnerFixture(t *testing.T) (Invocation, *apiTestParent, *apiTestLifecycle) {
	t.Helper()
	inv, parent, life := apiOwnerFixture(t)
	// This compares bounds, not expiry. Every original/phase lifetime exceeds
	// the enclosing test/CI failure bound, including a lawful scheduler pause.
	// Re-seal the modified synthetic job instead of weakening source validation.
	inv.VerifiedInput.Job.Limits.MaxRuntime = time.Hour
	expiry := time.Now().UTC().Truncate(time.Microsecond).Add(2 * time.Hour)
	inv.VerifiedInput.AcceptedAttempt.LeaseExpiresAt = expiry
	inv.VerifiedInput.Job.HarnessBinding.EvidenceExpiresAt = &expiry
	digest, err := domain.AttachedWorkerJobContextDigestV1(inv.VerifiedInput.Job, inv.VerifiedInput.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	inv.VerifiedInput.AcceptedAttempt.ContextDigest = digest
	parent.phaseWindow = time.Hour
	return inv, parent, life
}
