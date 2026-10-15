package attachedworkerdaemon

import (
	"context"
	"testing"
	"time"
)

type invocationAcquisitionFixture struct {
	ctx     context.Context
	entry   time.Time
	ledger  PreparedAPILedgerV1
	cleared bool
}

func (a *invocationAcquisitionFixture) OriginalContext() context.Context    { return a.ctx }
func (a *invocationAcquisitionFixture) OriginalEntry() time.Time            { return a.entry }
func (a *invocationAcquisitionFixture) OriginalLedger() PreparedAPILedgerV1 { return a.ledger }
func (a *invocationAcquisitionFixture) Clear()                              { a.cleared = true }

type observingAcquiredParent struct {
	*apiTestParent
	seen PreparedInputAcquisitionV1
}

// A legal Context implementation need not be comparable. It must deny safely,
// not panic at the private owner identity boundary.
type nonComparableAcquisitionContext struct {
	context.Context
	marker []byte
}

func TestAPIOwnerAcquisitionRejectsForeignCallerBeforeParent(t *testing.T) {
	for _, mode := range []string{"foreign", "non-comparable"} {
		t.Run(mode, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			original, cancel := context.WithTimeout(t.Context(), time.Hour)
			defer cancel()
			caller, stop := context.WithTimeout(t.Context(), time.Hour)
			defer stop()
			var ownerCtx context.Context = original
			if mode == "non-comparable" {
				ownerCtx = nonComparableAcquisitionContext{Context: original, marker: []byte{1}}
				caller = ownerCtx
			}
			inv.InputAcquisition = &invocationAcquisitionFixture{ctx: ownerCtx, entry: time.Now(), ledger: &parent.ledger}
			defer inv.Clear()
			observed := &observingAcquiredParent{apiTestParent: parent}
			executed := false
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
				executed = true
				return AttemptResult{}, nil
			}), life, observed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = runner.Run(caller, inv); err == nil || observed.seen != nil || executed || len(parent.order) != 0 {
				t.Fatalf("mode=%s foreign caller reached original parent: prepared=%t execute=%t err=%v", mode, observed.seen != nil, executed, err)
			}
		})
	}
}

func (p *observingAcquiredParent) PrepareOriginalAPIInvocation(ctx context.Context, id InvocationIdentity, input *VerifiedAttachedInputV1, process AttemptSpec, a PreparedInputAcquisitionV1) (*PreparedAPIDataV1, error) {
	p.seen = a
	return p.apiTestParent.PrepareOriginalAPIInvocation(ctx, id, input, process, a)
}

// Genuine runner, synthetic source/credential collaborators only. No SDK,
// provider/network, source/currentness/native qualification is demonstrated.
func TestAPIOwnerAcquisitionRetainsLedgerAndEntryBeforeIssue(t *testing.T) {
	for _, mode := range []string{"same", "fresh-ledger", "rebased-entry"} {
		t.Run(mode, func(t *testing.T) {
			// The production contract requires real monotonic Time values.
			// Go's synctest clock deliberately omits that component (pinned
			// runtime/time.go). Sample one clock; no sleep or narrow timing
			// window decides correctness. The enclosing lifetime exceeds CI.
			inv, parent, life := apiOwnerFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			defer cancel()
			a := &invocationAcquisitionFixture{ctx: ctx, entry: time.Now().Add(-2 * time.Second), ledger: &parent.ledger}
			inv.InputAcquisition = a
			defer inv.Clear()
			parent.mutate = func(d *PreparedAPIDataV1) {
				if mode != "rebased-entry" {
					d.Entry = a.entry
					d.InvocationDeadline = a.entry.Add(d.ParentInput.Job.Limits.MaxRuntime)
					d.ProcessDeadline = d.InvocationDeadline
				}
				if mode == "fresh-ledger" {
					d.Ledger = &apiTestLedger{}
				}
			}
			observed := &observingAcquiredParent{apiTestParent: parent}
			executed := false
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, in PreparedInvocationV1) (AttemptResult, error) {
				executed = true
				if in.Ledger != a.ledger {
					t.Error("executor received replenished ledger")
				}
				if err := authorizeSyntheticAPISend(in); err != nil {
					return AttemptResult{}, err
				}
				return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
			}), life, observed)
			if err != nil {
				t.Fatal(err)
			}
			result, err := runner.Run(ctx, inv)
			if observed.seen != a {
				t.Fatal("original parent did not receive acquisition owner")
			}
			if mode == "same" {
				if !result.Succeeded(err) || !executed {
					t.Fatalf("same acquisition denied: executed=%t err=%v failure=%s", executed, err, result.FailureCode)
				}
			} else if err == nil || executed {
				t.Fatalf("mode=%s escaped pre-Issue gate: execute=%t err=%v", mode, executed, err)
			}
			if mode != "same" {
				for _, step := range parent.order {
					if step == "issue" {
						t.Fatalf("mode=%s reached Issue", mode)
					}
				}
			}
		})
	}
}
