package attachedworkerdaemon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
)

type structuralAPISender struct{ calls int }

func (s *structuralAPISender) Authorize([]byte, string) (time.Time, error) {
	s.calls++
	return time.Now().Add(time.Hour), nil
}

type noncomparableAPIContext map[string]string

func (noncomparableAPIContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (noncomparableAPIContext) Done() <-chan struct{}       { return nil }
func (noncomparableAPIContext) Err() error                  { return nil }
func (noncomparableAPIContext) Value(any) any               { return nil }

func TestAPISendBorrowRejectsStructuralAndZero(t *testing.T) {
	fake := &structuralAPISender{}
	if b, err := BindPreparedAPISendV1(fake, context.Background(), InvocationIdentity{}, nil,
		ports.ProviderInvocationCredentialV1{}, ports.ProviderCredentialMaterializationV1{}, nil, ""); b != nil || err == nil || fake.calls != 0 {
		t.Fatalf("structural sender became genuine authority: borrow=%v err=%v calls=%d", b, err, fake.calls)
	}
	for _, b := range []*PreparedAPISendBorrowV1{nil, {}} {
		b.Revoke()
		if cutoff, err := b.Authorize(); err == nil || !cutoff.IsZero() {
			t.Fatalf("zero borrow authorized: cutoff=%v err=%v", cutoff, err)
		}
	}
}

func TestAPIOwnerBindsOriginalSendBorrow(t *testing.T) {
	for _, name := range []string{"valid", "context", "uncomparable context", "identity", "ledger", "handle", "generation", "plan", "body", "profile", "second bind", "structural path", "second consume", "revoke", "second handle seal", "second plan seal", "locator overcap"} {
		t.Run(name, func(t *testing.T) {
			inv, parent, life := longLivedAPIOwnerFixture(t)
			var saved *PreparedAPISendBorrowV1
			var original *preparedAPIPhysicalSend
			if name == "locator overcap" {
				life.mutatePlan = func(p *ports.ProviderCredentialMaterializationV1) {
					p.FilePath = strings.Repeat("x", int(parent.profile.Limits.ProcessFrameBytes)+1)
				}
			}
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, in PreparedInvocationV1) (AttemptResult, error) {
				original = in.PhysicalSend.(*preparedAPIPhysicalSend)
				if name == "second handle seal" {
					if original.sealHandle(in.Credential) == nil {
						t.Error("second handle seal accepted")
					}
				}
				if name == "second plan seal" {
					if original.sealPlan(in.Materialization) == nil {
						t.Error("second plan seal accepted")
					}
				}
				id, ledger, handle, plan, body, profile := in.Identity, in.Ledger, in.Credential, in.Materialization, in.APIRequest.Body, in.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest
				switch name {
				case "context":
					ctx = context.WithValue(ctx, struct{}{}, true)
				case "uncomparable context":
					ctx = noncomparableAPIContext{}
				case "identity":
					id.FenceToken++
				case "ledger":
					ledger = &apiTestLedger{}
				case "handle":
					handle.HandleID += "x"
				case "generation":
					handle.ProviderResource.CredentialGeneration++
				case "plan":
					plan.FilePath += "x"
				case "body":
					body = []byte("foreign")
				case "profile":
					profile = "foreign"
				}
				b, bindErr := BindPreparedAPISendV1(in.PhysicalSend, ctx, id, ledger, handle, plan, body, profile)
				wantBind := name == "valid" || name == "second bind" || name == "structural path" || name == "second consume" || name == "revoke"
				if (bindErr == nil && b != nil) != wantBind {
					t.Errorf("%s: borrow=%v err=%v wantBind=%v", name, b, bindErr, wantBind)
				}
				if b != nil {
					// Value copies retain the same private original state.
					copyBorrow := *b
					saved = &copyBorrow
					switch name {
					case "second bind":
						if again, err := BindPreparedAPISendV1(in.PhysicalSend, ctx, id, ledger, handle, plan, body, profile); again != nil || err == nil {
							t.Error("second bind accepted")
						}
					case "structural path":
						if _, err := in.PhysicalSend.Authorize(body, profile); err == nil {
							t.Error("structural route bypassed exclusive borrow")
						}
					case "revoke":
						b.Revoke()
					}
					_, _ = b.Authorize()
					if name == "second consume" {
						_, _ = saved.Authorize()
					}
				}
				// Swallowed guard failure cannot be upgraded by process booleans.
				return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
			t.Cleanup(cancel)
			result, runErr := runner.Run(ctx, inv)
			if result.Succeeded(runErr) != (name == "valid") || life.releaseCount != 1 {
				t.Fatalf("%s: success=%v err=%v releases=%d", name, result.Succeeded(runErr), runErr, life.releaseCount)
			}
			physicalCalls := 0
			for _, phase := range parent.order {
				if phase == "physical_send" {
					physicalCalls++
				}
			}
			wantCalls := 0
			if name == "valid" || name == "second consume" {
				wantCalls = 1
			}
			if physicalCalls != wantCalls {
				t.Errorf("%s: physical Source calls=%d want=%d", name, physicalCalls, wantCalls)
			}
			if name == "locator overcap" && original != nil {
				t.Error("oversized locator reached executor")
			}
			if original != nil && (original.ctx != nil || original.ownerCtx != nil || original.cancel.Load() != nil || original.data != nil || original.ledger != nil || original.runner != nil || original.body != nil || original.plan != (ports.ProviderCredentialMaterializationV1{})) {
				t.Error("retired embedded send retains original references")
			}
			if saved != nil {
				if cutoff, err := saved.Authorize(); err == nil || !cutoff.IsZero() {
					t.Error("saved borrow survived callback retirement")
				}
			}
		})
	}
}

type boundedAPIBorrowLedger struct {
	copy, live, nodes                            uint64
	remainingCopy, remainingLive, remainingNodes uint64
}

func (l *boundedAPIBorrowLedger) Reserve(c, v, n uint64) error {
	if c > l.remainingCopy || v > l.remainingLive || n > l.remainingNodes {
		return ErrCredentialUnavailable
	}
	l.remainingCopy -= c
	l.remainingLive -= v
	l.remainingNodes -= n
	l.copy += c
	l.live += v
	l.nodes += n
	return nil
}
func (l *boundedAPIBorrowLedger) EndLive(v uint64) error {
	if v > l.live {
		return ErrCredentialUnavailable
	}
	l.live -= v
	return nil
}

func TestAPISendBorrowPreIssueFunding(t *testing.T) {
	inv, parent, _ := longLivedAPIOwnerFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	data, err := parent.PrepareOriginalAPIInvocation(ctx, inv.Identity, inv.VerifiedInput, inv.Process, inv.InputAcquisition)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(parent.Clear)
	beforeCopy, beforeLive, beforeNodes := parent.ledger.copy, parent.ledger.live, parent.ledger.nodes
	s, err := preparePiResultCustody(inv.Identity, data)
	if err != nil {
		t.Fatal(err)
	}
	c, v, n := parent.ledger.copy-beforeCopy, parent.ledger.live-beforeLive, parent.ledger.nodes-beforeNodes
	if s.send.borrow.send != &s.send || s.send.data != data || s.send.identity != inv.Identity || !samePiLedger(s.send.ledger, data.Ledger) {
		t.Fatal("prepaid original send/borrow not initialized together")
	}
	if err := s.clear(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"exact", "copy short", "live short", "nodes short"} {
		t.Run(name, func(t *testing.T) {
			ledger := &boundedAPIBorrowLedger{remainingCopy: c, remainingLive: v, remainingNodes: n}
			switch name {
			case "copy short":
				ledger.remainingCopy--
			case "live short":
				ledger.remainingLive--
			case "nodes short":
				ledger.remainingNodes--
			}
			data.Ledger = ledger
			got, err := preparePiResultCustody(inv.Identity, data)
			if name != "exact" {
				if err == nil || got != nil || ledger.copy != 0 || ledger.live != 0 || ledger.nodes != 0 {
					t.Fatalf("unfunded allocation: slot=%v err=%v debits=%d/%d/%d", got, err, ledger.copy, ledger.live, ledger.nodes)
				}
				return
			}
			if err != nil || got == nil {
				t.Fatalf("exact budget: %v", err)
			}
			if err := got.clear(); err != nil || ledger.live != 0 || ledger.copy != c || ledger.nodes != n {
				t.Fatalf("live retirement/refund: err=%v copy=%d live=%d nodes=%d", err, ledger.copy, ledger.live, ledger.nodes)
			}
		})
	}
}

func TestAPISendBorrowRevokesHeldSourceBeforeJoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered, done := make(chan struct{}), make(chan struct{})
	var result time.Time
	var callErr error
	send := &preparedAPIPhysicalSend{ctx: ctx, body: []byte("original"), profile: "profile", bound: true,
		authorize: func() (time.Time, error) { close(entered); <-ctx.Done(); return time.Now().Add(time.Hour), nil }}
	send.cancel.Store(&preparedAPICancel{cancel: cancel})
	borrow := &PreparedAPISendBorrowV1{send: send}
	go func() { defer close(done); result, callErr = borrow.Authorize() }()
	t.Cleanup(func() { cancel(); apiWait(t, done) })
	apiWait(t, entered)
	borrow.Revoke() // must not wait for the mutex held by Source
	apiWait(t, done)
	if callErr == nil || !result.IsZero() || !send.retire().IsZero() {
		t.Fatalf("held Source escaped revocation: result=%v err=%v", result, callErr)
	}
}

func TestAPISendBorrowConcurrentConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var mu sync.Mutex
	calls := 0
	send := &preparedAPIPhysicalSend{ctx: ctx, body: []byte("original"), profile: "profile", bound: true,
		authorize: func() (time.Time, error) { mu.Lock(); calls++; mu.Unlock(); return time.Now().Add(time.Hour), nil }}
	send.cancel.Store(&preparedAPICancel{cancel: cancel})
	borrow := &PreparedAPISendBorrowV1{send: send}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, _ = borrow.Authorize() })
	}
	done := make(chan struct{})
	go func() { defer close(done); wg.Wait() }()
	apiWait(t, done)
	if calls > 1 || !send.retire().IsZero() {
		t.Fatalf("concurrent borrow: Source calls=%d, completion must fail", calls)
	}
}
