package providercredential

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type piFileLedger struct {
	copy, live, nodes          uint64
	maxCopy, maxLive, maxNodes uint64
}

func newPiFileLedger() *piFileLedger {
	return &piFileLedger{maxCopy: ^uint64(0), maxLive: ^uint64(0), maxNodes: ^uint64(0)}
}
func (l *piFileLedger) Reserve(c, v, n uint64) error {
	if c > l.maxCopy-l.copy || v > l.maxLive-l.live || n > l.maxNodes-l.nodes {
		return ErrInvalid
	}
	l.copy += c
	l.live += v
	l.nodes += n
	return nil
}
func (l *piFileLedger) EndLive(v uint64) error {
	if v > l.live {
		return ErrInvalid
	}
	l.live -= v
	return nil
}

func TestPiFileUsePreIssueFundingAndRetirement(t *testing.T) {
	for _, mode := range []string{"exact", "copy short", "live short", "nodes short"} {
		t.Run(mode, func(t *testing.T) {
			service, bindings, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			file := prepareFileFixture(t, service, request)
			body := []byte("original complete immutable request")
			metadata := uint64(reflect.TypeOf(PreparedPiFileUseV1{}).Size()) + uint64(reflect.TypeOf(preparedPiFileUse{}).Size()) + uint64(reflect.TypeOf(immutableAPIFileUse{}).Size())
			metadata += uint64(reflect.TypeOf(ports.ProviderInvocationCredentialV1{}).Size()) + uint64(reflect.TypeOf((*preparedPiFileUse)(nil)).Size())
			live, work := metadata+4096+256, metadata+2*uint64(len(body))+8*uint64(file.maxBytes)+4*file.pathBytes+16384
			ledger := &piFileLedger{maxCopy: work, maxLive: live, maxNodes: 3}
			switch mode {
			case "copy short":
				ledger.maxCopy--
			case "live short":
				ledger.maxLive--
			case "nodes short":
				ledger.maxNodes--
			}
			p, err := file.PreparePiFileUseV1(ledger, body, request.HarnessBinding.Backend.BackendProfileDigest)
			if mode != "exact" {
				if p != nil || err == nil || ledger.copy != 0 || ledger.live != 0 || ledger.nodes != 0 {
					t.Fatalf("unfunded recipe: p=%v err=%v ledger=%+v", p, err, ledger)
				}
				return
			}
			if err != nil || p == nil || ledger.copy != work || ledger.live != live || ledger.nodes != 3 {
				t.Fatalf("exact funding: p=%v err=%v ledger=%+v", p, err, ledger)
			}
			if len(service.handles) != 0 || bindings.loadCalls != 0 || secrets.readCalls != 0 || secrets.recoverCalls != 0 {
				t.Fatal("preparation touched real lifecycle/backend")
			}
			if p.Nonce() != p.Nonce() {
				t.Error("nonce regenerated")
			}
			if again, err := file.PreparePiFileUseV1(ledger, body, request.HarnessBinding.Backend.BackendProfileDigest); again != nil || !errors.Is(err, ErrConsumed) {
				t.Error("prepared recipe replenished")
			}
			copyRecipe := *p
			if err := copyRecipe.Close(); err != nil {
				t.Fatal(err)
			}
			if p.state.file != nil || p.state.body != nil || p.state.use.Load() != nil || p.state.ledger != nil || p.Nonce() != ([32]byte{}) || !bytes.Equal(body, []byte("original complete immutable request")) {
				t.Error("Close retained aliases or cleared caller body")
			}
			if err := p.Close(); err != nil || ledger.live != 0 || ledger.copy != work || ledger.nodes != 3 {
				t.Fatalf("repeat Close/refund: err=%v ledger=%+v", err, ledger)
			}
		})
	}
}

func TestPiFileUseInvalidPreparation(t *testing.T) {
	for _, mode := range []string{"empty body", "huge body", "wrong profile", "nil ledger", "typed nil ledger"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			f := prepareFileFixture(t, s, request)
			body, profile := []byte("original"), request.HarnessBinding.Backend.BackendProfileDigest
			ledger := newPiFileLedger()
			var l interface {
				Reserve(uint64, uint64, uint64) error
				EndLive(uint64) error
			} = ledger
			switch mode {
			case "empty body":
				body = nil
			case "huge body":
				body = make([]byte, (1<<20)+1)
			case "wrong profile":
				profile = "wrong"
			case "nil ledger":
				l = nil
			case "typed nil ledger":
				var absent *piFileLedger
				l = absent
			}
			if p, err := f.PreparePiFileUseV1(l, body, profile); p != nil || err == nil || ledger.copy != 0 || ledger.live != 0 {
				t.Fatalf("invalid recipe funded: p=%v err=%v ledger=%+v", p, err, ledger)
			}
		})
	}
	var zero PreparedPiFileUseV1
	if zero.Nonce() != ([32]byte{}) || zero.Close() != nil || zero.BindMaterialized(context.Background(), ports.ProviderInvocationCredentialV1{}, ports.ProviderCredentialMaterializationV1{}, nil) == nil {
		t.Error("zero recipe usable")
	}
}

func TestPiFileUseCallbackRetirementCancelsBeforeJoinAndZero(t *testing.T) {
	for _, mode := range []string{"return", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			file := prepareFileFixture(t, service, request)
			ledger := newPiFileLedger()
			recipe, err := file.PreparePiFileUseV1(ledger, []byte("original request"), request.HarnessBinding.Backend.BackendProfileDigest)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := recipe.Close(); err != nil {
					t.Error(err)
				}
			})
			reserved := ledger.live
			h := issueImmutableAPI(t, service, request)
			var use *immutableAPIFileUse
			var alias []byte
			var plan ports.ProviderCredentialMaterializationV1
			synctest.Test(t, func(t *testing.T) {
				entered, release, done, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				go func() {
					defer close(done)
					defer func() {
						if got := recover(); (got != nil) != (mode == "panic") {
							t.Errorf("panic boundary: %v", got)
						}
					}()
					_ = service.MaterializeProviderCredential(context.Background(), h, func(value ports.ProviderCredentialMaterializationV1, secret []byte) error {
						plan = value
						if secret != nil {
							t.Error("file callback received plaintext")
						}
						use = service.handles[h.HandleID].fileUse.Load()
						alias = use.secret
						<-use.gate // actual original nonqueue verifier/consumer gate
						close(entered)
						go func() { <-use.ctx.Done(); close(cancelled); <-release; use.gate <- struct{}{} }()
						if mode == "panic" {
							panic("public test panic")
						}
						if mode == "error" {
							return errors.New("public failure")
						}
						return nil
					})
				}()
				<-entered
				<-cancelled
				synctest.Wait()
				select {
				case <-done:
					t.Error("callback returned before genuine gate join")
				default:
				}
				if len(alias) == 0 || bytes.Equal(alias, make([]byte, len(alias))) {
					t.Error("secret zeroed while consumer still owns its gate")
				}
				if err := recipe.Close(); !errors.Is(err, ErrConsumed) || ledger.live != reserved {
					t.Fatalf("live refunded before callback gate/zero/unlock: err=%v live=%d want=%d", err, ledger.live, reserved)
				}
				close(release)
				<-done
				if use.active.Load() || use.secret != nil || use.send.Load() != nil || !bytes.Equal(alias, make([]byte, len(alias))) || service.handles[h.HandleID].fileUse.Load() != nil {
					t.Error("retirement failed alias-drop/zero ordering")
				}
			})
			if err := recipe.Close(); err != nil || ledger.live != 0 {
				t.Fatalf("joined recipe retirement: err=%v live=%d", err, ledger.live)
			}
			expectImmutableRelease(t, service, h, plan)
		})
	}
}

func TestPiFileUseIssueSelectsExactPreparedOwner(t *testing.T) {
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	file := prepareFileFixture(t, service, request)
	ledger := newPiFileLedger()
	body := []byte("original complete request")
	profile := request.HarnessBinding.Backend.BackendProfileDigest
	original, err := file.PreparePiFileUseV1(ledger, body, profile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := original.Close(); err != nil {
			t.Error(err)
		}
	})
	foreign := prepareFileFixture(t, service, request)
	before := ledger.live
	if got, err := foreign.PreparePiFileUseV1(ledger, body, profile); got != nil || !errors.Is(err, ErrConsumed) || ledger.live != before || service.preparedPiFiles[file.scope] != original.state {
		t.Fatal("foreign identical pre-Issue owner replaced original registration")
	}
	h := issueImmutableAPI(t, service, request)
	if service.handles[h.HandleID].piFileUse != original.state || service.preparedPiFiles[file.scope] != nil {
		t.Fatal("original Issue did not select/consume exact preparation")
	}
	lateFile := prepareFileFixture(t, service, request)
	late, err := lateFile.PreparePiFileUseV1(ledger, body, profile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := late.Close(); err != nil {
			t.Error(err)
		}
	})
	var plan ports.ProviderCredentialMaterializationV1
	if err := service.MaterializeProviderCredential(t.Context(), h, func(p ports.ProviderCredentialMaterializationV1, secret []byte) error {
		plan = p
		use := service.handles[h.HandleID].fileUse.Load()
		if secret != nil || use.prepared != file || original.state.callback != 1 {
			t.Error("active use lost original owner")
		}
		if err := late.BindMaterialized(t.Context(), h, p, nil); err == nil {
			t.Error("late identical prepared owner admitted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if original.state.callback != 2 {
		t.Fatal("callback complete state not recorded after zero/unlock")
	}
	expectImmutableRelease(t, service, h, plan)
}

func TestPiFileUseIssueRejectsDifferentTemplateOrBinding(t *testing.T) {
	for _, mode := range []string{"filename", "profile"} {
		t.Run(mode, func(t *testing.T) {
			service, _, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
			file := prepareFileFixture(t, service, request)
			recipe, err := file.PreparePiFileUseV1(newPiFileLedger(), []byte("original"), request.HarnessBinding.Backend.BackendProfileDigest)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := recipe.Close(); err != nil {
					t.Error(err)
				}
			})
			if mode == "filename" {
				service.planner.(*fixedDeliveryPlanner).template.FileName = "foo-key"
			} else {
				request.HarnessBinding.Backend.BackendProfileDigest = strings.Repeat("3", 64)
			}
			if h, err := service.IssueProviderCredential(t.Context(), request); err == nil || h != (ports.ProviderInvocationCredentialV1{}) || len(service.handles) != 0 || secrets.readCalls != 0 {
				t.Fatalf("foreign issue acquired original recipe: handle=%+v err=%v", h, err)
			}
		})
	}
}

func TestPiFileUseCloseAfterIssueDeniesMaterializeBeforeSecretRead(t *testing.T) {
	service, _, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	file := prepareFileFixture(t, service, request)
	ledger := newPiFileLedger()
	recipe, err := file.PreparePiFileUseV1(ledger, []byte("original"), request.HarnessBinding.Backend.BackendProfileDigest)
	if err != nil {
		t.Fatal(err)
	}
	h := issueImmutableAPI(t, service, request)
	if err := recipe.Close(); err != nil || ledger.live != 0 {
		t.Fatal("precallback retirement failed", err)
	}
	called := false
	err = service.MaterializeProviderCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error { called = true; return nil })
	if !errors.Is(err, ErrConsumed) || called || secrets.readCalls != 0 {
		t.Fatalf("closed recipe used unfunded record: err=%v called=%v reads=%d", err, called, secrets.readCalls)
	}
	if err := service.ReleaseProviderCredential(t.Context(), h); err != nil {
		t.Fatal(err)
	}
}

type piBindPauseContext struct {
	context.Context
	entered, resume chan struct{}
	once            sync.Once
}

func (p *piBindPauseContext) Err() error {
	p.once.Do(func() { close(p.entered); <-p.resume })
	return p.Context.Err()
}

func TestPiFileUseCloseRacingBindWithPublishedActiveUse(t *testing.T) {
	service, _, _, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	file := prepareFileFixture(t, service, request)
	ledger := newPiFileLedger()
	recipe, err := file.PreparePiFileUseV1(ledger, []byte("original"), request.HarnessBinding.Backend.BackendProfileDigest)
	if err != nil {
		t.Fatal(err)
	}
	h := issueImmutableAPI(t, service, request)
	ctx := &piBindPauseContext{Context: context.Background(), entered: make(chan struct{}), resume: make(chan struct{})}
	allowReturn, callbackBound, materialized, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce, returnOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(ctx.resume) }) }
	finish := func() { returnOnce.Do(func() { close(allowReturn) }) }
	wait := func(ch <-chan struct{}, operation string) {
		t.Helper()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-ch:
		case <-timer.C:
			select {
			case <-ch:
			default:
				t.Fatalf("timeout awaiting %s", operation)
			}
		}
	}
	var bindErr, closeErr, materializeErr error
	var ioCancelled bool
	var plan ports.ProviderCredentialMaterializationV1
	go func() {
		defer close(materialized)
		materializeErr = service.MaterializeProviderCredential(ctx, h, func(p ports.ProviderCredentialMaterializationV1, _ []byte) error {
			plan = p
			bindErr = recipe.BindMaterialized(ctx, h, p, nil)
			use := service.handles[h.HandleID].fileUse.Load()
			ioCancelled = use.ctx.Err() != nil
			close(callbackBound)
			<-allowReturn
			return nil
		})
	}()
	t.Cleanup(func() {
		resume()
		finish()
		wait(materialized, "Materialize cleanup")
		if err := recipe.Close(); err != nil {
			t.Error(err)
		}
	})
	wait(ctx.entered, "bind holding recipe mutex with original active-use already published")
	go func() { defer close(closed); closeErr = recipe.Close() }()
	t.Cleanup(func() { resume(); finish(); wait(closed, "concurrent Close cleanup") })
	// Observe the actual atomic revocation condition, not a sleep or mutex
	// scheduling assumption. Timer bounds failure; the state decides readiness.
	timer, tick := time.NewTimer(10*time.Second), time.NewTicker(time.Millisecond)
	defer timer.Stop()
	defer tick.Stop()
	for !recipe.state.stopped.Load() {
		select {
		case <-tick.C:
		case <-timer.C:
			if !recipe.state.stopped.Load() {
				t.Fatal("Close did not revoke before waiting for bind mutex")
			}
		}
	}
	resume()
	wait(callbackBound, "bind denial after concurrent Close")
	wait(closed, "active callback Close")
	if bindErr == nil || !ioCancelled || !errors.Is(closeErr, ErrConsumed) || ledger.live == 0 {
		t.Fatalf("missed cancellation/early refund: bind=%v cancel=%v close=%v live=%d", bindErr, ioCancelled, closeErr, ledger.live)
	}
	finish()
	wait(materialized, "joined Materialize")
	if materializeErr != nil {
		t.Fatal(materializeErr)
	}
	if err := recipe.Close(); err != nil || ledger.live != 0 {
		t.Fatalf("final retirement: err=%v live=%d", err, ledger.live)
	}
	expectImmutableRelease(t, service, h, plan)
}

type piPublishPauseContext struct {
	context.Context
	entered, resume chan struct{}
	once            sync.Once
}

func (p *piPublishPauseContext) Done() <-chan struct{} {
	p.once.Do(func() { close(p.entered); <-p.resume })
	return nil // parent Background has no cancellation; original use gets its own
}

func TestPiFileUseCloseBeforeActualMaterializeUsePublication(t *testing.T) {
	service, _, secrets, request, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	file := prepareFileFixture(t, service, request)
	ledger := newPiFileLedger()
	recipe, err := file.PreparePiFileUseV1(ledger, []byte("original"), request.HarnessBinding.Backend.BackendProfileDigest)
	if err != nil {
		t.Fatal(err)
	}
	h := issueImmutableAPI(t, service, request)
	ctx := &piPublishPauseContext{Context: context.Background(), entered: make(chan struct{}), resume: make(chan struct{})}
	done := make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(ctx.resume) }) }
	var materializeErr error
	called := false
	go func() {
		defer close(done)
		materializeErr = service.MaterializeProviderCredential(ctx, h, func(ports.ProviderCredentialMaterializationV1, []byte) error { called = true; return nil })
	}()
	// If an assertion fails, release the owned context constructor before
	// waiting/retiring. No sleep or deadline decides the publication ordering.
	wait := func(ch <-chan struct{}, op string) {
		t.Helper()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-ch:
		case <-timer.C:
			select {
			case <-ch:
			default:
				t.Fatalf("timeout awaiting %s", op)
			}
		}
	}
	t.Cleanup(func() {
		resume()
		wait(done, "Materialize cleanup")
		if err := recipe.Close(); err != nil {
			t.Error(err)
		}
	})
	wait(ctx.entered, "original IO context construction before use publication")
	if recipe.state.use.Load() != nil {
		t.Fatal("control did not precede actual original-use publication")
	}
	live := ledger.live
	if err := recipe.Close(); !errors.Is(err, ErrConsumed) || ledger.live != live {
		t.Fatalf("prepublication close/refund: err=%v live=%d want=%d", err, ledger.live, live)
	}
	resume()
	wait(done, "cancelled original Materialize")
	use := recipe.state.use.Load()
	if !errors.Is(materializeErr, ErrConsumed) || called || secrets.readCalls != 1 || use == nil || use.ctx.Err() == nil || use.secret != nil || use.active.Load() {
		t.Fatalf("raced published use escaped cancellation: err=%v callback=%v reads=%d", materializeErr, called, secrets.readCalls)
	}
	if err := recipe.Close(); err != nil || ledger.live != 0 {
		t.Fatalf("postpublication cleanup: err=%v live=%d", err, ledger.live)
	}
	expectImmutableRelease(t, service, h, service.handles[h.HandleID].plan)
}
