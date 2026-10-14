package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func piFactory(t *testing.T, origin string, hc *http.Client, delegate attachedworkersession.ExchangeFactory) *SessionSourceFactory {
	t.Helper()
	f, err := NewPiPhaseSessionSourceFactory(origin+PathV1, origin+attachedworkerreceipt.PathV1, origin+PiAPIUsePathV1, hc, delegate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("close private session: %v", err)
		}
	})
	return f
}

func TestPiPhaseSessionSameAcceptedInputReceiptAndPhase(t *testing.T) {
	binding, bearer, r, reply := piClientFixture(t)
	expectedBearer := bytes.Clone(bearer)
	service, auth, _, _, materialization := fixtureInput(t)
	auth.bearer = expectedBearer
	sealed, err := service.Load(t.Context(), expectedBearer, materialization)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clearResult(&sealed) })
	// Use this original constructor-backed accepted head/tuple, not a fabricated
	// narrower API/native input. The actual BoundMaterializer Pi API path remains
	// unsupported; this fixture exercises only the three session HTTP channels.
	r.Authorization = portsForPiMaterialization(materialization, sealed.AcceptedAttempt.Revision)
	binding.CapabilityDigest = r.Authorization.CapabilityDigest
	reply.AcceptedAttempt = sealed.AcceptedAttempt
	reply.AcceptedAttemptRevision = sealed.AcceptedAttempt.Revision
	reply.CheckedAt = sealed.AcceptedAttempt.UpdatedAt.Add(time.Second)
	reply.UseExpiresAt = reply.CheckedAt.Add(time.Second + 123*time.Nanosecond)
	body, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(body) })
	phaseBody := piReplyBytes(t, reply)
	var inputCalls, receiptCalls, phaseCalls atomic.Int32
	server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer "+string(expectedBearer) {
			t.Error("session channel did not use SAME accepted bearer")
		}
		switch req.URL.Path {
		case PathV1:
			inputCalls.Add(1)
			piReplyHeaders(w)
			_, _ = w.Write(body)
		case attachedworkerreceipt.PathV1:
			receiptCalls.Add(1)
			w.WriteHeader(http.StatusForbidden)
		case PiAPIUsePathV1:
			phaseCalls.Add(1)
			piReplyHeaders(w)
			_, _ = w.Write(phaseBody)
		default:
			t.Error("unpinned session origin/path")
		}
	}))
	port := &exchangePortFixture{}
	opens := 0
	factory := piFactory(t, server.URL, hc, receiptExchangeFactory(func(got attachedworkersession.ConnectionBindingV1, token []byte) (attachedworkersession.ExchangePort, error) {
		opens++
		if got != binding || !bytes.Equal(token, expectedBearer) {
			t.Error("delegate binding/bearer replaced")
		}
		return port, nil
	}))
	if _, err := factory.RequestPiAPIUse(t.Context(), r); !errors.Is(err, ErrUnavailable) {
		t.Fatal("phase available before accepted Open")
	}
	exchange, err := factory.Open(binding, bearer)
	if err != nil {
		t.Fatal(err)
	}
	clear(bearer)
	got, err := factory.Load(t.Context(), materialization)
	if err != nil || got.AcceptedAttempt != sealed.AcceptedAttempt {
		t.Fatalf("original sealed channel err=%v", err)
	}
	clearResult(&got)
	if _, err := factory.Publish(t.Context(), attachedworkerdaemontransport.ReceiptSubmissionV1{Request: materialization, Nonce: "nonce-fixture", Candidate: attachedworkeroutput.Candidate{Status: "succeeded", Summary: "synthetic"}, Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}}); err == nil {
		t.Fatal("fixture forbidden receipt became positive")
	}
	for _, phase := range []pisdkphasewire.Phase{pisdkphasewire.PreIssue, pisdkphasewire.PhysicalSend, pisdkphasewire.ImmutableFinalize} {
		r.Phase = phase
		got, err := factory.RequestPiAPIUse(t.Context(), r)
		if err != nil || got != reply {
			t.Fatalf("same-session phase=%s err=%v", phase, err)
		}
	}
	if opens != 1 || inputCalls.Load() != 1 || receiptCalls.Load() != 1 || phaseCalls.Load() != 3 {
		t.Fatalf("channel counts open=%d input=%d receipt=%d phase=%d", opens, inputCalls.Load(), receiptCalls.Load(), phaseCalls.Load())
	}
	if _, err := factory.Open(binding, expectedBearer); !errors.Is(err, ErrUnavailable) {
		t.Fatal("second accepted Session replaced original")
	}
	if err := exchange.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.RequestPiAPIUse(t.Context(), r); !errors.Is(err, ErrUnavailable) || phaseCalls.Load() != 3 {
		t.Fatal("session retirement retained phase")
	}
	if factory.phase != nil || factory.source != nil || factory.publisher != nil || port.closes != 0 {
		t.Fatal("session-facing retirement retained bearer or blocked on delegate close")
	}
	if err := factory.Close(); err != nil || port.closes != 1 {
		t.Fatalf("original delegate cleanup err=%v closes=%d", err, port.closes)
	}
}

func portsForPiMaterialization(r attachedworkerdaemontransport.MaterializationRequestV1, revision uint64) ports.AttachedWorkerSealedInputAuthorization {
	a := ports.AuthorizationForAttachedInput(r.TenantID, r.OwnerUserID, r.WorkerID, r.ConnectionID, r.EnrollmentGeneration, r.ConnectionGeneration, r.Attempt, r.AttemptSequence)
	a.ExpectedAttemptRevision = revision
	return a
}

func TestPiPhaseSessionRetiresBeforeBlockedDelegateClose(t *testing.T) {
	for _, owner := range []string{"session", "factory"} {
		t.Run(owner, func(t *testing.T) {
			binding, bearer, r, reply := piClientFixture(t)
			data := piReplyBytes(t, reply)
			started := make(chan struct{})
			handlerDone := make(chan struct{})
			releaseHandler := make(chan struct{})
			var handlerReleaseOnce sync.Once
			server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(handlerDone)
				_, _ = io.Copy(io.Discard, io.LimitReader(req.Body, pisdkphasewire.RequestMaxBytes+1))
				piReplyHeaders(w)
				_, _ = w.Write(data)
				w.(http.Flusher).Flush()
				close(started)
				select {
				case <-req.Context().Done():
				case <-releaseHandler:
				}
			}))
			t.Cleanup(func() { handlerReleaseOnce.Do(func() { close(releaseHandler) }) })
			closeStarted, closeRelease := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(closeRelease) }) })
			port := &exchangePortFixture{closeStarted: closeStarted, closeRelease: closeRelease}
			factory := piFactory(t, server.URL, hc, &exchangeFixture{port: port})
			// Cleanup runs LIFO: release BEFORE piFactory's Close can wait on
			// the deliberately stalled delegate; retain one idempotent owner.
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(closeRelease) })
				handlerReleaseOnce.Do(func() { close(releaseHandler) })
			})
			exchange, err := factory.Open(binding, bearer)
			if err != nil {
				t.Fatal(err)
			}
			phase := factory.phase
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			requestDone := make(chan error, 1)
			requestExecuteDone := make(chan struct{})
			piCleanupJoin(t, func() {
				cancel()
				releaseOnce.Do(func() { close(closeRelease) })
				handlerReleaseOnce.Do(func() { close(releaseHandler) })
				_ = exchange.(interface{ Close() error }).Close()
			}, requestExecuteDone)
			go func() {
				defer close(requestExecuteDone)
				got, err := factory.RequestPiAPIUse(ctx, r)
				if got != (pisdkphasewire.Reply{}) {
					requestDone <- errors.New("late applied metadata escaped")
					return
				}
				requestDone <- err
			}()
			piWait(t, ctx, started, "held response")
			factoryDone := make(chan error, 1)
			if owner == "factory" {
				factoryExecuteDone := make(chan struct{})
				piCleanupJoin(t, func() { releaseOnce.Do(func() { close(closeRelease) }) }, factoryExecuteDone)
				go func() { defer close(factoryExecuteDone); factoryDone <- factory.Close() }()
				piWait(t, ctx, closeStarted, "blocked delegate Close")
			} else {
				if err := exchange.(interface{ Close() error }).Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-requestDone:
				if !errors.Is(err, ErrUnavailable) || ctx.Err() != nil {
					t.Fatalf("retire err=%v original parent=%v", err, ctx.Err())
				}
			case <-ctx.Done():
				t.Fatal("phase waited on delegate Close")
			}
			phase.mu.Lock()
			cleared := phase.closed && phase.bearer == nil && phase.binding == (attachedworkersession.ConnectionBindingV1{})
			phase.mu.Unlock()
			if !cleared {
				t.Fatal("phase owner kept accepted bearer")
			}
			handlerReleaseOnce.Do(func() { close(releaseHandler) })
			piWait(t, ctx, handlerDone, "HTTP peer join")
			if _, err := factory.RequestPiAPIUse(t.Context(), r); !errors.Is(err, ErrUnavailable) {
				t.Fatal("postretire metadata success")
			}
			releaseOnce.Do(func() { close(closeRelease) })
			if owner == "factory" {
				select {
				case err := <-factoryDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					select {
					case err := <-factoryDone:
						if err != nil {
							t.Fatal(err)
						}
					default:
						t.Fatal("delegate did not join after release")
					}
				}
			}
		})
	}
}

func TestPiPhaseSessionOpenFailureRaceAndPreclosed(t *testing.T) {
	for _, mode := range []string{"preclosed", "bad_binding", "bad_bearer", "delegate_error", "nil_port", "closed_during_open"} {
		t.Run(mode, func(t *testing.T) {
			binding, bearer, request, _ := piClientFixture(t)
			var calls atomic.Int32
			server, hc := piLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			var opens atomic.Int32
			port := &exchangePortFixture{}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			var factory *SessionSourceFactory
			delegate := receiptExchangeFactory(func(attachedworkersession.ConnectionBindingV1, []byte) (attachedworkersession.ExchangePort, error) {
				opens.Add(1)
				if mode == "closed_during_open" {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return port, ctx.Err()
					}
				}
				if mode == "delegate_error" {
					return port, errors.New("synthetic secret diagnostics")
				}
				if mode == "nil_port" {
					return nil, nil
				}
				return port, nil
			})
			factory = piFactory(t, server.URL, hc, delegate)
			if mode == "preclosed" {
				_ = factory.Close()
			}
			if mode == "bad_binding" {
				binding.ConnectionGeneration = 0
			}
			if mode == "bad_bearer" {
				bearer = bytes.Repeat([]byte{'x'}, 1025)
			}
			done := make(chan error, 1)
			executeDone := make(chan struct{})
			piCleanupJoin(t, func() { cancel(); once.Do(func() { close(release) }) }, executeDone)
			go func() { defer close(executeDone); _, err := factory.Open(binding, bearer); done <- err }()
			if mode == "closed_during_open" {
				piWait(t, ctx, entered, "held Open")
				factory.mu.Lock()
				phase := factory.phase
				factory.mu.Unlock()
				if phase == nil {
					t.Fatal("pending accepted phase ownership not published for retirement")
				}
				_ = factory.Close()
				phase.mu.Lock()
				cleared := phase.closed && phase.bearer == nil
				phase.mu.Unlock()
				if !cleared {
					t.Fatal("Open-held phase bearer retained past Close")
				}
				once.Do(func() { close(release) })
			}
			select {
			case err := <-done:
				want := ErrUnavailable
				if mode == "bad_binding" || mode == "bad_bearer" {
					want = ErrInvalid
				}
				if !errors.Is(err, want) {
					t.Fatalf("mode=%s err=%v want=%v", mode, err, want)
				}
			case <-ctx.Done():
				select {
				case err := <-done:
					want := ErrUnavailable
					if mode == "bad_binding" || mode == "bad_bearer" {
						want = ErrInvalid
					}
					if !errors.Is(err, want) {
						t.Fatalf("mode=%s err=%v want=%v", mode, err, want)
					}
				default:
					t.Fatal("Open did not join")
				}
			}
			factory.mu.Lock()
			closed := factory.state == sourceClosed && factory.source == nil && factory.publisher == nil && factory.phase == nil
			factory.mu.Unlock()
			if !closed || calls.Load() != 0 {
				t.Fatal("partial/failed Open retained channel authority")
			}
			wantOpens := int32(1)
			if mode == "preclosed" || mode == "bad_binding" || mode == "bad_bearer" {
				wantOpens = 0
			}
			if opens.Load() != wantOpens {
				t.Fatalf("opens=%d want=%d", opens.Load(), wantOpens)
			}
			if _, err := factory.RequestPiAPIUse(t.Context(), request); !errors.Is(err, ErrUnavailable) {
				t.Fatal("failed Open phase still usable")
			}
		})
	}
}

func TestPiPhaseSessionConstructorExactOriginAndOldConstructors(t *testing.T) {
	server, hc := piLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("constructor network") }))
	delegate := &exchangeFixture{port: &exchangePortFixture{}}
	for _, phase := range []string{"https://foreign.invalid" + PiAPIUsePathV1, server.URL + PiAPIUsePathV1 + "?", server.URL + "/wrong", server.URL + "/attached-worker/v1/%70i-api-use"} {
		if _, err := NewPiPhaseSessionSourceFactory(server.URL+PathV1, server.URL+attachedworkerreceipt.PathV1, phase, hc, delegate); !errors.Is(err, ErrInvalid) {
			t.Error("phase origin/path admitted")
		}
	}
	if _, err := NewPiPhaseSessionSourceFactory(server.URL+PathV1, "https://foreign.invalid"+attachedworkerreceipt.PathV1, server.URL+PiAPIUsePathV1, hc, delegate); !errors.Is(err, ErrInvalid) {
		t.Error("receipt foreign origin admitted")
	}
	for _, bad := range []string{server.URL + PathV1 + "?", server.URL + "/attached-worker/v1/%73ealed-input"} {
		if _, err := NewPiPhaseSessionSourceFactory(bad, server.URL+attachedworkerreceipt.PathV1, server.URL+PiAPIUsePathV1, hc, delegate); !errors.Is(err, ErrInvalid) {
			t.Error("input endpoint alias admitted")
		}
	}
	for _, bad := range []string{server.URL + attachedworkerreceipt.PathV1 + "?", server.URL + "/attached-worker/v1/%6futput-receipt"} {
		if _, err := NewPiPhaseSessionSourceFactory(server.URL+PathV1, bad, server.URL+PiAPIUsePathV1, hc, delegate); !errors.Is(err, ErrInvalid) {
			t.Error("receipt endpoint alias admitted")
		}
	}
	for _, ctor := range []func() (*SessionSourceFactory, error){
		func() (*SessionSourceFactory, error) {
			return NewSessionSourceFactory(server.URL+PathV1, nil, delegate)
		},
		func() (*SessionSourceFactory, error) {
			return NewReceiptSessionSourceFactory(server.URL+PathV1, server.URL+attachedworkerreceipt.PathV1, nil, delegate)
		},
	} {
		f, err := ctor()
		if err != nil {
			t.Fatal(err)
		}
		if f.phaseEndpoint != "" || f.phase != nil {
			t.Fatal("old constructor silently opted into Pi phase")
		}
		_ = f.Close()
	}
}

func TestPiPhaseSessionConcurrentRetirementIdempotent(t *testing.T) {
	b, bearer, r, reply := piClientFixture(t)
	body := piReplyBytes(t, reply)
	var calls atomic.Int32
	server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); piReplyHeaders(w); _, _ = w.Write(body) }))
	port := &exchangePortFixture{}
	f := piFactory(t, server.URL, hc, &exchangeFixture{port: port})
	exchange, err := f.Open(b, bearer)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = exchange.(interface{ Close() error }).Close(); _ = f.Close() })
	}
	wg.Wait()
	if port.closes != 1 {
		t.Fatalf("original delegate closed %d times", port.closes)
	}
	if _, err := f.RequestPiAPIUse(t.Context(), r); !errors.Is(err, ErrUnavailable) || calls.Load() != 0 {
		t.Fatal("concurrent retirement admitted phase")
	}
}
