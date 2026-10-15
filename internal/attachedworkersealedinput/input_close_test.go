package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
)

type inputCloseBody struct {
	reader     io.Reader
	read       func()
	close      func()
	once       sync.Once
	closes     atomic.Int32
	closeError error
}

func (b *inputCloseBody) Read(p []byte) (int, error) {
	b.once.Do(func() {
		if b.read != nil {
			b.read()
		}
	})
	return b.reader.Read(p)
}

func (b *inputCloseBody) Close() error {
	b.closes.Add(1)
	if b.close != nil {
		b.close()
	}
	return b.closeError
}

type inputLoadOutcome struct {
	input attachedworkerdaemontransport.SealedInputV1
	err   error
}

func inputClosePayload(t *testing.T) (attachedworkerdaemontransport.MaterializationRequestV1, attachedworkerdaemontransport.SealedInputV1, []byte) {
	t.Helper()
	service, _, _, _, request := fixtureInput(t)
	input, err := service.Load(t.Context(), []byte("test-connection-bearer"), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clearResult(&input) })
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(body) })
	return request, input, body
}

func TestClientInputCloseRevokesHeldOriginalLoad(t *testing.T) {
	for _, phase := range []string{"round_trip", "body_read", "body_close"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				request, _, payload := inputClosePayload(t)
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				hold := func() { close(entered); <-release }
				body := &inputCloseBody{reader: bytes.NewReader(payload)}
				if phase == "body_read" {
					body.read = hold
				} else if phase == "body_close" {
					body.close = hold
				}
				seen := make(chan context.Context, 1)
				var calls atomic.Int32
				client, err := NewClientSource("https://control.invalid"+PathV1, &http.Client{Transport: receiptRoundTrip(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					seen <- r.Context()
					if phase == "round_trip" {
						hold()
					}
					response := receiptJSONResponse(http.StatusOK, payload)
					response.Body = body
					return response, nil
				})}, []byte("test-connection-bearer"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				done := make(chan inputLoadOutcome, 1)
				go func() {
					input, err := client.Load(ctx, request)
					done <- inputLoadOutcome{input: input, err: err}
				}()
				callCtx := <-seen
				<-entered
				if err := client.Close(); err != nil {
					t.Error(err)
				}
				synctest.Wait()
				if callCtx.Err() == nil || ctx.Err() != nil {
					t.Errorf("Close did not cancel only its original HTTP call: call=%v caller=%v", callCtx.Err(), ctx.Err())
				}
				// Close revokes authority, not a claim that the non-cooperating
				// transport/body has joined. Load remains the original owner.
				var early *inputLoadOutcome
				select {
				case got := <-done:
					early = &got
					t.Error("Load returned before held original transport/body retired")
				default:
				}
				unblock()
				var got inputLoadOutcome
				if early != nil {
					got = *early
				} else {
					got = <-done
				}
				defer clearResult(&got.input)
				if !errors.Is(got.err, ErrUnavailable) || !reflect.DeepEqual(got.input, attachedworkerdaemontransport.SealedInputV1{}) {
					t.Errorf("revoked %s returned late material: err=%v contextBytes=%d artifacts=%d", phase, got.err, len(got.input.Context), len(got.input.Artifacts))
				}
				if calls.Load() != 1 || body.closes.Load() != 1 {
					t.Errorf("original call/close counts=%d/%d want1/1", calls.Load(), body.closes.Load())
				}
				if again, err := client.Load(ctx, request); !errors.Is(err, ErrUnavailable) {
					clearResult(&again)
					t.Errorf("closed source admitted new call: %v", err)
				}
				if calls.Load() != 1 {
					t.Error("Close permitted another physical input call")
				}
			})
		})
	}
}

func TestClientInputClosePreservesTransferredInputAndRejectsCloseFailure(t *testing.T) {
	for _, failClose := range []bool{false, true} {
		name := "lawful_transfer"
		if failClose {
			name = "body_close_error"
		}
		t.Run(name, func(t *testing.T) {
			request, expected, payload := inputClosePayload(t)
			body := &inputCloseBody{reader: bytes.NewReader(payload)}
			if failClose {
				body.closeError = errors.New("private body retirement error")
			}
			client, err := NewClientSource("https://control.invalid"+PathV1, &http.Client{Transport: receiptRoundTrip(func(*http.Request) (*http.Response, error) {
				response := receiptJSONResponse(http.StatusOK, payload)
				response.Body = body
				return response, nil
			})}, []byte("test-connection-bearer"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			got, err := client.Load(t.Context(), request)
			defer clearResult(&got)
			if failClose {
				if !errors.Is(err, ErrUnavailable) || !reflect.DeepEqual(got, attachedworkerdaemontransport.SealedInputV1{}) {
					t.Errorf("failed body retirement released input: err=%v bytes=%d", err, len(got.Context))
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, expected) || body.closes.Load() != 1 {
				t.Fatalf("lawful original input transfer: err=%v closeCalls=%d", err, body.closes.Load())
			}
			if err := client.Close(); err != nil || !reflect.DeepEqual(got, expected) {
				t.Errorf("Close cleared already transferred caller-owned input: %v", err)
			}
		})
	}
}
