package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const receiptTestOrigin = "https://control.invalid"

type receiptRoundTrip func(*http.Request) (*http.Response, error)

func (trip receiptRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return trip(request)
}

type receiptExchangeFactory func(attachedworkersession.ConnectionBindingV1, []byte) (attachedworkersession.ExchangePort, error)

func (factory receiptExchangeFactory) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	return factory(binding, bearer)
}

func newReceiptFactory(t *testing.T, transport http.RoundTripper, delegate attachedworkersession.ExchangeFactory) *SessionSourceFactory {
	t.Helper()
	factory, err := NewReceiptSessionSourceFactory(receiptTestOrigin+PathV1,
		receiptTestOrigin+attachedworkerreceipt.PathV1, &http.Client{Transport: transport}, delegate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Errorf("close receipt factory: %v", err)
		}
	})
	return factory
}

func receiptSubmission(t *testing.T) attachedworkerdaemontransport.ReceiptSubmissionV1 {
	t.Helper()
	_, _, _, _, request := fixtureInput(t)
	return attachedworkerdaemontransport.ReceiptSubmissionV1{
		Request: request, Nonce: "receipt-fixture-nonce",
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "fixture ready"},
		Observation: attachedworkeroutput.ProcessObservationV1{
			Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
		},
	}
}

func receiptJSONResponse(status int, data []byte) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(data))}
}

func TestReceiptSessionBindsBearerAndReplaysOnlyExactSubmission(t *testing.T) {
	submission := receiptSubmission(t)
	var receiptBodies [][]byte
	var sealedReads, semanticEffects int
	transport := receiptRoundTrip(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer accepted-fixture-bearer" {
			t.Error("HTTP channel did not use accepted bearer snapshot")
		}
		if request.Method != http.MethodPost || request.URL.Host != "control.invalid" {
			t.Errorf("unexpected request method/origin: %s %s", request.Method, request.URL.Host)
		}
		if request.URL.Path == PathV1 {
			sealedReads++
			return receiptJSONResponse(http.StatusOK, []byte(`{}`)), nil
		}
		if request.URL.Path != attachedworkerreceipt.PathV1 {
			return nil, errors.New("unexpected fixture path")
		}
		data, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		receiptBodies = append(receiptBodies, bytes.Clone(data))
		var receipt ydbstore.AttachedWorkerOutputReceiptRequest
		if err := json.Unmarshal(data, &receipt); err != nil {
			return nil, err
		}
		if len(receiptBodies) == 1 {
			semanticEffects++ // Commit occurred; its first response was lost.
			return receiptJSONResponse(http.StatusServiceUnavailable, []byte(`{}`)), nil
		}
		if !bytes.Equal(data, receiptBodies[0]) {
			t.Error("response-loss retry changed the immutable receipt")
		}
		fingerprint, err := attachedworkeroutput.CandidateFingerprint(receipt.Candidate)
		if err != nil {
			return nil, err
		}
		observation, err := receipt.Observation.Digest()
		if err != nil {
			return nil, err
		}
		result := ydbstore.AttachedWorkerOutputReceiptResult{
			Status: ports.AttachedWorkerExecutionReplayed,
			Receipt: ydbstore.AttachedWorkerOutputReceiptV1{
				Version: 1, Ready: true, Binding: receipt.Authorization, Nonce: receipt.Nonce,
				Status: receipt.Candidate.Status, CandidateFingerprint: fingerprint, ObservationDigest: observation,
				CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("ab", 32)),
			},
		}
		data, err = json.Marshal(result)
		return receiptJSONResponse(http.StatusOK, data), err
	})
	var exchangeBearer []byte
	port := &exchangePortFixture{}
	factory := newReceiptFactory(t, transport, receiptExchangeFactory(func(_ attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
		exchangeBearer = bytes.Clone(bearer)
		return port, nil
	}))
	if _, err := factory.Publish(t.Context(), submission); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pre-open publish: got %v want unavailable", err)
	}
	if _, err := factory.Load(t.Context(), submission.Request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pre-open load: got %v want unavailable", err)
	}
	bearer := []byte("accepted-fixture-bearer")
	exchange, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, bearer)
	if err != nil {
		t.Fatal(err)
	}
	clear(bearer)
	if !bytes.Equal(exchangeBearer, []byte("accepted-fixture-bearer")) {
		t.Fatal("exchange did not receive accepted bearer")
	}
	if _, err := factory.Load(t.Context(), submission.Request); err != nil {
		t.Fatal(err)
	}
	commitment, err := factory.Publish(t.Context(), submission)
	if err != nil || commitment.Status != domain.AttachedWorkerTerminalSucceeded ||
		!bytes.Equal(commitment.CanonicalDigest, bytes.Repeat([]byte{0xab}, 32)) ||
		len(receiptBodies) != 2 || semanticEffects != 1 || sealedReads != 1 {
		t.Fatalf("publish err=%v status=%s receipts=%d effects=%d reads=%d", err, commitment.Status, len(receiptBodies), semanticEffects, sealedReads)
	}
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("replacement")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("second open replaced authority: %v", err)
	}
	if err := exchange.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Publish(t.Context(), submission); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("post-close publish: %v", err)
	}
	if _, err := factory.Load(t.Context(), submission.Request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("post-close load: %v", err)
	}
	if len(receiptBodies) != 2 || sealedReads != 1 || port.closes != 0 {
		t.Fatalf("retired authority performed operation: receipts=%d reads=%d delegate closes=%d", len(receiptBodies), sealedReads, port.closes)
	}
}

func TestReceiptSessionCloseCancelsInFlightChannels(t *testing.T) {
	for _, operation := range []string{"load", "publish"} {
		for _, closeOwner := range []string{"session", "factory"} {
			t.Run(operation+"/"+closeOwner, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				t.Cleanup(cancel)
				started := make(chan struct{})
				var calls atomic.Int32
				transport := receiptRoundTrip(func(request *http.Request) (*http.Response, error) {
					calls.Add(1)
					close(started)
					<-request.Context().Done()
					return nil, request.Context().Err()
				})
				factory := newReceiptFactory(t, transport, &exchangeFixture{port: &exchangePortFixture{}})
				exchange, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("fixture-bearer"))
				if err != nil {
					t.Fatal(err)
				}
				submission := receiptSubmission(t)
				done := make(chan error, 1)
				go func() {
					if operation == "load" {
						_, err := factory.Load(ctx, submission.Request)
						done <- err
					} else {
						_, err := factory.Publish(ctx, submission)
						done <- err
					}
				}()
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("channel did not start before failure bound")
				}
				if closeOwner == "session" {
					err = exchange.(interface{ Close() error }).Close()
				} else {
					err = factory.Close()
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					var wantErr error = ErrUnavailable
					if operation == "publish" {
						wantErr = attachedworkerreceipt.ErrUnavailable
					}
					if !errors.Is(err, wantErr) || ctx.Err() != nil {
						t.Fatalf("owner close did not cancel channel independently: err=%v parent=%v", err, ctx.Err())
					}
				case <-ctx.Done():
					t.Fatal("close failed to cancel in-flight channel")
				}
				if _, err := factory.Publish(t.Context(), submission); !errors.Is(err, ErrUnavailable) || calls.Load() != 1 {
					t.Fatalf("closed channel retried: err=%v calls=%d", err, calls.Load())
				}
			})
		}
	}
}

func TestReceiptSessionOpenCloseRaceAndPartialFailure(t *testing.T) {
	for _, name := range []string{"closed-during-open", "delegate-error", "nil-port", "preclosed"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			entered, release := make(chan struct{}), make(chan struct{})
			var opens atomic.Int32
			port := &exchangePortFixture{}
			delegate := receiptExchangeFactory(func(_ attachedworkersession.ConnectionBindingV1, _ []byte) (attachedworkersession.ExchangePort, error) {
				opens.Add(1)
				if name == "closed-during-open" {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return port, ctx.Err()
					}
				}
				if name == "nil-port" {
					return nil, nil
				}
				if name == "delegate-error" {
					return port, errors.New("fixture exchange failed")
				}
				return port, nil
			})
			factory := newReceiptFactory(t, receiptRoundTrip(func(*http.Request) (*http.Response, error) {
				t.Error("unaccepted connection attempted HTTP")
				return nil, errors.New("fixture unexpected HTTP")
			}), delegate)
			if name == "preclosed" {
				if err := factory.Close(); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() {
				_, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("fixture-bearer"))
				done <- err
			}()
			if name == "closed-during-open" {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("delegate open did not start")
				}
				if err := factory.Close(); err != nil {
					t.Fatal(err)
				}
				close(release)
			}
			select {
			case err := <-done:
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("open result=%v want unavailable", err)
				}
			case <-ctx.Done():
				t.Fatal("open did not retire")
			}
			if factory.source != nil || factory.publisher != nil || factory.state != sourceClosed {
				t.Fatal("failed/racing open retained HTTP authority")
			}
			wantOpens, wantCloses := int32(1), 1
			if name == "preclosed" {
				wantOpens, wantCloses = 0, 0
			}
			if name == "nil-port" {
				wantCloses = 0
			}
			if opens.Load() != wantOpens || port.closes != wantCloses {
				t.Fatalf("delegate opens=%d/%d closes=%d/%d", opens.Load(), wantOpens, port.closes, wantCloses)
			}
			if _, err := factory.Publish(t.Context(), receiptSubmission(t)); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("failed open retained publisher: %v", err)
			}
		})
	}
}

func TestReceiptSessionConstructorRejectsUnpinnedEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"https://other.invalid" + attachedworkerreceipt.PathV1,
		receiptTestOrigin + ":443" + attachedworkerreceipt.PathV1,
		"http://control.invalid" + attachedworkerreceipt.PathV1,
		receiptTestOrigin + "/other", receiptTestOrigin + attachedworkerreceipt.PathV1 + "?redirect=1", "%",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if _, err := NewReceiptSessionSourceFactory(receiptTestOrigin+PathV1, endpoint, nil,
				&exchangeFixture{port: &exchangePortFixture{}}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("constructor result=%v want invalid", err)
			}
		})
	}
	if _, err := NewReceiptSessionSourceFactory(receiptTestOrigin+"/wrong-input", receiptTestOrigin+attachedworkerreceipt.PathV1, nil,
		&exchangeFixture{port: &exchangePortFixture{}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid input accepted: %v", err)
	}
	if _, err := NewReceiptSessionSourceFactory(receiptTestOrigin+PathV1, receiptTestOrigin+attachedworkerreceipt.PathV1, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil exchange accepted: %v", err)
	}
}

func TestOrdinarySessionSourceStillDeniesReceiptChannel(t *testing.T) {
	factory, err := NewSessionSourceFactory(receiptTestOrigin+PathV1, nil, &exchangeFixture{port: &exchangePortFixture{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = factory.Close() })
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("fixture-bearer")); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Publish(t.Context(), receiptSubmission(t)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ordinary source enabled receipt channel: %v", err)
	}
}
