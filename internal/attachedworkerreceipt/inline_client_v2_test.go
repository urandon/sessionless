package attachedworkerreceipt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type inlineClientRoundTripV2 func(*http.Request) (*http.Response, error)

func (f inlineClientRoundTripV2) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestInlineClientV2OwnsFreshHTTP1DefaultTransport(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		t.Run(strconv.FormatBool(supplied), func(t *testing.T) {
			var input *http.Client
			if supplied {
				input = &http.Client{}
			}
			c, err := NewInlineClientV2("https://receipt.invalid", input, []byte("fixture-bearer"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			transport, ok := c.http.Transport.(*http.Transport)
			if !ok || transport == http.DefaultTransport {
				t.Fatal("nil/default client inherited ambient HTTP transport")
			}
			if transport.Protocols == nil || !transport.Protocols.HTTP1() || transport.Protocols.HTTP2() || transport.Protocols.UnencryptedHTTP2() ||
				!transport.DisableKeepAlives || !transport.DisableCompression || transport.Proxy != nil || transport.DialContext == nil ||
				transport.MaxResponseHeaderBytes != maxInlineHeaderBytesV2 || transport.TLSHandshakeTimeout <= 0 || transport.ResponseHeaderTimeout <= 0 {
				t.Fatal("owned transport lacks pre-read bounds/fresh HTTP1-only connection policy")
			}
			if input != nil && input.Transport != nil {
				t.Fatal("constructor mutated borrowed client")
			}
		})
	}
}

type inlineClientResponseBodyV2 struct {
	reader        io.Reader
	reads, closes int
	closeErr      error
	readHook      func([]byte) (int, error)
	closeHook     func() error
}

func (b *inlineClientResponseBodyV2) Read(p []byte) (int, error) {
	b.reads++
	if b.readHook != nil {
		return b.readHook(p)
	}
	return b.reader.Read(p)
}
func (b *inlineClientResponseBodyV2) Close() error {
	b.closes++
	if b.closeHook != nil {
		return b.closeHook()
	}
	return b.closeErr
}

type inlineClientOriginalBodyV2 struct {
	reader        io.Reader
	inTransport   *bool
	reads, closes int
	readBeforeDo  bool
}

func (b *inlineClientOriginalBodyV2) Read(p []byte) (int, error) {
	b.reads++
	if !*b.inTransport {
		b.readBeforeDo = true
	}
	return b.reader.Read(p)
}
func (b *inlineClientOriginalBodyV2) Close() error { b.closes++; return nil }

func inlineClientFixtureV2(t *testing.T, body []byte) (ydbstore.AttachedWorkerInlinePrepareV2, ydbstore.AttachedWorkerInlineBodyV2, ydbstore.AttachedWorkerInlineReconcileV2) {
	t.Helper()
	expires := time.Now().Add(12 * time.Second)
	hash := sha256.Sum256(body)
	answer := sha256.Sum256([]byte("Private answer"))
	prepare := ydbstore.AttachedWorkerInlinePrepareV2{
		Version: 2, Authorization: ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-client"}, Nonce: "inline-client-nonce",
		Candidate:   attachedworkeroutput.InlineCandidateV2{Summary: "Private answer", Artifacts: []attachedworkeroutput.InlineArtifactV2{{Name: "report.json", MediaType: "application/json", Size: uint64(len(body)), SHA256: hex.EncodeToString(hash[:])}}},
		Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, ExpiresAt: expires,
		Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: "client-session", ReservationID: "client-reservation", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("a", 64)), AcceptedAt: time.Now().Add(-time.Second), SchemaDigest: strings.Repeat("b", 64), FinalDigest: strings.Repeat("c", 64), AnswerDigest: hex.EncodeToString(answer[:])},
	}
	fp, err := ydbstore.InlineOutputRequestFingerprintV2(prepare)
	if err != nil {
		t.Fatalf("fixture fingerprint: %v", err)
	}
	submit := ydbstore.AttachedWorkerInlineBodyV2{Version: 2, Authorization: prepare.Authorization, Nonce: prepare.Nonce, Fingerprint: fp, Capability: strings.Repeat("d", 64), ExpiresAt: expires}
	reconcile := ydbstore.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: prepare.Authorization, Nonce: prepare.Nonce, Fingerprint: fp, BodySendBurned: true}
	return prepare, submit, reconcile
}

func inlineClientResultV2(submit ydbstore.AttachedWorkerInlineBodyV2) ydbstore.AttachedWorkerInlineResultV2 {
	return ydbstore.AttachedWorkerInlineResultV2{Version: 2, Stage: ydbstore.InlineReadyV2, Nonce: submit.Nonce, Fingerprint: submit.Fingerprint, ExpiresAt: submit.ExpiresAt, CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("e", 64))}
}

func inlineClientMarshalV2(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	return raw
}
func inlineClientResponseV2(raw []byte) (*http.Response, *inlineClientResponseBodyV2) {
	body := &inlineClientResponseBodyV2{reader: bytes.NewReader(raw)}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}, "Content-Length": []string{strconv.Itoa(len(raw))}}, ContentLength: int64(len(raw)), Body: body}, body
}
func inlineClientContextV2(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func inlineClientNewV2(t *testing.T, rt inlineClientRoundTripV2) *InlineClientV2 {
	t.Helper()
	c, err := NewInlineClientV2("https://receipt.invalid", &http.Client{Transport: rt}, []byte("private-client-bearer"))
	if err != nil {
		t.Fatalf("construct client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func inlineClientConsumeV2(t *testing.T, r *http.Request) []byte {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if closeErr := r.Body.Close(); closeErr != nil {
		t.Errorf("close outgoing fixture stream: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("consume outgoing fixture stream: %v", err)
	}
	return raw
}

func TestInlineClientV2OneExchangeFixedFramingAndOriginalBytes(t *testing.T) {
	for _, path := range []string{PathInlinePrepareV2, PathInlineBodyV2, PathInlineReconcileV2} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				original := []byte(" {\"n\":9007199254740993123456789,\"x\":1e999} \n")
				prepare, submit, reconcile := inlineClientFixtureV2(t, original)
				ctx := inlineClientContextV2(t)
				wantDeadline, _ := ctx.Deadline()
				inTransport := false
				spy := &inlineClientOriginalBodyV2{reader: bytes.NewReader(original), inTransport: &inTransport}
				calls := 0
				var responseBody *inlineClientResponseBodyV2
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					calls++
					inTransport = true
					defer func() { inTransport = false }()
					deadline, ok := r.Context().Deadline()
					if !ok || !deadline.Equal(wantDeadline) || r.Method != http.MethodPost || r.URL.String() != "https://receipt.invalid"+path || r.GetBody != nil || len(r.TransferEncoding) != 0 || r.Header.Get("Idempotency-Key") != "" || r.Header.Get("X-Idempotency-Key") != "" || r.Header.Get("Authorization") != "Bearer private-client-bearer" || !inlineHeadersV2(r) {
						t.Fatalf("outgoing method=%s url=%s deadline=%v get-body=%v headers-admitted=%v", r.Method, r.URL, deadline, r.GetBody != nil, inlineHeadersV2(r))
					}
					raw := inlineClientConsumeV2(t, r)
					defer clear(raw)
					if int64(len(raw)) != r.ContentLength || r.Header.Get("Content-Length") != strconv.Itoa(len(raw)) {
						t.Errorf("outgoing bytes=%d content-length=%d header=%q", len(raw), r.ContentLength, r.Header.Get("Content-Length"))
					}
					switch path {
					case PathInlineBodyV2:
						if r.Header.Get("Content-Type") != InlineBodyMediaTypeV2 || !bytes.Equal(raw, original) || spy.readBeforeDo || spy.closes != 0 {
							t.Error("body was transformed, read before Do, or caller-owned reader closed")
						}
						metadata, err := base64.RawStdEncoding.Strict().DecodeString(r.Header.Get(InlineRequestHeaderV2))
						defer clear(metadata)
						if err != nil || !bytes.Equal(metadata, inlineClientMarshalV2(t, submit)) {
							t.Errorf("outgoing metadata changed: %v", err)
						}
					case PathInlinePrepareV2:
						if r.Header.Get("Content-Type") != "application/json" || !bytes.Equal(raw, inlineClientMarshalV2(t, prepare)) {
							t.Error("prepare encoding changed")
						}
					case PathInlineReconcileV2:
						if !bytes.Equal(raw, inlineClientMarshalV2(t, reconcile)) {
							t.Error("reconcile encoding changed")
						}
					}
					response, body := inlineClientResponseV2(inlineClientMarshalV2(t, inlineClientResultV2(submit)))
					responseBody = body
					return response, nil
				})
				var result ydbstore.AttachedWorkerInlineResultV2
				var err error
				switch path {
				case PathInlinePrepareV2:
					result, err = client.Prepare(ctx, prepare)
				case PathInlineBodyV2:
					result, err = client.Submit(ctx, submit, int64(len(original)), spy)
				case PathInlineReconcileV2:
					result, err = client.Reconcile(ctx, reconcile)
				}
				if err != nil || result.Stage != ydbstore.InlineReadyV2 || calls != 1 || responseBody.closes != 1 || len(client.active) != 0 {
					t.Fatalf("exchange result stage=%s err=%v calls=%d response-closes=%d active=%d", result.Stage, err, calls, responseBody.closes, len(client.active))
				}
			})
		})
	}
}

func TestInlineClientV2RejectsOriginAndMissingOriginalWindow(t *testing.T) {
	for _, origin := range []string{"http://receipt.invalid", "https://receipt.invalid/", "https://receipt.invalid/path", "https://user@receipt.invalid", "https://receipt.invalid?x=1", "https://receipt.invalid?", "https://receipt.invalid#fragment", "https://receipt.invalid#", "https://receipt.invalid:0", "https://receipt.invalid:65536", "//receipt.invalid", ""} {
		t.Run(origin, func(t *testing.T) {
			c, err := NewInlineClientV2(origin, nil, []byte("private-bearer"))
			if c != nil || !errors.Is(err, ErrInvalid) {
				t.Fatalf("origin=%q constructed=%v err=%v", origin, c != nil, err)
			}
		})
	}
	for _, name := range []string{"missing", "too-long", "expired", "cancelled", "zero-expiry", "expired-expiry"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				client := inlineClientNewV2(t, func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not send") })
				prepare, _, _ := inlineClientFixtureV2(t, []byte("{}"))
				ctx := context.Background()
				switch name {
				case "too-long":
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 16*time.Second)
					t.Cleanup(cancel)
				case "expired":
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					t.Cleanup(cancel)
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				default:
					if name != "missing" {
						ctx = inlineClientContextV2(t)
					}
				}
				if name == "zero-expiry" {
					prepare.ExpiresAt = time.Time{}
				}
				if name == "expired-expiry" {
					prepare.ExpiresAt = time.Now().Add(-time.Second)
				}
				if _, err := client.Prepare(ctx, prepare); !errors.Is(err, ErrInvalid) || calls != 0 {
					t.Fatalf("window=%s err=%v requests=%d", name, err, calls)
				}
			})
		})
	}
}

func TestInlineClientV2EarlierExpiryAndExpiredReadyReconciliation(t *testing.T) {
	for _, path := range []string{PathInlinePrepareV2, PathInlineBodyV2, PathInlineReconcileV2} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prepare, submit, reconcile := inlineClientFixtureV2(t, []byte("{}"))
				expiry := time.Now().Add(3 * time.Second)
				prepare.ExpiresAt = expiry
				submit.ExpiresAt = expiry
				submit.Fingerprint, _ = ydbstore.InlineOutputRequestFingerprintV2(prepare)
				reconcile.Fingerprint = submit.Fingerprint
				ctx := inlineClientContextV2(t)
				want := expiry
				if path == PathInlineReconcileV2 {
					want, _ = ctx.Deadline()
					submit.ExpiresAt = time.Now().Add(-time.Hour)
				}
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					d, _ := r.Context().Deadline()
					if !d.Equal(want) {
						t.Errorf("forwarded deadline=%v want=%v", d, want)
					}
					inlineClientConsumeV2(t, r)
					response, _ := inlineClientResponseV2(inlineClientMarshalV2(t, inlineClientResultV2(submit)))
					return response, nil
				})
				var err error
				switch path {
				case PathInlinePrepareV2:
					_, err = client.Prepare(ctx, prepare)
				case PathInlineBodyV2:
					_, err = client.Submit(ctx, submit, 2, strings.NewReader("{}"))
				default:
					_, err = client.Reconcile(ctx, reconcile)
				}
				if err != nil {
					t.Fatalf("earlier expiry/post-READY metadata path=%s err=%v", path, err)
				}
			})
		})
	}
}

func TestInlineClientV2NoRetryOrRedirectAndPrivateFailures(t *testing.T) {
	for _, status := range []int{0, http.StatusTemporaryRedirect, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prepare, _, _ := inlineClientFixtureV2(t, []byte("{}"))
				calls := 0
				var responseBody *inlineClientResponseBodyV2
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					calls++
					if status == 0 {
						return nil, errors.New("private-client-bearer private candidate")
					}
					response, body := inlineClientResponseV2([]byte("private candidate"))
					response.StatusCode = status
					response.Header.Set("Location", "https://elsewhere.invalid/secret")
					responseBody = body
					return response, nil
				})
				_, err := client.Prepare(inlineClientContextV2(t), prepare)
				if !errors.Is(err, ErrUnavailable) || calls != 1 || strings.Contains(err.Error(), "private") {
					t.Fatalf("ambiguous/status=%d err=%v requests=%d", status, err, calls)
				}
				if responseBody != nil && (responseBody.closes != 1 || responseBody.reads != 0) {
					t.Errorf("error body reads/closes=%d/%d want=0/1", responseBody.reads, responseBody.closes)
				}
			})
		})
	}
}

func TestInlineClientV2RejectsMalformedAndDivergentResponses(t *testing.T) {
	for _, name := range []string{"unknown", "case-alias", "duplicate-alias", "trailing", "invalid-UTF8", "version", "stage", "nonce", "fingerprint", "expiry", "oversize", "duplicate-media", "duplicate-length", "oversize-headers", "close-error", "short-response", "long-response"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prepare, submit, _ := inlineClientFixtureV2(t, []byte("{}"))
				result := inlineClientResultV2(submit)
				switch name {
				case "version":
					result.Version = 1
				case "stage":
					result.Stage = "unsupported"
				case "nonce":
					result.Nonce = "wrong-nonce"
				case "fingerprint":
					result.Fingerprint = strings.Repeat("f", 64)
				case "expiry":
					result.ExpiresAt = result.ExpiresAt.Add(time.Second)
				}
				raw := inlineClientMarshalV2(t, result)
				switch name {
				case "unknown":
					raw = bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":2,"answer":"private"`), 1)
				case "case-alias":
					raw = bytes.Replace(raw, []byte(`"version"`), []byte(`"VERSION"`), 1)
				case "duplicate-alias":
					raw = bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":2,"Version":2`), 1)
				case "trailing":
					raw = append(raw, []byte(`{}`)...)
				case "invalid-UTF8":
					raw = append(raw, 0xff)
				case "oversize":
					raw = bytes.Repeat([]byte("x"), maxInlineControlBytesV2+1)
				}
				response, body := inlineClientResponseV2(raw)
				switch name {
				case "duplicate-media":
					response.Header.Add("Content-Type", "application/json")
				case "duplicate-length":
					response.Header.Add("Content-Length", response.Header.Get("Content-Length"))
				case "oversize-headers":
					response.Header.Set("X-Padding", strings.Repeat("x", maxInlineHeaderBytesV2))
				case "close-error":
					body.closeErr = errors.New("private response close")
				case "short-response":
					body.reader = bytes.NewReader(raw[:len(raw)-1])
				case "long-response":
					body.reader = bytes.NewReader(append(bytes.Clone(raw), 'x'))
				}
				calls := 0
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					calls++
					inlineClientConsumeV2(t, r)
					return response, nil
				})
				actual, err := client.Prepare(inlineClientContextV2(t), prepare)
				if !errors.Is(err, ErrUnavailable) || actual.Version != 0 || calls != 1 || body.closes != 1 {
					t.Fatalf("malformed=%s err=%v result-version=%d requests=%d closes=%d", name, err, actual.Version, calls, body.closes)
				}
			})
		})
	}
}

func TestInlineClientV2CapabilityNeverReturnsAfterBodyBurn(t *testing.T) {
	for _, path := range []string{PathInlineBodyV2, PathInlineReconcileV2} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, submit, reconcile := inlineClientFixtureV2(t, []byte("{}"))
				result := inlineClientResultV2(submit)
				result.Stage = ydbstore.InlinePreparedV2
				result.CanonicalDigest = ""
				result.Capability = submit.Capability
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					inlineClientConsumeV2(t, r)
					response, _ := inlineClientResponseV2(inlineClientMarshalV2(t, result))
					return response, nil
				})
				var actual ydbstore.AttachedWorkerInlineResultV2
				var err error
				if path == PathInlineBodyV2 {
					actual, err = client.Submit(inlineClientContextV2(t), submit, 2, strings.NewReader("{}"))
				} else {
					actual, err = client.Reconcile(inlineClientContextV2(t), reconcile)
				}
				if !errors.Is(err, ErrUnavailable) || actual.Capability != "" {
					t.Fatalf("burned path=%s capability=%q err=%v", path, actual.Capability, err)
				}
			})
		})
	}
}

func TestInlineClientV2PreparedCapabilityRequiresUnburnedUnexpiredResponse(t *testing.T) {
	for _, name := range []string{"prepare", "unburned-reconcile", "expired-capability"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prepare, submit, reconcile := inlineClientFixtureV2(t, []byte("{}"))
				reconcile.BodySendBurned = false
				result := inlineClientResultV2(submit)
				result.Stage = ydbstore.InlinePreparedV2
				result.CanonicalDigest = ""
				result.Capability = submit.Capability
				if name == "expired-capability" {
					result.ExpiresAt = time.Now().Add(-time.Second)
				}
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					inlineClientConsumeV2(t, r)
					response, _ := inlineClientResponseV2(inlineClientMarshalV2(t, result))
					return response, nil
				})
				var actual ydbstore.AttachedWorkerInlineResultV2
				var err error
				if name == "prepare" {
					actual, err = client.Prepare(inlineClientContextV2(t), prepare)
				} else {
					actual, err = client.Reconcile(inlineClientContextV2(t), reconcile)
				}
				if name == "expired-capability" {
					if !errors.Is(err, ErrUnavailable) || actual.Capability != "" {
						t.Fatalf("expired capability=%q err=%v", actual.Capability, err)
					}
				} else if err != nil || actual.Capability != submit.Capability {
					t.Fatalf("original unconsumed capability=%q err=%v", actual.Capability, err)
				}
			})
		})
	}
}

func TestInlineClientV2HeaderBudgetAndUnconsumedRequestFailWithoutBodyRead(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run("oversized-header-"+strconv.FormatBool(oversized), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, submit, _ := inlineClientFixtureV2(t, []byte("{}"))
				inTransport := false
				spy := &inlineClientOriginalBodyV2{reader: strings.NewReader("{}"), inTransport: &inTransport}
				calls := 0
				secret := []byte("private-client-bearer")
				if oversized {
					secret = bytes.Repeat([]byte("x"), maxInlineHeaderBytesV2)
				}
				client, err := NewInlineClientV2("https://receipt.invalid", &http.Client{Transport: inlineClientRoundTripV2(func(r *http.Request) (*http.Response, error) {
					calls++
					response, _ := inlineClientResponseV2(inlineClientMarshalV2(t, inlineClientResultV2(submit)))
					return response, nil
				})}, secret)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				_, err = client.Submit(inlineClientContextV2(t), submit, 2, spy)
				wantErr, wantCalls := ErrUnavailable, 1
				if oversized {
					wantErr, wantCalls = ErrInvalid, 0
				}
				if !errors.Is(err, wantErr) || calls != wantCalls || spy.reads != 0 || spy.closes != 0 {
					t.Fatalf("header/unconsumed err=%v requests=%d reads/closes=%d/%d want=%v/%d/0/0", err, calls, spy.reads, spy.closes, wantErr, wantCalls)
				}
			})
		})
	}
}

func TestInlineClientV2MaximumBodyAndEscapedPrepare(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		body := bytes.Repeat([]byte("x"), maxInlineBodyBytesV2)
		prepare, submit, _ := inlineClientFixtureV2(t, body)
		prepare.Candidate.Summary = strings.Repeat("<", 32_000)
		answer := sha256.Sum256([]byte(prepare.Candidate.Summary))
		prepare.Producer.AnswerDigest = hex.EncodeToString(answer[:])
		prepare.Candidate.Artifacts = []attachedworkeroutput.InlineArtifactV2{{Name: "a.txt", MediaType: "text/plain", Size: 32 << 10, SHA256: strings.Repeat("a", 64)}, {Name: "b.txt", MediaType: "text/plain", Size: 32 << 10, SHA256: strings.Repeat("b", 64)}}
		submit.Fingerprint, _ = ydbstore.InlineOutputRequestFingerprintV2(prepare)
		calls := 0
		client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
			calls++
			raw := inlineClientConsumeV2(t, r)
			if r.URL.Path == PathInlineBodyV2 && !bytes.Equal(raw, body) {
				t.Error("exact 64KiB body changed")
			}
			if r.URL.Path == PathInlinePrepareV2 && (len(raw) > maxInlinePrepareBytesV2 || !bytes.Contains(raw, []byte(`\u003c`))) {
				t.Error("maximum-valid escaped answer was truncated or exceeded transport bound")
			}
			response, _ := inlineClientResponseV2(inlineClientMarshalV2(t, inlineClientResultV2(submit)))
			return response, nil
		})
		if _, err := client.Prepare(inlineClientContextV2(t), prepare); err != nil {
			t.Fatalf("maximum escaped prepare: %v", err)
		}
		if _, err := client.Submit(inlineClientContextV2(t), submit, int64(len(body)), bytes.NewReader(body)); err != nil {
			t.Fatalf("maximum body: %v", err)
		}
		if _, err := client.Submit(inlineClientContextV2(t), submit, maxInlineBodyBytesV2+1, bytes.NewReader(body)); !errors.Is(err, ErrInvalid) || calls != 2 {
			t.Fatalf("oversize body err=%v requests=%d want invalid/2", err, calls)
		}
	})
}

func TestInlineClientV2BodyLengthMismatchIsNotRetried(t *testing.T) {
	for _, body := range []string{"x", "xyz"} {
		t.Run(body, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				_, submit, _ := inlineClientFixtureV2(t, []byte("{}"))
				calls := 0
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					calls++
					_, err := io.ReadAll(r.Body)
					_ = r.Body.Close()
					if err == nil {
						t.Error("transport framing did not reject mismatched original reader")
					}
					response, _ := inlineClientResponseV2(inlineClientMarshalV2(t, inlineClientResultV2(submit)))
					return response, nil
				})
				if _, err := client.Submit(inlineClientContextV2(t), submit, 2, strings.NewReader(body)); !errors.Is(err, ErrUnavailable) || calls != 1 {
					t.Fatalf("length mismatch input=%q err=%v requests=%d", body, err, calls)
				}
			})
		})
	}
}

func TestInlineClientV2OriginalDeadlineCoversResponseReadAndClose(t *testing.T) {
	for _, phase := range []string{"read", "close"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				prepare, submit, _ := inlineClientFixtureV2(t, []byte("{}"))
				ctx := inlineClientContextV2(t)
				deadline, _ := ctx.Deadline()
				calls := 0
				var responseBody *inlineClientResponseBodyV2
				client := inlineClientNewV2(t, func(r *http.Request) (*http.Response, error) {
					calls++
					inlineClientConsumeV2(t, r)
					response, body := inlineClientResponseV2(inlineClientMarshalV2(t, inlineClientResultV2(submit)))
					responseBody = body
					if phase == "read" {
						body.readHook = func([]byte) (int, error) { <-r.Context().Done(); return 0, r.Context().Err() }
					} else {
						body.closeHook = func() error { <-r.Context().Done(); return r.Context().Err() }
					}
					return response, nil
				})
				actual, err := client.Prepare(ctx, prepare)
				if !errors.Is(err, ErrUnavailable) || actual.Version != 0 || calls != 1 || responseBody.closes != 1 || !time.Now().Equal(deadline) {
					t.Fatalf("bounded response %s err=%v version=%d requests/closes=%d/%d now=%v want=%v", phase, err, actual.Version, calls, responseBody.closes, time.Now(), deadline)
				}
			})
		})
	}
}

func TestInlineClientV2CloseCancelsConcurrentCallsAndClearsOwnedBearer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		prepare, _, _ := inlineClientFixtureV2(t, []byte("{}"))
		entered := make(chan struct{}, 2)
		results := make(chan error, 2)
		input := []byte("private-close-bearer")
		client, err := NewInlineClientV2("https://receipt.invalid", &http.Client{Transport: inlineClientRoundTripV2(func(r *http.Request) (*http.Response, error) {
			entered <- struct{}{}
			<-r.Context().Done()
			return nil, r.Context().Err()
		})}, input)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		owned := client.bearer
		ctx := inlineClientContextV2(t)
		for range 2 {
			go func() { _, err := client.Prepare(ctx, prepare); results <- err }()
		}
		for range 2 {
			<-entered
		}
		if err := client.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		for range 2 {
			if err := <-results; !errors.Is(err, ErrUnavailable) {
				t.Errorf("cancelled exchange err=%v want unavailable", err)
			}
		}
		if len(client.active) != 0 || !bytes.Equal(owned, make([]byte, len(owned))) || string(input) != "private-close-bearer" {
			t.Errorf("Close active=%d cleared-owned=%v caller-secret-preserved=%v", len(client.active), bytes.Equal(owned, make([]byte, len(owned))), string(input) == "private-close-bearer")
		}
		if _, err := client.Prepare(ctx, prepare); !errors.Is(err, ErrUnavailable) || len(entered) != 0 {
			t.Fatalf("call after Close err=%v queued exchanges=%d", err, len(entered))
		}
		if err := client.Close(); err != nil {
			t.Fatalf("idempotent Close: %v", err)
		}
	})
}
