package attachedworkerreceipt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

type inlineStoreFixtureV2 struct {
	binder   *receiptBinderFixture
	calls    int
	claimed  bool
	read     bool
	observed []byte
	err      error
	result   attachedworkeroutput.AttachedWorkerInlineResultV2
	deadline time.Time
}

func (s *inlineStoreFixtureV2) authorize(auth ports.AttachedWorkerSealedInputAuthorization) error {
	s.calls++
	if s.binder.calls == 0 || auth.PresentedSecretDigest != domain.DigestAttachedWorkerConnectionSecret([]byte("server-derived")) {
		return errors.New("binder was not first")
	}
	return s.err
}
func (s *inlineStoreFixtureV2) PrepareAttachedWorkerInlineOutput(ctx context.Context, _ ports.BlobStore, r attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.deadline, _ = ctx.Deadline()
	return s.result, s.authorize(r.Authorization)
}
func (s *inlineStoreFixtureV2) ReconcileAttachedWorkerInlineOutput(ctx context.Context, r attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.deadline, _ = ctx.Deadline()
	return s.result, s.authorize(r.Authorization)
}
func (s *inlineStoreFixtureV2) SubmitAttachedWorkerInlineOutput(ctx context.Context, _ ports.BlobStore, r attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.deadline, _ = ctx.Deadline()
	if err := s.authorize(r.Authorization); err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	s.claimed = true
	if s.read {
		var err error
		s.observed, err = io.ReadAll(body)
		if err != nil || int64(len(s.observed)) != length {
			return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ydbstore.ErrAttachedWorkerAttemptConflict
		}
	}
	return s.result, nil
}

type inlineClaimReaderV2 struct {
	store       *inlineStoreFixtureV2
	reader      io.Reader
	reads       int
	beforeClaim bool
}

func (r *inlineClaimReaderV2) Read(p []byte) (int, error) {
	r.reads++
	if !r.store.claimed {
		r.beforeClaim = true
		return 0, errors.New("read before durable claim")
	}
	return r.reader.Read(p)
}
func (r *inlineClaimReaderV2) Close() error { return nil }

type inlineDeadlineWriterV2 struct {
	*httptest.ResponseRecorder
	deadlines           []time.Time
	err                 error
	writeDeadlines      []time.Time
	writeErr            error
	writeErrAt          int
	writes              int
	beforeWriteDeadline bool
	flushes             int
	beforeFlushDeadline bool
	flushErr            error
}

func (w *inlineDeadlineWriterV2) SetWriteDeadline(deadline time.Time) error {
	w.writeDeadlines = append(w.writeDeadlines, deadline)
	if w.writeErrAt > 0 && len(w.writeDeadlines) != w.writeErrAt {
		return nil
	}
	return w.writeErr
}

func (w *inlineDeadlineWriterV2) WriteHeader(status int) {
	w.writes++
	if len(w.writeDeadlines) == 0 || w.writeDeadlines[len(w.writeDeadlines)-1].IsZero() {
		w.beforeWriteDeadline = true
	}
	w.ResponseRecorder.WriteHeader(status)
}

func (w *inlineDeadlineWriterV2) Write(payload []byte) (int, error) {
	w.writes++
	if len(w.writeDeadlines) == 0 || w.writeDeadlines[len(w.writeDeadlines)-1].IsZero() {
		w.beforeWriteDeadline = true
	}
	return w.ResponseRecorder.Write(payload)
}

func (w *inlineDeadlineWriterV2) FlushError() error {
	w.flushes++
	if len(w.writeDeadlines) == 0 || w.writeDeadlines[len(w.writeDeadlines)-1].IsZero() || len(w.deadlines) == 0 || w.deadlines[len(w.deadlines)-1].IsZero() {
		w.beforeFlushDeadline = true
	}
	w.ResponseRecorder.Flush()
	return w.flushErr
}

// This wrapper intentionally supports writes/flush but not read deadlines.
type inlineWriteOnlyWriterV2 struct{ *httptest.ResponseRecorder }

func (*inlineWriteOnlyWriterV2) SetWriteDeadline(time.Time) error { return nil }
func (w *inlineWriteOnlyWriterV2) FlushError() error              { w.ResponseRecorder.Flush(); return nil }

type inlineReadOnlyWriterV2 struct{ *httptest.ResponseRecorder }

func (*inlineReadOnlyWriterV2) SetReadDeadline(time.Time) error { return nil }

func inlineExpectAbortV2(t *testing.T, action func()) {
	t.Helper()
	defer func() {
		if failure := recover(); failure != http.ErrAbortHandler {
			t.Errorf("abort failure=%v want http.ErrAbortHandler", failure)
		}
	}()
	action()
	t.Error("handler returned normally; want http.ErrAbortHandler")
}

func TestInlineV2UnsupportedWriteDeadlineAbortsWithoutEffects(t *testing.T) {
	for _, tc := range []struct {
		name      string
		supported bool
		failAt    int
	}{
		{name: "unsupported"},
		{name: "initial-write-deadline-error", supported: true, failAt: 1},
		{name: "tightened-write-deadline-error", supported: true, failAt: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			request := inlineBodyRequestV2(t, []byte("content"))
			reader := &inlineClaimReaderV2{store: store, reader: strings.NewReader("content")}
			request.Body = reader
			recorder := httptest.NewRecorder()
			var writer http.ResponseWriter = &inlineReadOnlyWriterV2{ResponseRecorder: recorder}
			var deadlineWriter *inlineDeadlineWriterV2
			if tc.supported {
				deadlineWriter = &inlineDeadlineWriterV2{ResponseRecorder: recorder, writeErr: errors.New("private write deadline failure"), writeErrAt: tc.failAt}
				writer = deadlineWriter
			}
			inlineExpectAbortV2(t, func() { HandlerInlineV2(service).ServeHTTP(writer, request) })
			if store.calls != 0 || reader.reads != 0 || recorder.Body.Len() != 0 || recorder.Flushed || (deadlineWriter != nil && deadlineWriter.writes != 0) {
				t.Fatalf("abort %s store-calls=%d reads=%d response-bytes=%d flushed=%v", tc.name, store.calls, reader.reads, recorder.Body.Len(), recorder.Flushed)
			}
		})
	}
}

func TestInlineV2FlushFailureAbortsWithoutFallbackWrite(t *testing.T) {
	service, store := inlineFixtureV2(t)
	raw := inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()})
	writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder(), flushErr: errors.New("private flush error")}
	inlineExpectAbortV2(t, func() {
		HandlerInlineV2(service).ServeHTTP(writer, inlineControlRequestV2(t, PathInlinePrepareV2, raw))
	})
	if store.calls != 1 || writer.writes != 2 || writer.flushes != 1 || writer.beforeWriteDeadline || writer.beforeFlushDeadline || strings.Contains(writer.Body.String(), "private") {
		t.Fatalf("flush failure calls/writes/flushes=%d/%d/%d write/flush-before-deadline=%v/%v", store.calls, writer.writes, writer.flushes, writer.beforeWriteDeadline, writer.beforeFlushDeadline)
	}
}

func TestInlineV2ResponseWriteDeadlineCoversSameWindow(t *testing.T) {
	for _, tc := range []struct {
		name, path     string
		want           int
		parent, expiry time.Duration
		deny           bool
	}{
		{name: "prepare-success", path: PathInlinePrepareV2, want: http.StatusOK},
		{name: "prepare-original-expiry", path: PathInlinePrepareV2, want: http.StatusOK, expiry: 5 * time.Second},
		{name: "reconcile-parent", path: PathInlineReconcileV2, want: http.StatusOK, parent: 10 * time.Second},
		{name: "body-original-expiry", path: PathInlineBodyV2, want: http.StatusOK, expiry: 5 * time.Second},
		{name: "body-parent-earlier", path: PathInlineBodyV2, want: http.StatusOK, parent: 3 * time.Second, expiry: 5 * time.Second},
		{name: "binding-error", path: PathInlineBodyV2, want: http.StatusUnauthorized, deny: true, expiry: 5 * time.Second},
		{name: "framing-error", path: PathInlinePrepareV2, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				service, store := inlineFixtureV2(t)
				start := time.Now()
				expected := start.Add(15 * time.Second)
				prepare := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()}
				if tc.path == PathInlinePrepareV2 && tc.expiry > 0 {
					prepare.ExpiresAt = start.Add(tc.expiry)
					expected = prepare.ExpiresAt
				}
				var input any = prepare
				if tc.path == PathInlineReconcileV2 {
					input = attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: inlineAuthV2()}
				}
				request := inlineControlRequestV2(t, tc.path, inlineMarshalV2(t, input))
				if tc.path == PathInlineBodyV2 {
					request = inlineBodyRequestV2(t, []byte("content"))
					body := attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: start.Add(tc.expiry)}
					request.Header.Set(InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(inlineMarshalV2(t, body)))
					expected = body.ExpiresAt
				}
				if tc.parent > 0 {
					parent := start.Add(tc.parent)
					ctx, cancel := context.WithDeadline(request.Context(), parent)
					t.Cleanup(cancel)
					request = request.WithContext(ctx)
					if parent.Before(expected) {
						expected = parent
					}
				}
				if tc.deny {
					request.Header.Set("Authorization", "Bearer denied")
				}
				if tc.want == http.StatusBadRequest {
					request.ContentLength = -1
				}
				writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
				HandlerInlineV2(service).ServeHTTP(writer, request)
				if writer.Code != tc.want || writer.writes == 0 || writer.beforeWriteDeadline || writer.beforeFlushDeadline || writer.flushes != 1 {
					t.Fatalf("response %s status=%d writes=%d write-before-deadline=%v flush-before-deadline=%v flushes=%d want=%d/positive/false/false/1", tc.name, writer.Code, writer.writes, writer.beforeWriteDeadline, writer.beforeFlushDeadline, writer.flushes, tc.want)
				}
				if got := writer.Header().Get("Content-Length"); got != strconv.Itoa(writer.Body.Len()) {
					t.Errorf("response %s content-length=%q want=%d", tc.name, got, writer.Body.Len())
				}
				if tc.want != http.StatusOK && (writer.Body.String() != http.StatusText(tc.want)+"\n" || writer.Header().Get("Content-Type") != "text/plain; charset=utf-8") {
					t.Errorf("response %s changed generic error body/media: body=%q media=%q", tc.name, writer.Body.String(), writer.Header().Get("Content-Type"))
				}
				if len(writer.writeDeadlines) == 0 || !writer.writeDeadlines[len(writer.writeDeadlines)-1].Equal(expected) {
					t.Errorf("response %s physical-write-deadlines=%v want final=%v retained through server tail", tc.name, writer.writeDeadlines, expected)
				}
				if tc.want == http.StatusOK && !store.deadline.Equal(expected) {
					t.Errorf("response %s forwarded deadline=%v want=%v", tc.name, store.deadline, expected)
				}
				if len(writer.deadlines) == 0 || !writer.deadlines[len(writer.deadlines)-1].Equal(expected) {
					t.Errorf("response %s final physical read deadlines=%v want=%v retained through server tail", tc.name, writer.deadlines, expected)
				}
				// Model the server-owned response tail after ServeHTTP returns. This
				// is not a real HTTP/2 END_STREAM or TCP conformance test.
				if err := writer.FlushError(); err != nil || writer.beforeFlushDeadline || writer.flushes != 2 {
					t.Errorf("response %s simulated server tail err=%v before-deadline=%v flushes=%d want=nil/false/2", tc.name, err, writer.beforeFlushDeadline, writer.flushes)
				}
			})
		})
	}
}

type inlineRetainingBinderV2 struct {
	base     *receiptBinderFixture
	borrowed []byte
}

func (b *inlineRetainingBinderV2) BindOutputReceiptBearer(bearer []byte, auth ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error) {
	b.borrowed = bearer
	return b.base.BindOutputReceiptBearer(bearer, auth)
}

type inlinePayloadWriterV2 struct {
	*inlineDeadlineWriterV2
	borrowed []byte
}

func (w *inlinePayloadWriterV2) Write(payload []byte) (int, error) {
	w.borrowed = payload
	return w.ResponseRecorder.Write(payload)
}

func (w *inlineDeadlineWriterV2) SetReadDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return w.err
}

type inlinePanicBinderV2 struct{ failure any }

func (b *inlinePanicBinderV2) BindOutputReceiptBearer([]byte, ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error) {
	panic(b.failure)
}

func TestInlineV2RetainsDeadlinesAndOriginalPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run("panic-"+strconv.FormatBool(panics), func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			original := errors.New("private original handler failure")
			if panics {
				service.binder = &inlinePanicBinderV2{failure: original}
			}
			raw := inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()})
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			var failure any
			func() {
				defer func() { failure = recover() }()
				HandlerInlineV2(service).ServeHTTP(writer, inlineControlRequestV2(t, PathInlinePrepareV2, raw))
			}()
			if panics {
				if failure != original || store.calls != 0 || writer.writes != 0 || writer.flushes != 0 {
					t.Fatalf("original panic=%v want=%v calls/writes/flushes=%d/%d/%d", failure, original, store.calls, writer.writes, writer.flushes)
				}
			} else if failure != nil || writer.Code != http.StatusOK || store.calls != 1 || writer.flushes != 1 {
				t.Fatalf("handler replaced success: panic=%v status=%d calls/flushes=%d/%d", failure, writer.Code, store.calls, writer.flushes)
			}
			for _, deadline := range writer.deadlines {
				if deadline.IsZero() {
					t.Error("handler cleared a read deadline before the server-owned response tail")
				}
			}
			for _, deadline := range writer.writeDeadlines {
				if deadline.IsZero() {
					t.Error("handler cleared a write deadline before the server-owned response tail")
				}
			}
		})
	}
}

type inlineDeadlineReaderV2 struct {
	reader         io.Reader
	writer         *inlineDeadlineWriterV2
	reads          int
	beforeDeadline bool
}

func (r *inlineDeadlineReaderV2) Read(p []byte) (int, error) {
	r.reads++
	if len(r.writer.deadlines) == 0 || r.writer.deadlines[len(r.writer.deadlines)-1].IsZero() {
		r.beforeDeadline = true
		return 0, errors.New("physical read deadline missing before read")
	}
	return r.reader.Read(p)
}

func (*inlineDeadlineReaderV2) Close() error { return nil }

func TestInlineV2ControlDeadlinePrecedesEveryReadAndReachesStore(t *testing.T) {
	for _, path := range []string{PathInlinePrepareV2, PathInlineReconcileV2} {
		for _, earlierParent := range []bool{false, true} {
			t.Run(path+"/earlier-parent-"+strconv.FormatBool(earlierParent), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					service, store := inlineFixtureV2(t)
					var input any = attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()}
					if path == PathInlineReconcileV2 {
						input = attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: inlineAuthV2()}
					}
					raw := inlineMarshalV2(t, input)
					request := inlineControlRequestV2(t, path, raw)
					expected := time.Now().Add(15 * time.Second)
					if earlierParent {
						expected = time.Now().Add(10 * time.Second)
						ctx, cancel := context.WithDeadline(request.Context(), expected)
						t.Cleanup(cancel)
						request = request.WithContext(ctx)
					}
					writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
					reader := &inlineDeadlineReaderV2{reader: bytes.NewReader(raw), writer: writer}
					request.Body = reader
					HandlerInlineV2(service).ServeHTTP(writer, request)
					if writer.Code != http.StatusOK || reader.beforeDeadline || reader.reads == 0 || store.calls != 1 {
						t.Fatalf("control path=%s status=%d read-before-deadline=%v reads=%d store-calls=%d want=200/false/positive/1", path, writer.Code, reader.beforeDeadline, reader.reads, store.calls)
					}
					if len(writer.deadlines) == 0 || !writer.deadlines[len(writer.deadlines)-1].Equal(expected) || !store.deadline.Equal(expected) {
						t.Errorf("control path=%s physical-deadlines=%v forwarded=%v want=%v retained", path, writer.deadlines, store.deadline, expected)
					}
				})
			})
		}
	}
}

func inlineFixtureV2(t *testing.T) (*InlineServiceV2, *inlineStoreFixtureV2) {
	t.Helper()
	binder := &receiptBinderFixture{}
	store := &inlineStoreFixtureV2{binder: binder, result: attachedworkeroutput.AttachedWorkerInlineResultV2{
		Version: 2, Stage: attachedworkeroutput.InlineReadyV2, Nonce: "inline-test-nonce", Fingerprint: strings.Repeat("a", 64), CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("b", 64)), ExpiresAt: time.Unix(2_000_000_000, 0).UTC(),
	}}
	service, err := NewInlineServiceV2(binder, store, receiptBlobFixture{})
	if err != nil {
		t.Fatalf("construct inline service: %v", err)
	}
	return service, store
}

func inlineMarshalV2(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	return b
}
func inlineAuthV2() ports.AttachedWorkerSealedInputAuthorization {
	return ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-a"}
}
func inlineBodyRequestV2(t *testing.T, content []byte) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, PathInlineBodyV2, bytes.NewReader(content))
	request.Header.Set("Authorization", "Bearer bound-bearer")
	request.Header.Set("Content-Type", InlineBodyMediaTypeV2)
	request.Header.Set(InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: inlineAuthV2(), Nonce: "inline-test-nonce", Fingerprint: strings.Repeat("a", 64), Capability: strings.Repeat("c", 64), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()})))
	return request
}
func inlineControlRequestV2(t *testing.T, path string, raw []byte) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer bound-bearer")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestInlineV2BodyBindsClaimsThenReadsOriginalBytes(t *testing.T) {
	for _, size := range []int{1, maxInlineBodyBytesV2} {
		t.Run(stringSizeV2(size), func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			store.read = true
			content := bytes.Repeat([]byte("x"), size)
			if size == maxInlineBodyBytesV2 {
				copy(content, []byte(`{"n":9007199254740993123456789}`))
			}
			request := inlineBodyRequestV2(t, content)
			reader := &inlineClaimReaderV2{store: store, reader: bytes.NewReader(content)}
			request.Body = reader
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			before := time.Now()
			HandlerInlineV2(service).ServeHTTP(writer, request)
			after := time.Now()
			if writer.Code != http.StatusOK || !store.claimed || reader.beforeClaim || reader.reads == 0 || !bytes.Equal(store.observed, content) {
				t.Fatalf("body size=%d status=%d claimed=%v read-before-claim=%v reads=%d original-bytes=%v", size, writer.Code, store.claimed, reader.beforeClaim, reader.reads, bytes.Equal(store.observed, content))
			}
			if len(writer.deadlines) == 0 || writer.deadlines[0].Before(before) || writer.deadlines[0].After(after.Add(15*time.Second)) || writer.deadlines[len(writer.deadlines)-1].IsZero() {
				t.Errorf("read deadlines=%v; want bounded and retained", writer.deadlines)
			}
			secrets := []string{"bound-bearer", strings.Repeat("c", 64)}
			if len(content) >= 64 {
				secrets = append(secrets, string(content))
			}
			for _, secret := range secrets {
				if strings.Contains(writer.Body.String(), secret) {
					t.Errorf("response exposed private input of length %d", len(secret))
				}
			}
		})
	}
}
func stringSizeV2(n int) string {
	return strconv.Itoa(n) + "-bytes"
}

func TestInlineV2UnauthorizedAndDeniedBodiesAreNeverRead(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bearer   string
		storeErr error
		want     int
		calls    int
	}{
		{name: "binding-denied", bearer: "Bearer wrong", want: http.StatusUnauthorized},
		{name: "membership-denied", bearer: "Bearer bound-bearer", storeErr: domain.ErrMembershipDenied, want: http.StatusUnauthorized, calls: 1},
		{name: "claim-conflict", bearer: "Bearer bound-bearer", storeErr: ydbstore.ErrAttachedWorkerAttemptConflict, want: http.StatusConflict, calls: 1},
		{name: "backend-failure", bearer: "Bearer bound-bearer", storeErr: errors.New("private bearer bound-bearer and content"), want: http.StatusServiceUnavailable, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			store.err = tc.storeErr
			store.read = true
			request := inlineBodyRequestV2(t, []byte("private content"))
			request.Header.Set("Authorization", tc.bearer)
			reader := &inlineClaimReaderV2{store: store, reader: strings.NewReader("private content")}
			request.Body = reader
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			if writer.Code != tc.want || store.calls != tc.calls || reader.reads != 0 {
				t.Fatalf("status/calls/reads=%d/%d/%d want=%d/%d/0", writer.Code, store.calls, reader.reads, tc.want, tc.calls)
			}
			if strings.Contains(writer.Body.String(), "private") || strings.Contains(writer.Body.String(), "bound-bearer") {
				t.Error("private failure text escaped")
			}
		})
	}
}

func TestInlineV2RejectsBodyFramingBeforeStoreOrRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"zero-length", func(r *http.Request) { r.ContentLength = 0 }},
		{"negative-length", func(r *http.Request) { r.ContentLength = -1 }},
		{"too-large", func(r *http.Request) { r.ContentLength = maxInlineBodyBytesV2 + 1 }},
		{"chunked", func(r *http.Request) { r.TransferEncoding = []string{"chunked"} }},
		{"trailer", func(r *http.Request) { r.Trailer = http.Header{"X-Later": nil} }},
		{"wrong-media", func(r *http.Request) { r.Header.Set("Content-Type", "application/octet-stream") }},
		{"duplicate-media", func(r *http.Request) { r.Header.Add("Content-Type", InlineBodyMediaTypeV2) }},
		{"duplicate-metadata", func(r *http.Request) { r.Header.Add(InlineRequestHeaderV2, r.Header.Get(InlineRequestHeaderV2)) }},
		{"case-alias-metadata", func(r *http.Request) {
			r.Header[strings.ToLower(InlineRequestHeaderV2)] = []string{r.Header.Get(InlineRequestHeaderV2)}
		}},
		{"duplicate-authorization", func(r *http.Request) { r.Header.Add("Authorization", "Bearer bound-bearer") }},
		{"invalid-base64", func(r *http.Request) { r.Header.Set(InlineRequestHeaderV2, "%%%") }},
		{"padded-base64", func(r *http.Request) { r.Header.Set(InlineRequestHeaderV2, r.Header.Get(InlineRequestHeaderV2)+"=") }},
		{"oversized-headers", func(r *http.Request) { r.Header.Set("X-Padding", strings.Repeat("x", maxInlineHeaderBytesV2)) }},
		{"conflicting-length", func(r *http.Request) { r.Header.Set("Content-Length", "999") }},
		{"duplicate-length", func(r *http.Request) { r.Header["Content-Length"] = []string{"7", "7"} }},
		{"query", func(r *http.Request) { r.URL.RawQuery = "x=1" }},
		{"empty-query", func(r *http.Request) { r.URL.ForceQuery = true }},
		{"method", func(r *http.Request) { r.Method = http.MethodGet }},
		{"v1-no-fallback", func(r *http.Request) { r.URL.Path = PathV1 }},
		{"slash-no-redirect", func(r *http.Request) { r.URL.Path += "/" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			request := inlineBodyRequestV2(t, []byte("content"))
			tc.mutate(request)
			reader := &inlineClaimReaderV2{store: store, reader: strings.NewReader("content")}
			request.Body = reader
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			if writer.Code < 400 || writer.Code >= 500 || store.calls != 0 || reader.reads != 0 || writer.Header().Get("Location") != "" {
				t.Fatalf("rejected framing status=%d calls=%d reads=%d location=%q", writer.Code, store.calls, reader.reads, writer.Header().Get("Location"))
			}
		})
	}
}

func TestInlineV2ExactLengthCheckedAfterClaim(t *testing.T) {
	for _, content := range []string{"short", "too-long-content"} {
		t.Run(content, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			store.read = true
			request := inlineBodyRequestV2(t, []byte("1234567"))
			request.Body = &inlineClaimReaderV2{store: store, reader: strings.NewReader(content)}
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			if writer.Code != http.StatusConflict || !store.claimed {
				t.Fatalf("length mismatch status=%d claimed=%v", writer.Code, store.claimed)
			}
		})
	}
}

func TestInlineV2ClosedCanonicalControl(t *testing.T) {
	base := inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: inlineAuthV2(), Nonce: "inline-test-nonce", Fingerprint: strings.Repeat("a", 64)})
	for _, tc := range []struct {
		name string
		raw  []byte
		want int
	}{
		{"canonical", base, http.StatusOK},
		{"space", append([]byte(" "), base...), http.StatusBadRequest},
		{"newline", append(append([]byte(nil), base...), '\n'), http.StatusBadRequest},
		{"case-alias", bytes.Replace(base, []byte(`"version"`), []byte(`"VERSION"`), 1), http.StatusBadRequest},
		{"duplicate-case-alias", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"Version":2`), 1), http.StatusBadRequest},
		{"duplicate", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1), http.StatusBadRequest},
		{"unknown", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":2,"unknown":true`), 1), http.StatusBadRequest},
		{"escaped-key", bytes.Replace(base, []byte(`"version"`), []byte(`"\u0076ersion"`), 1), http.StatusBadRequest},
		{"invalid-surrogate", bytes.Replace(base, []byte("inline-test-nonce"), []byte(`\ud800`), 1), http.StatusBadRequest},
		{"trailing-value", append(append([]byte(nil), base...), []byte(`{}`)...), http.StatusBadRequest},
		{"v1", bytes.Replace(base, []byte(`"version":2`), []byte(`"version":1`), 1), http.StatusBadRequest},
		{"digest-supplied", bytes.Replace(base, []byte(`"PresentedSecretDigest":""`), []byte(`"PresentedSecretDigest":"caller-secret"`), 1), http.StatusBadRequest},
		{"oversize-control", bytes.Repeat([]byte("x"), maxInlineControlBytesV2+1), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			request := inlineControlRequestV2(t, PathInlineReconcileV2, tc.raw)
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			wantCalls := 0
			if tc.want == http.StatusOK {
				wantCalls = 1
			}
			if writer.Code != tc.want || store.calls != wantCalls {
				t.Fatalf("control status=%d calls=%d want=%d/%d", writer.Code, store.calls, tc.want, wantCalls)
			}
		})
	}
}

func TestInlineV2PrepareBoundAndCompactResponse(t *testing.T) {
	for _, size := range []int{maxInlinePrepareBytesV2, maxInlinePrepareBytesV2 + 1} {
		t.Run(stringSizeV2(size), func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			input := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()}
			base := inlineMarshalV2(t, input)
			input.Candidate.Summary = strings.Repeat("x", size-len(base))
			raw := inlineMarshalV2(t, input)
			if len(raw) != size {
				t.Fatalf("fixture encoded bytes=%d want=%d", len(raw), size)
			}
			request := inlineControlRequestV2(t, PathInlinePrepareV2, raw)
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			want := http.StatusOK
			if size > maxInlinePrepareBytesV2 {
				want = http.StatusBadRequest
			}
			if writer.Code != want {
				t.Fatalf("prepare bytes=%d status=%d want=%d", size, writer.Code, want)
			}
			if size > maxInlinePrepareBytesV2 && store.calls != 0 {
				t.Error("oversized prepare reached store")
			}
			if writer.Body.Len() > maxInlineControlBytesV2 || strings.Contains(writer.Body.String(), strings.Repeat("x", 64)) {
				t.Error("response was not compact identity-only")
			}
		})
	}
	for _, field := range []string{"nonce", "fingerprint", "capability", "canonical-digest", "stage"} {
		t.Run(field, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			private := strings.Repeat("private-payload", maxInlineControlBytesV2)
			switch field {
			case "nonce":
				store.result.Nonce = domain.IdempotencyKey(private)
			case "fingerprint":
				store.result.Fingerprint = private
			case "capability":
				store.result.Capability = private
			case "canonical-digest":
				store.result.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(private)
			case "stage":
				store.result.Stage = attachedworkeroutput.AttachedWorkerInlineStageV2(private)
			}
			raw := inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()})
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, inlineControlRequestV2(t, PathInlinePrepareV2, raw))
			if writer.Code != http.StatusServiceUnavailable || writer.Body.Len() > maxInlineControlBytesV2 || strings.Contains(writer.Body.String(), "private-payload") {
				t.Fatalf("invalid result field=%s status=%d response bytes=%d", field, writer.Code, writer.Body.Len())
			}
		})
	}
}

func TestInlineV2BodyReadDeadlineIsRequiredAndTightened(t *testing.T) {
	service, store := inlineFixtureV2(t)
	request := inlineBodyRequestV2(t, []byte("content"))
	reader := &inlineClaimReaderV2{store: store, reader: strings.NewReader("content")}
	request.Body = reader
	unsupported := &inlineWriteOnlyWriterV2{ResponseRecorder: httptest.NewRecorder()}
	inlineExpectAbortV2(t, func() { HandlerInlineV2(service).ServeHTTP(unsupported, request) })
	if unsupported.Body.Len() != 0 || unsupported.Flushed || reader.reads != 0 || store.calls != 0 {
		t.Fatalf("unsupported deadline response-bytes/flushed/calls/reads=%d/%v/%d/%d", unsupported.Body.Len(), unsupported.Flushed, store.calls, reader.reads)
	}
	deadline := time.Now().Add(time.Hour) // a failure bound, not an authorization clock.
	metadata := attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: deadline}
	request = inlineBodyRequestV2(t, []byte("content"))
	request.Header.Set(InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(inlineMarshalV2(t, metadata)))
	parentDeadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(request.Context(), parentDeadline)
	t.Cleanup(cancel)
	request = request.WithContext(ctx)
	writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
	HandlerInlineV2(service).ServeHTTP(writer, request)
	if writer.Code != http.StatusOK || len(writer.deadlines) == 0 || !writer.deadlines[len(writer.deadlines)-1].Equal(parentDeadline) || !store.deadline.Equal(parentDeadline) {
		t.Fatalf("tightened read deadline status=%d physical=%v forwarded=%v want=%v", writer.Code, writer.deadlines, store.deadline, parentDeadline)
	}
}

func TestInlineV2NeverReturnsBodyAuthorityAfterBurn(t *testing.T) {
	service, store := inlineFixtureV2(t)
	store.result.Stage = attachedworkeroutput.InlinePreparedV2
	store.result.CanonicalDigest = ""
	store.result.Capability = strings.Repeat("c", 64)
	request := attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: inlineAuthV2(), BodySendBurned: true}
	result, err := service.Reconcile(context.Background(), []byte("bound-bearer"), request)
	if !errors.Is(err, ErrUnavailable) || result.Capability != "" {
		t.Fatalf("burned reconcile result=%+v err=%v", result, err)
	}
	body := attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0)}
	result, err = service.Submit(context.Background(), []byte("bound-bearer"), body, 1, strings.NewReader("x"))
	if !errors.Is(err, ErrUnavailable) || result.Capability != "" {
		t.Fatalf("submit capability result=%+v err=%v", result, err)
	}
}

func TestInlineV2BodyMetadataAndDeadlineFailBeforeRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*attachedworkeroutput.AttachedWorkerInlineBodyV2)
	}{
		{"zero-expiry", func(r *attachedworkeroutput.AttachedWorkerInlineBodyV2) { r.ExpiresAt = time.Time{} }},
		{"expired", func(r *attachedworkeroutput.AttachedWorkerInlineBodyV2) { r.ExpiresAt = time.Unix(1, 0) }},
		{"v1", func(r *attachedworkeroutput.AttachedWorkerInlineBodyV2) { r.Version = 1 }},
		{"supplied-digest", func(r *attachedworkeroutput.AttachedWorkerInlineBodyV2) {
			r.Authorization.PresentedSecretDigest = "caller-secret"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			input := attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0)}
			tc.mutate(&input)
			request := inlineBodyRequestV2(t, []byte("content"))
			request.Header.Set(InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(inlineMarshalV2(t, input)))
			reader := &inlineClaimReaderV2{store: store, reader: strings.NewReader("content")}
			request.Body = reader
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			if writer.Code != http.StatusBadRequest || store.calls != 0 || reader.reads != 0 {
				t.Fatalf("metadata status/calls/reads=%d/%d/%d want=400/0/0", writer.Code, store.calls, reader.reads)
			}
		})
	}
	service, store := inlineFixtureV2(t)
	request := inlineBodyRequestV2(t, []byte("content"))
	expiry := time.Now().Add(10 * time.Second)
	input := attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: expiry}
	request.Header.Set(InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(inlineMarshalV2(t, input)))
	writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
	HandlerInlineV2(service).ServeHTTP(writer, request)
	if writer.Code != http.StatusOK || store.calls != 1 || len(writer.deadlines) == 0 || !writer.deadlines[len(writer.deadlines)-1].Equal(expiry) || !store.deadline.Equal(expiry) {
		t.Fatalf("original expiry status=%d calls=%d physical=%v forwarded=%v want=%v", writer.Code, store.calls, writer.deadlines, store.deadline, expiry)
	}
}

func TestInlineV2UnsupportedReadDeadlineNeverReadsAnyEndpoint(t *testing.T) {
	for _, path := range []string{PathInlinePrepareV2, PathInlineReconcileV2, PathInlineBodyV2} {
		for _, rejected := range []bool{false, true} {
			t.Run(path+"/rejected-"+strconv.FormatBool(rejected), func(t *testing.T) {
				service, store := inlineFixtureV2(t)
				var input any = attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()}
				if path == PathInlineReconcileV2 {
					input = attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: inlineAuthV2()}
				}
				raw := inlineMarshalV2(t, input)
				request := inlineControlRequestV2(t, path, raw)
				if path == PathInlineBodyV2 {
					raw = []byte("content")
					request = inlineBodyRequestV2(t, raw)
				}
				reader := &inlineClaimReaderV2{store: store, reader: bytes.NewReader(raw)}
				request.Body = reader
				recorder := httptest.NewRecorder()
				var writer http.ResponseWriter = &inlineWriteOnlyWriterV2{ResponseRecorder: recorder}
				if rejected {
					writer = &inlineDeadlineWriterV2{ResponseRecorder: recorder, err: errors.New("private deadline error")}
				}
				inlineExpectAbortV2(t, func() { HandlerInlineV2(service).ServeHTTP(writer, request) })
				if recorder.Body.Len() != 0 || recorder.Flushed || store.calls != 0 || reader.reads != 0 {
					t.Fatalf("unsupported deadline path=%s response-bytes/flushed/calls/reads=%d/%v/%d/%d want=0/false/0/0", path, recorder.Body.Len(), recorder.Flushed, store.calls, reader.reads)
				}
			})
		}
	}
}

func TestInlineV2ClearsOwnedBearerAndResponseBuffers(t *testing.T) {
	service, store := inlineFixtureV2(t)
	binder := &inlineRetainingBinderV2{base: store.binder}
	service.binder = binder
	raw := inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()})
	original := bytes.Clone(raw)
	writer := &inlinePayloadWriterV2{inlineDeadlineWriterV2: &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}}
	HandlerInlineV2(service).ServeHTTP(writer, inlineControlRequestV2(t, PathInlinePrepareV2, raw))
	if writer.Code != http.StatusOK || len(binder.borrowed) == 0 || len(writer.borrowed) == 0 {
		t.Fatalf("buffer lifecycle status=%d borrowed bearer/response bytes=%d/%d", writer.Code, len(binder.borrowed), len(writer.borrowed))
	}
	if !bytes.Equal(binder.borrowed, make([]byte, len(binder.borrowed))) || !bytes.Equal(writer.borrowed, make([]byte, len(writer.borrowed))) {
		t.Error("owned bearer or serialized response buffer remained populated after handler return")
	}
	if !bytes.Equal(raw, original) {
		t.Error("handler changed caller-owned request source rather than only its own read buffer")
	}
	if !json.Valid(writer.Body.Bytes()) {
		t.Error("clearing response buffer damaged the delivered response copy")
	}
}

func TestInlineV2HeaderLimitIncludesAllHeaders(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(strconv.Itoa(extra), func(t *testing.T) {
			service, store := inlineFixtureV2(t)
			raw := inlineMarshalV2(t, attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), ExpiresAt: time.Unix(2_000_000_000, 0).UTC()})
			request := inlineControlRequestV2(t, PathInlinePrepareV2, raw)
			used := len("Host: ") + len(request.Host) + 4
			for key, values := range request.Header {
				for _, value := range values {
					used += len(key) + len(value) + 4
				}
			}
			request.Header.Set("X-Padding", strings.Repeat("p", maxInlineHeaderBytesV2-used-len("X-Padding")-4+extra))
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			want := http.StatusOK
			if extra != 0 {
				want = http.StatusBadRequest
			}
			if writer.Code != want || (extra != 0 && store.calls != 0) {
				t.Fatalf("header ceiling extra=%d status=%d calls=%d want=%d", extra, writer.Code, store.calls, want)
			}
		})
	}
}
