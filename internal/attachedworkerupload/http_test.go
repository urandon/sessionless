package attachedworkerupload

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeruploadwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type controlReadProbe struct{ reads, closes int }

func (p *controlReadProbe) Read([]byte) (int, error) { p.reads++; return 0, io.EOF }
func (p *controlReadProbe) Close() error             { p.closes++; return nil }

func controlHTTPRequest(t *testing.T, bearer, body []byte) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, PathV1, bytes.NewReader(body)).WithContext(t.Context())
	r.Header.Set("Authorization", "Bearer "+string(bearer))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestAttemptOutputUploadHTTPAuthenticatesAndReturnsClosedCustody(t *testing.T) {
	service, store, bearer, request := controlFixture(t)
	body, err := attachedworkeruploadwire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	r := controlHTTPRequest(t, bearer, body)
	w := httptest.NewRecorder()
	Handler(service).ServeHTTP(w, r)
	reply, err := attachedworkeruploadwire.DecodeReply(w.Body.Bytes())
	if err != nil || w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" ||
		reply.Status != ports.AttachedWorkerExecutionApplied || reply.Capability != nil || store.calls != 1 || store.ctx != r.Context() {
		t.Fatalf("closed HTTP custody response: status=%d err=%v storecalls=%d", w.Code, err, store.calls)
	}
	if bytes.Contains(w.Body.Bytes(), bearer) || bytes.Contains(w.Body.Bytes(), []byte("presented_secret_digest")) ||
		bytes.Contains(w.Body.Bytes(), []byte("grant_digest")) || bytes.Contains(w.Body.Bytes(), []byte("source")) {
		t.Fatal("HTTP custody exposed bearer/private grant or source evidence")
	}
}

func TestAttemptOutputUploadHTTPHeaderGuardsBeforeBody(t *testing.T) {
	for name, mutate := range map[string]func(*http.Request){
		"wrong method":        func(r *http.Request) { r.Method = http.MethodGet },
		"foreign path":        func(r *http.Request) { r.URL.Path = "/other" },
		"raw path":            func(r *http.Request) { r.URL.RawPath = PathV1 },
		"query":               func(r *http.Request) { r.URL.RawQuery = "x=1" },
		"force query":         func(r *http.Request) { r.URL.ForceQuery = true },
		"duplicate type":      func(r *http.Request) { r.Header.Add("Content-Type", "application/json") },
		"duplicate auth":      func(r *http.Request) { r.Header.Add("Authorization", r.Header.Get("Authorization")) },
		"encoding":            func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") },
		"transfer":            func(r *http.Request) { r.TransferEncoding = []string{"chunked"} },
		"over content length": func(r *http.Request) { r.ContentLength = attachedworkeruploadwire.AdmitMaxBytes + 1 },
		"over bearer": func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", attachedworkertransport.ConnectionBearerMaxBytes+1))
		},
		"empty bearer": func(r *http.Request) { r.Header.Set("Authorization", "Bearer ") },
	} {
		t.Run(name, func(t *testing.T) {
			service, store, bearer, _ := controlFixture(t)
			r := controlHTTPRequest(t, bearer, []byte("x"))
			probe := &controlReadProbe{}
			r.Body = probe
			mutate(r)
			w := httptest.NewRecorder()
			Handler(service).ServeHTTP(w, r)
			if w.Code == http.StatusOK || store.calls != 0 || probe.reads != 0 || probe.closes != 1 {
				t.Fatalf("header guard %s read body/called store: status=%d reads=%d closes=%d calls=%d", name, w.Code, probe.reads, probe.closes, store.calls)
			}
		})
	}
}

func TestAttemptOutputUploadHTTPClosedSyntaxBeforeStore(t *testing.T) {
	service, store, bearer, request := controlFixture(t)
	body, err := attachedworkeruploadwire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	for name, malformed := range map[string][]byte{
		"unknown":           bytes.Replace(body, []byte(`"version":1,`), []byte(`"unknown":1,"version":1,`), 1),
		"duplicate":         bytes.Replace(body, []byte(`"version":1,`), []byte(`"version":1,"version":1,`), 1),
		"trailing":          append(bytes.Clone(body), []byte(`{}`)...),
		"over arena":        bytes.Repeat([]byte("x"), attachedworkeruploadwire.AdmitMaxBytes+1),
		"unbounded nesting": []byte(strings.Repeat("[", 2000) + strings.Repeat("]", 2000)),
	} {
		t.Run(name, func(t *testing.T) {
			r := controlHTTPRequest(t, bearer, malformed)
			// Unknown length forces the fixed arena/sentinel path rather than
			// rejecting solely from the attacker-supplied length header.
			r.ContentLength = -1
			w := httptest.NewRecorder()
			Handler(service).ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest || store.calls != 0 {
				t.Fatalf("malformed %s reached store: status=%d calls=%d", name, w.Code, store.calls)
			}
		})
	}
}

func TestAttemptOutputUploadHTTPExpiredOrAmbiguousDoesNotGrant(t *testing.T) {
	service, store, bearer, request := controlFixture(t)
	body, err := attachedworkeruploadwire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	store.result.Status = ports.AttachedWorkerExecutionExpired
	w := httptest.NewRecorder()
	Handler(service).ServeHTTP(w, controlHTTPRequest(t, bearer, body))
	if w.Code != http.StatusOK || w.Body.String() != `{"status":"denied"}` {
		t.Fatalf("expiry exposed a settlement/capability: status=%d", w.Code)
	}
	store.err = ErrUnavailable
	w = httptest.NewRecorder()
	Handler(service).ServeHTTP(w, controlHTTPRequest(t, bearer, body))
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "control-") {
		t.Fatalf("ambiguous backend emitted usable/private metadata: status=%d", w.Code)
	}
}
