package attachedworkerupload

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkeruploadwire"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Every listener, token and store result is test-owned structural data. The
// real Handler/Service/binder paths below do NOT prove SQL, Source, provenance,
// native fit, original cutoff or an authorized serving/profile constructor.
func uploadClientBinding(r attachedworkeruploadwire.Request) attachedworkersession.ConnectionBindingV1 {
	a := r.Authorization
	return attachedworkersession.ConnectionBindingV1{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID, ConnectionID: a.ConnectionID,
		EnrollmentGeneration: a.EnrollmentGeneration, ConnectionGeneration: a.ConnectionGeneration, ProtocolVersion: attachedworkerprotocol.ProtocolVersionV1,
		CapabilityDigest: a.CapabilityDigest, AuthenticationExpires: time.UnixMicro(a.LeaseExpiresAtUnixMicro).Add(time.Hour)}
}

func uploadClientLoopback(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	s := httptest.NewUnstartedServer(handler)
	s.EnableHTTP2 = true // The private client must still negotiate only H1.
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.Config.ReadHeaderTimeout = 5 * time.Second
	s.StartTLS()
	t.Cleanup(s.Close)
	tr := s.Client().Transport.(*http.Transport).Clone()
	tr.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	tr.TLSClientConfig.MinVersion = tls.VersionTLS12
	tr.DisableCompression = true
	tr.TLSHandshakeTimeout, tr.ResponseHeaderTimeout = 5*time.Second, 5*time.Second
	tr.MaxResponseHeaderBytes = 4096 // Test failure bound, not a native budget.
	t.Cleanup(tr.CloseIdleConnections)
	return s, &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func newUploadClient(t *testing.T, endpoint string, hc *http.Client, bearer []byte, original attachedworkeruploadwire.Request, head domain.AttachedWorkerAttemptV1) *Client {
	t.Helper()
	c, err := NewClient(endpoint, hc, uploadClientBinding(original), bearer, original, head)
	if err != nil {
		t.Fatal("construct bounded client:", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func uploadClientContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func uploadReplyBytes(t *testing.T, r attachedworkeruploadwire.Reply) []byte {
	t.Helper()
	b, err := attachedworkeruploadwire.EncodeReply(r)
	if err != nil {
		t.Fatal("encode synthetic control reply:", err)
	}
	return b
}

func uploadReplyHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
}

func uploadWireReply(s *controlStoreFixture) attachedworkeruploadwire.Reply {
	p := s.result.Plan
	r := attachedworkeruploadwire.Reply{Status: s.result.Status, AcceptedAttempt: s.result.AcceptedAttempt,
		PlanDigest: p.PlanDigest, CreatedAt: p.Identity.CreatedAt, CheckedAt: s.result.CheckedAt, ExpiresAt: p.Identity.ExpiresAt,
		Ordinals: make([]attachedworkeruploadwire.Ordinal, len(p.Ordinals))}
	for i, o := range p.Ordinals {
		r.Ordinals[i] = attachedworkeruploadwire.Ordinal{Ordinal: uint32(i), State: o.State}
	}
	return r
}

func uploadDecodeRequest(t *testing.T, r *http.Request) attachedworkeruploadwire.Request {
	t.Helper()
	var arena [attachedworkeruploadwire.RequestMaxBytes + 1]byte
	n, err := io.ReadFull(r.Body, arena[:])
	if err != io.ErrUnexpectedEOF || n == 0 || n > attachedworkeruploadwire.RequestMaxBytes {
		t.Errorf("synthetic peer bounded request n=%d err=%v", n, err)
		return attachedworkeruploadwire.Request{}
	}
	v, err := attachedworkeruploadwire.DecodeRequest(arena[:n])
	if err != nil {
		t.Error("synthetic peer request decode:", err)
	}
	return v
}

func uploadClientWait(t *testing.T, ctx context.Context, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		select {
		case <-done:
		default:
			t.Fatalf("%s did not complete within test failure bound", what)
		}
	}
}

func TestAttemptOutputUploadClientFiniteScheduleActualHandler(t *testing.T) {
	service, store, bearer, original := controlFixture(t)
	second := original.Artifacts[0]
	second.Ordinal, second.Name = 1, "second.txt"
	original.Artifacts = append(original.Artifacts, second)
	plan, err := attachedworkeroutput.NewAttemptOutputUploadPlanV1(store.result.Plan.Identity, original.Artifacts)
	if err != nil {
		t.Fatal(err)
	}
	store.result.Plan = plan
	head := store.result.AcceptedAttempt
	var calls atomic.Int32
	var mu sync.Mutex
	peers := map[string]bool{}
	handler := Handler(service)
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		mu.Lock()
		defer mu.Unlock()
		peers[r.RemoteAddr] = true
		input := uploadDecodeRequest(t, r)
		if r.Method != http.MethodPost || r.URL.Path != PathV1 || r.ProtoMajor != 1 || !r.Close || r.Header.Get("Accept-Encoding") != "identity" ||
			r.Header.Get("Authorization") != "Bearer "+string(bearer) {
			t.Error("fixed H1/private headers changed")
		}
		encoded, err := attachedworkeruploadwire.EncodeRequest(input)
		if err != nil {
			t.Error(err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(encoded))
		if input.Operation == attachedworkeruploadwire.Claim {
			store.result.Plan, err = store.result.Plan.SpendOrdinal(input.Ordinal, strings.Repeat("b", 64), store.result.CheckedAt)
			if err != nil {
				t.Error("synthetic durable spend:", err)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, head)
	ctx := uploadClientContext(t)
	for _, step := range []struct {
		op      attachedworkeruploadwire.Operation
		ordinal uint32
	}{
		{attachedworkeruploadwire.Admit, 0}, {attachedworkeruploadwire.Claim, 0}, {attachedworkeruploadwire.Reconcile, 0},
		{attachedworkeruploadwire.Claim, 1}, {attachedworkeruploadwire.Reconcile, 0},
	} {
		got, err := c.Exchange(ctx, step.op, step.ordinal, original.CallerDeadline)
		if err != nil || got.Reply.Capability != nil || got.Reply.PlanDigest != plan.PlanDigest || got.SentAt.IsZero() || got.ReceivedAt.Before(got.SentAt) {
			t.Fatalf("finite step op=%s ordinal=%d err=%v", step.op, step.ordinal, err)
		}
	}
	for _, step := range []struct {
		op      attachedworkeruploadwire.Operation
		ordinal uint32
	}{
		{attachedworkeruploadwire.Admit, 0}, {attachedworkeruploadwire.Claim, 0}, {attachedworkeruploadwire.Claim, 1}, {attachedworkeruploadwire.Reconcile, 0},
	} {
		if _, err := c.Exchange(ctx, step.op, step.ordinal, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
			t.Errorf("schedule reset op=%s ordinal=%d err=%v", step.op, step.ordinal, err)
		}
	}
	mu.Lock()
	peerCount := len(peers)
	mu.Unlock()
	if calls.Load() != 5 || peerCount != 5 {
		t.Fatalf("calls=%d distinct private H1 peers=%d want5", calls.Load(), peerCount)
	}
}

func TestAttemptOutputUploadClientLostAdmitAndClaimDoNotReplay(t *testing.T) {
	_, store, bearer, original := controlFixture(t)
	head, reply := store.result.AcceptedAttempt, uploadWireReply(store)
	var calls atomic.Int32
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		input := uploadDecodeRequest(t, r)
		if input.Operation == attachedworkeruploadwire.Admit || input.Operation == attachedworkeruploadwire.Claim {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close() // Represents an ambiguous reply, NOT no dispatch.
			return
		}
		if input.Authorization != original.Authorization || input.Nonce != original.Nonce || input.ProducerDigest != original.ProducerDigest || input.FinalDigest != original.FinalDigest {
			t.Error("lost-admit reconcile identity changed")
		}
		uploadReplyHeaders(w)
		_, _ = w.Write(uploadReplyBytes(t, reply))
	}))
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, head)
	ctx := uploadClientContext(t)
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline); !errors.Is(err, ErrUnavailable) {
		t.Fatal("lost admit:", err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("lost admit retried:", err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Claim, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("claim before reconstructed original plan:", err)
	}
	got, err := c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline)
	if err != nil || got.Reply.PlanDigest != reply.PlanDigest || c.plan == nil {
		t.Fatal("lost-admit recovery failed:", err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Claim, 0, original.CallerDeadline); !errors.Is(err, ErrUnavailable) {
		t.Fatal("lost claim:", err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Claim, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("lost claim refunded:", err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline); err != nil {
		t.Fatal("last read-only reconcile:", err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("failed slots reopened:", err)
	}
	if calls.Load() != 4 || !c.claimed[0] {
		t.Fatalf("physical calls=%d spent=%v want4,true", calls.Load(), c.claimed[0])
	}
}

func TestAttemptOutputUploadClientFullHeadSubstitutionAndNoRollback(t *testing.T) {
	for _, mode := range []string{"reservation", "progress", "created", "rollback", "terminal_reversal", "older_check"} {
		t.Run(mode, func(t *testing.T) {
			_, store, bearer, original := controlFixture(t)
			head, reply := store.result.AcceptedAttempt, uploadWireReply(store)
			var calls atomic.Int32
			s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				_ = uploadDecodeRequest(t, r)
				v := reply
				v.Ordinals = append([]attachedworkeruploadwire.Ordinal(nil), reply.Ordinals...)
				if n > 1 {
					switch mode {
					case "reservation":
						v.AcceptedAttempt.ReservationID = "foreign-reservation"
					case "progress":
						v.AcceptedAttempt.ProgressSequence = 0
					case "created":
						v.AcceptedAttempt.CreatedAt = v.AcceptedAttempt.CreatedAt.Add(-time.Microsecond)
					case "rollback":
						if n == 2 {
							v.Ordinals[0].State = attachedworkeroutput.UploadOrdinalWritePendingV1
						}
					case "terminal_reversal":
						if n == 2 {
							v.Ordinals[0].State = attachedworkeroutput.UploadOrdinalNotDispatchedV1
						} else {
							v.Ordinals[0].State = attachedworkeroutput.UploadOrdinalWritePendingV1
						}
					case "older_check":
						v.CheckedAt = v.CheckedAt.Add(-time.Microsecond)
					}
				}
				uploadReplyHeaders(w)
				_, _ = w.Write(uploadReplyBytes(t, v))
			}))
			c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, head)
			ctx := uploadClientContext(t)
			if _, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline); err != nil {
				t.Fatal(err)
			}
			_, err := c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline)
			if mode == "rollback" || mode == "terminal_reversal" {
				if err != nil {
					t.Fatal("valid forward observation:", err)
				}
				_, err = c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline)
			}
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("mode=%s substituted/backward reply admitted err=%v", mode, err)
			}
		})
	}
}

func TestAttemptOutputUploadClientFramingAndClosedErrors(t *testing.T) {
	for _, mode := range []string{"denied", "pending", "status", "redirect", "type", "duplicate_type", "encoding", "duplicate_encoding", "cache", "duplicate_cache", "oversize", "unknown_wire", "trailing", "short_length", "header_overflow", "trailer", "noncap_class"} {
		t.Run(mode, func(t *testing.T) {
			_, store, bearer, original := controlFixture(t)
			reply := uploadWireReply(store)
			good := uploadReplyBytes(t, reply)
			var calls atomic.Int32
			s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = uploadDecodeRequest(t, r)
				uploadReplyHeaders(w)
				body := good
				switch mode {
				case "denied":
					body = []byte(`{"status":"denied"}`)
				case "pending":
					body = []byte(`{"status":"pending"}`)
				case "status":
					w.WriteHeader(http.StatusForbidden)
				case "redirect":
					w.Header().Set("Location", PathV1)
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "type":
					w.Header().Set("Content-Type", "text/plain")
				case "duplicate_type":
					w.Header().Add("Content-Type", "application/json")
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				case "duplicate_encoding":
					w.Header().Add("Content-Encoding", "identity")
					w.Header().Add("Content-Encoding", "identity")
				case "cache":
					w.Header().Set("Cache-Control", "public")
				case "duplicate_cache":
					w.Header().Add("Cache-Control", "no-store")
				case "oversize":
					body = bytes.Repeat([]byte{'x'}, attachedworkeruploadwire.ReplyMaxBytes+1)
				case "unknown_wire":
					body = bytes.Replace(good, []byte(`"status":"applied",`), []byte(`"status":"applied","unknown":true,`), 1)
				case "trailing":
					body = append(bytes.Clone(good), ' ')
				case "short_length":
					w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
				case "header_overflow":
					w.Header().Set("X-Padding", strings.Repeat("x", 8192))
				case "trailer":
					w.Header().Set("Trailer", "X-Private")
					defer w.Header().Set("X-Private", "secret")
				case "noncap_class":
					body = bytes.Repeat([]byte{'x'}, attachedworkeruploadwire.ReplyWithoutCapabilityMaxBytes+1)
				}
				_, _ = w.Write(body)
			}))
			c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
			got, err := c.Exchange(uploadClientContext(t), attachedworkeruploadwire.Admit, 0, original.CallerDeadline)
			if mode == "pending" {
				if err != nil || got.Reply.Status != attachedworkeruploadwire.Pending || got.Reply.Capability != nil || c.plan != nil {
					t.Fatal("pending became authority:", err)
				}
			} else {
				want := ErrUnavailable
				if mode == "denied" {
					want = ErrUnauthorized
				}
				if !errors.Is(err, want) || err.Error() != want.Error() || got.Reply.Status != "" || c.plan != nil {
					t.Fatalf("mode=%s err=%v leaked/retained metadata=%v", mode, err, got.Reply.Status != "" || c.plan != nil)
				}
			}
			if calls.Load() != 1 || !c.admitted {
				t.Fatalf("mode=%s calls=%d lost spent admit=%v", mode, calls.Load(), c.admitted)
			}
		})
	}
}

func TestAttemptOutputUploadClientCapabilityBoundsAreMetadataOnly(t *testing.T) {
	_, store, bearer, original := controlFixture(t)
	reply := uploadWireReply(store)
	var calls atomic.Int32
	capExpiry := reply.CheckedAt.Add(time.Second)
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		input := uploadDecodeRequest(t, r)
		v := reply
		v.Ordinals = append([]attachedworkeruploadwire.Ordinal(nil), reply.Ordinals...)
		if input.Operation == attachedworkeruploadwire.Claim {
			v.Ordinals[0].State = attachedworkeroutput.UploadOrdinalWritePendingV1
			metadata := original.Artifacts[0]
			hash, err := hex.DecodeString(metadata.SHA256)
			if err != nil {
				t.Error(err)
				return
			}
			base := "https://storage.invalid/" + store.result.Plan.Ordinals[0].Key + "?signature="
			v.Capability = &attachedworkeruploadwire.Capability{Version: 1, Method: http.MethodPut, URL: base + strings.Repeat("a", attachedworkeruploadwire.URLMaxBytes-len(base)),
				Headers: attachedworkeruploadwire.SignedHeaders{ContentLength: "0", ContentType: metadata.MediaType, ContentMD5: metadata.ContentMD5, IfNoneMatch: "*", ChecksumSHA256: base64.StdEncoding.EncodeToString(hash)}, ExpiresAt: capExpiry}
		}
		uploadReplyHeaders(w)
		_, _ = w.Write(uploadReplyBytes(t, v)) // >2KiB capability uses actual H1 chunking.
	}))
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
	ctx := uploadClientContext(t)
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline); err != nil {
		t.Fatal(err)
	}
	got, err := c.Exchange(ctx, attachedworkeruploadwire.Claim, 0, original.CallerDeadline)
	if err != nil || got.Reply.Capability == nil || len(got.Reply.Capability.URL) != attachedworkeruploadwire.URLMaxBytes ||
		!got.Reply.Capability.ExpiresAt.Equal(capExpiry) || !got.Reply.ExpiresAt.Equal(reply.ExpiresAt) || !capExpiry.Before(got.Reply.ExpiresAt) {
		t.Fatal("capability metadata bound or narrower expiry changed:", err)
	}
	if calls.Load() != 2 {
		t.Fatal("metadata client issued a body or replay")
	}
}

func TestAttemptOutputUploadClientOwnedConfigAndOriginalAliases(t *testing.T) {
	service, store, bearer, original := controlFixture(t)
	expected := bytes.Clone(bearer)
	originalName := original.Artifacts[0].Name
	var calls atomic.Int32
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+string(expected) {
			t.Error("caller token mutation reached private header")
		}
		Handler(service).ServeHTTP(w, r)
	}))
	tr := hc.Transport.(*http.Transport)
	head := store.result.AcceptedAttempt
	zone := time.FixedZone("synthetic-original-location", 3*60*60)
	head.CreatedAt, head.UpdatedAt, head.LeaseExpiresAt = head.CreatedAt.In(zone), head.UpdatedAt.In(zone), head.LeaseExpiresAt.In(zone)
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, head)
	clear(bearer)
	original.Artifacts[0].Name = "mutated.txt"
	hc.Timeout = time.Nanosecond
	tr.TLSClientConfig.InsecureSkipVerify = true
	tr.Proxy = http.ProxyFromEnvironment
	if c.original.Artifacts[0].Name != originalName || c.client.Timeout != 10*time.Second || c.transport == tr || c.transport.Proxy != nil || c.transport.TLSClientConfig.InsecureSkipVerify ||
		c.transport.TLSClientConfig.RootCAs == tr.TLSClientConfig.RootCAs || !c.transport.DisableKeepAlives || !c.transport.Protocols.HTTP1() || c.transport.Protocols.HTTP2() {
		t.Fatal("caller mutation or pooled/HTTP2 config reached private client")
	}
	if _, err := c.Exchange(uploadClientContext(t), attachedworkeruploadwire.Admit, 0, original.CallerDeadline); err != nil || calls.Load() != 1 {
		t.Fatal("owned clone failed actual handler:", err)
	}
	for _, value := range []any{c, ExchangeResult{Reply: uploadWireReply(store)}} {
		b, err := json.Marshal(value)
		if err != nil || string(b) != `"[REDACTED]"` || strings.Contains(fmt.Sprintf("%#v", value), string(expected)) || strings.Contains(fmt.Sprint(value), "control-tenant") {
			t.Fatal("private diagnostics leaked")
		}
	}
}

func TestAttemptOutputUploadClientRejectsInvalidConstructorAndDeadlineExpansion(t *testing.T) {
	_, store, bearer, original := controlFixture(t)
	var calls atomic.Int32
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = uploadDecodeRequest(t, r)
		uploadReplyHeaders(w)
		_, _ = w.Write(uploadReplyBytes(t, uploadWireReply(store)))
	}))
	for _, endpoint := range []string{"http://control.invalid" + PathV1, s.URL + PathV1 + "?", s.URL + PathV1 + "?x=1", s.URL + PathV1 + "#x", s.URL + "/attached-worker/v1/%6futput-upload", "https://user:pass@control.invalid" + PathV1, "https://control.invalid:" + PathV1, "%"} {
		if _, err := NewClient(endpoint, hc, uploadClientBinding(original), bearer, original, store.result.AcceptedAttempt); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid endpoint err=%v", err)
		}
	}
	for _, token := range [][]byte{nil, []byte("invalid"), bytes.Repeat([]byte{'x'}, 1025)} {
		if _, err := NewClient(s.URL+PathV1, hc, uploadClientBinding(original), token, original, store.result.AcceptedAttempt); !errors.Is(err, ErrInvalid) {
			t.Error("invalid/unbounded token admitted")
		}
	}
	for _, bad := range []*http.Client{nil, {Timeout: time.Second}, {Transport: hc.Transport, Timeout: 0}, {Transport: uploadClientRoundTrip(func(*http.Request) (*http.Response, error) { return nil, errors.New("private diagnostic") }), Timeout: time.Second}} {
		if _, err := NewClient(s.URL+PathV1, bad, uploadClientBinding(original), bearer, original, store.result.AcceptedAttempt); !errors.Is(err, ErrInvalid) || err.Error() != ErrInvalid.Error() {
			t.Error("unsafe client/config exposed raw error or was accepted")
		}
	}
	for _, edit := range []func(*attachedworkeruploadwire.Request){
		func(r *attachedworkeruploadwire.Request) { r.Operation = attachedworkeruploadwire.Reconcile },
		func(r *attachedworkeruploadwire.Request) {
			r.Artifacts = append(r.Artifacts, r.Artifacts[0], r.Artifacts[0])
		},
		func(r *attachedworkeruploadwire.Request) {
			r.CallerDeadline.RemainingNanoseconds = uint64(15*time.Second) + 1
		},
	} {
		r := original
		edit(&r)
		if _, err := NewClient(s.URL+PathV1, hc, uploadClientBinding(original), bearer, r, store.result.AcceptedAttempt); !errors.Is(err, ErrInvalid) {
			t.Error("invalid original admission accepted")
		}
	}
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
	if _, err := c.Exchange(t.Context(), attachedworkeruploadwire.Admit, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing original deadline admitted:", err)
	}
	ctx := uploadClientContext(t)
	tight := original.CallerDeadline
	tight.RemainingNanoseconds--
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, tight); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("W expanded:", err)
	}
	later, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if _, err := c.Exchange(later, attachedworkeruploadwire.Reconcile, 0, tight); !errors.Is(err, ErrInvalid) {
		t.Fatal("context deadline renewed:", err)
	}
	if calls.Load() != 1 {
		t.Fatal("invalid constructor/deadline performed HTTP")
	}
}

type uploadClientRoundTrip func(*http.Request) (*http.Response, error)

func (f uploadClientRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAttemptOutputUploadClientRejectsNoncanonicalResponseFraming(t *testing.T) {
	for _, mode := range []string{"http2", "http10", "duplicate_length", "negative_length", "unknown_transfer", "conflicting_transfer", "uncompressed"} {
		t.Run(mode, func(t *testing.T) {
			_, store, bearer, original := controlFixture(t)
			good := uploadReplyBytes(t, uploadWireReply(store))
			s, hc := uploadClientLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("test-only response spy contacted network") }))
			c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
			var calls int
			// Rejected by the constructor: a finite test-only response seam proves
			// the decoded HTTP boundary, not production TLS/backend authority.
			c.client.Transport = uploadClientRoundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				v := &http.Response{StatusCode: http.StatusOK, ProtoMajor: 1, ProtoMinor: 1, ContentLength: int64(len(good)),
					Header: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}, Body: io.NopCloser(bytes.NewReader(good)), Request: r}
				switch mode {
				case "http2":
					v.ProtoMajor, v.ProtoMinor = 2, 0
				case "http10":
					v.ProtoMinor = 0
				case "duplicate_length":
					v.Header["Content-Length"] = []string{strconv.Itoa(len(good)), strconv.Itoa(len(good))}
				case "negative_length":
					v.ContentLength = -2
				case "unknown_transfer":
					v.TransferEncoding, v.ContentLength = []string{"gzip"}, -1
				case "conflicting_transfer":
					v.TransferEncoding = []string{"chunked"}
				case "uncompressed":
					v.Uncompressed = true
				}
				return v, nil
			})
			got, err := c.Exchange(uploadClientContext(t), attachedworkeruploadwire.Admit, 0, original.CallerDeadline)
			if !errors.Is(err, ErrUnavailable) || got.Reply.Status != "" || c.plan != nil || calls != 1 {
				t.Fatalf("mode=%s response framing accepted: err=%v calls=%d", mode, err, calls)
			}
		})
	}
}

type uploadClientHeldClose struct {
	*bytes.Reader
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *uploadClientHeldClose) Close() error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

func TestAttemptOutputUploadClientCloseDuringBodyCleanupCannotPublishReply(t *testing.T) {
	_, store, bearer, original := controlFixture(t)
	good := uploadReplyBytes(t, uploadWireReply(store))
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("test-only held body contacted network") }))
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
	entered, release, callDone, closeDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	closeStarted := false
	ctx := uploadClientContext(t)
	result := make(chan error, 1)
	// A held synthetic Body.Close is intentionally NOT a native transport or
	// quiescence proof. It causally exposes the final publication boundary.
	c.client.Transport = uploadClientRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, ProtoMajor: 1, ProtoMinor: 1, ContentLength: int64(len(good)),
			Header: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}},
			Body:   &uploadClientHeldClose{Reader: bytes.NewReader(good), entered: entered, release: release}, Request: r}, nil
	})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = c.Close()
		failureCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		uploadClientWait(t, failureCtx, callDone, "held body call cleanup")
		if closeStarted {
			uploadClientWait(t, failureCtx, closeDone, "held body Close cleanup")
		}
	})
	go func() {
		defer close(callDone)
		v, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline)
		if v.Reply.Status != "" {
			err = errors.New("reply escaped retirement during body cleanup")
		}
		result <- err
	}()
	uploadClientWait(t, ctx, entered, "body Close boundary")
	closeStarted = true
	go func() { defer close(closeDone); _ = c.Close() }()
	uploadClientWait(t, ctx, c.life.Done(), "Close retirement linearization")
	select {
	case <-closeDone:
		t.Fatal("Close did not join held body")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	uploadClientWait(t, ctx, closeDone, "held body Close return")
	uploadClientWait(t, ctx, callDone, "held body Exchange return")
	if err := <-result; !errors.Is(err, ErrUnavailable) || c.plan != nil {
		t.Fatal("late decoded reply escaped cleanup/Close:", err)
	}
}

func TestAttemptOutputUploadClientCloseCancelsJoinsAndRejectsConcurrentCalls(t *testing.T) {
	_, store, bearer, original := controlFixture(t)
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("test boundary spy must not contact listener") }))
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
	entered, cancelled, release, callDone, closeDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	closeStarted := false
	ctx := uploadClientContext(t)
	// This TEST-ONLY spy is rejected by the real constructor. It separates
	// cancellation from return so Close's actual owned-call join is observable.
	c.client.Transport = uploadClientRoundTrip(func(r *http.Request) (*http.Response, error) {
		deadline, _ := ctx.Deadline()
		got, ok := r.Context().Deadline()
		if !ok || !got.Equal(deadline) || r.GetBody != nil || !r.Close || r.ContentLength <= 0 {
			t.Error("original request/deadline/replay boundary changed")
		}
		close(entered)
		<-r.Context().Done()
		close(cancelled)
		<-release
		return nil, errors.New("private bearer/backend error must not escape")
	})
	results := make(chan error, 1)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = c.Close()
		failureCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		uploadClientWait(t, failureCtx, callDone, "cleanup call join")
		if closeStarted {
			uploadClientWait(t, failureCtx, closeDone, "cleanup Close join")
		}
	})
	go func() {
		defer close(callDone)
		_, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline)
		results <- err
	}()
	uploadClientWait(t, ctx, entered, "inflight metadata")
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Reconcile, 0, original.CallerDeadline); !errors.Is(err, ErrInvalid) {
		t.Fatal("concurrent exchange admitted:", err)
	}
	closeStarted = true
	go func() { defer close(closeDone); _ = c.Close() }()
	uploadClientWait(t, ctx, cancelled, "Close cancellation")
	select {
	case <-closeDone:
		t.Fatal("Close returned before client-owned call released")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	uploadClientWait(t, ctx, closeDone, "Close return")
	uploadClientWait(t, ctx, callDone, "call return")
	if err := <-results; !errors.Is(err, ErrUnavailable) || err.Error() != ErrUnavailable.Error() {
		t.Fatal("late error escaped closed envelope:", err)
	}
	if c.bearer != nil || c.binding != (attachedworkersession.ConnectionBindingV1{}) || c.plan != nil || len(c.original.Artifacts) != 0 || c.inflight != nil || c.calls != 1 {
		t.Fatal("Close retained owned metadata or reset schedule")
	}
	if _, err := c.Exchange(ctx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed client reopened:", err)
	}
}

func TestAttemptOutputUploadClientCallerCancellationStalledEOF(t *testing.T) {
	_, store, bearer, original := controlFixture(t)
	good := uploadReplyBytes(t, uploadWireReply(store))
	started, handlerDone, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	s, hc := uploadClientLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		_ = uploadDecodeRequest(t, r)
		uploadReplyHeaders(w)
		_, _ = w.Write(good)
		w.(http.Flusher).Flush() // Valid body but no bounded EOF yet.
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	c := newUploadClient(t, s.URL+PathV1, hc, bearer, original, store.result.AcceptedAttempt)
	ctx := uploadClientContext(t)
	callCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan struct{})
	result := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		once.Do(func() { close(release) })
		_ = c.Close()
		failureCtx, failureCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer failureCancel()
		uploadClientWait(t, failureCtx, done, "cancelled call cleanup")
	})
	go func() {
		defer close(done)
		_, err := c.Exchange(callCtx, attachedworkeruploadwire.Admit, 0, original.CallerDeadline)
		result <- err
	}()
	uploadClientWait(t, ctx, started, "stalled response EOF")
	cancel()
	uploadClientWait(t, ctx, done, "original caller cancellation")
	if err := <-result; !errors.Is(err, ErrUnavailable) || c.plan != nil {
		t.Fatal("cancelled partial reply retained authority:", err)
	}
	once.Do(func() { close(release) })
	uploadClientWait(t, ctx, handlerDone, "test-owned peer retirement")
}
