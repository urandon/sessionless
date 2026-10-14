package attachedworkersealedinput

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Every fixture here is synthetic structural data, not source-currentness,
// profile/S/deadline origin, real API authority or native-fit evidence.
func piClientFixture(t *testing.T) (attachedworkersession.ConnectionBindingV1, []byte, pisdkphasewire.Request, pisdkphasewire.Reply) {
	t.Helper()
	created := time.Unix(20, 123456000).UTC()
	h := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", ConnectionID: "connection-1",
		EnrollmentGeneration: 2, ConnectionGeneration: 3, RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1", LeaseID: "lease-1", LeaseGeneration: 4,
		ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64)),
		State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 1, WorkerAttemptSequence: 1, ProgressSequence: 1, CreatedAt: created, UpdatedAt: created, LeaseExpiresAt: created.Add(time.Hour), Revision: 7}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	h.FenceToken = fence
	a := ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID,
		EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID, AttemptSequence: 1,
		LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest,
		CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}
	binding := attachedworkersession.ConnectionBindingV1{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID,
		EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, ProtocolVersion: attachedworkerprotocol.ProtocolVersionV1, CapabilityDigest: h.CapabilityDigest, AuthenticationExpires: created.Add(2 * time.Hour)}
	secret, err := attachedworkertransport.ParseConnectionSecret(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	token, err := attachedworkertransport.NewConnectionBearer(binding.TenantID, binding.OwnerUserID, binding.WorkerID, binding.ConnectionID, secret)
	if err != nil {
		t.Fatal(err)
	}
	r := pisdkphasewire.Request{Version: 1, Authorization: a, ExpectedHarnessBindingDigest: strings.Repeat("d", 64), ExpectedAttachedPiAPIInvocationPolicyDigest: strings.Repeat("e", 64),
		CallerDeadline: pisdkphasewire.CallerDeadline{Version: 1, RemainingNanoseconds: uint64(time.Minute)}, Phase: pisdkphasewire.PhysicalSend}
	reply := pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: h, AcceptedAttemptRevision: h.Revision, HarnessBindingDigest: r.ExpectedHarnessBindingDigest,
		AttachedPiAPIInvocationPolicyDigest: r.ExpectedAttachedPiAPIInvocationPolicyDigest, CheckedAt: created.Add(time.Second), UseExpiresAt: created.Add(2*time.Second + 123*time.Nanosecond)}
	return binding, token.Bytes(), r, reply
}

func piLoopback(t *testing.T, handler http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	transport.TLSClientConfig.MinVersion = tls.VersionTLS12
	transport.DisableCompression = true
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.ResponseHeaderTimeout = 5 * time.Second
	transport.MaxResponseHeaderBytes = 4096 // Explicit synthetic header failure bound, NOT serving/native P.
	t.Cleanup(transport.CloseIdleConnections)
	return server, &http.Client{Transport: transport, Timeout: 10 * time.Second}
}
func piReplyBytes(t *testing.T, r pisdkphasewire.Reply) []byte {
	t.Helper()
	b, err := pisdkphasewire.EncodeReply(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func piReplyHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
}
func piClient(t *testing.T, endpoint string, httpClient *http.Client, binding attachedworkersession.ConnectionBindingV1, bearer []byte) *PiAPIClient {
	t.Helper()
	c, err := NewPiAPIClient(endpoint, httpClient, binding, bearer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func piWait(t *testing.T, ctx context.Context, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		select {
		case <-ch:
		default:
			t.Fatalf("%s did not complete before failure bound", what)
		}
	}
}

// Cleanup cancellation/release + independent done join is installed before
// launching each owned operation, without consuming its result channel.
func piCleanupJoin(t *testing.T, cancel func(), done <-chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		cancel()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			select {
			case <-done:
			default:
				t.Error("synthetic operation did not join after cleanup release")
			}
		}
	})
}

func TestPiAPIClientAllPhasesOneH1DoAndOwnedConfig(t *testing.T) {
	binding, bearer, request, reply := piClientFixture(t)
	expectedBearer := bytes.Clone(bearer)
	body := piReplyBytes(t, reply)
	var calls atomic.Int32
	var mu sync.Mutex
	peers := map[string]bool{}
	server, httpClient := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		mu.Lock()
		peers[r.RemoteAddr] = true
		mu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != PiAPIUsePathV1 || r.URL.RawQuery != "" || r.ProtoMajor != 1 || !r.Close {
			t.Errorf("non-fixed H1 request")
		}
		if r.Header.Get("Authorization") != "Bearer "+string(expectedBearer) || len(r.Header.Values("Authorization")) != 1 || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("fixed headers/bearer not preserved")
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, pisdkphasewire.RequestMaxBytes+1))
		if err != nil || len(data) > pisdkphasewire.RequestMaxBytes {
			t.Error("unbounded/noncomplete canonical request")
			return
		}
		decoded, err := pisdkphasewire.DecodeRequest(data)
		if err != nil || decoded.Authorization != request.Authorization || decoded.CallerDeadline != request.CallerDeadline {
			t.Error("closed request/whole tuple mismatch")
		}
		if r.ContentLength != int64(len(data)) {
			t.Error("exact Content-Length lost")
		}
		piReplyHeaders(w)
		_, _ = w.Write(body)
	}))
	original := httpClient.Transport.(*http.Transport)
	c := piClient(t, server.URL+PiAPIUsePathV1, httpClient, binding, bearer)
	clear(bearer)
	if c.transport == original || c.transport.TLSClientConfig == original.TLSClientConfig || c.transport.TLSClientConfig.RootCAs == original.TLSClientConfig.RootCAs || c.client.Timeout != httpClient.Timeout || c.transport.ResponseHeaderTimeout != original.ResponseHeaderTimeout || c.transport.MaxResponseHeaderBytes != original.MaxResponseHeaderBytes {
		t.Fatal("private ownership or exact explicit bounds lost")
	}
	if !c.transport.DisableKeepAlives || !c.transport.Protocols.HTTP1() || c.transport.Protocols.HTTP2() || c.transport.Protocols.UnencryptedHTTP2() || c.transport.Proxy != nil || c.client.Jar != nil {
		t.Fatal("private H1/no-pool surface not closed")
	}
	if original.DisableKeepAlives || original.Protocols != nil {
		t.Fatal("caller/global transport mutated")
	}
	original.TLSClientConfig.RootCAs.AddCert(server.Certificate())
	original.TLSClientConfig.InsecureSkipVerify = true
	httpClient.Timeout = time.Nanosecond // Owned client preserves admitted failure bounds.
	if c.transport.TLSClientConfig.InsecureSkipVerify || c.client.Timeout != 10*time.Second {
		t.Fatal("caller mutation changed private bounds/trust")
	}
	for _, phase := range []pisdkphasewire.Phase{pisdkphasewire.PreIssue, pisdkphasewire.PhysicalSend, pisdkphasewire.ImmutableFinalize} {
		request.Phase = phase
		got, err := c.RequestPiAPIUse(t.Context(), request)
		if err != nil || got != reply {
			t.Fatalf("phase=%s err=%v whole reply=%v", phase, err, got == reply)
		}
	}
	mu.Lock()
	count := len(peers)
	mu.Unlock()
	if calls.Load() != 3 || count != 3 {
		t.Fatalf("application calls=%d distinct H1 peers=%d want3", calls.Load(), count)
	}
}

func TestPiAPIClientRejectsUnsafeConfigurationAndEndpoint(t *testing.T) {
	b, bearer, _, _ := piClientFixture(t)
	server, base := piLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("constructor opened network") }))
	for _, endpoint := range []string{"http://control.invalid" + PiAPIUsePathV1, server.URL + PiAPIUsePathV1 + "?", server.URL + PiAPIUsePathV1 + "?x=1", server.URL + PiAPIUsePathV1 + "#x", server.URL + "/attached-worker/v1/%70i-api-use", server.URL + "/wrong", "https://u:p@control.invalid" + PiAPIUsePathV1, "https://control.invalid:" + PiAPIUsePathV1, "%"} {
		if _, err := NewPiAPIClient(endpoint, base, b, bearer); !errors.Is(err, ErrInvalid) {
			t.Errorf("endpoint admitted err=%v", err)
		}
	}
	for _, tc := range []struct {
		name string
		edit func(*http.Client, *http.Transport)
	}{
		{"timeout_missing", func(c *http.Client, _ *http.Transport) { c.Timeout = 0 }},
		{"timeout_over", func(c *http.Client, _ *http.Transport) { c.Timeout = time.Minute + time.Nanosecond }},
		{"jar", func(c *http.Client, _ *http.Transport) { c.Jar, _ = cookiejar.New(nil) }},
		{"ambient", func(_ *http.Client, tr *http.Transport) { tr.Proxy = http.ProxyFromEnvironment }},
		{"insecure", func(_ *http.Client, tr *http.Transport) { tr.TLSClientConfig.InsecureSkipVerify = true }},
		{"nil_tls", func(_ *http.Client, tr *http.Transport) { tr.TLSClientConfig = nil }},
		{"tls_min", func(_ *http.Client, tr *http.Transport) { tr.TLSClientConfig.MinVersion = 0 }},
		{"tls_foreign_name", func(_ *http.Client, tr *http.Transport) { tr.TLSClientConfig.ServerName = "foreign.invalid" }},
		{"tls_timeout", func(_ *http.Client, tr *http.Transport) { tr.TLSHandshakeTimeout = 0 }},
		{"header_timeout", func(_ *http.Client, tr *http.Transport) { tr.ResponseHeaderTimeout = 0 }},
		{"header_timeout_over", func(_ *http.Client, tr *http.Transport) { tr.ResponseHeaderTimeout = cBoundOver() }},
		{"header_cap", func(_ *http.Client, tr *http.Transport) { tr.MaxResponseHeaderBytes = 0 }},
		{"compression", func(_ *http.Client, tr *http.Transport) { tr.DisableCompression = false }},
		{"custom_tls_dial", func(_ *http.Client, tr *http.Transport) {
			tr.DialTLSContext = func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("unreachable") }
		}},
		{"custom_verify", func(_ *http.Client, tr *http.Transport) {
			tr.TLSClientConfig.VerifyConnection = func(tls.ConnectionState) error { return nil }
		}},
		{"client_certificate", func(_ *http.Client, tr *http.Transport) {
			tr.TLSClientConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return nil, nil }
		}},
		{"tls_keylog", func(_ *http.Client, tr *http.Transport) { tr.TLSClientConfig.KeyLogWriter = io.Discard }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := *base
			tr := base.Transport.(*http.Transport).Clone()
			c.Transport = tr
			tc.edit(&c, tr)
			if _, err := NewPiAPIClient(server.URL+PiAPIUsePathV1, &c, b, bearer); !errors.Is(err, ErrInvalid) {
				t.Fatalf("unsafe config admitted %v", err)
			}
		})
	}
	for _, c := range []*http.Client{nil, {Timeout: time.Second}, {Transport: receiptRoundTrip(func(*http.Request) (*http.Response, error) { return nil, nil }), Timeout: time.Second}} {
		if _, err := NewPiAPIClient(server.URL+PiAPIUsePathV1, c, b, bearer); !errors.Is(err, ErrInvalid) {
			t.Error("unknown transport/client admitted")
		}
	}
	for _, bearer := range [][]byte{nil, []byte("probe"), bytes.Repeat([]byte{'x'}, 1025), []byte("slw1.invalid")} {
		if _, err := NewPiAPIClient(server.URL+PiAPIUsePathV1, base, b, bearer); !errors.Is(err, ErrInvalid) {
			t.Error("invalid/unbounded original connection bearer admitted")
		}
	}
	b.ConnectionGeneration = 0
	if _, err := NewPiAPIClient(server.URL+PiAPIUsePathV1, base, b, bearer); !errors.Is(err, ErrInvalid) {
		t.Error("unaccepted binding admitted")
	}
}
func cBoundOver() time.Duration { return 10*time.Second + time.Nanosecond }

func TestPiAPIClientStaticScopeDeniesBeforeHTTP(t *testing.T) {
	b, bearer, r, _ := piClientFixture(t)
	var calls atomic.Int32
	server, httpClient := piLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	c := piClient(t, server.URL+PiAPIUsePathV1, httpClient, b, bearer)
	for _, tc := range []struct {
		name string
		edit func(*ports.AttachedWorkerSealedInputAuthorization)
	}{
		{"tenant", func(a *ports.AttachedWorkerSealedInputAuthorization) { a.TenantID = "other" }},
		{"owner", func(a *ports.AttachedWorkerSealedInputAuthorization) { a.OwnerUserID = "other" }},
		{"worker", func(a *ports.AttachedWorkerSealedInputAuthorization) { a.WorkerID = "other" }},
		{"connection", func(a *ports.AttachedWorkerSealedInputAuthorization) { a.ConnectionID = "other" }},
		{"enrollment", func(a *ports.AttachedWorkerSealedInputAuthorization) { a.EnrollmentGeneration++ }},
		{"generation", func(a *ports.AttachedWorkerSealedInputAuthorization) { a.ConnectionGeneration++ }},
		{"capability", func(a *ports.AttachedWorkerSealedInputAuthorization) {
			a.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("f", 64))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := r
			tc.edit(&changed.Authorization)
			a := &changed.Authorization
			fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
			if err != nil {
				t.Fatal(err)
			}
			a.FenceToken = fence
			got, err := c.RequestPiAPIUse(t.Context(), changed)
			if !errors.Is(err, ErrInvalid) || got != (pisdkphasewire.Reply{}) {
				t.Fatalf("foreign static scope err=%v", err)
			}
		})
	}
	r.CallerDeadline.RemainingNanoseconds = 0
	_, err := c.RequestPiAPIUse(t.Context(), r)
	if !errors.Is(err, ErrInvalid) {
		t.Fatal("malformed request admitted")
	}
	if calls.Load() != 0 {
		t.Fatalf("foreign/malformed request crossed HTTP %d", calls.Load())
	}
}

func TestPiAPIClientClosedResponseAndNoReplay(t *testing.T) {
	b, bearer, r, reply := piClientFixture(t)
	good := piReplyBytes(t, reply)
	for _, mode := range []string{"denied", "status", "content_type", "duplicate_type", "encoding", "duplicate_encoding", "cache", "duplicate_cache", "malformed", "extra_phase", "foreign_M", "foreign_tuple", "oversize", "syntax_ceiling", "trailing", "partial", "redirect", "dropped", "headers_over"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				piReplyHeaders(w)
				data := good
				switch mode {
				case "denied":
					data = []byte(`{"status":"denied"}`)
				case "status":
					w.WriteHeader(http.StatusForbidden)
				case "content_type":
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
				case "malformed":
					data = []byte(`{"status":"applied"}`)
				case "extra_phase":
					data = bytes.Replace(good, []byte(`"status":"applied",`), []byte(`"status":"applied","phase":"physical_send",`), 1)
				case "foreign_M":
					changed := reply
					changed.AttachedPiAPIInvocationPolicyDigest = strings.Repeat("f", 64)
					data = piReplyBytes(t, changed)
				case "foreign_tuple":
					changed := reply
					changed.AcceptedAttempt.ConnectionID = "foreign"
					data = piReplyBytes(t, changed)
				case "oversize":
					data = bytes.Repeat([]byte{'x'}, pisdkphasewire.ReplyMaxBytes+1)
				case "syntax_ceiling":
					// Exact 2990-body fit but INVALID canonical reply; this is a
					// byte ceiling control, never a source-valid maximum positive.
					data = bytes.Repeat([]byte{'x'}, pisdkphasewire.ReplyMaxBytes)
				case "trailing":
					data = append(bytes.Clone(good), ' ')
				case "partial":
					w.Header().Set("Content-Length", strconv.Itoa(len(good)+1))
				case "redirect":
					w.Header().Set("Location", PiAPIUsePathV1)
					w.WriteHeader(http.StatusTemporaryRedirect)
				case "dropped":
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				case "headers_over":
					w.Header().Set("X-Padding", strings.Repeat("x", 8192))
				}
				_, _ = w.Write(data)
			}))
			c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
			got, err := c.RequestPiAPIUse(t.Context(), r)
			want := ErrUnavailable
			if mode == "denied" {
				want = ErrUnauthorized
			}
			if !errors.Is(err, want) || got != (pisdkphasewire.Reply{}) || err.Error() != want.Error() {
				t.Fatalf("mode=%s err=%v leaked metadata=%v", mode, err, got != (pisdkphasewire.Reply{}))
			}
			if calls.Load() != 1 {
				t.Fatalf("mode=%s request replay count=%d want1", mode, calls.Load())
			}
		})
	}
}

func TestPiAPIClientOriginalContextAndRetirementCancelStalledEOF(t *testing.T) {
	for _, cause := range []string{"caller", "client_close"} {
		t.Run(cause, func(t *testing.T) {
			b, bearer, r, reply := piClientFixture(t)
			good := piReplyBytes(t, reply)
			started := make(chan struct{})
			finished := make(chan struct{})
			releaseHandler := make(chan struct{})
			var releaseOnce sync.Once
			server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				defer close(finished)
				// Consume only bounded synthetic POST bytes before waiting on H1
				// disconnect. An unread body prevents the server background reader
				// from observing the disconnect while this handler is still active.
				_, _ = io.Copy(io.Discard, io.LimitReader(req.Body, pisdkphasewire.RequestMaxBytes+1))
				piReplyHeaders(w)
				_, _ = w.Write(good)
				w.(http.Flusher).Flush()
				close(started)
				select {
				case <-req.Context().Done():
				case <-releaseHandler:
				}
			}))
			t.Cleanup(func() { releaseOnce.Do(func() { close(releaseHandler) }) })
			c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			callCtx, callCancel := context.WithCancel(ctx)
			t.Cleanup(callCancel)
			done := make(chan error, 1)
			executeDone := make(chan struct{})
			piCleanupJoin(t, func() { callCancel(); releaseOnce.Do(func() { close(releaseHandler) }); _ = c.Close() }, executeDone)
			go func() {
				defer close(executeDone)
				got, err := c.RequestPiAPIUse(callCtx, r)
				if got != (pisdkphasewire.Reply{}) {
					done <- errors.New("late metadata escaped")
					return
				}
				done <- err
			}()
			piWait(t, ctx, started, "stalled EOF")
			if cause == "caller" {
				callCancel()
			} else {
				_ = c.Close()
			}
			select {
			case err := <-done:
				if !errors.Is(err, ErrUnavailable) || ctx.Err() != nil {
					t.Fatalf("independent cancellation err=%v parent=%v", err, ctx.Err())
				}
			case <-ctx.Done():
				t.Fatal("stalled EOF did not cancel")
			}
			// Client cancellation must complete BEFORE the test-owned release.
			// Release/join the synthetic peer independently: eventual remote
			// cancellation is not the original local-use cutoff proof.
			releaseOnce.Do(func() { close(releaseHandler) })
			piWait(t, ctx, finished, "server retirement")
			if cause == "client_close" {
				c.mu.Lock()
				cleared := c.closed && c.bearer == nil && c.binding == (attachedworkersession.ConnectionBindingV1{})
				c.mu.Unlock()
				if !cleared {
					t.Fatal("owned bearer/binding retained")
				}
			}
		})
	}
}

func TestPiAPIClientRequestBoundaryNoGetBodyOrDeadlineRenewal(t *testing.T) {
	b, bearer, r, _ := piClientFixture(t)
	server, hc := piLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("spy should prevent network") }))
	c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), struct{}{}, "same-value"), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	calls := 0
	// A TEST-ONLY request-boundary spy is not admitted by the real constructor.
	// It proves fields, not genuine TLS/source/HTTP conformance or permission.
	c.client.Transport = receiptRoundTrip(func(req *http.Request) (*http.Response, error) {
		calls++
		got, ok := req.Context().Deadline()
		if !ok || !got.Equal(deadline) || req.Context().Value(struct{}{}) != "same-value" || req.GetBody != nil || !req.Close || req.ContentLength <= 0 {
			t.Error("context/replay boundary changed")
		}
		return nil, errors.New("synthetic raw key/diagnostic must not escape")
	})
	_, err := c.RequestPiAPIUse(ctx, r)
	if !errors.Is(err, ErrUnavailable) || err.Error() != ErrUnavailable.Error() || calls != 1 {
		t.Fatalf("safe one-Do boundary err=%v calls=%d", err, calls)
	}
	_ = c.Close()
	_, err = c.RequestPiAPIUse(ctx, r)
	if !errors.Is(err, ErrUnavailable) || calls != 1 {
		t.Fatal("closed client called transport")
	}
}

func TestPiAPIClientCertificateTrustRemainsOwned(t *testing.T) {
	b, bearer, r, reply := piClientFixture(t)
	data := piReplyBytes(t, reply)
	server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { piReplyHeaders(w); _, _ = w.Write(data) }))
	c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
	// The original CertPool and private pool must be distinct even though the
	// trusted public test certificate content is equal at construction.
	pool := hc.Transport.(*http.Transport).TLSClientConfig.RootCAs
	if pool == c.transport.TLSClientConfig.RootCAs {
		t.Fatal("shallow root pool")
	}
	originalCount := len(c.transport.TLSClientConfig.RootCAs.Subjects())
	pool.AddCert(&x509.Certificate{RawSubject: []byte("foreign synthetic subject")})
	if len(c.transport.TLSClientConfig.RootCAs.Subjects()) != originalCount {
		t.Fatal("caller mutated admitted trust pool")
	}
	if _, err := c.RequestPiAPIUse(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	// A private explicit empty trust pool cannot inherit insecure/default roots.
	bad := *hc
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.RootCAs = x509.NewCertPool()
	bad.Transport = tr
	d := piClient(t, server.URL+PiAPIUsePathV1, &bad, b, bearer)
	if _, err := d.RequestPiAPIUse(t.Context(), r); !errors.Is(err, ErrUnavailable) {
		t.Fatal("untrusted certificate accepted")
	}
}

func TestPiAPIClientLateHeldCompletionCannotEscapeRetirement(t *testing.T) {
	b, bearer, r, reply := piClientFixture(t)
	data := piReplyBytes(t, reply)
	server, hc := piLoopback(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("test boundary spy must not contact listener") }))
	c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	// TEST-ONLY held completion, deliberately ignoring cancellation until the
	// explicit release; this transport is rejected by the real constructor.
	c.client.Transport = receiptRoundTrip(func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, ProtoMajor: 1, Header: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}, ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data)), Request: req}, nil
	})
	done := make(chan error, 1)
	executeDone := make(chan struct{})
	piCleanupJoin(t, func() { cancel(); once.Do(func() { close(release) }); _ = c.Close() }, executeDone)
	go func() {
		defer close(executeDone)
		got, err := c.RequestPiAPIUse(ctx, r)
		if got != (pisdkphasewire.Reply{}) {
			done <- errors.New("late structural applied reply escaped")
			return
		}
		done <- err
	}()
	piWait(t, ctx, entered, "held completion")
	_ = c.Close()
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnavailable) || ctx.Err() != nil {
			t.Fatalf("late retirement err=%v original=%v", err, ctx.Err())
		}
	case <-ctx.Done():
		t.Fatal("released completion did not join")
	}
}

func TestPiAPIClientCompleteRequestSyntaxCeilingOverTLS(t *testing.T) {
	b, _, r, reply := piClientFixture(t)
	a := &r.Authorization
	a.TenantID = domain.TenantID(strings.Repeat("t", 160))
	a.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
	a.WorkerID = domain.AttachedWorkerID(strings.Repeat("w", 160))
	a.ConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	a.RunID = domain.RunID(strings.Repeat("r", 128))
	a.AttemptID = domain.AttemptID(strings.Repeat("a", 128))
	a.LeaseID = domain.LeaseID(strings.Repeat("l", 128))
	a.EnrollmentGeneration = math.MaxUint64
	a.ConnectionGeneration = math.MaxUint64
	a.LeaseGeneration = math.MaxUint64
	a.ExpectedAttemptRevision = math.MaxUint64
	a.LeaseExpiresAtUnixMicro = math.MaxInt64
	fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	a.FenceToken = fence
	r.Phase = pisdkphasewire.ImmutableFinalize
	r.CallerDeadline.RemainingNanoseconds = math.MaxInt64
	b.TenantID = a.TenantID
	b.OwnerUserID = a.OwnerUserID
	b.WorkerID = a.WorkerID
	b.ConnectionID = a.ConnectionID
	b.EnrollmentGeneration = a.EnrollmentGeneration
	b.ConnectionGeneration = a.ConnectionGeneration
	h := &reply.AcceptedAttempt
	h.TenantID = a.TenantID
	h.OwnerUserID = a.OwnerUserID
	h.WorkerID = a.WorkerID
	h.ConnectionID = a.ConnectionID
	h.RunID = a.RunID
	h.AttemptID = a.AttemptID
	h.LeaseID = a.LeaseID
	h.LeaseGeneration = a.LeaseGeneration
	h.EnrollmentGeneration = a.EnrollmentGeneration
	h.ConnectionGeneration = a.ConnectionGeneration
	h.Revision = a.ExpectedAttemptRevision
	h.FenceToken = a.FenceToken
	h.LeaseExpiresAt = time.UnixMicro(math.MaxInt64).UTC()
	reply.AcceptedAttemptRevision = h.Revision
	secret, err := attachedworkertransport.ParseConnectionSecret(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	token, err := attachedworkertransport.NewConnectionBearer(b.TenantID, b.OwnerUserID, b.WorkerID, b.ConnectionID, secret)
	if err != nil {
		t.Fatal(err)
	}
	bearer := token.Bytes()
	encoded, err := pisdkphasewire.EncodeRequest(r)
	if err != nil || len(encoded) != pisdkphasewire.RequestMaxBytes {
		t.Fatalf("syntax request cap len=%d err=%v", len(encoded), err)
	}
	body := piReplyBytes(t, reply)
	server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, err := io.ReadAll(io.LimitReader(req.Body, pisdkphasewire.RequestMaxBytes+1))
		if err != nil || !bytes.Equal(data, encoded) || req.ContentLength != int64(pisdkphasewire.RequestMaxBytes) {
			t.Error("full syntax-ceiling request changed")
		}
		piReplyHeaders(w)
		_, _ = w.Write(body)
	}))
	c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
	got, err := c.RequestPiAPIUse(t.Context(), r)
	if err != nil || got != reply {
		t.Fatalf("complete syntactic-bound fixture err=%v", err)
	}
	// Constructor-backed structurally lawful metadata at large field endpoints,
	// NOT a legitimate current-source maximum or native model/media-fit proof.
}

func TestPiAPIClientConcurrentCallsKeepOriginalAssociation(t *testing.T) {
	b, bearer, first, reply := piClientFixture(t)
	second := first
	second.Authorization.ExpectedAttemptRevision++
	second.ExpectedHarnessBindingDigest = strings.Repeat("f", 64)
	second.Phase = pisdkphasewire.ImmutableFinalize
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, err := io.ReadAll(io.LimitReader(req.Body, pisdkphasewire.RequestMaxBytes+1))
		if err != nil {
			t.Error(err)
			return
		}
		r, err := pisdkphasewire.DecodeRequest(data)
		if err != nil {
			t.Error(err)
			return
		}
		s := reply
		if r.Authorization.ExpectedAttemptRevision == first.Authorization.ExpectedAttemptRevision {
			close(entered)
			select {
			case <-release:
			case <-req.Context().Done():
				return
			}
		} else {
			s.AcceptedAttempt.Revision = second.Authorization.ExpectedAttemptRevision
			s.AcceptedAttemptRevision = s.AcceptedAttempt.Revision
			s.HarnessBindingDigest = second.ExpectedHarnessBindingDigest
		}
		piReplyHeaders(w)
		_, _ = w.Write(piReplyBytes(t, s))
	}))
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	c := piClient(t, server.URL+PiAPIUsePathV1, hc, b, bearer)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	type result struct {
		reply pisdkphasewire.Reply
		err   error
	}
	done := make(chan result, 1)
	executeDone := make(chan struct{})
	piCleanupJoin(t, func() { cancel(); once.Do(func() { close(release) }); _ = c.Close() }, executeDone)
	go func() {
		defer close(executeDone)
		s, err := c.RequestPiAPIUse(ctx, first)
		done <- result{reply: s, err: err}
	}()
	piWait(t, ctx, entered, "first associated request")
	got, err := c.RequestPiAPIUse(ctx, second)
	if err != nil || got.AcceptedAttemptRevision != second.Authorization.ExpectedAttemptRevision || got.HarnessBindingDigest != second.ExpectedHarnessBindingDigest {
		t.Fatalf("second call association err=%v", err)
	}
	once.Do(func() { close(release) })
	select {
	case got := <-done:
		if got.err != nil || got.reply != reply {
			t.Fatalf("first held call associated with another reply: %v", got.err)
		}
	case <-ctx.Done():
		select {
		case got := <-done:
			if got.err != nil || got.reply != reply {
				t.Fatalf("first held call associated with another reply: %v", got.err)
			}
		default:
			t.Fatal("first associated call did not join")
		}
	}
}
