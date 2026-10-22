package controlapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This fixture simulates identity/burn/claim bookkeeping only. It is not a
// transactional authority, genuine native producer, or persistence/custody proof.
type inlineMuxStoreV2 struct {
	mu                             sync.Mutex
	prepare                        attachedworkeroutput.AttachedWorkerInlinePrepareV2
	fingerprint                    string
	body                           []byte
	binds, prepares, submits, recs int
	claimed, ready                 bool
	observedHashes                 []string
	failures                       []string
}

const inlineMuxBearerV2 = "owned-synthetic-inline-bearer"

func (s *inlineMuxStoreV2) BindOutputReceiptBearer(bearer []byte, auth ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.binds++
	if !bytes.Equal(bearer, []byte(inlineMuxBearerV2)) || auth != s.prepare.Authorization {
		return auth, s.fail("original synthetic identity did not reach binder")
	}
	auth.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte(inlineMuxBearerV2))
	return auth, nil
}

func (s *inlineMuxStoreV2) fail(reason string) error {
	s.failures = append(s.failures, reason)
	return errors.New("controlled inline mux fixture rejected input")
}

func (s *inlineMuxStoreV2) authorize(ctx context.Context, auth ports.AttachedWorkerSealedInputAuthorization, requireExpiry bool) error {
	want := s.prepare.Authorization
	want.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte(inlineMuxBearerV2))
	deadline, ok := ctx.Deadline()
	if ctx.Err() != nil || !ok || deadline.After(time.Now().Add(15*time.Second)) || requireExpiry && deadline.After(s.prepare.ExpiresAt) || auth != want || s.binds != s.prepares+s.submits+s.recs+1 {
		return s.fail("store did not receive bounded context and prior binder identity")
	}
	return nil
}

func (s *inlineMuxStoreV2) failureSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.failures...)
}

func (s *inlineMuxStoreV2) result(stage attachedworkeroutput.AttachedWorkerInlineStageV2) attachedworkeroutput.AttachedWorkerInlineResultV2 {
	v := attachedworkeroutput.AttachedWorkerInlineResultV2{Version: 2, Stage: stage, Nonce: s.prepare.Nonce, Fingerprint: s.fingerprint, ExpiresAt: s.prepare.ExpiresAt}
	if stage == attachedworkeroutput.InlinePreparedV2 {
		v.Capability = strings.Repeat("c", 64)
	} else {
		v.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("d", 64))
	}
	return v
}

func (s *inlineMuxStoreV2) PrepareAttachedWorkerInlineOutput(ctx context.Context, _ ports.BlobStore, v attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx, v.Authorization, true); err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	s.prepares++
	v.Authorization.PresentedSecretDigest = ""
	if s.prepares != 1 || !reflect.DeepEqual(v, s.prepare) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, s.fail("prepare changed original semantic metadata or repeated")
	}
	return s.result(attachedworkeroutput.InlinePreparedV2), nil
}

func (s *inlineMuxStoreV2) SubmitAttachedWorkerInlineOutput(ctx context.Context, _ ports.BlobStore, v attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.authorize(ctx, v.Authorization, true); err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	s.submits++
	if s.prepares != 1 || s.submits != 1 || s.claimed || v.Version != 2 || v.Nonce != s.prepare.Nonce || v.Fingerprint != s.fingerprint || v.Capability != strings.Repeat("c", 64) || !v.ExpiresAt.Equal(s.prepare.ExpiresAt) || length != int64(len(s.body)) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, s.fail("body metadata/order/one-shot framing changed")
	}
	// This is only a fake claim before the fake store's first application Read;
	// no durable burn, irreversible token or permission is established here.
	s.claimed = true
	observed, err := io.ReadAll(io.LimitReader(body, attachedworkeroutput.MaxInlineTotalBytesV2+1))
	defer clear(observed)
	if err != nil || !bytes.Equal(observed, s.body) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, s.fail("body bytes were truncated, reordered or reserialized")
	}
	verified, err := attachedworkeroutput.VerifyInlineV2(s.prepare.Candidate, observed)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, s.fail("original text/JSON bodies failed content verification")
	}
	verified.Clear()
	offset := 0
	for _, artifact := range s.prepare.Candidate.Artifacts {
		end := offset + int(artifact.Size)
		s.observedHashes = append(s.observedHashes, inlineMuxHashV2(observed[offset:end]))
		offset = end
	}
	s.ready = true
	return s.result(attachedworkeroutput.InlineReadyV2), nil
}

func (s *inlineMuxStoreV2) ReconcileAttachedWorkerInlineOutput(ctx context.Context, v attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Client context deadlines do not become server context deadlines over HTTP.
	// Reconcile has only its server entry window, not the export's old expiry.
	if err := s.authorize(ctx, v.Authorization, false); err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	s.recs++
	if s.recs != 1 || !s.ready || !s.claimed || v.Version != 2 || v.Nonce != s.prepare.Nonce || v.Fingerprint != s.fingerprint || !v.BodySendBurned {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, s.fail("reconcile did not preserve simulated burned READY identity")
	}
	return s.result(attachedworkeroutput.InlineReadyV2), nil
}

type inlineMuxNoBlobsV2 struct{}

func (inlineMuxNoBlobsV2) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	panic("wire-only fixture must not perform blob writes")
}
func (inlineMuxNoBlobsV2) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	panic("wire-only fixture must not perform blob reads")
}
func (inlineMuxNoBlobsV2) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	panic("wire-only fixture must not perform blob deletion")
}

func inlineMuxHashV2(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

// Real client -> real inline handler -> real control mux over owned TLS HTTP/1.
// Success checks strict compact responses and real ResponseController support;
// only binder/store/blob dependencies are fakes. This does not qualify native
// production, authorization transactions, persistent burns or canonical copies.
func TestInlineOutputV2ClientMaximumCandidateThroughControlMuxTLS(t *testing.T) {
	text := bytes.Repeat([]byte("<&é>"), (32<<10)/5)
	text = append(text, []byte("abc")...)
	jsonBody := []byte("{\"original\":\"")
	jsonBody = append(jsonBody, bytes.Repeat([]byte("<&é>"), ((32<<10)-len(jsonBody)-2)/5)...)
	jsonBody = append(jsonBody, bytes.Repeat([]byte("x"), (32<<10)-len(jsonBody)-2)...)
	jsonBody = append(jsonBody, '"', '}')
	body := append(bytes.Clone(text), jsonBody...)
	// Registered before the server/transport cleanups so even an early failure
	// stops their owned work before releasing the original backing bytes.
	t.Cleanup(func() { clear(text); clear(jsonBody); clear(body) })
	now := time.Now().UTC()
	expires := now.Add(14 * time.Second)
	longID := func(prefix string) string { return prefix + strings.Repeat("x", 160-len(prefix)) }
	auth := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: domain.TenantID(longID("tenant")), OwnerUserID: domain.UserID(longID("owner")), WorkerID: domain.AttachedWorkerID(longID("worker")),
		ConnectionID: domain.AttachedWorkerConnectionID(longID("connection")), RunID: domain.RunID(longID("run")), AttemptID: domain.AttemptID(longID("attempt")), LeaseID: domain.LeaseID(longID("lease")),
		EnrollmentGeneration: 1, ConnectionGeneration: 2, AttemptSequence: 1, LeaseGeneration: 3, LeaseExpiresAtUnixMicro: expires.Add(time.Minute).UnixMicro(),
		ContextDigest:    domain.AttachedWorkerContextDigest(strings.Repeat("b", 64)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("c", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("d", 64)),
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(auth.TenantID, auth.OwnerUserID, auth.WorkerID, auth.RunID, auth.AttemptID, auth.LeaseID, auth.LeaseGeneration)
	if err != nil {
		t.Fatalf("construct canonical fixture fence: %v", err)
	}
	auth.FenceToken = fence
	// Exactly 32 KiB and 32,000 runes, including multibyte UTF-8 and JSON HTML
	// escaping. Wire expansion is charged separately from semantic content size.
	summary := strings.Repeat("<", 31_229) + "&>\"" + strings.Repeat("é", 768)
	prepare := attachedworkeroutput.AttachedWorkerInlinePrepareV2{
		Version: 2, Authorization: auth, Nonce: domain.IdempotencyKey(longID("nonce")), ExpiresAt: expires,
		Candidate: attachedworkeroutput.InlineCandidateV2{Summary: summary, Artifacts: []attachedworkeroutput.InlineArtifactV2{
			{Name: strings.Repeat("&", 251) + ".txt", MediaType: "text/plain", Size: uint64(len(text)), SHA256: inlineMuxHashV2(text)},
			{Name: strings.Repeat("<", 250) + ".json", MediaType: "application/json", Size: uint64(len(jsonBody)), SHA256: inlineMuxHashV2(jsonBody)},
		}},
		Producer:    attachedworkeroutput.InlineProducerBindingV2{SessionID: domain.SessionID(longID("session")), ReservationID: domain.QuotaReservationID(longID("reservation")), HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("e", 64)), AcceptedAt: now, SchemaDigest: strings.Repeat("f", 64), FinalDigest: strings.Repeat("a", 64), AnswerDigest: inlineMuxHashV2([]byte(summary))},
		Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true, CredentialReleaseRequired: true, CredentialReleased: true},
	}
	encoded, err := json.Marshal(prepare)
	if err != nil || len(encoded) > 256<<10 || !bytes.Contains(encoded, []byte(`\u003c`)) || len(summary) != 32<<10 || utf8.RuneCountInString(summary) != 32_000 || len(text) != 32<<10 || len(jsonBody) != 32<<10 || len(body) != 64<<10 || prepare.Candidate.Validate() != nil {
		t.Fatalf("maximum fixture invalid: control=%d summary=%d/runes=%d text=%d json=%d total=%d err=%v", len(encoded), len(summary), utf8.RuneCountInString(summary), len(text), len(jsonBody), len(body), err)
	}
	t.Logf("maximum semantic answer=%d bytes/%d runes, escaped prepare=%d bytes, ordered artifacts=%d+%d bytes", len(summary), utf8.RuneCountInString(summary), len(encoded), len(text), len(jsonBody))
	clear(encoded)
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(prepare)
	if err != nil {
		t.Fatalf("fingerprint maximum candidate: %v", err)
	}
	store := &inlineMuxStoreV2{prepare: prepare, fingerprint: fingerprint, body: body}
	service, err := attachedworkerreceipt.NewInlineServiceV2(store, store, inlineMuxNoBlobsV2{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), Options{AttachedWorkerInlineOutputV2: attachedworkerreceipt.HandlerInlineV2(service)}))
	server.Config.MaxHeaderBytes = MaxRequestHeaderBytes
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.StartTLS()
	t.Cleanup(server.Close)
	// Borrow only this fixture's certificate trust; do not use SkipVerify,
	// ambient default transport/proxy configuration or mutable global clients.
	fixtureTransport, ok := server.Client().Transport.(*http.Transport)
	if !ok || fixtureTransport.TLSClientConfig == nil || fixtureTransport.TLSClientConfig.InsecureSkipVerify || fixtureTransport.TLSClientConfig.RootCAs == nil {
		t.Fatal("owned TLS server omitted scoped certificate trust")
	}
	transport := fixtureTransport.Clone()
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport.Protocols, transport.Proxy = protocols, nil
	transport.DisableKeepAlives, transport.DisableCompression, transport.ForceAttemptHTTP2 = true, true, false
	transport.MaxResponseHeaderBytes, transport.MaxConnsPerHost = MaxRequestHeaderBytes, 1
	transport.TLSHandshakeTimeout, transport.ResponseHeaderTimeout = 5*time.Second, 5*time.Second
	t.Cleanup(transport.CloseIdleConnections)
	client, err := attachedworkerreceipt.NewInlineClientV2(server.URL, &http.Client{Transport: transport}, []byte(inlineMuxBearerV2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithDeadline(t.Context(), expires)
	defer cancel()
	var traceMu sync.Mutex
	connections, writes, responses := 0, 0, 0
	var traceFailures []string
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			traceMu.Lock()
			defer traceMu.Unlock()
			connections++
			tlsConn, ok := info.Conn.(*tls.Conn)
			if !ok || info.Reused || tlsConn.ConnectionState().NegotiatedProtocol != "" && tlsConn.ConnectionState().NegotiatedProtocol != "http/1.1" {
				traceFailures = append(traceFailures, "exchange did not use fresh owned TLS HTTP/1")
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			traceMu.Lock()
			defer traceMu.Unlock()
			writes++
			if info.Err != nil {
				traceFailures = append(traceFailures, "physical request write failed")
			}
		},
		GotFirstResponseByte: func() { traceMu.Lock(); responses++; traceMu.Unlock() },
	})
	prepared, err := client.Prepare(ctx, prepare)
	if err != nil || prepared != store.result(attachedworkeroutput.InlinePreparedV2) {
		t.Fatalf("prepare through real TLS mux: stage=%s err=%v fixture=%v", prepared.Stage, err, store.failureSnapshot())
	}
	submitted, err := client.Submit(ctx, attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: auth, Nonce: prepare.Nonce, Fingerprint: fingerprint, Capability: prepared.Capability, ExpiresAt: expires}, int64(len(body)), bytes.NewReader(body))
	if err != nil || submitted != store.result(attachedworkeroutput.InlineReadyV2) || submitted.Capability != "" {
		t.Fatalf("submit through real TLS mux: stage=%s err=%v fixture=%v", submitted.Stage, err, store.failureSnapshot())
	}
	reconciled, err := client.Reconcile(ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: auth, Nonce: prepare.Nonce, Fingerprint: fingerprint, BodySendBurned: true})
	if err != nil || reconciled != submitted || reconciled.Capability != "" {
		t.Fatalf("reconcile through real TLS mux: stage=%s err=%v fixture=%v", reconciled.Stage, err, store.failureSnapshot())
	}
	// Successful client decoding also checks actual response Content-Length,
	// bounded canonical closed JSON, EOF, nonce/fingerprint and capability rules.
	for _, result := range []attachedworkeroutput.AttachedWorkerInlineResultV2{prepared, submitted, reconciled} {
		raw, marshalErr := json.Marshal(result)
		if marshalErr != nil || len(raw) > 32<<10 || bytes.Contains(raw, []byte(inlineMuxBearerV2)) || bytes.Contains(raw, []byte(`"summary"`)) || bytes.Contains(raw, []byte(`"artifacts"`)) {
			t.Errorf("response did not remain compact metadata: bytes=%d err=%v", len(raw), marshalErr)
		}
		clear(raw)
	}
	store.mu.Lock()
	if store.binds != 3 || store.prepares != 1 || store.submits != 1 || store.recs != 1 || !store.claimed || !store.ready || !reflect.DeepEqual(store.observedHashes, []string{prepare.Candidate.Artifacts[0].SHA256, prepare.Candidate.Artifacts[1].SHA256}) || len(store.failures) != 0 {
		t.Errorf("fake bookkeeping binds=%d prepare/body/reconcile=%d/%d/%d claimed=%v ready=%v failures=%v", store.binds, store.prepares, store.submits, store.recs, store.claimed, store.ready, store.failures)
	}
	store.mu.Unlock()
	traceMu.Lock()
	if connections != 3 || writes != 3 || responses != 3 || len(traceFailures) != 0 {
		t.Errorf("physical exchanges connections=%d writes=%d responses=%d failures=%v", connections, writes, responses, traceFailures)
	}
	traceMu.Unlock()
	if !bytes.Equal(body[:len(text)], text) || !bytes.Equal(body[len(text):], jsonBody) {
		t.Error("borrowed original artifact content was changed")
	}
}
