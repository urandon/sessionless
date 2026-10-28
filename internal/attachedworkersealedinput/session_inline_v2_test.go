package attachedworkersealedinput

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
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This file tests only bearer custody and wire forwarding with cooperative
// in-process peers. Its synthetic DTOs are NOT private producer provenance,
// accepted jobs, YDB custody, native execution or the complete #235 proof.
const inlineSessionTestOriginV2 = "https://inline-session.invalid"
const inlineSessionTestBearerV2 = "synthetic-original-session-bearer"

var _ attachedworkerdaemontransport.InlineReceiptPublisherV2 = (*InlineSessionSourceFactoryV2)(nil)

type inlineSessionDTOsV2 struct {
	prepare   attachedworkeroutput.AttachedWorkerInlinePrepareV2
	body      attachedworkeroutput.AttachedWorkerInlineBodyV2
	reconcile attachedworkeroutput.AttachedWorkerInlineReconcileV2
	bytes     []byte
	request   attachedworkerdaemontransport.MaterializationRequestV1
	failure   attachedworkerdaemontransport.ReceiptSubmissionV1
}

func inlineSessionDTOFixtureV2(t *testing.T) inlineSessionDTOsV2 {
	t.Helper()
	submission := receiptSubmission(t)
	submission.Candidate = attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalFailed, FailureCode: "synthetic_failure"}
	submission.Observation.RunnerFailed = true
	request := submission.Request
	auth := ports.AuthorizationForAttachedInput(request.TenantID, request.OwnerUserID, request.WorkerID, request.ConnectionID,
		request.EnrollmentGeneration, request.ConnectionGeneration, request.Attempt, request.AttemptSequence)
	auth.LeaseExpiresAtUnixMicro = time.Now().UTC().Add(time.Minute).UnixMicro()
	body := []byte("original bounded inline text\n")
	bodyDigest, answerDigest := sha256.Sum256(body), sha256.Sum256([]byte("synthetic wire answer"))
	prepare := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: auth, Nonce: "inline-session-original",
		Candidate: attachedworkeroutput.InlineCandidateV2{Summary: "synthetic wire answer", Artifacts: []attachedworkeroutput.InlineArtifactV2{{Name: "report.txt", MediaType: "text/plain", Size: uint64(len(body)), SHA256: hex.EncodeToString(bodyDigest[:])}}},
		Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: "inline-session-fixture", ReservationID: "inline-reservation-fixture", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("a", 64)),
			SchemaDigest: strings.Repeat("b", 64), FinalDigest: strings.Repeat("c", 64), AnswerDigest: hex.EncodeToString(answerDigest[:]), AcceptedAt: time.Now().UTC().Add(-time.Minute)},
		Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, ExpiresAt: time.Now().UTC().Add(3 * time.Second)}
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(prepare)
	if err != nil {
		t.Fatal("synthetic inline fingerprint", err)
	}
	return inlineSessionDTOsV2{prepare: prepare, body: attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: auth, Nonce: prepare.Nonce, Fingerprint: fingerprint, Capability: strings.Repeat("d", 64), ExpiresAt: prepare.ExpiresAt},
		reconcile: attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: auth, Nonce: prepare.Nonce, Fingerprint: fingerprint, BodySendBurned: true}, bytes: body, request: request, failure: submission}
}

type inlineSessionPeerV2 struct {
	mu           sync.Mutex
	paths        []string
	dto          inlineSessionDTOsV2
	deadline     time.Time
	contextValue string
	fail         bool
	redirect     bool
	entered      chan string
	closeIdle    atomic.Int32
}

func (p *inlineSessionPeerV2) CloseIdleConnections() { p.closeIdle.Add(1) }
func (p *inlineSessionPeerV2) count() int            { p.mu.Lock(); defer p.mu.Unlock(); return len(p.paths) }
func (p *inlineSessionPeerV2) RoundTrip(request *http.Request) (*http.Response, error) {
	p.mu.Lock()
	p.paths = append(p.paths, request.URL.Path)
	p.mu.Unlock()
	if request.Method != http.MethodPost || request.URL.Scheme != "https" || request.URL.Host != "inline-session.invalid" || request.URL.RawQuery != "" || request.Header.Get("Authorization") != "Bearer "+inlineSessionTestBearerV2 {
		return nil, errors.New("synthetic channel changed original origin/method/bearer")
	}
	if p.contextValue != "" && request.Context().Value(inlineSessionContextKeyV2{}) != p.contextValue {
		return nil, errors.New("source replaced the original caller context")
	}
	if p.entered != nil {
		p.entered <- request.URL.Path
		<-request.Context().Done()
		_ = request.Body.Close()
		return nil, request.Context().Err()
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, 128<<10))
	closeErr := request.Body.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	if request.URL.Path == PathV1 || request.URL.Path == attachedworkerreceipt.PathV1 {
		return &http.Response{StatusCode: http.StatusUnauthorized, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}
	if request.GetBody != nil || request.ContentLength != int64(len(raw)) || len(request.TransferEncoding) != 0 {
		return nil, errors.New("inline request has replay or inexact framing")
	}
	deadline, ok := request.Context().Deadline()
	if !ok || !deadline.Equal(p.deadline) {
		return nil, errors.New("inline forwarding renewed/changed original deadline")
	}
	result := attachedworkeroutput.AttachedWorkerInlineResultV2{Version: 2, Nonce: p.dto.prepare.Nonce, Fingerprint: p.dto.body.Fingerprint, ExpiresAt: p.dto.prepare.ExpiresAt}
	switch request.URL.Path {
	case attachedworkerreceipt.PathInlinePrepareV2:
		var input attachedworkeroutput.AttachedWorkerInlinePrepareV2
		if json.Unmarshal(raw, &input) != nil || !reflect.DeepEqual(input, p.dto.prepare) {
			return nil, errors.New("Prepare changed immutable metadata")
		}
		result.Stage, result.Capability = attachedworkeroutput.InlinePreparedV2, p.dto.body.Capability
	case attachedworkerreceipt.PathInlineBodyV2:
		metadata, err := base64.RawStdEncoding.Strict().DecodeString(request.Header.Get(attachedworkerreceipt.InlineRequestHeaderV2))
		var input attachedworkeroutput.AttachedWorkerInlineBodyV2
		if err != nil || json.Unmarshal(metadata, &input) != nil || !reflect.DeepEqual(input, p.dto.body) || !bytes.Equal(raw, p.dto.bytes) || request.Header.Get("Content-Type") != attachedworkerreceipt.InlineBodyMediaTypeV2 {
			return nil, errors.New("Submit changed body/metadata/framing")
		}
		result.Stage, result.CanonicalDigest = attachedworkeroutput.InlineReadyV2, domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("e", 64))
	case attachedworkerreceipt.PathInlineReconcileV2:
		var input attachedworkeroutput.AttachedWorkerInlineReconcileV2
		if json.Unmarshal(raw, &input) != nil || !reflect.DeepEqual(input, p.dto.reconcile) {
			return nil, errors.New("Reconcile changed original commitment")
		}
		result.Stage, result.CanonicalDigest = attachedworkeroutput.InlineReadyV2, domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("e", 64))
	default:
		return nil, errors.New("unexpected source path")
	}
	if p.fail {
		return nil, errors.New("synthetic unknown response")
	}
	if p.redirect {
		return &http.Response{StatusCode: http.StatusTemporaryRedirect, Header: http.Header{"Location": {inlineSessionTestOriginV2 + "/foreign"}}, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}, "Content-Length": {strconv.Itoa(len(encoded))}}, ContentLength: int64(len(encoded)), Body: io.NopCloser(bytes.NewReader(encoded))}, nil
}

type inlineSessionPortV2 struct {
	closes  atomic.Int32
	entered chan struct{}
	release chan struct{}
	err     error
}

func (*inlineSessionPortV2) Exchange(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	return nil, nil
}
func (p *inlineSessionPortV2) Close() error {
	p.closes.Add(1)
	if p.entered != nil {
		close(p.entered)
	}
	if p.release != nil {
		<-p.release
	}
	return p.err
}

func newInlineSessionFactoryV2(t *testing.T, peer http.RoundTripper, delegate attachedworkersession.ExchangeFactory) *InlineSessionSourceFactoryV2 {
	t.Helper()
	factory, err := NewInlineSessionSourceFactoryV2(inlineSessionTestOriginV2, &http.Client{Transport: peer}, delegate)
	if err != nil {
		t.Fatal("inline factory constructor", err)
	}
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Error("cleanup inline factory", err)
		}
	})
	return factory
}

func callInlineSessionV2(t *testing.T, ctx context.Context, f *InlineSessionSourceFactoryV2, d inlineSessionDTOsV2, operation string) error {
	t.Helper()
	check := func(value, zero any, err error) error {
		if err != nil && !reflect.DeepEqual(value, zero) {
			t.Errorf("%s returned nonzero wire data with error: %v", operation, err)
		}
		return err
	}
	switch operation {
	case "load":
		value, err := f.Load(ctx, d.request)
		return check(value, attachedworkerdaemontransport.SealedInputV1{}, err)
	case "publish":
		value, err := f.Publish(ctx, d.failure)
		return check(value, attachedworkerdaemontransport.ReceiptCommitmentV1{}, err)
	case "prepare":
		value, err := f.Prepare(ctx, d.prepare)
		return check(value, attachedworkeroutput.AttachedWorkerInlineResultV2{}, err)
	case "submit":
		value, err := f.Submit(ctx, d.body, int64(len(d.bytes)), bytes.NewReader(d.bytes))
		return check(value, attachedworkeroutput.AttachedWorkerInlineResultV2{}, err)
	case "reconcile":
		value, err := f.Reconcile(ctx, d.reconcile)
		return check(value, attachedworkeroutput.AttachedWorkerInlineResultV2{}, err)
	default:
		return errors.New("invalid fixed test operation")
	}
}

func TestInlineSessionV2ExplicitPrivateTypeAndCanonicalOrigin(t *testing.T) {
	peer := &inlineSessionPeerV2{}
	delegate := &exchangeFixture{port: &exchangePortFixture{}}
	for _, origin := range []string{"", "http://inline-session.invalid", "https://user@inline-session.invalid", inlineSessionTestOriginV2 + "/", inlineSessionTestOriginV2 + PathV1, inlineSessionTestOriginV2 + "?", inlineSessionTestOriginV2 + "?q=1", inlineSessionTestOriginV2 + "#fragment", "https://inline-session.invalid:0", "https://inline-session.invalid:65536", "https:///missing", "%"} {
		t.Run(origin, func(t *testing.T) {
			if factory, err := NewInlineSessionSourceFactoryV2(origin, &http.Client{Transport: peer}, delegate); err == nil || factory != nil {
				t.Fatalf("untrusted origin accepted: %q", origin)
			}
		})
	}
	if factory, err := NewInlineSessionSourceFactoryV2(inlineSessionTestOriginV2, &http.Client{Transport: peer}, nil); err == nil || factory != nil {
		t.Fatal("nil delegate accepted")
	}
	inline := newInlineSessionFactoryV2(t, peer, delegate)
	ordinary, err := NewSessionSourceFactory(inlineSessionTestOriginV2+PathV1, &http.Client{Transport: peer}, delegate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ordinary.Close() })
	receipt, err := NewReceiptSessionSourceFactory(inlineSessionTestOriginV2+PathV1, inlineSessionTestOriginV2+attachedworkerreceipt.PathV1, &http.Client{Transport: peer}, delegate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receipt.Close() })
	for _, value := range []any{ordinary, receipt, inline.base} {
		if _, ok := value.(attachedworkerdaemontransport.InlineReceiptPublisherV2); ok {
			t.Fatal("ordinary/base factory acquired inline interface")
		}
	}
	typ := reflect.TypeFor[InlineSessionSourceFactoryV2]()
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.Anonymous || field.IsExported() {
			t.Fatalf("wrapper exposes base/bearer field: %s", field.Name)
		}
	}
	if peer.count() != 0 || delegate.calls != 0 {
		t.Fatalf("constructor performed HTTP/delegate effects: HTTP=%d opens=%d", peer.count(), delegate.calls)
	}
}

type inlineSessionBorrowedReaderV2 struct {
	*bytes.Reader
	closes atomic.Int32
}

type inlineSessionContextKeyV2 struct{}

func (r *inlineSessionBorrowedReaderV2) Close() error { r.closes.Add(1); return nil }

func TestInlineSessionV2OriginalBearerAllChannelsAndExactForwarding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := inlineSessionDTOFixtureV2(t)
		ctx, cancel := context.WithDeadline(context.WithValue(t.Context(), inlineSessionContextKeyV2{}, "original-call"), d.prepare.ExpiresAt)
		defer cancel()
		peer := &inlineSessionPeerV2{dto: d, deadline: d.prepare.ExpiresAt, contextValue: "original-call"}
		port := &inlineSessionPortV2{}
		var delegateBearer []byte
		factory := newInlineSessionFactoryV2(t, peer, receiptExchangeFactory(func(_ attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
			delegateBearer = bytes.Clone(bearer)
			return port, nil
		}))
		for _, op := range []string{"load", "publish", "prepare", "submit", "reconcile"} {
			if err := callInlineSessionV2(t, ctx, factory, d, op); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("pre-open %s err=%v", op, err)
			}
		}
		bearer := []byte(inlineSessionTestBearerV2)
		exchange, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, bearer)
		if err != nil {
			t.Fatal(err)
		}
		clear(bearer)
		if string(delegateBearer) != inlineSessionTestBearerV2 || peer.count() != 0 {
			t.Fatal("Open performed HTTP or changed original bearer")
		}
		defer clear(delegateBearer)
		if err := callInlineSessionV2(t, ctx, factory, d, "load"); err == nil {
			t.Fatal("controlled input401 accepted")
		}
		if err := callInlineSessionV2(t, ctx, factory, d, "publish"); err == nil {
			t.Fatal("controlled V1 failure401 accepted")
		}
		result, err := factory.Prepare(ctx, d.prepare)
		if err != nil || result.Stage != attachedworkeroutput.InlinePreparedV2 || result.Capability != d.body.Capability {
			t.Fatalf("Prepare forwarding: %+v %v", result, err)
		}
		reader := &inlineSessionBorrowedReaderV2{Reader: bytes.NewReader(d.bytes)}
		originalBody := bytes.Clone(d.bytes)
		result, err = factory.Submit(ctx, d.body, int64(len(d.bytes)), reader)
		if err != nil || result.Stage != attachedworkeroutput.InlineReadyV2 || reader.closes.Load() != 0 || !bytes.Equal(d.bytes, originalBody) {
			t.Fatalf("Submit forwarding/body ownership: stage=%s closes=%d err=%v", result.Stage, reader.closes.Load(), err)
		}
		result, err = factory.Reconcile(ctx, d.reconcile)
		if err != nil || result.Stage != attachedworkeroutput.InlineReadyV2 {
			t.Fatalf("Reconcile forwarding: %+v %v", result, err)
		}
		if peer.count() != 5 {
			t.Fatalf("channel calls=%d want exactly5", peer.count())
		}
		if err := exchange.(interface{ Close() error }).Close(); err != nil {
			t.Fatal(err)
		}
		for _, op := range []string{"load", "publish", "prepare", "submit", "reconcile"} {
			if err := callInlineSessionV2(t, ctx, factory, d, op); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("post-retirement %s err=%v", op, err)
			}
		}
		if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("replacement")); !errors.Is(err, ErrUnavailable) {
			t.Fatal("retired factory reopened", err)
		}
		if peer.count() != 5 || port.closes.Load() != 0 {
			t.Fatal("Session retirement performed HTTP/foreign cleanup")
		}
		if err := factory.Close(); err != nil || port.closes.Load() != 1 || peer.closeIdle.Load() != 0 {
			t.Fatalf("Close borrowed caller transport or lost exchange: closes=%d transport=%d err=%v", port.closes.Load(), peer.closeIdle.Load(), err)
		}
	})
}

func TestInlineSessionV2OpeningRetirementClearsAllThreeRegisteredClients(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := inlineSessionDTOFixtureV2(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		peer := &inlineSessionPeerV2{dto: d, deadline: d.prepare.ExpiresAt}
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		t.Cleanup(unblock)
		port := &inlineSessionPortV2{}
		factory := newInlineSessionFactoryV2(t, peer, receiptExchangeFactory(func(attachedworkersession.ConnectionBindingV1, []byte) (attachedworkersession.ExchangePort, error) {
			close(entered)
			select {
			case <-release:
				return port, nil
			case <-ctx.Done():
				return port, ctx.Err()
			}
		}))
		done := make(chan error, 1)
		go func() {
			_, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte(inlineSessionTestBearerV2))
			done <- err
		}()
		joined := false
		t.Cleanup(func() {
			unblock()
			cancel()
			if !joined {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("opening worker not joined")
				}
			}
		})
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("foreign Open not entered", ctx.Err())
		}
		base := factory.base
		base.mu.Lock()
		input, publisher, inline, state := base.source, base.publisher, base.inline, base.state
		base.mu.Unlock()
		if state != sourceOpening || input == nil || publisher == nil || inline == nil {
			t.Fatal("owned clients were invisible while foreign Open blocked")
		}
		ownedBearer := input.bearer
		if err := factory.RetireConnectionSourcesV1(); err != nil {
			t.Fatal(err)
		}
		base.mu.Lock()
		cleared := base.source == nil && base.publisher == nil && base.inline == nil && base.state == sourceClosed
		base.mu.Unlock()
		if !cleared || base.life.Err() == nil || !bytes.Equal(ownedBearer, make([]byte, len(ownedBearer))) {
			t.Fatal("opening retirement retained an owned client/bearer/lifetime")
		}
		if _, err := input.Load(ctx, d.request); err == nil {
			t.Fatal("retained input client still usable")
		}
		if _, err := publisher.Publish(ctx, d.failure); err == nil {
			t.Fatal("retained V1 client still usable")
		}
		if _, err := inline.Prepare(ctx, d.prepare); err == nil {
			t.Fatal("retained inline client still usable")
		}
		if err := factory.Close(); err != nil || port.closes.Load() != 0 {
			t.Fatal("Close waited for/unowned late exchange", err)
		}
		unblock()
		select {
		case err := <-done:
			joined = true
			if !errors.Is(err, ErrUnavailable) {
				t.Fatal("late Open installed", err)
			}
		case <-ctx.Done():
			t.Fatal("late Open not joined", ctx.Err())
		}
		if port.closes.Load() != 1 || peer.count() != 0 {
			t.Fatalf("late exchange cleanup/HTTP: closes=%d HTTP=%d", port.closes.Load(), peer.count())
		}
	})
}

func TestInlineSessionV2RetirementCancelsAllActiveChannelsBeforeForeignCleanup(t *testing.T) {
	for _, owner := range []string{"session", "factory"} {
		t.Run(owner, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := inlineSessionDTOFixtureV2(t)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				peer := &inlineSessionPeerV2{dto: d, entered: make(chan string, 5)}
				port := &inlineSessionPortV2{entered: make(chan struct{}), release: make(chan struct{})}
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(port.release) }) }
				t.Cleanup(release)
				factory := newInlineSessionFactoryV2(t, peer, receiptExchangeFactory(func(attachedworkersession.ConnectionBindingV1, []byte) (attachedworkersession.ExchangePort, error) {
					return port, nil
				}))
				exchange, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte(inlineSessionTestBearerV2))
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 5)
				for _, op := range []string{"load", "publish", "prepare", "submit", "reconcile"} {
					go func() { done <- callInlineSessionV2(t, ctx, factory, d, op) }()
				}
				joined := 0
				t.Cleanup(func() {
					release()
					cancel()
					for joined < 5 {
						select {
						case <-done:
							joined++
						case <-time.After(time.Second):
							t.Error("active channel not joined")
							return
						}
					}
				})
				seen := map[string]bool{}
				for range 5 {
					select {
					case path := <-peer.entered:
						seen[path] = true
					case <-ctx.Done():
						t.Fatal("active channels not entered", ctx.Err())
					}
				}
				if len(seen) != 5 {
					t.Fatalf("not all five channels exercised: %v", seen)
				}
				closeDone := make(chan error, 1)
				if owner == "session" {
					if err := exchange.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					go func() { closeDone <- factory.Close() }()
					select {
					case <-port.entered:
					case <-ctx.Done():
						t.Fatal("foreign Close not entered", ctx.Err())
					}
				}
				if err := factory.RetireConnectionSourcesV1(); err != nil {
					t.Fatal("retirement blocked behind foreign cleanup", err)
				}
				if owner == "factory" {
					// This concurrent empty Close is not an aggregate quiescence
					// assertion: the original foreign cleanup remains held below.
					if err := factory.Close(); err != nil || port.closes.Load() != 1 {
						t.Fatal("concurrent Close reacquired foreign cleanup", err)
					}
				}
				for joined < 5 {
					select {
					case err := <-done:
						joined++
						if err == nil || ctx.Err() != nil {
							t.Fatalf("owner retirement failed independent cancellation: err=%v parent=%v", err, ctx.Err())
						}
					case <-ctx.Done():
						t.Fatal("active channels did not cancel", ctx.Err())
					}
				}
				for _, op := range []string{"load", "publish", "prepare", "submit", "reconcile"} {
					if err := callInlineSessionV2(t, ctx, factory, d, op); !errors.Is(err, ErrUnavailable) {
						t.Fatalf("closed %s: %v", op, err)
					}
				}
				if peer.count() != 5 {
					t.Fatalf("retirement retried requests: %d", peer.count())
				}
				release()
				if owner == "factory" {
					select {
					case err := <-closeDone:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("foreign Close not joined", ctx.Err())
					}
				} else {
					if err := factory.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if port.closes.Load() != 1 {
					t.Fatalf("foreign cleanup calls=%d want1", port.closes.Load())
				}
			})
		})
	}
}

func TestInlineSessionV2FailedOpenAndFirstCleanupErrorRemainFenced(t *testing.T) {
	for _, mode := range []string{"invalid-bearer", "delegate-error", "nil-port", "preclosed", "close-error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				peer := &inlineSessionPeerV2{}
				port := &inlineSessionPortV2{}
				var opens atomic.Int32
				factory := newInlineSessionFactoryV2(t, peer, receiptExchangeFactory(func(attachedworkersession.ConnectionBindingV1, []byte) (attachedworkersession.ExchangePort, error) {
					opens.Add(1)
					if mode == "delegate-error" {
						return port, errors.New("private-open-detail")
					}
					if mode == "nil-port" {
						return nil, nil
					}
					return port, nil
				}))
				if mode == "preclosed" {
					if err := factory.Close(); err != nil {
						t.Fatal(err)
					}
				}
				bearer := []byte(inlineSessionTestBearerV2)
				if mode == "invalid-bearer" {
					bearer = []byte{1}
				}
				_, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, bearer)
				if mode == "close-error" {
					if err != nil {
						t.Fatal(err)
					}
					port.err = errors.New("private-close-detail")
					err = factory.Close()
					if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "private-close-detail") {
						t.Fatal("first foreign cleanup error lost/leaked", err)
					}
					if err := factory.Close(); err != nil || port.closes.Load() != 1 {
						t.Fatal("foreign cleanup automatically repeated", err)
					}
				} else if err == nil {
					t.Fatal("failed Open installed authority")
				}
				if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte(inlineSessionTestBearerV2)); !errors.Is(err, ErrUnavailable) {
					t.Fatal("failed/closed factory reopened", err)
				}
				factory.base.mu.Lock()
				closed := factory.base.state == sourceClosed && factory.base.source == nil && factory.base.publisher == nil && factory.base.inline == nil
				factory.base.mu.Unlock()
				if !closed || peer.count() != 0 {
					t.Fatalf("failure retained clients/performed HTTP: closed=%t calls=%d", closed, peer.count())
				}
				wantOpens := int32(1)
				if mode == "invalid-bearer" || mode == "preclosed" {
					wantOpens = 0
				}
				if opens.Load() != wantOpens {
					t.Fatalf("delegate opens=%d want%d", opens.Load(), wantOpens)
				}
				wantCloses := int32(1)
				if mode == "invalid-bearer" || mode == "preclosed" || mode == "nil-port" {
					wantCloses = 0
				}
				if port.closes.Load() != wantCloses {
					t.Fatalf("foreign cleanup calls=%d want%d", port.closes.Load(), wantCloses)
				}
			})
		})
	}
}

func TestInlineSessionV2NoRetryRedirectOrBorrowedTransportClose(t *testing.T) {
	for _, mode := range []string{"unknown", "redirect"} {
		for _, op := range []string{"prepare", "submit", "reconcile"} {
			t.Run(mode+"/"+op, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					d := inlineSessionDTOFixtureV2(t)
					ctx, cancel := context.WithDeadline(t.Context(), d.prepare.ExpiresAt)
					defer cancel()
					peer := &inlineSessionPeerV2{dto: d, deadline: d.prepare.ExpiresAt, fail: mode == "unknown", redirect: mode == "redirect"}
					var redirects atomic.Int32
					jar := &inlineSessionJarV2{}
					client := &http.Client{Transport: peer, Jar: jar, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { redirects.Add(1); return nil }}
					redirectPointer := reflect.ValueOf(client.CheckRedirect).Pointer()
					factory, err := NewInlineSessionSourceFactoryV2(inlineSessionTestOriginV2, client, &exchangeFixture{port: &exchangePortFixture{}})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = factory.Close() })
					if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte(inlineSessionTestBearerV2)); err != nil {
						t.Fatal(err)
					}
					if err := callInlineSessionV2(t, ctx, factory, d, op); err == nil {
						t.Fatal("unknown/redirect response inferred success")
					}
					if err := factory.Close(); err != nil {
						t.Fatal(err)
					}
					if peer.count() != 1 || redirects.Load() != 0 || jar.calls.Load() != 0 || peer.closeIdle.Load() != 0 {
						t.Fatalf("retry/redirect/ambient jar/transport close: HTTP=%d redirect=%d jar=%d close=%d", peer.count(), redirects.Load(), jar.calls.Load(), peer.closeIdle.Load())
					}
					if client.Transport != peer || client.Jar != jar || client.Timeout != time.Minute || reflect.ValueOf(client.CheckRedirect).Pointer() != redirectPointer {
						t.Fatal("factory mutated caller client configuration")
					}
				})
			})
		}
	}
}

func TestInlineSessionV2PreservesEarlierCallerAndExportDeadline(t *testing.T) {
	for _, bound := range []string{"caller-earlier", "export-earlier"} {
		for _, operation := range []string{"prepare", "submit", "reconcile"} {
			t.Run(bound+"/"+operation, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					d := inlineSessionDTOFixtureV2(t)
					callerDeadline := time.Now().UTC().Add(4 * time.Second)
					if bound == "caller-earlier" {
						callerDeadline = time.Now().UTC().Add(2 * time.Second)
					}
					want := callerDeadline
					if operation != "reconcile" && d.prepare.ExpiresAt.Before(want) {
						want = d.prepare.ExpiresAt
					}
					ctx, cancel := context.WithDeadline(t.Context(), callerDeadline)
					defer cancel()
					peer := &inlineSessionPeerV2{dto: d, deadline: want}
					factory := newInlineSessionFactoryV2(t, peer, &exchangeFixture{port: &exchangePortFixture{}})
					if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte(inlineSessionTestBearerV2)); err != nil {
						t.Fatal(err)
					}
					if err := callInlineSessionV2(t, ctx, factory, d, operation); err != nil {
						t.Fatalf("original %s budget: %v", bound, err)
					}
					if peer.count() != 1 {
						t.Fatal("single bounded forwarding performed extra HTTP")
					}
				})
			})
		}
	}
}

type inlineSessionJarV2 struct{ calls atomic.Int32 }

func (j *inlineSessionJarV2) SetCookies(*url.URL, []*http.Cookie) { j.calls.Add(1) }
func (j *inlineSessionJarV2) Cookies(*url.URL) []*http.Cookie     { j.calls.Add(1); return nil }

func TestInlineSessionV2NilAndCancelledCallsReturnNoAuthority(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := inlineSessionDTOFixtureV2(t)
		var absent *InlineSessionSourceFactoryV2
		for _, op := range []string{"load", "publish", "prepare", "submit", "reconcile"} {
			if err := callInlineSessionV2(t, t.Context(), absent, d, op); err == nil {
				t.Fatal("nil factory returned authority", op)
			}
		}
		if absent.Close() != nil || absent.RetireConnectionSourcesV1() != nil {
			t.Fatal("nil cleanup not idempotent")
		}
		peer := &inlineSessionPeerV2{dto: d, deadline: d.prepare.ExpiresAt}
		factory := newInlineSessionFactoryV2(t, peer, &exchangeFixture{port: &exchangePortFixture{}})
		if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte(inlineSessionTestBearerV2)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		for _, op := range []string{"load", "publish", "prepare", "submit", "reconcile"} {
			if err := callInlineSessionV2(t, ctx, factory, d, op); err == nil {
				t.Fatal("cancelled call returned authority", op)
			}
		}
		if peer.count() != 0 {
			t.Fatal("nil/cancelled calls performed HTTP")
		}
	})
}
