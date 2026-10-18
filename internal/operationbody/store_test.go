package operationbody_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

func bodyScope() domain.AttemptOperationScopeV1 {
	h := strings.Repeat("a", 64)
	return domain.AttemptOperationScopeV1{TenantID: "body-tenant", OwnerUserID: "body-owner", SessionID: "body-session", RunID: "body-run", AttemptID: "body-attempt", LeaseID: "body-lease", FenceToken: 1, PhysicalInvocationClaimID: "body-claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}
}
func bodyLimits() domain.AgentLoopLimitsV1 {
	return domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 2, MaxCompactionCalls: 1, MaxToolDispatches: 1, MaxWebSearches: 1, MaxInputTokensPerCall: 10, MaxAggregateInputTokens: 20, MaxOutputTokensPerCall: 10, MaxAggregateOutputTokens: 20, MaxActiveDuration: time.Minute, MaxNetworkOperationDuration: 4 * time.Second, MaxCleanupDuration: time.Second, MaxContextViewBytes: 1024, MaxSummaryBytes: 256, MaxToolArgumentBytes: 256, MaxToolResultBytes: 1024, MaxMCPOutboundMessages: 1, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 1, MaxMCPInboundBytes: 1024, MaxOperations: 2, MaxAggregateBodyBytes: 1024}
}

// Owned cooperative fake storage, not provider/Store authority or actual S3
// immutability/absence proof. Its context-honoring operations allow deterministic
// cancellation tests without unjoinable I/O, goroutine wrappers or real network.
type bodyBlobs struct {
	mu                                      sync.Mutex
	objects                                 map[string][]byte
	puts, opens, closes, deletes, readBytes int
	keys                                    []string
	deadlines                               []time.Time
	missingDeadline                         bool
	putFault, openFault, forge              string
	closeFault                              bool
	blockPut, blockOpen                     <-chan struct{}
	entered                                 chan struct{}
}

func newBodyBlobs() *bodyBlobs { return &bodyBlobs{objects: make(map[string][]byte)} }
func (b *bodyBlobs) deadline(ctx context.Context) {
	d, ok := ctx.Deadline()
	if !ok {
		b.missingDeadline = true
	}
	b.deadlines = append(b.deadlines, d)
}
func (b *bodyBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	b.mu.Lock()
	b.puts++
	b.keys = append(b.keys, key)
	b.deadline(ctx)
	block, entered, fault, forge := b.blockPut, b.entered, b.putFault, b.forge
	b.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return domain.BlobRef{}, ctx.Err()
		}
	}
	if ctx.Err() != nil {
		return domain.BlobRef{}, ctx.Err()
	}
	raw, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil {
		return domain.BlobRef{}, err
	}
	h := sha256.Sum256(raw)
	ref := domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(raw)), SHA256: hex.EncodeToString(h[:])}
	b.mu.Lock()
	b.objects[key] = bytes.Clone(raw)
	b.mu.Unlock()
	if fault != "" {
		return domain.BlobRef{}, errors.New("owned ambiguous PUT fixture")
	}
	switch forge {
	case "tenant":
		ref.TenantID = "foreign-tenant"
	case "key":
		ref.Key += ".foreign"
	case "size":
		ref.Size++
	case "digest":
		ref.SHA256 = strings.Repeat("b", 64)
	case "uppercase":
		ref.SHA256 = strings.ToUpper(ref.SHA256)
	}
	return ref, nil
}
func (b *bodyBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.mu.Lock()
	b.opens++
	b.deadline(ctx)
	raw, found := b.objects[ref.Key]
	raw = bytes.Clone(raw)
	fault, closeFault, block, entered := b.openFault, b.closeFault, b.blockOpen, b.entered
	b.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !found || tenant != ref.TenantID {
		return nil, errors.New("owned fixture object missing/foreign")
	}
	if fault == "open" {
		return nil, errors.New("owned open failure")
	}
	var reader io.Reader = bytes.NewReader(raw)
	if fault == "corrupt" && len(raw) > 0 {
		raw[0] ^= 1
		reader = bytes.NewReader(raw)
	}
	if fault == "oversize" {
		reader = io.LimitReader(repeatingBodyReader{}, 2<<20)
	}
	if fault == "read" {
		reader = bodyErrorReader{}
	}
	return &bodyReader{owner: b, ctx: ctx, reader: reader, closeFault: closeFault}, nil
}
func (b *bodyBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes++
	return errors.New("unexpected delete")
}
func (b *bodyBlobs) counts() (int, int, int, int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.puts, b.opens, b.closes, b.deletes, b.readBytes
}

type repeatingBodyReader struct{}

func (repeatingBodyReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

type bodyErrorReader struct{}

func (bodyErrorReader) Read([]byte) (int, error) { return 0, errors.New("owned read failure") }

type bodyReader struct {
	owner              *bodyBlobs
	ctx                context.Context
	reader             io.Reader
	closeFault, closed bool
}

func (r *bodyReader) Read(p []byte) (int, error) {
	if r.ctx.Err() != nil {
		return 0, r.ctx.Err()
	}
	n, err := r.reader.Read(p)
	r.owner.mu.Lock()
	r.owner.readBytes += n
	r.owner.mu.Unlock()
	return n, err
}
func (r *bodyReader) Close() error {
	if !r.closed {
		r.closed = true
		r.owner.mu.Lock()
		r.owner.closes++
		r.owner.mu.Unlock()
	}
	if r.closeFault {
		return errors.New("owned close failure")
	}
	return nil
}

func newBodyStore(t *testing.T, b *bodyBlobs) *operationbody.StoreV1 {
	t.Helper()
	s, err := operationbody.NewV1(bodyScope(), bodyLimits(), b)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOperationBodyCanonicalIdentityOwnedRoundTripAndScopedNamespace(t *testing.T) {
	b := newBodyBlobs()
	s := newBodyStore(t, b)
	if p, o, _, d, _ := b.counts(); p != 0 || o != 0 || d != 0 {
		t.Fatal("constructor performed blob I/O")
	}
	body := []byte("bounded original bytes")
	want := bytes.Clone(body)
	r, err := s.Write(t.Context(), body)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(want)
	digest := hex.EncodeToString(h[:])
	expected := "opbody-v1." + strconv.Itoa(len(want)) + "." + digest
	if r.Ref != expected || r.Digest != digest || r.Bytes != uint64(len(want)) {
		t.Fatalf("canonical raw body reference changed: got=%s want=%s", r.Ref, expected)
	}
	clear(body)
	got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), r.Ref, 1<<20)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("bounded verified roundtrip: bytes=%d err=%v", len(got), err)
	}
	clear(got)
	again, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), r.Ref, 1024)
	if err != nil || !bytes.Equal(again, want) {
		t.Fatal("returned body aliases custody")
	}
	b.mu.Lock()
	key := b.keys[0]
	missing := b.missingDeadline
	b.mu.Unlock()
	prefix := domain.SessionRunObjectPrefix(bodyScope().TenantID, bodyScope().SessionID, bodyScope().RunID) + "attempt-operations-v1/"
	tail := strings.TrimPrefix(key, prefix)
	scopeHash, ref, ok := strings.Cut(tail, "/")
	if !strings.HasPrefix(key, prefix) || !ok || len(scopeHash) != 64 || ref != expected || missing {
		t.Fatalf("fixed namespace/deadline missing: key=%s missingDeadline=%t", key, missing)
	}
	if _, err := hex.DecodeString(scopeHash); err != nil {
		t.Fatal("scope namespace is not a SHA256 digest")
	}
	other := bodyScope()
	other.FenceToken++
	second, err := operationbody.NewV1(other, bodyLimits(), b)
	if err != nil {
		t.Fatal(err)
	}
	otherRef, err := second.Write(t.Context(), want)
	if err != nil || otherRef != r {
		t.Fatal("raw reference should describe identical bytes, not supply scope authority")
	}
	b.mu.Lock()
	otherKey := b.keys[1]
	b.mu.Unlock()
	if key == otherKey {
		t.Fatal("different full scope reused original object namespace")
	}
	if p, o, c, d, _ := b.counts(); p != 2 || o != 4 || c != 4 || d != 0 {
		t.Fatalf("unexpected owned I/O: put=%d open=%d close=%d delete=%d", p, o, c, d)
	}
}

func TestOperationBodyInvalidConstructorsAndReadsBeforeBlobIO(t *testing.T) {
	b := newBodyBlobs()
	scope, limits := bodyScope(), bodyLimits()
	for _, tc := range []struct {
		name     string
		scope    domain.AttemptOperationScopeV1
		limits   domain.AgentLoopLimitsV1
		nilBlobs bool
	}{
		{"missing_scope", domain.AttemptOperationScopeV1{}, limits, false},
		{"nil_blobs", scope, limits, true},
		{"missing_limits", scope, domain.AgentLoopLimitsV1{}, false},
		{"nonnormal_session", func() domain.AttemptOperationScopeV1 { x := scope; x.SessionID = "."; return x }(), limits, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.nilBlobs {
				_, err = operationbody.NewV1(tc.scope, tc.limits, nil)
			} else {
				_, err = operationbody.NewV1(tc.scope, tc.limits, b)
			}
			if err == nil {
				t.Fatal("invalid constructor accepted")
			}
		})
	}
	s := newBodyStore(t, b)
	r, err := s.Write(t.Context(), []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	p0, o0, _, _, _ := b.counts()
	foreign := scope
	foreign.PhysicalInvocationClaimID = "other-claim"
	for _, tc := range []struct {
		name, ref string
		scope     domain.AttemptOperationScopeV1
		cap       uint64
	}{
		{"foreign_scope", r.Ref, foreign, 4},
		{"leading_zero", strings.Replace(r.Ref, ".4.", ".04.", 1), scope, 4},
		{"uppercase_hash", "opbody-v1.4." + strings.ToUpper(r.Digest), scope, 4},
		{"zero_size", "opbody-v1.0." + r.Digest, scope, 4},
		{"path_injection", "../" + r.Ref, scope, 4},
		{"too_small_cap", r.Ref, scope, 3},
		{"zero_cap", r.Ref, scope, 0},
		{"global_cap_overflow", r.Ref, scope, (1 << 20) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := s.ReadAttemptOperationBody(t.Context(), tc.scope, tc.ref, tc.cap); err == nil || got != nil {
				t.Fatalf("invalid read returned bytes=%d err=%v", len(got), err)
			}
		})
	}
	if p, o, _, d, _ := b.counts(); p != p0 || o != o0 || d != 0 {
		t.Fatal("invalid scope/ref/cap reached blob I/O")
	}
	var nilStore *operationbody.StoreV1
	var zero operationbody.StoreV1
	for _, store := range []*operationbody.StoreV1{nilStore, &zero} {
		if _, err := store.Write(t.Context(), []byte("x")); err == nil {
			t.Fatal("unconstructed store wrote")
		}
		if got, err := store.ReadAttemptOperationBody(t.Context(), scope, r.Ref, 4); err == nil || got != nil {
			t.Fatal("unconstructed store read")
		}
	}
}

func TestOperationBodyWriteUncertaintyLatchesButRetainsVerifiedReads(t *testing.T) {
	for _, fault := range []string{"put", "tenant", "key", "size", "digest", "uppercase", "open", "corrupt", "read", "close"} {
		t.Run(fault, func(t *testing.T) {
			b := newBodyBlobs()
			s := newBodyStore(t, b)
			original, err := s.Write(t.Context(), []byte("retained"))
			if err != nil {
				t.Fatal(err)
			}
			b.mu.Lock()
			switch fault {
			case "put":
				b.putFault = "ambiguous"
			case "tenant", "key", "size", "digest", "uppercase":
				b.forge = fault
			case "close":
				b.closeFault = true
			default:
				b.openFault = fault
			}
			b.mu.Unlock()
			if r, err := s.Write(t.Context(), []byte("second")); err == nil || r.Ref != "" || r.Bytes != 0 {
				t.Fatal("uncertain write returned usable reference")
			}
			p0, _, _, _, _ := b.counts()
			if p0 != 2 {
				t.Fatalf("uncertain write retried or skipped actual attempt: puts=%d", p0)
			}
			b.mu.Lock()
			b.putFault, b.forge, b.openFault = "", "", ""
			b.closeFault = false
			b.mu.Unlock()
			if _, err := s.Write(t.Context(), []byte("third")); err == nil {
				t.Fatal("write uncertainty did not latch future writes")
			}
			if p, _, _, d, _ := b.counts(); p != p0 || d != 0 {
				t.Fatal("uncertain write retried/deleted original namespace")
			}
			got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), original.Ref, 1024)
			if err != nil || string(got) != "retained" {
				t.Fatalf("write failure removed retained reconciliation reads: %v", err)
			}
		})
	}
}

func TestOperationBodyActualReadBoundsCorruptionAndCloseFailure(t *testing.T) {
	for _, fault := range []string{"oversize", "corrupt", "read", "close"} {
		t.Run(fault, func(t *testing.T) {
			b := newBodyBlobs()
			s := newBodyStore(t, b)
			r, err := s.Write(t.Context(), []byte("body"))
			if err != nil {
				t.Fatal(err)
			}
			_, _, closes0, _, bytes0 := b.counts()
			b.mu.Lock()
			if fault == "close" {
				b.closeFault = true
			} else {
				b.openFault = fault
			}
			b.mu.Unlock()
			if got, err := s.ReadAttemptOperationBody(t.Context(), bodyScope(), r.Ref, 4); err == nil || got != nil {
				t.Fatalf("corrupt/broken actual stream escaped: bytes=%d err=%v", len(got), err)
			}
			_, _, closes, _, read := b.counts()
			if closes != closes0+1 || read-bytes0 > 5 {
				t.Fatalf("actual bounded stream/close invariant: read=%d cap+1=5 closes=%d", read-bytes0, closes-closes0)
			}
		})
	}
}

func TestOperationBodyCapacityChargedPerActualWriteAndSharedByCopies(t *testing.T) {
	b := newBodyBlobs()
	limits := bodyLimits()
	limits.MaxOperations = 1
	s, err := operationbody.NewV1(bodyScope(), limits, b)
	if err != nil {
		t.Fatal(err)
	}
	copyStore := *s
	if _, err := s.Write(t.Context(), []byte("same")); err != nil {
		t.Fatal(err)
	}
	if _, err := copyStore.Write(t.Context(), []byte("same")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(t.Context(), []byte("same")); err == nil {
		t.Fatal("wrapper copy or duplicate body refunded actual write capacity")
	}
	if p, _, _, _, _ := b.counts(); p != 2 {
		t.Fatalf("physical attempt guard puts=%d want2", p)
	}
	b = newBodyBlobs()
	s = newBodyStore(t, b)
	for range 2 {
		if _, err := s.Write(t.Context(), bytes.Repeat([]byte{'b'}, 512)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Write(t.Context(), []byte("x")); err == nil {
		t.Fatal("aggregate physical byte ceiling refunded repeated content")
	}
	if p, _, _, d, _ := b.counts(); p != 2 || d != 0 {
		t.Fatal("exhausted bytes performed another PUT/Delete")
	}
}

func TestOperationBodyFailFastSharedGateAndOwnedClone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) })
		entered := make(chan struct{}, 1)
		b.blockPut, b.entered = release, entered
		s := newBodyStore(t, b)
		copied := *s
		body := []byte("before")
		winner := make(chan error, 1)
		go func() { _, err := s.Write(t.Context(), body); winner <- err }()
		<-entered
		copy(body, []byte("after!"))
		duplicate := make(chan error, 1)
		go func() { _, err := copied.Write(t.Context(), []byte("duplicate")); duplicate <- err }()
		read := make(chan error, 1)
		go func() {
			_, err := copied.ReadAttemptOperationBody(t.Context(), bodyScope(), "opbody-v1.6."+strings.Repeat("a", 64), 6)
			read <- err
		}()
		synctest.Wait()
		select {
		case err := <-duplicate:
			if err == nil {
				t.Error("concurrent copied writer admitted")
			}
		default:
			t.Error("concurrent copied writer queued instead of failing fast")
		}
		select {
		case err := <-read:
			if err == nil {
				t.Error("read overlapped active writer")
			}
		default:
			t.Error("read queued behind active writer")
		}
		once.Do(func() { close(release) })
		if err := <-winner; err != nil {
			t.Fatal(err)
		}
		b.mu.Lock()
		stored := bytes.Clone(b.objects[b.keys[0]])
		b.blockPut, b.entered = nil, nil
		b.mu.Unlock()
		if string(stored) != "before" {
			t.Fatal("caller mutation changed privately cloned write bytes")
		}
		if _, err := copied.Write(t.Context(), []byte("next")); err != nil {
			t.Fatalf("concurrency denial tainted original or charged extra I/O: %v", err)
		}
		if p, _, _, d, _ := b.counts(); p != 2 || d != 0 {
			t.Fatalf("failfast duplicate reached backend: put=%d delete=%d", p, d)
		}
	})
}

func TestOperationBodyCallDeadlineUsesMinimumAndCancellationLatchesWrite(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		network, caller, want time.Duration
	}{{"policy", 4 * time.Second, 0, 4 * time.Second}, {"global", 30 * time.Second, 0, 10 * time.Second}, {"caller", 4 * time.Second, time.Second, time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBodyBlobs()
				b.blockPut = make(chan struct{})
				limits := bodyLimits()
				limits.MaxNetworkOperationDuration = tc.network
				s, err := operationbody.NewV1(bodyScope(), limits, b)
				if err != nil {
					t.Fatal(err)
				}
				ctx := t.Context()
				if tc.caller > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.caller)
					defer cancel()
				}
				at := time.Now()
				if _, err := s.Write(ctx, []byte("timed")); err == nil {
					t.Fatal("blocked cooperative PUT ignored finite call deadline")
				}
				b.mu.Lock()
				deadline, missing := b.deadlines[0], b.missingDeadline
				b.blockPut = nil
				b.mu.Unlock()
				if missing || !deadline.Equal(at.Add(tc.want)) {
					t.Fatalf("I/O deadline=%s want=%s", deadline, at.Add(tc.want))
				}
				if _, err := s.Write(t.Context(), []byte("after")); err == nil {
					t.Fatal("timed-out write uncertainty was retried")
				}
				if p, o, _, d, _ := b.counts(); p != 1 || o != 0 || d != 0 {
					t.Fatal("timeout caused hidden retry/read/delete")
				}
			})
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	b := newBodyBlobs()
	s := newBodyStore(t, b)
	if _, err := s.Write(ctx, []byte("cancelled")); err == nil {
		t.Fatal("cancelled call wrote")
	}
	if p, o, _, _, _ := b.counts(); p != 0 || o != 0 {
		t.Fatal("pre-cancelled call reached backend")
	}
}

func TestOperationBodyReadCancellationUsesSameFailFastGateAcrossCopies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		s := newBodyStore(t, b)
		copied := *s
		ref, err := s.Write(t.Context(), []byte("retained"))
		if err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) })
		entered := make(chan struct{}, 1)
		b.mu.Lock()
		b.blockOpen, b.entered = release, entered
		b.mu.Unlock()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		reader := make(chan error, 1)
		go func() { _, err := s.ReadAttemptOperationBody(ctx, bodyScope(), ref.Ref, 1024); reader <- err }()
		<-entered
		writer := make(chan error, 1)
		go func() { _, err := copied.Write(t.Context(), []byte("overlap")); writer <- err }()
		otherReader := make(chan error, 1)
		go func() {
			_, err := copied.ReadAttemptOperationBody(t.Context(), bodyScope(), ref.Ref, 1024)
			otherReader <- err
		}()
		synctest.Wait()
		select {
		case err := <-writer:
			if err == nil {
				t.Error("writer overlapped original active reader")
			}
		default:
			t.Error("writer queued behind active reader")
		}
		select {
		case err := <-otherReader:
			if err == nil {
				t.Error("second copied reader overlapped active reader")
			}
		default:
			t.Error("copied reader queued instead of failing fast")
		}
		cancel()
		if err := <-reader; err == nil {
			t.Fatal("cooperative blocked read ignored caller cancellation")
		}
		once.Do(func() { close(release) })
		b.mu.Lock()
		b.blockOpen, b.entered = nil, nil
		b.mu.Unlock()
		if p, o, _, d, _ := b.counts(); p != 1 || o != 2 || d != 0 {
			t.Fatalf("shared read gate leaked concurrent backend work: put=%d open=%d delete=%d", p, o, d)
		}
	})
}
