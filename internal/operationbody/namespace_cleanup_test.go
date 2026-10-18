package operationbody_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Cooperative fake immutable storage and explicit fake absence observation.
// No Store retirement/custody authorization, actual deletion service or S3
// consistency is claimed. Every destructive call remains exact and observable.
type cleanupBlobs struct {
	*bodyBlobs
	fault           string
	absences        int
	deleted         []domain.BlobRef
	cleanupDeadline time.Time
	deadlineChanged bool
	latePut         chan pendingCleanupPut
}

type pendingCleanupPut struct {
	key  string
	body []byte
}

func (b *cleanupBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, raw io.Reader) (domain.BlobRef, error) {
	if b.latePut != nil {
		body, err := io.ReadAll(io.LimitReader(raw, (1<<20)+1))
		if err != nil || len(body) > 1<<20 {
			return domain.BlobRef{}, errors.New("bounded pending PUT fixture")
		}
		b.mu.Lock()
		b.puts++
		b.keys = append(b.keys, key)
		b.mu.Unlock()
		b.latePut <- pendingCleanupPut{key: key, body: body}
		return domain.BlobRef{}, errors.New("remote PUT outcome still pending")
	}
	if b.fault == "put_absent" {
		b.mu.Lock()
		b.puts++
		b.keys = append(b.keys, key)
		b.mu.Unlock()
		return domain.BlobRef{}, errors.New("ambiguous PUT without persistence")
	}
	return b.bodyBlobs.Put(ctx, tenant, key, raw)
}
func (b *cleanupBlobs) cleanupTime(ctx context.Context) {
	d, ok := ctx.Deadline()
	if !ok || (!b.cleanupDeadline.IsZero() && !b.cleanupDeadline.Equal(d)) {
		b.deadlineChanged = true
	}
	b.cleanupDeadline = d
}
func (b *cleanupBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes++
	b.cleanupTime(ctx)
	if ctx.Err() != nil || tenant != ref.TenantID {
		return errors.New("invalid owned delete")
	}
	b.deleted = append(b.deleted, ref)
	if b.fault == "delete_before" {
		return errors.New("owned Delete unknown before effect")
	}
	delete(b.objects, ref.Key)
	if b.fault == "delete_after" {
		return errors.New("owned Delete ACK lost")
	}
	return nil
}
func (b *cleanupBlobs) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.absences++
	b.cleanupTime(ctx)
	if ctx.Err() != nil || tenant != ref.TenantID || b.fault == "absence" {
		return errors.New("absence unavailable")
	}
	if _, present := b.objects[ref.Key]; present {
		return errors.New("exact object still present")
	}
	return nil
}
func cleanupOwner(t *testing.T, blobs ports.BlobStore, limits domain.AgentLoopLimitsV1) (*operationbody.StoreV1, *operationbody.TerminalOutputV1) {
	t.Helper()
	if err := limits.Validate(); err != nil {
		t.Fatalf("valid original policy fixture required: %v", err)
	}
	s, err := operationbody.NewV1(bodyScope(), limits, blobs)
	if err != nil {
		t.Fatal(err)
	}
	p, sub := terminalOutputLimits(8, 1<<20)
	owner, err := s.AdmitTerminalOutput(t.Context(), p, sub)
	if err != nil {
		t.Fatal(err)
	}
	return s, owner
}
func cleanupContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	return ctx, cancel
}

func TestOperationNamespaceCleanupExactOriginalInventory(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	s, owner := cleanupOwner(t, b, retainedLimits())
	// No durable operation is fabricated: even unreserved successful bodies are
	// the original physical owner's residue and must be enumerated.
	var total uint64
	for _, body := range []string{"unreserved request", "unreserved result", "unreserved request"} {
		if _, err := s.Write(t.Context(), []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	total = uint64(len("unreserved request") + len("unreserved result"))
	pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	summary, manifest := []byte("retained summary"), []byte("retained manifest")
	plan := retainedPlan(t, 1, 1, summary, manifest)
	if err := pair.Write(t.Context(), plan, summary, manifest); err != nil {
		t.Fatal(err)
	}
	capsule := []byte("private capsule fixture")
	cp := terminalCapsulePlan(t, capsule)
	if err := owner.WriteCapsule(t.Context(), cp, capsule); err != nil {
		t.Fatal(err)
	}
	ctx, _ := cleanupContext(t)
	r, err := owner.CloseOperationNamespace(ctx)
	if err != nil || r == nil {
		t.Fatal("known original namespace cleanup", err)
	}
	if err := owner.VerifyNamespaceErasure(r); err != nil {
		t.Fatal("original physical receipt", err)
	}
	count, ce := r.DeletedBodies()
	bytes, be := r.DeletedBytes()
	scope, se := r.Scope()
	ns, ne := r.NamespaceDigest()
	inv, ie := r.InventoryDigest()
	at, ae := r.ClosedAt()
	if ce != nil || be != nil || se != nil || ne != nil || ie != nil || ae != nil || count != 2 || bytes != total || scope != bodyScope() || len(ns) != 64 || len(inv) != 64 || at.IsZero() || at.Nanosecond()%1000 != 0 {
		t.Fatal("exact content-free observation", count, bytes, ce, be, se, ne, ie, ae)
	}
	if _, err := json.Marshal(r); err == nil {
		t.Error("serialized physical observation")
	}
	copyReceipt := *r
	if owner.VerifyNamespaceErasure(&copyReceipt) == nil {
		t.Error("copied receipt minted original ownership")
	}
	_, foreign := cleanupOwner(t, &cleanupBlobs{bodyBlobs: newBodyBlobs()}, retainedLimits())
	if foreign.VerifyNamespaceErasure(r) == nil {
		t.Error("foreign same-scope owner accepted receipt")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deletes != 2 || b.absences != 2 || b.deadlineChanged || len(b.deleted) != 2 {
		t.Error("exact synchronous delete/absence calls", b.deletes, b.absences, b.deadlineChanged)
	}
	for _, ref := range []domain.BlobRef{plan.Summary, plan.Manifest, cp.Capsule} {
		if _, ok := b.objects[ref.Key]; !ok {
			t.Error("cleanup erased retained/public namespace", ref.Key)
		}
	}
}

func TestOperationNamespaceCleanupFencesAllOriginalCopies(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	s, owner := cleanupOwner(t, b, retainedLimits())
	alias := *s
	copyOwner := *owner
	if _, err := s.Write(t.Context(), []byte("known")); err != nil {
		t.Fatal(err)
	}
	pair, err := s.ReserveRetainedPair(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	visible, err := owner.ReserveVisibleOutput(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := cleanupContext(t)
	if _, err := copyOwner.CloseOperationNamespace(ctx); err != nil {
		t.Fatal(err)
	}
	puts, opens, _, deletes, _ := b.counts()
	if _, err := alias.Write(t.Context(), []byte("late")); err == nil {
		t.Error("body alias wrote after fence")
	}
	if _, err := alias.ReserveRetainedPair(t.Context(), 2, 2); err == nil {
		t.Error("retained reservation after fence")
	}
	if _, err := copyOwner.ReserveVisibleOutput(t.Context(), 1); err == nil {
		t.Error("visible reservation after fence")
	}
	summary, manifest := []byte("s"), []byte("m")
	if pair.Write(t.Context(), retainedPlan(t, 1, 1, summary, manifest), summary, manifest) == nil {
		t.Error("reserved retained pair wrote after fence")
	}
	capsule := []byte("capsule")
	if owner.WriteCapsule(t.Context(), terminalCapsulePlan(t, capsule), capsule) == nil {
		t.Error("capsule wrote after fence")
	}
	notice := []byte(`{"schema":"sessionless.run-terminal-notice.v1","code":"cancelled","cancelled":true}`)
	noticeRef := terminalCapsulePlan(t, notice).Capsule
	noticeID := domain.SessionEventID("namespace-fenced-notice")
	noticeRef.Key = domain.SessionEventObjectPrefix(bodyScope().TenantID, bodyScope().SessionID, noticeID) + "payloads/sha256/" + noticeRef.SHA256 + ".json"
	if owner.WriteFailureNotice(t.Context(), domain.SessionEventDraft{ID: noticeID, Kind: domain.SessionEventSystemNotice, Payload: noticeRef}, notice) == nil {
		t.Error("protected failure slot reopened fenced namespace")
	}
	ref := terminalCapsulePlan(t, []byte("x")).Capsule
	ref.Key = domain.SessionRunObjectPrefix(bodyScope().TenantID, bodyScope().SessionID, bodyScope().RunID) + "artifacts/sha256/" + ref.SHA256
	if visible.Write(t.Context(), ref, []byte("x")) == nil {
		t.Error("reserved visible output wrote after fence")
	}
	if _, err := owner.CloseOperationNamespace(ctx); err == nil {
		t.Error("copy restored erase attempt")
	}
	p2, o2, _, d2, _ := b.counts()
	if p2 != puts || o2 != opens || d2 != deletes {
		t.Error("fenced calls performed storage effects", p2-puts, o2-opens, d2-deletes)
	}
}

func TestOperationNamespaceCleanupUnknownPUTCannotBecomeErasure(t *testing.T) {
	for _, mode := range []string{"put_persisted", "put_absent"} {
		t.Run(mode, func(t *testing.T) {
			b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
			s, owner := cleanupOwner(t, b, retainedLimits())
			if mode == "put_persisted" {
				b.putFault = "ambiguous"
			} else {
				b.fault = "put_absent"
			}
			if _, err := s.Write(t.Context(), []byte("unknown request")); err == nil {
				t.Fatal("causal unknown write required")
			}
			ctx, _ := cleanupContext(t)
			if r, err := owner.CloseOperationNamespace(ctx); err == nil || r != nil {
				t.Fatal("unknown PUT became erasure")
			}
			b.putFault, b.fault = "", ""
			if r, err := owner.CloseOperationNamespace(ctx); err == nil || r != nil {
				t.Error("fresh call resolved unknown PUT")
			}
			if _, err := s.Write(t.Context(), []byte("retry")); err == nil {
				t.Error("tainted cleanup reopened writes")
			}
			_, _, _, deletes, _ := b.counts()
			if deletes != 0 || b.absences != 0 {
				t.Error("unknown PUT allowed destructive/absence probe", deletes, b.absences)
			}
		})
	}
}

func TestOperationNamespaceCleanupFailuresNeverRetry(t *testing.T) {
	for _, fault := range []string{"corrupt", "read", "close", "delete_before", "delete_after", "absence"} {
		t.Run(fault, func(t *testing.T) {
			b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
			s, owner := cleanupOwner(t, b, retainedLimits())
			if _, err := s.Write(t.Context(), []byte("original body")); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "corrupt", "read":
				b.openFault = fault
			case "close":
				b.closeFault = true
			default:
				b.fault = fault
			}
			ctx, _ := cleanupContext(t)
			if r, err := owner.CloseOperationNamespace(ctx); err == nil || r != nil {
				t.Fatal("uncertain cleanup minted receipt", fault)
			}
			_, _, _, deletes, _ := b.counts()
			if (fault == "corrupt" || fault == "read" || fault == "close") && deletes != 0 {
				t.Error("destruction preceded all original reads", fault, deletes)
			}
			p0, o0, _, d0, _ := b.counts()
			b.openFault, b.fault, b.closeFault = "", "", false
			if r, err := owner.CloseOperationNamespace(ctx); err == nil || r != nil {
				t.Error("failed cleanup retried")
			}
			p1, o1, _, d1, _ := b.counts()
			if p1 != p0 || o1 != o0 || d1 != d0 {
				t.Error("repeated cleanup storage effects")
			}
		})
	}
}

func TestOperationNamespaceCleanupUnsupportedStorageStillFences(t *testing.T) {
	b := newBodyBlobs()
	s, owner := cleanupOwner(t, b, retainedLimits())
	if _, err := s.Write(t.Context(), []byte("known")); err != nil {
		t.Fatal(err)
	}
	ctx, _ := cleanupContext(t)
	if r, err := owner.CloseOperationNamespace(ctx); err == nil || r != nil {
		t.Fatal("unsupported absence capability accepted")
	}
	if _, err := s.Write(t.Context(), []byte("late")); err == nil {
		t.Error("unsupported cleanup did not fence")
	}
	_, _, _, deletes, _ := b.counts()
	if deletes != 0 {
		t.Error("Delete called without explicit absence support")
	}
}

func TestOperationNamespaceCleanupFixedTwentyFourInventory(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	limits := retainedLimits()
	limits.MaxModelCalls = 8
	limits.MaxToolDispatches = 4
	limits.MaxOperations = 12
	s, owner := cleanupOwner(t, b, limits)
	for i := 0; i < 24; i++ {
		if _, err := s.Write(t.Context(), []byte("body-"+strconv.Itoa(i))); err != nil {
			t.Fatalf("ordinary physical slot%d: %v", i, err)
		}
	}
	if _, err := s.Write(t.Context(), []byte("first-over")); err == nil {
		t.Fatal("twenty-fifth write admitted")
	}
	ctx, _ := cleanupContext(t)
	r, err := owner.CloseOperationNamespace(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := r.DeletedBodies()
	if n != 24 {
		t.Error("bounded complete inventory", n)
	}
}

func TestOperationNamespaceCleanupDoesNotPretendActiveWriterJoined(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
		s, owner := cleanupOwner(t, b, retainedLimits())
		release := make(chan struct{})
		b.blockPut = release
		b.entered = make(chan struct{}, 1)
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		t.Cleanup(func() {
			cancel()
			unblock()
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Error("owned writer did not join cleanup")
			}
		})
		go func() { _, err := s.Write(ctx, []byte("owned active request")); done <- err }()
		synctest.Wait()
		select {
		case <-b.entered:
		default:
			t.Fatal("actual PUT not entered")
		}
		cleanup, _ := cleanupContext(t)
		if r, err := owner.CloseOperationNamespace(cleanup); !errors.Is(err, operationbody.ErrBusy) || r != nil {
			t.Error("active write fabricated join/erasure", err)
		}
		unblock()
		synctest.Wait()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			done <- err
		default:
			t.Fatal("original writer not joined")
		}
		if _, err := owner.CloseOperationNamespace(cleanup); err != nil {
			t.Fatal("actual now-idle known namespace", err)
		}
	})
}

func TestOperationNamespaceCleanupReturnedUnknownPUTMayStillCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &cleanupBlobs{bodyBlobs: newBodyBlobs(), latePut: make(chan pendingCleanupPut, 1)}
		s, owner := cleanupOwner(t, b, retainedLimits())
		ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		pendingContext, cancelPending := context.WithCancel(t.Context())
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		t.Cleanup(func() {
			cancelPending()
			unblock()
			synctest.Wait()
			select {
			case <-done:
			default:
				t.Error("test-owned remote pending PUT did not join")
			}
		})
		go func() {
			var pending pendingCleanupPut
			select {
			case pending = <-b.latePut:
			case <-pendingContext.Done():
				close(done)
				return
			}
			close(ready)
			<-release
			// A remote effect outlives the synchronous unknown return. This is
			// test-owned storage scheduling, NOT a goroutine in the adapter.
			b.mu.Lock()
			b.objects[pending.key] = pending.body
			b.mu.Unlock()
			close(done)
		}()
		if _, err := s.Write(t.Context(), []byte("late remote original request")); err == nil {
			t.Fatal("unknown original PUT return required")
		}
		synctest.Wait()
		select {
		case <-ready:
		default:
			t.Fatal("remote effect was not actually pending")
		}
		b.mu.Lock()
		absent := len(b.objects) == 0
		b.mu.Unlock()
		if !absent {
			t.Fatal("causal initial object absence required")
		}
		ctx, _ := cleanupContext(t)
		if receipt, err := owner.CloseOperationNamespace(ctx); receipt != nil || err == nil {
			t.Error("writer return plus current absence fabricated erasure")
		}
		_, _, _, deletes, _ := b.counts()
		if deletes != 0 || b.absences != 0 {
			t.Error("unknown original PUT allowed deletion/absence probe", deletes, b.absences)
		}
		unblock()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("released pending remote effect did not commit")
		}
		b.mu.Lock()
		late := len(b.objects)
		b.mu.Unlock()
		if late != 1 {
			t.Fatal("causal late commit did not occur", late)
		}
		if receipt, err := owner.CloseOperationNamespace(ctx); receipt != nil || err == nil {
			t.Error("late commit unlocked a new erase attempt")
		}
	})
}

func TestOperationNamespaceCleanupReceiptAndLifetimeDenials(t *testing.T) {
	b := &cleanupBlobs{bodyBlobs: newBodyBlobs()}
	s, owner := cleanupOwner(t, b, retainedLimits())
	if owner.VerifyNamespaceErasure(nil) == nil || owner.VerifyNamespaceErasure(new(operationbody.NamespaceErasureV1)) == nil {
		t.Error("absent/zero receipt authenticated")
	}
	if _, err := new(operationbody.NamespaceErasureV1).InventoryDigest(); err == nil {
		t.Error("zero receipt supplied a retained digest")
	}
	if receipt, err := owner.CloseOperationNamespace(nil); receipt != nil || err == nil {
		t.Error("nil cleanup context accepted")
	}
	if receipt, err := owner.CloseOperationNamespace(context.Background()); receipt != nil || err == nil {
		t.Error("unbounded cleanup context accepted")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if receipt, err := owner.CloseOperationNamespace(cancelled); receipt != nil || err == nil {
		t.Error("cancelled cleanup context accepted")
	}
	if _, err := s.Write(t.Context(), []byte("live original control")); err != nil {
		t.Fatal("invalid input fabricated a cleanup attempt", err)
	}
	ctx, _ := cleanupContext(t)
	r, err := owner.CloseOperationNamespace(ctx)
	if err != nil || owner.VerifyNamespaceErasure(r) != nil {
		t.Fatal("original finite positive after invalid inputs", err)
	}
}
