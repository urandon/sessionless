package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Test storage faults are installed before the original factory. Neither the
// SQL fixture nor this fake immutable storage grants publication authority.
type terminalSessionBlobs struct {
	*sessionCompactionBlobs
	stateMu                     sync.Mutex
	seed                        *ydbstore.LocalTerminalPublicationSessionConnectedSeed
	plan                        domain.TerminalCustodyPlanV1
	capsulePuts, eventPuts      int
	capsuleClosed               bool
	failure                     string
	mode                        string
	entered, cancelled, release chan struct{}
	releaseOnce                 sync.Once
	pending                     bool
}

func (b *terminalSessionBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	b.stateMu.Lock()
	capsule := b.plan.Capsule.Key != "" && key == b.plan.Capsule.Key
	event := b.plan.Capsule.Key != "" && !capsule
	if capsule {
		b.capsulePuts++
		writes, commits, reads := b.seed.TerminalPublicationCounts()
		if writes != 1 || commits != 1 || reads == 0 {
			b.failure = "capsule before original confirmed registration and separate readback"
		}
		if b.mode == "pause" {
			b.pending = true
			close(b.entered)
		}
	} else if event {
		b.eventPuts++
		if !b.capsuleClosed {
			b.failure = "event before verified capsule reader close"
		}
	}
	b.stateMu.Unlock()
	if capsule && b.mode == "pause" {
		// Deliberately uncooperative remote PUT: cancellation is observed, but
		// actual return/late effect remains pending until test-owned release.
		<-ctx.Done()
		close(b.cancelled)
		<-b.release
		b.stateMu.Lock()
		b.pending = false
		b.stateMu.Unlock()
		return domain.BlobRef{}, errors.New("cancelled ambiguous test PUT")
	}
	ref, err := b.sessionCompactionBlobs.Put(ctx, tenant, key, r)
	if capsule && b.mode == "unknown" && err == nil {
		return domain.BlobRef{}, errors.New("test PUT acknowledgement lost")
	}
	return ref, err
}

func (b *terminalSessionBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	r, err := b.sessionCompactionBlobs.Open(ctx, tenant, ref)
	if err != nil || ref.Key != b.plan.Capsule.Key || b.plan.Capsule.Key == "" {
		return r, err
	}
	if b.mode == "corrupt" {
		_ = r.Close()
		return io.NopCloser(bytes.NewReader([]byte("corrupt capsule"))), nil
	}
	return &terminalSessionReader{ReadCloser: r, closed: func() { b.stateMu.Lock(); b.capsuleClosed = true; b.stateMu.Unlock() }}, nil
}

type terminalSessionReader struct {
	io.ReadCloser
	closed func()
}

func (r *terminalSessionReader) Close() error {
	err := r.ReadCloser.Close()
	if err == nil {
		r.closed()
	}
	return err
}

func newTerminalPublicationSession(t *testing.T, mode, text string, limits ...ydbstore.LocalCompactionConnectedLimits) (*providerloopsession.LocalV1, *ydbstore.LocalTerminalPublicationSessionConnectedSeed, *sessionCompactionPeer, *terminalSessionBlobs) {
	t.Helper()
	peer := &sessionCompactionPeer{modelText: text}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout = 20 * time.Second
	server.Config.WriteTimeout = 20 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	blobs := &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), mode: mode}
	if mode == "pause" {
		blobs.entered, blobs.cancelled, blobs.release = make(chan struct{}), make(chan struct{}), make(chan struct{})
		t.Cleanup(func() { releaseTerminalPublication(blobs) })
	}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs, limits...)
	blobs.seed = seed
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("original factory", err)
	}
	t.Cleanup(func() {
		if mode == "pause" {
			releaseTerminalPublication(blobs)
		}
		if err := session.Close(); err != nil {
			t.Error("original session close", err)
		}
	})
	if _, err := session.Start(t.Context(), seed.Request); err != nil {
		t.Fatal("original invocation", err)
	}
	return session, seed, peer, blobs
}

func releaseTerminalPublication(b *terminalSessionBlobs) {
	b.releaseOnce.Do(func() { close(b.release) })
}

func prepareTerminalPublication(t *testing.T, session *providerloopsession.LocalV1, blobs *terminalSessionBlobs) (*providerloopsession.PreparedLocalTerminalV1, *ydbstore.LocalPreparedOutputV1) {
	t.Helper()
	h, err := session.PrepareTerminal(t.Context())
	if err != nil {
		t.Fatal("original freeze", err)
	}
	p, err := h.PrepareOutput(t.Context())
	if err != nil {
		t.Fatal("original prepared output", err)
	}
	blobs.plan, err = p.Plan()
	if err != nil {
		t.Fatal("original plan DATA", err)
	}
	return h, p
}

// Actual original Source/session/Store/keeper and HTTP; scripted SQL/funding
// and in-memory blobs. This is not native YDB, a real model or cloud proof.
func TestLocalSessionTerminalPublicationConnected(t *testing.T) {
	for _, compact := range []bool{false, true} {
		name, text, steps := "fitting", "original terminal answer", 1
		var limits []ydbstore.LocalCompactionConnectedLimits
		if compact {
			name, text, steps = "two_compactions", strings.Repeat("x", 4096), 3
			limits = []ydbstore.LocalCompactionConnectedLimits{{InputTokens: 8192, NetworkOperation: 5 * time.Second}}
		}
		t.Run(name, func(t *testing.T) {
			session, seed, peer, blobs := newTerminalPublicationSession(t, "", text, limits...)
			for i := 0; i < steps; i++ {
				if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted {
					peer.mu.Lock()
					kinds, sizes, failure := append([]string(nil), peer.kinds...), append([]int(nil), peer.sizes...), peer.failure
					peer.mu.Unlock()
					w, c, r := seed.TerminalPublicationCounts()
					t.Fatalf("original step%d err=%v attempted=%t compaction=%t stats=%+v kinds=%v sizes=%v publication=%d/%d/%d peer=%s", i, err, result.Model.SendAttempted, result.Compaction != nil, seed.Stats(), kinds, sizes, w, c, r, failure)
				}
			}
			h, p := prepareTerminalPublication(t, session, blobs)
			before, stats := seed.TerminalPublicationRun(t), seed.Stats()
			if err := h.PublishOutput(t.Context()); err != nil {
				w, c, r := seed.TerminalPublicationCounts()
				t.Fatalf("original terminal publication err=%v writes=%d commits=%d reads=%d storage=%s", err, w, c, r, blobs.failure)
			}
			after := seed.TerminalPublicationRun(t)
			if after.TerminalCustody == nil || after.TerminalCustody.Plan != blobs.plan || after.TerminalCustody.Correspondence != nil || after.TerminalCustody.LocalEvidence != nil || after.Status != domain.RunRunning {
				t.Fatal("publication changed terminal semantics or missing plan")
			}
			after.TerminalCustody = nil
			if !reflect.DeepEqual(before, after) || seed.Stats() != stats {
				t.Fatal("publication mutated existing Run/frontier or sent an operation")
			}
			if blobs.failure != "" || blobs.capsulePuts != 1 || blobs.eventPuts != 1 || !blobs.capsuleClosed {
				t.Fatal("original physical transfer order", blobs.failure, blobs.capsulePuts, blobs.eventPuts, blobs.capsuleClosed)
			}
			copyHandle := *h
			if copyHandle.PublishOutput(t.Context()) == nil || h.PublishOutput(t.Context()) == nil || p.Publish(t.Context()) == nil {
				t.Fatal("copied/replayed publication")
			}
			peer.mu.Lock()
			count, failure := len(peer.kinds), peer.failure
			peer.mu.Unlock()
			want := 1
			if compact {
				want = 5
			}
			if count != want || failure != "" {
				t.Fatal("publication extra HTTP", count, want, failure)
			}
			if err := session.Close(); err != nil {
				t.Fatal("original join", err)
			}
			if _, err := p.Plan(); err == nil {
				t.Fatal("Close retained prepared buffers")
			}
		})
	}
}

func TestLocalSessionTerminalPublicationCapsuleFailureConnected(t *testing.T) {
	for _, mode := range []string{"corrupt", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			session, seed, _, blobs := newTerminalPublicationSession(t, mode, "bounded answer")
			if _, err := session.RunModelStep(t.Context()); err != nil {
				t.Fatal("original MODEL", err)
			}
			h, _ := prepareTerminalPublication(t, session, blobs)
			if h.PublishOutput(t.Context()) == nil || blobs.eventPuts != 0 || blobs.capsulePuts != 1 || seed.TerminalPublicationRun(t).TerminalCustody == nil {
				t.Fatal("capsule uncertainty reached event PUT or lost cleanup plan", mode)
			}
			if h.PublishOutput(t.Context()) == nil || blobs.capsulePuts != 1 {
				t.Fatal("uncertain PUT retry", mode)
			}
		})
	}
}

func TestLocalSessionCloseJoinsDirectTerminalPublisherConnected(t *testing.T) {
	session, _, _, blobs := newTerminalPublicationSession(t, "pause", "bounded answer")
	if _, err := session.RunModelStep(t.Context()); err != nil {
		t.Fatal("original MODEL", err)
	}
	_, p := prepareTerminalPublication(t, session, blobs)
	call, cancel := context.WithCancel(t.Context())
	wait, finish := context.WithTimeout(t.Context(), 10*time.Second)
	defer finish()
	published, closed := make(chan struct{}), make(chan struct{})
	var publishErr error
	type closeResult struct {
		err     error
		pending bool
	}
	var result closeResult
	closeStarted := false
	// Registered BEFORE either goroutine. Broadcast completions preserve the
	// actual outcomes for both the assertion path and unconditional cleanup.
	t.Cleanup(func() {
		cancel()
		releaseTerminalPublication(blobs)
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if !joinTerminalPublicationCaller(cleanup, published) {
			t.Error("cleanup did not join publisher caller")
		}
		if closeStarted && !joinTerminalPublicationCaller(cleanup, closed) {
			t.Error("cleanup did not join Close caller")
		}
	})
	go func() { publishErr = p.Publish(call); close(published) }()
	if !joinTerminalPublicationCaller(wait, blobs.entered) {
		t.Fatal("publisher did not reach original capsule PUT")
	}
	closeStarted = true
	go func() {
		err := session.Close()
		blobs.stateMu.Lock()
		pending := blobs.pending
		blobs.stateMu.Unlock()
		result = closeResult{err, pending}
		close(closed)
	}()
	if !joinTerminalPublicationCaller(wait, blobs.cancelled) {
		t.Fatal("Close did not cancel original writer")
	}
	releaseTerminalPublication(blobs)
	if !joinTerminalPublicationCaller(wait, published) {
		t.Fatal("original publisher did not join")
	}
	if publishErr == nil {
		t.Error("cancelled writer became publication")
	}
	if !joinTerminalPublicationCaller(wait, closed) {
		t.Fatal("Close did not finish")
	}
	if result.err != nil || result.pending {
		t.Fatal("Close returned before actual direct writer join", result.err, result.pending)
	}
	if blobs.eventPuts != 0 {
		t.Error("event followed cancelled capsule")
	}
	if _, err := p.Plan(); err == nil {
		t.Error("joined writer retained buffers")
	}
}

func joinTerminalPublicationCaller(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}
