package ydbstore_test

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// A valid Context whose Done/Err change synchronously, while its standard
// AfterFunc delivery is explicitly held. WithoutCancel masks the parent's
// private cancelCtx Value; Go must use this Context's AfterFunc method instead
// of incorrectly treating its parent's cancellation identity as its own.
// This controls callback delivery only, never Source/Store authorization.
type heldTerminalCaller struct {
	context.Context
	mu         sync.Mutex
	done       chan struct{}
	err        error
	released   bool
	registered int
	callbacks  map[*heldTerminalCallback]struct{}
}

type heldTerminalCallback struct {
	f       func()
	started bool
	stopped bool
}

func newHeldTerminalCaller(parent context.Context) *heldTerminalCaller {
	return &heldTerminalCaller{Context: context.WithoutCancel(parent), done: make(chan struct{}), callbacks: make(map[*heldTerminalCallback]struct{})}
}
func (c *heldTerminalCaller) Done() <-chan struct{} { return c.done }
func (c *heldTerminalCaller) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
func (c *heldTerminalCaller) AfterFunc(f func()) func() bool {
	cb := &heldTerminalCallback{f: f}
	c.mu.Lock()
	c.registered++
	c.callbacks[cb] = struct{}{}
	start := c.released && c.err != nil
	if start {
		cb.started = true
		delete(c.callbacks, cb)
	}
	c.mu.Unlock()
	if start {
		f()
	}
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if cb.started || cb.stopped {
			return false
		}
		cb.stopped = true
		delete(c.callbacks, cb)
		return true
	}
}
func (c *heldTerminalCaller) cancel() {
	c.mu.Lock()
	if c.err == nil {
		c.err = context.Canceled
		close(c.done)
	}
	released := c.released
	c.mu.Unlock()
	if released {
		c.release()
	}
}
func (c *heldTerminalCaller) release() {
	c.mu.Lock()
	c.released = true
	var callbacks []func()
	if c.err != nil {
		for cb := range c.callbacks {
			if !cb.started && !cb.stopped {
				cb.started = true
				callbacks = append(callbacks, cb.f)
			}
			delete(c.callbacks, cb)
		}
	}
	c.mu.Unlock()
	for _, f := range callbacks {
		f()
	}
}

// Normal bounded immutable fake storage, wrapped BEFORE genuine constructor
// intake. One armed actual Open cancels only the ORIGINAL terminal caller.
type terminalCancelBlobs struct {
	base                 *sessionCompactionBlobs
	mu                   sync.Mutex
	caller               *heldTerminalCaller
	armed, fired         bool
	cancelDuringOpen     bool
	callLiveDuringCancel bool
}

func (b *terminalCancelBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	return b.base.Put(ctx, tenant, key, r)
}
func (b *terminalCancelBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	return b.base.Delete(ctx, tenant, ref)
}
func (b *terminalCancelBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.mu.Lock()
	if b.armed && !b.fired {
		b.fired = true
		if b.cancelDuringOpen {
			b.caller.cancel()
			b.callLiveDuringCancel = ctx.Err() == nil && errors.Is(b.caller.Err(), context.Canceled)
		}
	}
	b.mu.Unlock()
	return b.base.Open(ctx, tenant, ref)
}

// Real original session/Source/Store/issuer and one owned HTTP MODEL; SQL,
// storage, funding and calibration are fixtures, not native YDB/model proof.
func TestLocalSessionTerminalOriginalCallerCancellationDuringCapture(t *testing.T) {
	for _, cancelDuringOpen := range []bool{false, true} {
		name := "live_original_positive"
		if cancelDuringOpen {
			name = "cancelled_original_delayed_callback"
		}
		t.Run(name, func(t *testing.T) {
			caller := newHeldTerminalCaller(t.Context())
			t.Cleanup(func() { caller.cancel(); caller.release() })
			blobs := &terminalCancelBlobs{base: newSessionCompactionBlobs(), caller: caller, cancelDuringOpen: cancelDuringOpen}
			peer := &sessionCompactionPeer{modelText: "original terminal cancellation test"}
			server := httptest.NewUnstartedServer(peer)
			server.Config.ReadHeaderTimeout = 10 * time.Second
			server.Config.ReadTimeout = 20 * time.Second
			server.Config.WriteTimeout = 20 * time.Second
			server.Config.MaxHeaderBytes = 16 << 10
			t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
			server.Start()
			seed := ydbstore.NewLocalCompactionSessionConnectedSeed(t, server.URL+"/v1", blobs)
			session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
			if err != nil {
				t.Fatal("genuine original constructor", err)
			}
			t.Cleanup(func() {
				if err := session.Close(); err != nil {
					t.Error("bounded original session cleanup", err)
				}
			})
			if _, err := session.Start(t.Context(), seed.Request); err != nil {
				t.Fatal("original invocation entry", err)
			}
			if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted || result.Compaction != nil {
				t.Fatal("one actual completed MODEL before terminal capture", err)
			}
			before := seed.Stats()
			blobs.mu.Lock()
			blobs.armed = true
			blobs.mu.Unlock()
			h, err := session.PrepareTerminal(caller)
			blobs.mu.Lock()
			fired, live := blobs.fired, blobs.callLiveDuringCancel
			blobs.mu.Unlock()
			caller.mu.Lock()
			registered, released := caller.registered, caller.released
			caller.mu.Unlock()
			if !fired || registered == 0 || released {
				t.Fatalf("causal actual capture/held AfterFunc not reached: Open=%v callbacks=%d released=%v", fired, registered, released)
			}
			if cancelDuringOpen {
				if !live || !errors.Is(caller.Err(), context.Canceled) {
					t.Fatal("original cancellation was not visible while actual capture call stayed live")
				}
				select {
				case <-caller.Done():
				default:
					t.Fatal("cancelled ORIGINAL Done still open")
				}
				if h != nil || err == nil {
					t.Errorf("cancelled ORIGINAL caller acquired terminal owner despite delayed callback: handle=%v err=%v", h != nil, err)
				}
			} else if h == nil || err != nil {
				t.Fatalf("live original positive denied: %v", err)
			}
			copySession := *session
			if retry, err := copySession.PrepareTerminal(context.Background()); retry != nil || err == nil {
				t.Error("terminal failure/success request was renewed through a session alias")
			}
			if next, err := copySession.RunModelStep(t.Context()); err == nil || next.Model.SendAttempted || next.Compaction != nil || seed.Stats() != before {
				t.Error("terminal request allowed another effect through a session alias")
			}
			if err := session.Close(); err != nil {
				t.Fatal("bounded original terminal cleanup", err)
			}
			if _, err := seed.Source.Configuration(); err != nil {
				t.Fatal("terminal cleanup closed borrowed Source", err)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if peer.failure != "" || !reflect.DeepEqual(peer.kinds, []string{"sessionless.history.v1"}) {
				t.Fatalf("unexpected owned HTTP effects: kinds=%v failure=%s", peer.kinds, peer.failure)
			}
		})
	}
}
