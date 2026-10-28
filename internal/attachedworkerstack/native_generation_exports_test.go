//go:build darwin || linux

package attachedworkerstack

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

// This closed test-only selector cannot install a runner, projector, metadata
// reader, authority callback or executable. Every case uses the fixed inert
// installation, original Connector and synthetic concrete lifecycle fixture.
type NativeGenerationTransportCaseV3ForTest uint8

const (
	NativeGenerationTransportInitialV3ForTest NativeGenerationTransportCaseV3ForTest = iota + 1
	NativeGenerationTransportBlockedACKV3ForTest
	NativeGenerationTransportMismatchedACKV3ForTest
	NativeGenerationTransportReconnectV3ForTest
)

type NativeGenerationTransportObservationV3ForTest struct {
	Publishes, Reads, Available, BeforeACK, Opens, Retires int
	Events                                                 []string
}

type NativeGenerationTransportFixtureV3ForTest struct {
	Stack            *Stack
	Session          *attachedworkersession.Session
	Capability       attachedworkerprotocol.CapabilityManifestV1
	Executable       string
	ExecutableDigest attachedworkerdaemon.ExecutableDigest
	Arguments        []string
	Root             string
	Mode             attachedworkersession.ResourceGenerationConnectionModeV1
	base             *generationStackFixtureV3
	events           *generationTransportEventsV3ForTest
	entered          chan struct{}
	release          chan struct{}
	continueOnce     sync.Once
}

func NewNativeGenerationTransportFixtureV3ForTest(t *testing.T, choice NativeGenerationTransportCaseV3ForTest) *NativeGenerationTransportFixtureV3ForTest {
	t.Helper()
	if choice < NativeGenerationTransportInitialV3ForTest || choice > NativeGenerationTransportReconnectV3ForTest {
		t.Fatal("unknown closed native generation fixture case")
	}
	f := newGenerationStackFixtureV3(t)
	v := &NativeGenerationTransportFixtureV3ForTest{base: f, events: &generationTransportEventsV3ForTest{}, entered: make(chan struct{}), release: make(chan struct{}), Mode: attachedworkersession.ResourceGenerationInitialV1}
	t.Cleanup(v.ContinueACK)
	if choice == NativeGenerationTransportBlockedACKV3ForTest {
		f.metadata.hook = func(req *http.Request) error {
			close(v.entered)
			select {
			case <-v.release:
				return nil
			case <-req.Context().Done():
				return req.Context().Err()
			}
		}
	}
	if choice == NativeGenerationTransportMismatchedACKV3ForTest {
		f.metadata.mismatch = true
	}
	v.installOwner(t, f.peer)
	v.Stack, v.Session = f.stack(t), f.connect(t)
	if choice == NativeGenerationTransportReconnectV3ForTest {
		if err := v.Stack.BindPreparedGenerationConnectionV3(t.Context(), v.Session, attachedworkersession.ResourceGenerationInitialV1); err != nil {
			t.Fatal(err)
		}
		if err := v.Stack.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := v.Session.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := f.owner.Close(); err != nil {
			t.Fatal(err)
		}
		lease, err := f.worker.AcquireRuntime(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		checkpoint, err := lease.LoadReconnectCheckpoint(t.Context())
		if err != nil {
			_ = lease.Close()
			t.Fatal(err)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
		private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
		peer := &preparedSessionPeer{now: f.clock.Now(), public: private.Public().(ed25519.PublicKey), previous: &checkpoint}
		v.installOwner(t, peer)
		connector, err := attachedworkersession.New(f.worker, peer, f.owner, attachedworkersession.Config{Audience: "sessionless:attached-worker:v1", WorkerOffer: f.capability.ProtocolOffer,
			ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second, Now: f.clock.Now, Random: bytes.NewReader(bytes.Repeat([]byte{0x62}, 64)),
			RuntimePreflight: func(ctx context.Context, m attachedworkerlocal.ManifestV1) error {
				var err error
				v.Stack, err = newNativeGenerationStackV3(ctx, m, f.base.config, f.lifecycle, f.deps)
				return err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		v.Session, err = connector.Reconnect(t.Context(), attachedworkersession.ReconnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: f.capability})
		if err != nil {
			t.Fatal("genuine reconnect", err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := v.Session.Close(ctx); err != nil {
				t.Error(err)
			}
			if err := v.Stack.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		v.Mode = attachedworkersession.ResourceGenerationReconnectV1
		v.events.mu.Lock()
		v.events.events = nil
		v.events.ack = false
		v.events.mu.Unlock()
	}
	v.Capability, v.Executable, v.ExecutableDigest, v.Arguments, v.Root = f.capability, f.base.manifest.Harness.Executable, f.base.harnessDigest, slices.Clone(f.base.manifest.Harness.Arguments), f.base.scratch
	return v
}

func (v *NativeGenerationTransportFixtureV3ForTest) installOwner(t *testing.T, peer *preparedSessionPeer) {
	t.Helper()
	f := v.base
	source := &generationTransportSourceV3ForTest{peer: peer, events: v.events}
	owner, err := attachedresourcegenerationsession.NewV1(f.base.manifest.ControlPlaneOrigin, &http.Client{Transport: &generationTransportHTTPV3ForTest{base: f.metadata, events: v.events}}, source, f.lifecycle, f.base.config.PreparedCodexNativeGeneration.Baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	f.owner, f.peer, f.base.config.PreparedCodexNativeGeneration.GenerationOwner = owner, peer, owner
}

func (v *NativeGenerationTransportFixtureV3ForTest) CurrentTime() time.Time {
	return v.base.clock.Now()
}
func (v *NativeGenerationTransportFixtureV3ForTest) ACKEntered() <-chan struct{} { return v.entered }
func (v *NativeGenerationTransportFixtureV3ForTest) ContinueACK() {
	v.continueOnce.Do(func() { close(v.release) })
}
func (v *NativeGenerationTransportFixtureV3ForTest) CloseOriginalLifecycle() error {
	return v.base.lifecycle.Close()
}
func (v *NativeGenerationTransportFixtureV3ForTest) Observation() NativeGenerationTransportObservationV3ForTest {
	p, r := v.base.metadata.counts()
	v.events.mu.Lock()
	defer v.events.mu.Unlock()
	return NativeGenerationTransportObservationV3ForTest{Publishes: p, Reads: r, Available: v.events.available, BeforeACK: v.events.beforeACK, Opens: v.events.opens, Retires: v.events.retires, Events: slices.Clone(v.events.events)}
}

type generationTransportEventsV3ForTest struct {
	mu                                   sync.Mutex
	ack                                  bool
	available, beforeACK, opens, retires int
	events                               []string
}
type generationTransportHTTPV3ForTest struct {
	base   *generationStackHTTPV3
	events *generationTransportEventsV3ForTest
}

func (v *generationTransportHTTPV3ForTest) RoundTrip(req *http.Request) (*http.Response, error) {
	v.events.mu.Lock()
	v.events.events = append(v.events.events, "metadata-request")
	v.events.mu.Unlock()
	reply, err := v.base.RoundTrip(req)
	if err == nil && !v.base.mismatch {
		v.events.mu.Lock()
		v.events.ack = true
		v.events.events = append(v.events.events, "metadata-ack")
		v.events.mu.Unlock()
	}
	return reply, err
}

type generationTransportSourceV3ForTest struct {
	peer    *preparedSessionPeer
	events  *generationTransportEventsV3ForTest
	mu      sync.Mutex
	retired bool
	opened  bool
}

func (v *generationTransportSourceV3ForTest) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.retired || v.opened {
		return nil, errors.New("closed/already-open synthetic source")
	}
	if _, err := v.peer.Open(binding, bearer); err != nil {
		return nil, err
	}
	v.opened = true
	v.events.mu.Lock()
	v.events.opens++
	v.events.mu.Unlock()
	return v, nil
}
func (v *generationTransportSourceV3ForTest) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.retired || ctx.Err() != nil || len(batch.Frames) != 1 {
		return nil, errors.New("invalid synthetic idle exchange")
	}
	frame := batch.Frames[0]
	if frame.Kind == attachedworkerprotocol.MessageHeartbeat && frame.Heartbeat != nil && frame.Heartbeat.Available && frame.Heartbeat.ActiveAttempts == 0 {
		v.events.mu.Lock()
		v.events.available++
		if !v.events.ack {
			v.events.beforeACK++
		}
		v.events.events = append(v.events.events, "available")
		v.events.mu.Unlock()
	} else if frame.Kind != attachedworkerprotocol.MessageManifest {
		return nil, errors.New("fixture accepts only original Manifest and idle Available heartbeat")
	}
	return nil, nil // idle: no offer, job, terminal or execution authority
}
func (v *generationTransportSourceV3ForTest) RetireConnectionSourcesV1() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.retired {
		v.retired = true
		v.events.mu.Lock()
		v.events.retires++
		v.events.mu.Unlock()
	}
	return nil
}
func (v *generationTransportSourceV3ForTest) Close() error { return v.RetireConnectionSourcesV1() }
