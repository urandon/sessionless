package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

var sessionTestTime = time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)

func TestConnectorOwnsInitialAttachAndPersistsRestartFence(t *testing.T) {
	fixture := newSessionFixture(t)
	fixture.bootstrap.challengeHook = func() {
		snapshot, err := fixture.store.LoadSnapshot(context.Background())
		if err != nil || snapshot.Manifest.ConnectionGeneration != 1 || snapshot.Manifest.Revision != 2 {
			t.Fatalf("bootstrap before durable fence: snapshot=%+v err=%v", snapshot.Manifest, err)
		}
	}
	session, err := fixture.connector.Connect(context.Background(), fixture.input)
	if err != nil {
		t.Fatalf("connect error=%v challenge=%d activate=%d", err, fixture.bootstrap.challengeCalls.Load(), fixture.bootstrap.activateCalls.Load())
	}
	registerSessionCleanup(t, session)
	snapshot := session.Snapshot()
	if snapshot.State != StateReady || snapshot.TenantID != fixture.manifest.TenantID ||
		snapshot.OwnerUserID != fixture.manifest.OwnerUserID || snapshot.WorkerID != fixture.manifest.WorkerID ||
		snapshot.EnrollmentGeneration != 1 || snapshot.ConnectionGeneration != 1 ||
		snapshot.ProtocolVersion != attachedworkerprotocol.ProtocolVersionV1 || snapshot.ConnectionID != "connection-001" ||
		snapshot.CapabilityDigest == "" || snapshot.AuthenticationExpires == nil {
		t.Fatalf("ready snapshot=%+v", snapshot)
	}
	if fixture.bootstrap.challengeCalls.Load() != 1 || fixture.bootstrap.activateCalls.Load() != 1 || fixture.exchange.calls.Load() != 1 {
		t.Fatalf("calls challenge=%d activate=%d exchange=%d", fixture.bootstrap.challengeCalls.Load(), fixture.bootstrap.activateCalls.Load(), fixture.exchange.calls.Load())
	}
	if _, err := fixture.connector.Connect(context.Background(), fixture.input); !errors.Is(err, ErrAlreadyOwned) {
		t.Fatalf("second owner error=%v", err)
	}
	assertSessionFormattingIsSecretFree(t, fixture, session)
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if session.Snapshot().State != StateClosed || fixture.exchange.closed.Load() != 1 {
		t.Fatalf("close snapshot=%+v exchange closes=%d", session.Snapshot(), fixture.exchange.closed.Load())
	}

	// Connection identity is intentionally ephemeral. A fresh process sees the
	// durable generation/secret fence and cannot infer attach success or retry.
	restartBootstrap := fixture.newBootstrap()
	restart, err := New(fixture.store, restartBootstrap, &fakeFactory{exchange: &fakeExchange{}}, fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restart.Connect(context.Background(), fixture.input); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("restart error=%v", err)
	}
	if restartBootstrap.challengeCalls.Load() != 0 {
		t.Fatal("restart crossed the network fence")
	}
}

func TestConnectorFailsClosedBeforeNetworkOnInvalidLocalAuthority(t *testing.T) {
	t.Run("already cancelled caller", func(t *testing.T) {
		fixture := newSessionFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := fixture.connector.Connect(ctx, fixture.input); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
		assertNoNetworkAndGeneration(t, fixture, 0)
		session, err := fixture.connector.Connect(context.Background(), fixture.input)
		if err != nil {
			t.Fatalf("cancelled caller poisoned connector: %v", err)
		}
		registerSessionCleanup(t, session)
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cross worker capability", func(t *testing.T) {
		fixture := newSessionFixture(t)
		fixture.input.CapabilityManifest.WorkerID = "worker-other"
		if _, err := fixture.connector.Connect(context.Background(), fixture.input); !errors.Is(err, ErrInvalidAuthority) {
			t.Fatalf("error=%v", err)
		}
		assertNoNetworkAndGeneration(t, fixture, 0)
	})

	t.Run("protocol offer mismatch", func(t *testing.T) {
		fixture := newSessionFixture(t)
		fixture.input.CapabilityManifest.ProtocolOffer.Supported = []attachedworkerprotocol.ProtocolVersion{1, 2}
		fixture.input.CapabilityManifest.ProtocolOffer.Window.Maximum = 2
		if _, err := fixture.connector.Connect(context.Background(), fixture.input); !errors.Is(err, ErrInvalidAuthority) {
			t.Fatalf("error=%v", err)
		}
		assertNoNetworkAndGeneration(t, fixture, 0)
	})

	t.Run("logged out", func(t *testing.T) {
		fixture := newSessionFixture(t)
		if _, err := fixture.store.Logout(context.Background(), attachedworkerlocal.LogoutInputV1{ExpectedRevision: 1, RequestID: "logout-session-test-001"}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.connector.Connect(context.Background(), fixture.input); !errors.Is(err, attachedworkerlocal.ErrSecretRetired) {
			t.Fatalf("error=%v", err)
		}
		assertNoNetworkAndGeneration(t, fixture, 0)
	})
}

func TestSessionExchangeActionOwnsEnvelopeAndReleasesCommittedAttempt(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	snapshot := session.Snapshot()
	capabilityDigest := mustDecodeHex(t, string(snapshot.CapabilityDigest))
	firstBinding := sessionAttemptBinding(capabilityDigest, "first")
	secondBinding := sessionAttemptBinding(capabilityDigest, "second")
	step := 0
	var terminalBatch attachedworkerprotocol.BatchV1
	var terminalResponse attachedworkerprotocol.BatchV1
	var mainHandler func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)
	mainHandler = func(_ context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		if len(batch.Frames) != 1 {
			t.Fatalf("exchange step=%d frames=%d, want 1", step, len(batch.Frames))
		}
		frame := batch.Frames[0]
		wantSequence := uint64(4 + step)
		wantAck := uint64(2 + step)
		if frame.Sequence != wantSequence || frame.Ack != wantAck ||
			frame.MessageID != attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, wantSequence) ||
			frame.WorkerID != string(snapshot.WorkerID) || frame.EnrollmentGeneration != snapshot.EnrollmentGeneration ||
			frame.ConnectionGeneration != snapshot.ConnectionGeneration {
			t.Fatalf("exchange step=%d envelope=%+v want sequence=%d ack=%d", step, frame, wantSequence, wantAck)
		}
		platformFrame := attachedworkerprotocol.FrameV1{
			Version:   snapshot.ProtocolVersion,
			MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, uint64(3+step)),
			WorkerID:  string(snapshot.WorkerID), EnrollmentGeneration: snapshot.EnrollmentGeneration,
			ConnectionGeneration: snapshot.ConnectionGeneration, Sequence: uint64(3 + step), Ack: wantSequence,
		}
		switch step {
		case 0:
			if frame.Kind != attachedworkerprotocol.MessageHeartbeat || frame.Heartbeat == nil || !frame.Heartbeat.Available {
				t.Fatalf("heartbeat action=%+v", frame)
			}
			platformFrame.Kind = attachedworkerprotocol.MessageLeaseOffer
			platformFrame.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: firstBinding, AttemptSequence: 1}
		case 1:
			if frame.Kind != attachedworkerprotocol.MessageLeaseClaim || frame.LeaseClaim == nil ||
				!sessionSameAttemptBinding(frame.LeaseClaim.Binding, firstBinding) || frame.LeaseClaim.AttemptSequence != 1 {
				t.Fatalf("lease claim action=%+v", frame)
			}
			platformFrame.Kind = attachedworkerprotocol.MessageLeaseAccepted
			platformFrame.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: firstBinding, AttemptSequence: 2}
		case 2:
			if frame.Kind != attachedworkerprotocol.MessageTerminal || frame.Terminal == nil ||
				!sessionSameAttemptBinding(frame.Terminal.Binding, firstBinding) || frame.Terminal.AttemptSequence != 2 {
				t.Fatalf("terminal action=%+v", frame)
			}
			platformFrame.Kind = attachedworkerprotocol.MessageTerminalAck
			platformFrame.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
				Binding: firstBinding, AttemptSequence: 3,
				TerminalSequence: frame.Terminal.TerminalSequence, Status: frame.Terminal.Status,
				Result: frame.Terminal.Result, EvidenceDigest: append([]byte(nil), frame.Terminal.EvidenceDigest...),
			}
			terminalBatch = batch
		case 3:
			if frame.Kind != attachedworkerprotocol.MessageHeartbeat || frame.Heartbeat == nil || !frame.Heartbeat.Available {
				t.Fatalf("second heartbeat action=%+v", frame)
			}
			platformFrame.Kind = attachedworkerprotocol.MessageLeaseOffer
			platformFrame.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: secondBinding, AttemptSequence: 1}
		default:
			t.Fatalf("unexpected exchange step=%d", step)
		}
		step++
		response := attachedworkerprotocol.BatchV1{Version: snapshot.ProtocolVersion, Frames: []attachedworkerprotocol.FrameV1{platformFrame}}
		if step == 3 {
			terminalResponse = response
		}
		return &response, nil
	}
	fixture.exchange.setHandler(mainHandler)

	response, err := session.ExchangeAction(context.Background(), ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{
		ObservedAtUnixMicro: sessionTestTime.UnixMicro(), Available: true,
	}})
	if err != nil || response == nil || response.Kind != attachedworkerprotocol.MessageLeaseOffer {
		t.Fatalf("heartbeat response=%+v error=%v", response, err)
	}
	response, err = session.ExchangeAction(context.Background(), ActionV1{LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{
		Binding: firstBinding, AttemptSequence: 1,
	}})
	if err != nil || response == nil || response.Kind != attachedworkerprotocol.MessageLeaseAccepted {
		t.Fatalf("claim response=%+v error=%v", response, err)
	}
	evidence := bytes.Repeat([]byte{0x5a}, sha256.Size)
	response, err = session.ExchangeAction(context.Background(), ActionV1{Terminal: &attachedworkerprotocol.TerminalV1{
		Binding: firstBinding, AttemptSequence: 2, TerminalSequence: 1,
		Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted,
		EvidenceDigest: evidence,
	}})
	if err != nil || response == nil || response.Kind != attachedworkerprotocol.MessageTerminalAck {
		t.Fatalf("terminal response=%+v error=%v", response, err)
	}
	session.mu.Lock()
	committed, snapshotErr := session.machine.Snapshot()
	session.mu.Unlock()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	if committed.Attempt.Summary.State != attachedworkerprotocol.AttemptTerminalCommitted ||
		committed.Worker.Ack >= committed.Platform.Sequence {
		t.Fatalf("attempt retired before worker acknowledgement: snapshot=%+v", committed)
	}
	terminalAckSequence := committed.Platform.Sequence
	fixture.exchange.setHandler(func(_ context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		got, _ := json.Marshal(batch)
		want, _ := json.Marshal(terminalBatch)
		if !bytes.Equal(got, want) {
			t.Fatalf("terminal replay differs: got=%s want=%s", got, want)
		}
		return &terminalResponse, nil
	})
	if replay, err := session.Exchange(context.Background(), terminalBatch); err != nil || replay == nil ||
		len(replay.Frames) != 1 || replay.Frames[0].Kind != attachedworkerprotocol.MessageTerminalAck {
		t.Fatalf("terminal replay response=%+v error=%v", replay, err)
	}
	fixture.exchange.setHandler(mainHandler)
	response, err = session.ExchangeAction(context.Background(), ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{
		ObservedAtUnixMicro: sessionTestTime.Add(time.Second).UnixMicro(), Available: true,
	}})
	if err != nil || response == nil || response.Kind != attachedworkerprotocol.MessageLeaseOffer ||
		!sessionSameAttemptBinding(response.LeaseOffer.Binding, secondBinding) {
		t.Fatalf("second attempt response=%+v error=%v", response, err)
	}
	session.mu.Lock()
	retired, snapshotErr := session.machine.Snapshot()
	session.mu.Unlock()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	if retired.Worker.Ack < terminalAckSequence || retired.Attempt.Summary.State != attachedworkerprotocol.AttemptOffered ||
		!sessionSameAttemptBinding(retired.Attempt.Summary.Binding, secondBinding) {
		t.Fatalf("later acknowledgement did not retire before next offer: snapshot=%+v", retired)
	}
	if step != 4 {
		t.Fatalf("exchange steps=%d, want 4", step)
	}
}

func TestSessionExchangeActionRejectsAmbiguousVocabularyBeforeExchange(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	before := fixture.exchange.calls.Load()
	invalid := []ActionV1{
		{},
		{
			Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: sessionTestTime.UnixMicro(), Available: true},
			Drained:   &attachedworkerprotocol.DrainedV1{Revision: 1},
		},
	}
	for index, action := range invalid {
		if _, err := session.ExchangeAction(context.Background(), action); !errors.Is(err, ErrInvalidAuthority) {
			t.Errorf("case=%d error=%v", index, err)
		}
	}
	if fixture.exchange.calls.Load() != before {
		t.Fatalf("invalid action reached exchange calls=%d before=%d", fixture.exchange.calls.Load(), before)
	}
	secret := "attempt-secret-never-format"
	action := ActionV1{LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: attachedworkerprotocol.AttemptBindingV1{RunID: secret}}}
	if formatted := fmt.Sprintf("%+v %#v", action, action); strings.Contains(formatted, secret) {
		t.Fatalf("action formatting leaked payload: %s", formatted)
	}
}

func TestConnectorRejectsProtocolBaitSwitchAfterDurableFence(t *testing.T) {
	fixture := newSessionFixture(t)
	fixture.bootstrap.challengeMutation = func(response *attachedworkerhttp.ChallengeResponseV1) {
		response.Frame.Version = 2
		response.Frame.Challenge.SelectedVersion = 2
		response.Challenge.SelectedProtocolVersion = 2
	}
	if _, err := fixture.connector.Connect(context.Background(), fixture.input); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("error=%v", err)
	}
	if fixture.bootstrap.activateCalls.Load() != 0 {
		t.Fatal("protocol bait switch reached activation")
	}
	assertGeneration(t, fixture, 1)
}

func TestSessionRejectsCrossGenerationBeforeExchange(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	before := fixture.exchange.calls.Load()
	batch := heartbeatBatch(session.Snapshot(), 4, 2, true)
	batch.Frames[0].ConnectionGeneration++
	if _, err := session.Exchange(context.Background(), batch); !errors.Is(err, ErrInvalidAuthority) {
		t.Fatalf("error=%v", err)
	}
	if fixture.exchange.calls.Load() != before || session.Snapshot().State != StateReady {
		t.Fatalf("invalid frame reached port calls=%d state=%s", fixture.exchange.calls.Load(), session.Snapshot().State)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionClassifiesStaleBearerAndAmbiguousExchange(t *testing.T) {
	t.Run("unauthorized is fenced", func(t *testing.T) {
		fixture := newSessionFixture(t)
		session := mustReadySession(t, fixture)
		fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
			return nil, &attachedworkerhttp.ExchangeError{Kind: attachedworkerhttp.ErrorUnauthorized}
		})
		if _, err := session.Exchange(context.Background(), heartbeatBatch(session.Snapshot(), 4, 2, true)); err == nil {
			t.Fatal("unauthorized exchange succeeded")
		}
		if session.Snapshot().State != StateFenced || session.Snapshot().FailureCode != "unauthorized" {
			t.Fatalf("snapshot=%+v", session.Snapshot())
		}
		if _, err := session.Exchange(context.Background(), heartbeatBatch(session.Snapshot(), 4, 2, true)); !errors.Is(err, ErrSessionFenced) {
			t.Fatalf("post-fence error=%v", err)
		}
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unavailable requires reconciliation", func(t *testing.T) {
		fixture := newSessionFixture(t)
		session := mustReadySession(t, fixture)
		fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
			return nil, &attachedworkerhttp.ExchangeError{Kind: attachedworkerhttp.ErrorUnavailable}
		})
		if _, err := session.Exchange(context.Background(), heartbeatBatch(session.Snapshot(), 4, 2, true)); err == nil {
			t.Fatal("unavailable exchange succeeded")
		}
		if session.Snapshot().State != StateReconciliationRequired || session.Snapshot().FailureCode != "exchange_ambiguous" {
			t.Fatalf("snapshot=%+v", session.Snapshot())
		}
		if _, err := session.Exchange(context.Background(), heartbeatBatch(session.Snapshot(), 4, 2, true)); !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("post-ambiguity error=%v", err)
		}
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSessionCancellationKeepsSingleExchangeOwner(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		close(started)
		select {
		case <-release: // deliberately ignores request cancellation; the session must retain ownership
			return nil, nil
		case <-time.After(time.Second):
			return nil, errors.New("test exchange release was not observed")
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := session.Exchange(ctx, heartbeatBatch(session.Snapshot(), 4, 2, true))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("exchange did not reach the blocking fake")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled exchange did not return")
	}
	before := fixture.exchange.calls.Load()
	if _, err := session.Exchange(context.Background(), heartbeatBatch(session.Snapshot(), 4, 2, true)); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("second owner error=%v", err)
	}
	if fixture.exchange.calls.Load() != before {
		t.Fatal("second exchange owner reached the port")
	}
	releaseOnce.Do(func() { close(release) })
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := session.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorCancellationStopsAfterCurrentBootstrapEffect(t *testing.T) {
	t.Run("challenge", func(t *testing.T) {
		fixture := newSessionFixture(t)
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(release) })
			waitSignal(t, finished, "challenge did not finish during cleanup")
			waitForRuntimeRelease(t, fixture.store)
		})
		fixture.bootstrap.challengeHook = func() {
			defer close(finished)
			close(started)
			select {
			case <-release:
			case <-time.After(time.Second):
				t.Error("challenge release was not observed")
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := fixture.connector.Connect(ctx, fixture.input)
			done <- err
		}()
		waitSignal(t, started, "challenge did not start")
		cancel()
		if err := waitError(t, done, "cancelled connect did not return"); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("connect error=%v", err)
		}
		if fixture.bootstrap.activateCalls.Load() != 0 || fixture.factory.openCalls.Load() != 0 || fixture.exchange.calls.Load() != 0 {
			t.Fatal("later effect started before blocked challenge returned")
		}
		releaseOnce.Do(func() { close(release) })
		waitForRuntimeRelease(t, fixture.store)
		if fixture.bootstrap.activateCalls.Load() != 0 || fixture.factory.openCalls.Load() != 0 || fixture.exchange.calls.Load() != 0 {
			t.Fatal("cancelled challenge continued to a later effect")
		}
	})

	t.Run("activation", func(t *testing.T) {
		fixture := newSessionFixture(t)
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(release) })
			waitSignal(t, finished, "activation did not finish during cleanup")
			waitForRuntimeRelease(t, fixture.store)
		})
		fixture.bootstrap.activateHook = func() {
			defer close(finished)
			close(started)
			select {
			case <-release:
			case <-time.After(time.Second):
				t.Error("activation release was not observed")
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := fixture.connector.Connect(ctx, fixture.input)
			done <- err
		}()
		waitSignal(t, started, "activation did not start")
		cancel()
		if err := waitError(t, done, "cancelled connect did not return"); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("connect error=%v", err)
		}
		if fixture.factory.openCalls.Load() != 0 || fixture.exchange.calls.Load() != 0 {
			t.Fatal("later effect started before blocked activation returned")
		}
		releaseOnce.Do(func() { close(release) })
		waitForRuntimeRelease(t, fixture.store)
		if fixture.factory.openCalls.Load() != 0 || fixture.exchange.calls.Load() != 0 {
			t.Fatal("cancelled activation continued to a later effect")
		}
	})
}

func TestSessionOwnsInputsAcrossCancellation(t *testing.T) {
	t.Run("connect manifest", func(t *testing.T) {
		fixture := newSessionFixture(t)
		owned, err := cloneConnectInput(fixture.input)
		if err != nil {
			t.Fatal(err)
		}
		fixture.input.CapabilityManifest.ProtocolOffer.Supported[0] = 99
		fixture.input.CapabilityManifest.HarnessExecutableDigest[0] ^= 0xff
		fixture.input.CapabilityManifest.IsolationEvidence[0] = "mutated"
		fixture.input.CapabilityManifest.Features[0] = "mutated"
		if owned.CapabilityManifest.ProtocolOffer.Supported[0] != 1 ||
			owned.CapabilityManifest.HarnessExecutableDigest[0] == fixture.input.CapabilityManifest.HarnessExecutableDigest[0] ||
			owned.CapabilityManifest.IsolationEvidence[0] == fixture.input.CapabilityManifest.IsolationEvidence[0] ||
			owned.CapabilityManifest.Features[0] == fixture.input.CapabilityManifest.Features[0] {
			t.Fatal("connector retained mutable capability-manifest storage")
		}
	})

	t.Run("exchange batch", func(t *testing.T) {
		fixture := newSessionFixture(t)
		session := mustReadySession(t, fixture)
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(release) })
			waitSignal(t, finished, "exchange did not finish during cleanup")
		})
		observed := make(chan bool, 1)
		fixture.exchange.setHandler(func(_ context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
			defer close(finished)
			close(started)
			select {
			case <-release:
				observed <- batch.Frames[0].Heartbeat.Available
				return nil, nil
			case <-time.After(time.Second):
				return nil, errors.New("exchange release was not observed")
			}
		})
		batch := heartbeatBatch(session.Snapshot(), 4, 2, true)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := session.Exchange(ctx, batch)
			done <- err
		}()
		waitSignal(t, started, "exchange did not start")
		cancel()
		if err := waitError(t, done, "cancelled exchange did not return"); !errors.Is(err, context.Canceled) {
			t.Fatalf("exchange error=%v", err)
		}
		batch.Frames[0].Heartbeat.Available = false
		releaseOnce.Do(func() { close(release) })
		select {
		case available := <-observed:
			if !available {
				t.Fatal("exchange retained caller-owned batch storage")
			}
		case <-time.After(time.Second):
			t.Fatal("exchange did not observe owned batch")
		}
	})
}

func TestSessionPreCancelledExchangeDoesNotReachPort(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	before := fixture.exchange.calls.Load()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := session.Exchange(ctx, heartbeatBatch(session.Snapshot(), 4, 2, true)); !errors.Is(err, context.Canceled) {
		t.Fatalf("exchange error=%v", err)
	}
	if fixture.exchange.calls.Load() != before || session.Snapshot().State != StateReady {
		t.Fatal("pre-cancelled exchange reached the port or poisoned the session")
	}
}

func TestConnectorClosesFactoryAdapterReturnedWithError(t *testing.T) {
	fixture := newSessionFixture(t)
	fixture.factory.openErr = errors.New("factory failed after allocation")
	if _, err := fixture.connector.Connect(context.Background(), fixture.input); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("connect error=%v", err)
	}
	if fixture.exchange.closed.Load() != 1 {
		t.Fatalf("factory adapter closes=%d", fixture.exchange.closed.Load())
	}
}

func TestSessionAcceptsExactPlatformReplayAndRejectsDivergence(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	binding := session.Snapshot()
	drain := attachedworkerprotocol.FrameV1{
		Version:   binding.ProtocolVersion,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 3),
		WorkerID:  string(binding.WorkerID), EnrollmentGeneration: binding.EnrollmentGeneration,
		ConnectionGeneration: binding.ConnectionGeneration, Sequence: 3, Ack: 4,
		Kind: attachedworkerprotocol.MessageDrain, Drain: &attachedworkerprotocol.DrainV1{Revision: 1},
	}
	responses := []*attachedworkerprotocol.BatchV1{
		{Version: 1, Frames: []attachedworkerprotocol.FrameV1{drain}},
		{Version: 1, Frames: []attachedworkerprotocol.FrameV1{drain}},
	}
	var index atomic.Int32
	fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return responses[int(index.Add(1))-1], nil
	})
	if _, err := session.Exchange(context.Background(), heartbeatBatch(binding, 4, 2, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Exchange(context.Background(), heartbeatBatch(binding, 5, 3, false)); err != nil {
		t.Fatalf("exact replay error=%v", err)
	}
	divergent := drain
	divergent.Drain = &attachedworkerprotocol.DrainV1{Revision: 2}
	fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return &attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{divergent}}, nil
	})
	if _, err := session.Exchange(context.Background(), heartbeatBatch(binding, 6, 3, false)); !errors.Is(err, ErrReconciliationRequired) {
		t.Fatalf("divergent replay error=%v", err)
	}
	if session.Snapshot().State != StateReconciliationRequired {
		t.Fatalf("snapshot=%+v", session.Snapshot())
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionRevocationRequiresExactAcknowledgementThenFences(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	binding := session.Snapshot()
	revoke := attachedworkerprotocol.RevokeV1{Revision: 1, NextEnrollmentGeneration: 2, NextConnectionGeneration: 2}
	fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return &attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{{
			Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 3),
			WorkerID: string(binding.WorkerID), EnrollmentGeneration: 1, ConnectionGeneration: 1,
			Sequence: 3, Ack: 4, Kind: attachedworkerprotocol.MessageRevoke, Revoke: &revoke,
		}}}, nil
	})
	if _, err := session.Exchange(context.Background(), heartbeatBatch(binding, 4, 2, true)); err != nil {
		t.Fatal(err)
	}
	fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return nil, nil
	})
	ack := attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 5),
		WorkerID: string(binding.WorkerID), EnrollmentGeneration: 1, ConnectionGeneration: 1,
		Sequence: 5, Ack: 3, Kind: attachedworkerprotocol.MessageRevoked,
		Revoked: &attachedworkerprotocol.RevokedV1{Revision: 1, NextEnrollmentGeneration: 2, NextConnectionGeneration: 2},
	}}}
	if _, err := session.Exchange(context.Background(), ack); err != nil {
		t.Fatal(err)
	}
	if session.Snapshot().State != StateFenced || session.Snapshot().FailureCode != "revoked" {
		t.Fatalf("snapshot=%+v", session.Snapshot())
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type sessionFixture struct {
	store     *attachedworkerlocal.Store
	manifest  attachedworkerlocal.ManifestV1
	secret    attachedworkerlocal.SecretRecordV1
	input     ConnectInputV1
	config    Config
	bootstrap *fakeBootstrap
	exchange  *fakeExchange
	factory   *fakeFactory
	connector *Connector
}

func newSessionFixture(t *testing.T) *sessionFixture {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(parent, "docker")
	executableBytes := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(executable, executableBytes, 0o700); err != nil {
		t.Fatal(err)
	}
	cliConfig := filepath.Join(parent, "docker-config")
	if err := os.Mkdir(cliConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	executableDigest := fmt.Sprintf("%x", sha256.Sum256(executableBytes))
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x31}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{
			DockerPath: executable, DockerSHA256: executableDigest, CLIConfigDir: cliConfig,
			Host: "unix:///run/docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-001",
			Boundary: attachedworkerlocal.BoundaryLinuxRootless,
			Image:    "registry.example/worker@sha256:" + strings.Repeat("b", 64),
			UserID:   1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024,
			MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10,
		},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: executable, SHA256: executableDigest, Arguments: []string{"--attached"}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: sessionTestTime, UpdatedAt: sessionTestTime,
	}
	if runtime.GOOS == "darwin" {
		manifest.OCI.Boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	secret := attachedworkerlocal.SecretRecordV1{
		Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID,
		WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: append([]byte(nil), private...),
	}
	store, err := attachedworkerlocal.NewStore(filepath.Join(parent, "state"), func() time.Time { return sessionTestTime })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	offer := testOffer()
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: 1, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH, BuildID: "build-001",
		HarnessName: "codex", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: mustDecodeHex(t, executableDigest),
		IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{
			attachedworkerprotocol.IsolationFilesystemBoundary,
			attachedworkerprotocol.IsolationNetworkBoundary,
			attachedworkerprotocol.IsolationProcessBoundary,
		},
		Features: []attachedworkerprotocol.ProtocolFeatureV1{
			attachedworkerprotocol.FeatureCancellation,
			attachedworkerprotocol.FeatureProgress,
			attachedworkerprotocol.FeatureReconnect,
		},
		MaxConcurrentAttempts: 1,
	}
	bootstrap := &fakeBootstrap{now: sessionTestTime, publicKey: public}
	exchange := &fakeExchange{}
	factory := &fakeFactory{exchange: exchange}
	config := Config{
		Audience: "sessionless:attached-worker:v1", WorkerOffer: offer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: time.Second,
		Random: bytes.NewReader(append(bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x42}, 32)...)),
		Now:    func() time.Time { return sessionTestTime },
	}
	connector, err := New(store, bootstrap, factory, config)
	if err != nil {
		t.Fatal(err)
	}
	return &sessionFixture{
		store: store, manifest: manifest, secret: secret,
		input:  ConnectInputV1{ExpectedWorkerRevision: 7, CapabilityManifest: capability},
		config: config, bootstrap: bootstrap, exchange: exchange, factory: factory, connector: connector,
	}
}

func (fixture *sessionFixture) newBootstrap() *fakeBootstrap {
	return &fakeBootstrap{now: sessionTestTime, publicKey: ed25519.PrivateKey(fixture.secret.IdentityPrivateKey).Public().(ed25519.PublicKey)}
}

type fakeBootstrap struct {
	now               time.Time
	publicKey         ed25519.PublicKey
	challengeCalls    atomic.Int32
	activateCalls     atomic.Int32
	challengeHook     func()
	activateHook      func()
	challengeMutation func(*attachedworkerhttp.ChallengeResponseV1)
	challenge         domain.AttachedWorkerAttachChallenge
}

func (fake *fakeBootstrap) IssueChallenge(_ context.Context, input attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	fake.challengeCalls.Add(1)
	if fake.challengeHook != nil {
		fake.challengeHook()
	}
	request := attachedworkertransport.IssueChallengeRequest{
		WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ExpectedAudience: input.ExpectedAudience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: input.Purpose, Hello: input.Hello,
	}
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(
		input.TenantLocator, input.OwnerLocator, request.WorkerID, input.Hello.EnrollmentGeneration,
		input.Hello.ConnectionGeneration-1, request,
	)
	if err != nil || !ed25519.Verify(fake.publicKey, transcript, input.Proof) {
		return nil, &attachedworkerhttp.ExchangeError{Kind: attachedworkerhttp.ErrorUnauthorized}
	}
	platformNonce := bytes.Repeat([]byte{0x52}, 32)
	fake.challenge = domain.AttachedWorkerAttachChallenge{
		TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, ID: "challenge-001",
		WorkerID: request.WorkerID, ConnectionID: "connection-001", Purpose: input.Purpose, Audience: input.ExpectedAudience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1, TargetConnectionGeneration: input.Hello.ConnectionGeneration,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
		WorkerNonceDigest:   domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce),
		PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(platformNonce),
		CreatedAt:           fake.now, ExpiresAt: fake.now.Add(time.Minute), RetainUntil: fake.now.Add(time.Hour), Revision: 1,
	}
	response := &attachedworkerhttp.ChallengeResponseV1{Challenge: fake.challenge, Frame: attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
		WorkerID: input.Hello.WorkerID, EnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ConnectionGeneration: input.Hello.ConnectionGeneration, Sequence: 1, Ack: 1,
		Kind: attachedworkerprotocol.MessageChallenge, Challenge: &attachedworkerprotocol.ChallengeV1{
			WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: testOffer(), SelectedVersion: 1,
			WorkerNonce: append([]byte(nil), input.Hello.Hello.WorkerNonce...), PlatformNonce: platformNonce,
		},
	}}
	if fake.challengeMutation != nil {
		fake.challengeMutation(response)
	}
	return response, nil
}

func (fake *fakeBootstrap) Activate(_ context.Context, input attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	fake.activateCalls.Add(1)
	if fake.activateHook != nil {
		fake.activateHook()
	}
	channel := attachedworkertransport.ConnectionChannelBinding(
		fake.challenge.ID, fake.challenge.WorkerNonceDigest, fake.challenge.PlatformNonceDigest, input.ConnectionSecret.Digest(),
	)
	channelBytes := mustDecodeHexValue(string(channel))
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(fake.challenge.TenantID), OwnerUserID: string(fake.challenge.OwnerUserID), WorkerID: string(fake.challenge.WorkerID),
		IdentityPublicKey: fake.publicKey, EnrollmentGeneration: fake.challenge.ExpectedEnrollmentGeneration,
		ConnectionGeneration: fake.challenge.TargetConnectionGeneration, Version: input.Attach.Version, ChannelBinding: channelBytes,
	}
	if attachedworkerprotocol.VerifyAttachV1(auth, input.Attach) != nil {
		return nil, &attachedworkerhttp.ExchangeError{Kind: attachedworkerhttp.ErrorUnauthorized}
	}
	accepted := attachedworkerprotocol.FrameV1{
		Version: input.Attach.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2),
		WorkerID: input.Attach.WorkerID, EnrollmentGeneration: input.Attach.EnrollmentGeneration,
		ConnectionGeneration: input.Attach.ConnectionGeneration, Sequence: 2, Ack: 2,
		Kind: attachedworkerprotocol.MessageAttachAccepted, AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{
			WorkerOffer: input.Attach.Attach.WorkerOffer, PlatformOffer: input.Attach.Attach.PlatformOffer,
			SelectedVersion: input.Attach.Version, WorkerNonce: append([]byte(nil), input.Attach.Attach.WorkerNonce...),
			PlatformNonce:    append([]byte(nil), input.Attach.Attach.PlatformNonce...),
			CapabilityDigest: append([]byte(nil), input.Attach.Attach.CapabilityDigest...),
		},
	}
	return &attachedworkerhttp.ActivateResponseV1{
		Connection: attachedworkerhttp.ActivateConnectionV1{
			TenantID: fake.challenge.TenantID, OwnerUserID: fake.challenge.OwnerUserID, WorkerID: fake.challenge.WorkerID,
			ID: fake.challenge.ConnectionID, ActivationChallengeID: fake.challenge.ID,
			EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration,
			ProtocolVersion:  uint32(input.Attach.Version),
			CapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", input.Attach.Attach.CapabilityDigest)),
			SecretDigest:     input.ConnectionSecret.Digest(), ChannelBinding: channel,
			State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1,
			ConnectedAt: fake.now.Add(time.Minute), AuthExpiresAt: fake.now.Add(time.Hour), Revision: 1,
		},
		Accepted: accepted,
	}, nil
}

type fakeFactory struct {
	exchange  *fakeExchange
	bearer    string
	openCalls atomic.Int32
	openErr   error
}

func (factory *fakeFactory) Open(binding ConnectionBindingV1, rawBearer []byte) (ExchangePort, error) {
	factory.openCalls.Add(1)
	if !validBinding(binding) {
		return nil, ErrInvalidAuthority
	}
	if _, err := attachedworkertransport.ParseConnectionBearer(rawBearer); err != nil {
		return nil, err
	}
	factory.bearer = string(append([]byte(nil), rawBearer...))
	return factory.exchange, factory.openErr
}

type fakeExchange struct {
	mu      sync.Mutex
	handler func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)
	calls   atomic.Int32
	closed  atomic.Int32
}

func (fake *fakeExchange) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	fake.calls.Add(1)
	fake.mu.Lock()
	handler := fake.handler
	fake.mu.Unlock()
	if handler == nil {
		return nil, nil
	}
	return handler(ctx, batch)
}

func (fake *fakeExchange) setHandler(handler func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)) {
	fake.mu.Lock()
	fake.handler = handler
	fake.mu.Unlock()
}

func (fake *fakeExchange) Close() error {
	fake.closed.Add(1)
	return nil
}

func mustReadySession(t *testing.T, fixture *sessionFixture) *Session {
	t.Helper()
	session, err := fixture.connector.Connect(context.Background(), fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	registerSessionCleanup(t, session)
	return session
}

func registerSessionCleanup(t *testing.T, session *Session) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("session cleanup: %v", err)
		}
	})
}

func waitSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

func waitError(t *testing.T, result <-chan error, failure string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal(failure)
		return nil
	}
}

func waitForRuntimeRelease(t *testing.T, store *attachedworkerlocal.Store) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		lease, err := store.AcquireRuntime(context.Background())
		if err == nil {
			if closeErr := lease.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			return
		}
		if !errors.Is(err, attachedworkerlocal.ErrStateBusy) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("runtime lease was not released")
		}
	}
}

func sessionAttemptBinding(capabilityDigest []byte, suffix string) attachedworkerprotocol.AttemptBindingV1 {
	return attachedworkerprotocol.AttemptBindingV1{
		RunID: "run-" + suffix, AttemptID: "attempt-" + suffix, LeaseID: "lease-" + suffix,
		LeaseGeneration: 7, FenceToken: "fence-" + suffix,
		ExpiresAtUnixMicro: sessionTestTime.Add(30 * time.Minute).UnixMicro(),
		ContextDigest:      bytes.Repeat([]byte{0x61}, sha256.Size), CapabilityDigest: append([]byte(nil), capabilityDigest...),
		PolicyDigest: bytes.Repeat([]byte{0x62}, sha256.Size),
	}
}

func sessionSameAttemptBinding(left, right attachedworkerprotocol.AttemptBindingV1) bool {
	return left.RunID == right.RunID && left.AttemptID == right.AttemptID && left.LeaseID == right.LeaseID &&
		left.LeaseGeneration == right.LeaseGeneration && left.FenceToken == right.FenceToken &&
		left.ExpiresAtUnixMicro == right.ExpiresAtUnixMicro && bytes.Equal(left.ContextDigest, right.ContextDigest) &&
		bytes.Equal(left.CapabilityDigest, right.CapabilityDigest) && bytes.Equal(left.PolicyDigest, right.PolicyDigest)
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded := mustDecodeHexValue(value)
	if decoded == nil {
		t.Fatalf("invalid hex %q", value)
	}
	return decoded
}

func mustDecodeHexValue(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil
	}
	return decoded
}

func heartbeatBatch(binding SnapshotV1, sequence, ack uint64, available bool) attachedworkerprotocol.BatchV1 {
	return attachedworkerprotocol.BatchV1{Version: binding.ProtocolVersion, Frames: []attachedworkerprotocol.FrameV1{{
		Version:   binding.ProtocolVersion,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence),
		WorkerID:  string(binding.WorkerID), EnrollmentGeneration: binding.EnrollmentGeneration,
		ConnectionGeneration: binding.ConnectionGeneration, Sequence: sequence, Ack: ack,
		Kind:      attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: sessionTestTime.UnixMicro(), Available: available},
	}}}
}

func testOffer() attachedworkerprotocol.VersionOfferV1 {
	return attachedworkerprotocol.VersionOfferV1{
		Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{1},
	}
}

func assertNoNetworkAndGeneration(t *testing.T, fixture *sessionFixture, generation uint64) {
	t.Helper()
	if fixture.bootstrap.challengeCalls.Load() != 0 || fixture.bootstrap.activateCalls.Load() != 0 || fixture.exchange.calls.Load() != 0 {
		t.Fatalf("network calls challenge=%d activate=%d exchange=%d", fixture.bootstrap.challengeCalls.Load(), fixture.bootstrap.activateCalls.Load(), fixture.exchange.calls.Load())
	}
	assertGeneration(t, fixture, generation)
}

func assertGeneration(t *testing.T, fixture *sessionFixture, generation uint64) {
	t.Helper()
	manifest, err := fixture.store.Load(context.Background())
	if err != nil || manifest.ConnectionGeneration != generation {
		t.Fatalf("generation=%d err=%v", manifest.ConnectionGeneration, err)
	}
}

func assertSessionFormattingIsSecretFree(t *testing.T, fixture *sessionFixture, session *Session) {
	t.Helper()
	encoded, err := json.Marshal(session.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	formatted := fmt.Sprintf("%v %#v %s", session, session, encoded)
	for _, secret := range []string{
		fixture.bootstrapChallengeNonce(), fixture.factory.bearer,
		fmt.Sprintf("%x", fixture.secret.IdentityPrivateKey), strings.Repeat("41", 32), strings.Repeat("42", 32), strings.Repeat("52", 32),
	} {
		if secret != "" && strings.Contains(formatted, secret) {
			t.Fatalf("formatted session leaked secret material: %q", formatted)
		}
	}
}

func (fixture *sessionFixture) bootstrapChallengeNonce() string { return strings.Repeat("52", 32) }
