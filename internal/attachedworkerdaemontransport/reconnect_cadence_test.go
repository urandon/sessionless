package attachedworkerdaemontransport

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

type fakeReconnectPort struct {
	session recoveredSession
	err     error
	calls   int
}

func (port *fakeReconnectPort) Reconnect(context.Context, attachedworkersession.ReconnectInputV1) (recoveredSession, error) {
	port.calls++
	return port.session, port.err
}

type fakeRecoveredSession struct {
	*fakeSession
	recovery    attachedworkersession.ReconnectRecoveryV1
	recoveryErr error
	closeCalls  int
}

func (session *fakeRecoveredSession) ReconnectRecovery() (attachedworkersession.ReconnectRecoveryV1, error) {
	return session.recovery, session.recoveryErr
}

func (session *fakeRecoveredSession) Close(context.Context) error {
	session.closeCalls++
	return nil
}

func idleRecoveredFixture(t *testing.T) (*adapterFixture, *fakeRecoveredSession, *fakeReconnectPort) {
	t.Helper()
	fixture := newAdapterFixture(t)
	session := &fakeRecoveredSession{
		fakeSession: fixture.session,
		recovery: attachedworkersession.ReconnectRecoveryV1{
			ConnectionState:  attachedworkerprotocol.ConnectionReady,
			AttemptState:     attachedworkerprotocol.AttemptIdle,
			TerminalDecision: attachedworkerprotocol.ReconnectTerminalNone,
		},
	}
	return fixture, session, &fakeReconnectPort{session: session}
}

func TestReconnectIdleCadenceDisabledBeforeAnyNetwork(t *testing.T) {
	fixture, session, port := idleRecoveredFixture(t)
	_, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, cadenceConfig(false))
	if !errors.Is(err, attachedworkertransport.ErrPollingDisabled) || port.calls != 0 || session.closeCalls != 0 {
		t.Fatalf("disabled recovery err=%v reconnect_calls=%d close_calls=%d", err, port.calls, session.closeCalls)
	}
}

func TestReconnectIdleCadenceStopsBeforePollOnFailedOrActiveRecovery(t *testing.T) {
	for _, test := range []struct {
		name      string
		prepare   func(*fakeRecoveredSession, *fakeReconnectPort)
		wantCall  int
		wantClose int
	}{
		{name: "server reconciliation failed", prepare: func(_ *fakeRecoveredSession, port *fakeReconnectPort) {
			port.err = attachedworkersession.ErrReconciliationRequired
		}, wantCall: 1, wantClose: 0},
		{name: "active attempt is not idle poll authority", prepare: func(session *fakeRecoveredSession, _ *fakeReconnectPort) {
			session.recovery.AttemptState = attachedworkerprotocol.AttemptClaimed
		}, wantCall: 1, wantClose: 1},
		{name: "draining connection is not available presence", prepare: func(session *fakeRecoveredSession, _ *fakeReconnectPort) {
			session.recovery.ConnectionState = attachedworkerprotocol.ConnectionDraining
		}, wantCall: 1, wantClose: 1},
		{name: "recovery summary unavailable", prepare: func(session *fakeRecoveredSession, _ *fakeReconnectPort) {
			session.recoveryErr = attachedworkersession.ErrReconciliationRequired
		}, wantCall: 1, wantClose: 1},
		{name: "session is not ready", prepare: func(session *fakeRecoveredSession, _ *fakeReconnectPort) {
			session.snapshot.State = attachedworkersession.StateFenced
		}, wantCall: 1, wantClose: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, session, port := idleRecoveredFixture(t)
			test.prepare(session, port)
			result, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
				fixture.materializer, fixture.config, cadenceConfig(true))
			if err == nil || result != nil || port.calls != test.wantCall || session.closeCalls != test.wantClose || len(session.actions) != 0 {
				t.Fatalf("recovery result=%t err=%v reconnect_calls=%d close_calls=%d actions=%d", result != nil, err, port.calls, session.closeCalls, len(session.actions))
			}
		})
	}
}

func TestReconnectIdleCadenceForcesPostManifestCooldownAndOwnsSession(t *testing.T) {
	fixture, session, port := idleRecoveredFixture(t)
	config := cadenceConfig(true)
	config.StartWithCooldown = false
	// The forced cooldown clock must be checked before any reconnect side
	// effect, not after a generation and Manifest are already consumed.
	config.Now = func() time.Time { return time.Time{} }
	if result, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, config); !errors.Is(err, attachedworkertransport.ErrInvalidConfig) || result != nil ||
		port.calls != 0 || session.closeCalls != 0 || len(session.actions) != 0 {
		t.Fatalf("missing cooldown result=%t err=%v reconnect_calls=%d close_calls=%d actions=%d", result != nil, err, port.calls, session.closeCalls, len(session.actions))
	}
	fixture, session, port = idleRecoveredFixture(t)
	config.Now = func() time.Time { return adapterTestTime }
	result, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, config)
	if err != nil || result == nil || result.Source == nil || result.Adapter == nil || port.calls != 1 || session.closeCalls != 0 {
		t.Fatalf("idle recovery result=%t err=%v reconnect_calls=%d close_calls=%d", result != nil, err, port.calls, session.closeCalls)
	}
	if err := result.Close(context.Background()); err != nil || session.closeCalls != 1 {
		t.Fatalf("owned session close err=%v close_calls=%d", err, session.closeCalls)
	}
}

func TestReconnectIdleCadenceRejectsInvalidLocalAdapterBeforeNetwork(t *testing.T) {
	fixture, session, port := idleRecoveredFixture(t)
	fixture.config.Profile.Name = ""
	result, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, cadenceConfig(true))
	if !errors.Is(err, ErrInvalidConfiguration) || result != nil || port.calls != 0 || session.closeCalls != 0 {
		t.Fatalf("invalid adapter result=%t err=%v reconnect_calls=%d close_calls=%d", result != nil, err, port.calls, session.closeCalls)
	}
}

func TestReconnectIdleCadenceClockFailsClosedAfterManifest(t *testing.T) {
	fixture, session, port := idleRecoveredFixture(t)
	clockCalls := 0
	config := cadenceConfig(true)
	config.Now = func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return adapterTestTime
		}
		return time.Time{}
	}
	result, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, config)
	if !errors.Is(err, attachedworkertransport.ErrReconciliationRequired) ||
		errors.Is(err, attachedworkertransport.ErrInvalidConfig) || result != nil ||
		clockCalls != 2 || port.calls != 1 || session.closeCalls != 1 || len(session.actions) != 0 {
		t.Fatalf("clock drift result=%t err=%v clock_calls=%d reconnect_calls=%d close_calls=%d actions=%d",
			result != nil, err, clockCalls, port.calls, session.closeCalls, len(session.actions))
	}
}

func TestReconnectIdleCadenceDoesNotSendImmediateHeartbeat(t *testing.T) {
	fixture, session, port := idleRecoveredFixture(t)
	clockCalls := make(chan struct{}, 8)
	config := cadenceConfig(true)
	config.Now = func() time.Time {
		clockCalls <- struct{}{}
		return adapterTestTime
	}
	result, err := reconnectIdleCadence(context.Background(), port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, config)
	if err != nil || result == nil {
		t.Fatalf("idle recovery result=%t err=%v", result != nil, err)
	}
	// Clear clock reads from local preflight and construction; only the next
	// read proves Source.Next entered the live cadence gate.
	for len(clockCalls) > 0 {
		<-clockCalls
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, _, nextErr := result.Source.Next(ctx)
		finished <- nextErr
	}()
	select {
	case <-clockCalls: // Step reached the cadence gate without an outbound action.
	case <-ctx.Done():
		t.Fatal("Step did not reach the post-Manifest cadence gate")
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) || len(session.actions) != 0 {
		t.Fatalf("cooldown Next err=%v outbound_actions=%d", err, len(session.actions))
	}
	if err := result.Close(context.Background()); err != nil || session.closeCalls != 1 {
		t.Fatalf("owned session close err=%v close_calls=%d", err, session.closeCalls)
	}
}

func TestReconnectIdleCadenceCancelledPreflightCannotReconnect(t *testing.T) {
	fixture, session, port := idleRecoveredFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := reconnectIdleCadence(ctx, port, attachedworkersession.ReconnectInputV1{},
		fixture.materializer, fixture.config, cadenceConfig(true)); !errors.Is(err, context.Canceled) || result != nil ||
		port.calls != 0 || session.closeCalls != 0 {
		t.Fatalf("cancelled recovery result=%t err=%v reconnect_calls=%d close_calls=%d", result != nil, err, port.calls, session.closeCalls)
	}
}
