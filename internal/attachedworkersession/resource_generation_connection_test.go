package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestResourceGenerationConnectionRequiresOriginalReadyBinding(t *testing.T) {
	f := newSessionFixture(t)
	session := mustReadySession(t, f)
	expected := session.binding
	_, before := sessionProtocolState(t, session)
	calls := f.exchange.calls.Load()
	expires := sessionTestTime.Add(time.Minute)
	if err := session.CheckResourceGenerationConnectionV1(t.Context(), expected, ResourceGenerationInitialV1, &expires); err != nil {
		t.Fatal("original ready owner denied", err)
	}
	_, after := sessionProtocolState(t, session)
	if !reflect.DeepEqual(before, after) || f.exchange.calls.Load() != calls || session.nativeAttempt != nil {
		t.Fatal("metadata check changed protocol/attempt authority or sent an exchange")
	}
	for _, tc := range []struct {
		name   string
		change func(*ConnectionBindingV1)
	}{
		{"tenant", func(v *ConnectionBindingV1) { v.TenantID = "foreign" }},
		{"owner", func(v *ConnectionBindingV1) { v.OwnerUserID = "foreign" }},
		{"worker", func(v *ConnectionBindingV1) { v.WorkerID = "foreign" }},
		{"enrollment", func(v *ConnectionBindingV1) { v.EnrollmentGeneration++ }},
		{"connection generation", func(v *ConnectionBindingV1) { v.ConnectionGeneration++ }},
		{"protocol", func(v *ConnectionBindingV1) { v.ProtocolVersion++ }},
		{"connection ID", func(v *ConnectionBindingV1) { v.ConnectionID = "foreign" }},
		{"capability", func(v *ConnectionBindingV1) {
			v.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("f", 64))
		}},
		{"auth expiry", func(v *ConnectionBindingV1) { v.AuthenticationExpires = v.AuthenticationExpires.Add(time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foreign := expected
			tc.change(&foreign)
			if err := session.CheckResourceGenerationConnectionV1(t.Context(), foreign, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
				t.Fatal("changed Open binding accepted", err)
			}
		})
	}
	// A copied readiness/identity graph cannot become the original owner. Do
	// not copy mutexes: copy only the fields an adversary would try to reuse.
	copy := &Session{nativeIdentity: session, nativeFresh: session.nativeFresh, state: session.state,
		binding: session.binding, machine: session.machine, exchange: session.exchange, lease: session.lease, now: session.now}
	if err := copy.CheckResourceGenerationConnectionV1(t.Context(), expected, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
		t.Fatal("copied Session accepted", err)
	}
	for _, mode := range []ResourceGenerationConnectionModeV1{0, 3, ResourceGenerationReconnectV1} {
		if err := session.CheckResourceGenerationConnectionV1(t.Context(), expected, mode, nil); err != ErrInvalidAuthority {
			t.Fatal("unknown/reconnect mode accepted for initial Session", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, input := range []context.Context{nil, ctx} {
		if err := session.CheckResourceGenerationConnectionV1(input, expected, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
			t.Fatal("invalid context accepted", err)
		}
	}
	var absent *Session
	if err := absent.CheckResourceGenerationConnectionV1(t.Context(), expected, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
		t.Fatal("nil Session accepted", err)
	}
	if err := (&Session{}).CheckResourceGenerationConnectionV1(t.Context(), expected, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
		t.Fatal("zero Session accepted", err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.CheckResourceGenerationConnectionV1(t.Context(), expected, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
		t.Fatal("closed original owner accepted", err)
	}
}

func TestResourceGenerationConnectionUsesOriginalClockAndSeparatesDenial(t *testing.T) {
	f := newSessionFixture(t)
	now := sessionTestTime
	config := f.config
	config.Now = func() time.Time { return now }
	connector, err := New(f.store, f.bootstrap, f.factory, config)
	if err != nil {
		t.Fatal(err)
	}
	session, err := connector.Connect(t.Context(), f.input)
	if err != nil {
		t.Fatal(err)
	}
	registerSessionCleanup(t, session)
	for _, expiry := range []time.Time{{}, now.Add(-time.Nanosecond), now} {
		if err := session.CheckResourceGenerationConnectionV1(t.Context(), session.binding, ResourceGenerationInitialV1, &expiry); err != ErrInvalidAuthority {
			t.Fatal("positive expired evidence accepted", err)
		}
		if err := session.CheckResourceGenerationConnectionV1(t.Context(), session.binding, ResourceGenerationInitialV1, nil); err != nil {
			t.Fatal("expired evidence prevented identity-only denial/readback", err)
		}
	}
	now = session.binding.AuthenticationExpires
	if err := session.CheckResourceGenerationConnectionV1(t.Context(), session.binding, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
		t.Fatal("original authentication window renewed", err)
	}
}

func TestResourceGenerationConnectionDoesNotGateOnAttemptIdle(t *testing.T) {
	session, _, _, _, _ := nativeClaimedSession(t)
	_, before := sessionProtocolState(t, session)
	if before.Attempt.Summary.State != attachedworkerprotocol.AttemptClaimed {
		t.Fatal("fixture did not reach genuine claimed state")
	}
	if err := session.CheckResourceGenerationConnectionV1(t.Context(), session.binding, ResourceGenerationInitialV1, nil); err != nil {
		t.Fatal("metadata check required idle and could block owned completion", err)
	}
	_, after := sessionProtocolState(t, session)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("metadata check changed original claim")
	}
}

func TestResourceGenerationConnectionRejectsPendingProtocolRevoke(t *testing.T) {
	f := newSessionFixture(t)
	session := mustReadySession(t, f)
	binding := session.Snapshot()
	revoke := attachedworkerprotocol.RevokeV1{Revision: 1, NextEnrollmentGeneration: 2, NextConnectionGeneration: 2}
	f.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return &attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{{
			Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 3),
			WorkerID: string(binding.WorkerID), EnrollmentGeneration: 1, ConnectionGeneration: 1,
			Sequence: 3, Ack: 4, Kind: attachedworkerprotocol.MessageRevoke, Revoke: &revoke,
		}}}, nil
	})
	if _, err := session.Exchange(t.Context(), heartbeatBatch(binding, 4, 2, true)); err != nil {
		t.Fatal("receive genuine protocol revoke", err)
	}
	_, before := sessionProtocolState(t, session)
	if session.Snapshot().State != StateReady || before.Revoke == nil || *before.Revoke != revoke {
		t.Fatal("fixture did not establish Ready owner with pending protocol revoke")
	}
	calls := f.exchange.calls.Load()
	expires := sessionTestTime.Add(time.Minute)
	for _, expiry := range []*time.Time{nil, &expires} {
		if err := session.CheckResourceGenerationConnectionV1(t.Context(), session.binding, ResourceGenerationInitialV1, expiry); err != ErrInvalidAuthority {
			t.Errorf("pending protocol revoke accepted for metadata proof: positive=%v err=%v", expiry != nil, err)
		}
	}
	_, after := sessionProtocolState(t, session)
	if !reflect.DeepEqual(before, after) || f.exchange.calls.Load() != calls || session.Snapshot().State != StateReady {
		t.Fatal("denied metadata proof changed pending revoke/ACK ownership")
	}
}

func TestResourceGenerationConnectionReconnectCannotInitialize(t *testing.T) {
	f := newSessionFixture(t)
	initial := mustReadySession(t, f)
	previousConfig, previousSnapshot := sessionProtocolState(t, initial)
	if err := initial.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	bootstrap := &reconnectBootstrap{now: f.bootstrap.now,
		publicKey:      ed25519.PrivateKey(f.secret.IdentityPrivateKey).Public().(ed25519.PublicKey),
		previousConfig: previousConfig, previousSnapshot: previousSnapshot}
	config := f.config
	config.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x61}, 32), bytes.Repeat([]byte{0x62}, 32)...))
	connector, err := New(f.store, bootstrap, &fakeFactory{exchange: &fakeExchange{}}, config)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := connector.Reconnect(t.Context(), ReconnectInputV1(f.input))
	if err != nil {
		t.Fatal("genuine signed reconnect", err)
	}
	registerSessionCleanup(t, resumed)
	if err := resumed.CheckResourceGenerationConnectionV1(t.Context(), resumed.binding, ResourceGenerationReconnectV1, nil); err != nil {
		t.Fatal("original reconnected owner denied metadata read", err)
	}
	if err := resumed.CheckResourceGenerationConnectionV1(t.Context(), resumed.binding, ResourceGenerationInitialV1, nil); err != ErrInvalidAuthority {
		t.Fatal("reconnect masqueraded as initial publication", err)
	}
	if err := resumed.CheckResourceGenerationConnectionV1(t.Context(), initial.binding, ResourceGenerationReconnectV1, nil); err != ErrInvalidAuthority {
		t.Fatal("old connection binding survived reconnect", err)
	}
}
