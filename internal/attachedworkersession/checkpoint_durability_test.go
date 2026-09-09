package attachedworkersession

import (
	"context"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

func TestIdleCheckpointAdvancesWithExactCAS(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	before, err := session.lease.LoadReconnectCheckpoint(context.Background())
	if err != nil || before.Revision != 1 {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	if _, err := session.ExchangeAction(context.Background(), ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{
		ObservedAtUnixMicro: sessionTestTime.UnixMicro(), Available: true,
	}}); err != nil {
		t.Fatal(err)
	}
	after, err := session.lease.LoadReconnectCheckpoint(context.Background())
	if err != nil || after.Revision != before.Revision+1 ||
		after.MachineSnapshot.Worker.Sequence != before.MachineSnapshot.Worker.Sequence+1 ||
		after.MachineSnapshot.Platform.Sequence != before.MachineSnapshot.Platform.Sequence {
		t.Fatalf("after=%+v err=%v", after, err)
	}
	if err := session.lease.RetireReconnectCheckpoint(context.Background(), before.Revision); !errors.Is(err, attachedworkerlocal.ErrStateConflict) {
		t.Fatalf("stale retire error=%v", err)
	}
	if loaded, err := session.lease.LoadReconnectCheckpoint(context.Background()); err != nil || loaded.Revision != after.Revision {
		t.Fatalf("stale CAS changed checkpoint=%+v err=%v", loaded, err)
	}
}

func TestCheckpointDurabilityAmbiguityPoisonsSessionAfterValidatedEffect(t *testing.T) {
	fixture := newSessionFixture(t)
	state := checkpointFaultState{store: fixture.store}
	connector, err := newConnector(state, fixture.bootstrap, fixture.factory, fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = connector.Connect(context.Background(), fixture.input)
	if !errors.Is(err, ErrReconciliationRequired) || !errors.Is(err, attachedworkerlocal.ErrStateAmbiguous) {
		t.Fatalf("connect error=%v", err)
	}
	if fixture.bootstrap.challengeCalls.Load() != 1 || fixture.bootstrap.activateCalls.Load() != 1 ||
		fixture.factory.openCalls.Load() != 1 || fixture.exchange.calls.Load() != 1 {
		t.Fatalf("effects challenge=%d activate=%d open=%d exchange=%d",
			fixture.bootstrap.challengeCalls.Load(), fixture.bootstrap.activateCalls.Load(),
			fixture.factory.openCalls.Load(), fixture.exchange.calls.Load())
	}
	if connector.state != StateReconciliationRequired {
		t.Fatalf("connector state=%s", connector.state)
	}
	lease, err := fixture.store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	checkpoint, err := lease.LoadReconnectCheckpoint(context.Background())
	if err != nil || checkpoint.Revision != 1 {
		t.Fatalf("visible ambiguous checkpoint=%+v err=%v", checkpoint, err)
	}
}

type checkpointFaultState struct {
	store *attachedworkerlocal.Store
}

func (state checkpointFaultState) AcquireRuntime(ctx context.Context) (localRuntimeLease, error) {
	lease, err := state.store.AcquireRuntime(ctx)
	if err != nil {
		return nil, err
	}
	return &checkpointFaultLease{RuntimeLease: lease}, nil
}

type checkpointFaultLease struct {
	*attachedworkerlocal.RuntimeLease
}

func (lease *checkpointFaultLease) PersistReconnectCheckpoint(
	ctx context.Context,
	expectedRevision uint64,
	next attachedworkerlocal.ReconnectCheckpointV1,
) error {
	if err := lease.RuntimeLease.PersistReconnectCheckpoint(ctx, expectedRevision, next); err != nil {
		return err
	}
	return attachedworkerlocal.ErrStateAmbiguous
}
