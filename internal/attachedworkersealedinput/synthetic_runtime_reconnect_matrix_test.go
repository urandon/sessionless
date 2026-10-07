package attachedworkersealedinput

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

// Each checkpoint is produced by the real authenticated session protocol and
// persisted by the local store. Reconnect then exercises the exported pinned
// composition, not a mocked ReconnectRecovery result or edited scalar state.
func TestSyntheticPinnedReconnectFencesPersistedNonIdleHeads(t *testing.T) {
	for _, test := range []struct {
		name              string
		drainOnFirst      bool
		ambiguousClaim    bool
		wantConnection    attachedworkerprotocol.ConnectionState
		wantAttempt       attachedworkerprotocol.AttemptState
		wantExchangeSteps int
	}{
		{name: "claimed", wantConnection: attachedworkerprotocol.ConnectionReady,
			wantAttempt: attachedworkerprotocol.AttemptClaimed, wantExchangeSteps: 2},
		{name: "draining idle", drainOnFirst: true, wantConnection: attachedworkerprotocol.ConnectionDraining,
			wantAttempt: attachedworkerprotocol.AttemptIdle, wantExchangeSteps: 1},
		{name: "ambiguous claim", ambiguousClaim: true, wantConnection: attachedworkerprotocol.ConnectionReady,
			wantAttempt: attachedworkerprotocol.AttemptOffered, wantExchangeSteps: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newJoinedSuccessFixture(t)
			config := fixture.config
			fixture.exchange.drainOnFirst = test.drainOnFirst
			fixture.exchange.ambiguousClaim = test.ambiguousClaim
			connector, err := attachedworkersession.New(fixture.store, config.Bootstrap, fixture.factory, config.Session)
			if err != nil {
				t.Fatal(err)
			}
			session, err := connector.Connect(context.Background(), config.Connect)
			if err != nil {
				t.Fatalf("construct authenticated checkpoint session: %v", err)
			}
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := session.Close(cleanupCtx); err != nil {
					t.Errorf("close checkpoint session: %v", err)
				}
			})
			response, err := session.ExchangeAction(context.Background(), attachedworkersession.ActionV1{
				Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: joinedTestTime.UnixMicro(), Available: true},
			})
			wantResponse := attachedworkerprotocol.MessageLeaseOffer
			if test.drainOnFirst {
				wantResponse = attachedworkerprotocol.MessageDrain
			}
			if err != nil || response == nil || response.Kind != wantResponse {
				t.Fatalf("initial protocol response=%+v error=%v, want %s", response, err, wantResponse)
			}
			if !test.drainOnFirst {
				response, err = session.ExchangeAction(context.Background(), attachedworkersession.ActionV1{
					LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: fixture.binding, AttemptSequence: 1},
				})
				if test.ambiguousClaim {
					if response != nil || !errors.Is(err, attachedworkersession.ErrReconciliationRequired) {
						t.Fatalf("ambiguous claim response=%+v error=%v", response, err)
					}
				} else if err != nil || response == nil || response.Kind != attachedworkerprotocol.MessageLeaseAccepted {
					t.Fatalf("accepted claim response=%+v error=%v", response, err)
				}
			}
			if err := session.Close(context.Background()); err != nil {
				t.Fatalf("close checkpoint session: %v", err)
			}
			if fixture.exchange.steps != test.wantExchangeSteps || fixture.exchange.closed.Load() != 1 {
				t.Fatalf("checkpoint exchange steps=%d closed=%d, want %d/1", fixture.exchange.steps,
					fixture.exchange.closed.Load(), test.wantExchangeSteps)
			}
			lease, err := fixture.store.AcquireRuntime(context.Background())
			if err != nil {
				t.Fatalf("checkpoint session leaked runtime lease: %v", err)
			}
			checkpoint, err := lease.LoadReconnectCheckpoint(context.Background())
			if err != nil {
				_ = lease.Close()
				t.Fatal(err)
			}
			if err := lease.Close(); err != nil {
				t.Fatal(err)
			}
			if checkpoint.ConnectionGeneration != 1 || checkpoint.MachineSnapshot.Connection != test.wantConnection ||
				checkpoint.MachineSnapshot.Attempt.Summary.State != test.wantAttempt {
				t.Fatalf("persisted checkpoint generation=%d connection=%s attempt=%s, want 1/%s/%s",
					checkpoint.ConnectionGeneration, checkpoint.MachineSnapshot.Connection,
					checkpoint.MachineSnapshot.Attempt.Summary.State, test.wantConnection, test.wantAttempt)
			}
			fixture.clock.Set(joinedTestTime.Add(2 * time.Minute))
			config.Bootstrap = newJoinedReconnectBootstrap(fixture.clock.Now(), fixture.manifest, checkpoint, fixture.public)
			config.Session.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x51}, 32), bytes.Repeat([]byte{0x52}, 32)...))
			config.Poll.Random = bytes.NewReader(bytes.Repeat([]byte{0x53}, 8))
			newExchange := &joinedExchange{idleHeartbeat: make(chan struct{}, 1)}
			config.Exchange = &joinedFactory{exchange: newExchange}
			owner, err := ReconnectSyntheticPinnedRuntime(context.Background(), config)
			if owner != nil || (!errors.Is(err, attachedworkerdaemontransport.ErrReconciliationRequired) &&
				!errors.Is(err, attachedworkersession.ErrReconciliationRequired)) {
				if owner != nil {
					_ = owner.Close(context.Background())
				}
				t.Fatalf("non-idle checkpoint reopened runtime: owner=%v error=%v", owner, err)
			}
			if newExchange.closed.Load() != 1 || len(newExchange.idleHeartbeat) != 0 {
				t.Errorf("fenced reconnect exchange closed=%d polls=%d, want 1/0; reconnect error=%v",
					newExchange.closed.Load(), len(newExchange.idleHeartbeat), err)
			}
			commands, err := os.ReadFile(fixture.commandLog)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(commands), "container create ") || strings.Contains(string(commands), "container start ") ||
				fixture.authorizer.calls != 0 || fixture.jobs.calls != 0 || fixture.blobs.calls != 0 {
				t.Errorf("fenced recovery launched effect: commands=%q sealed=%d jobs=%d blobs=%d",
					commands, fixture.authorizer.calls, fixture.jobs.calls, fixture.blobs.calls)
			}
			finalLease, err := fixture.store.AcquireRuntime(context.Background())
			if err != nil {
				t.Fatalf("fenced reconnect leaked runtime lease: %v", err)
			}
			defer func() {
				if err := finalLease.Close(); err != nil {
					t.Errorf("close fenced proof lease: %v", err)
				}
			}()
			finalLocal, err := finalLease.LoadSnapshot(context.Background())
			if err != nil || finalLocal.Manifest.ConnectionGeneration != 2 {
				t.Fatalf("fenced local generation=%d error=%v, want 2", finalLocal.Manifest.ConnectionGeneration, err)
			}
			finalCheckpoint, err := finalLease.LoadReconnectCheckpoint(context.Background())
			if test.drainOnFirst {
				if !errors.Is(err, attachedworkerlocal.ErrStateMissing) {
					t.Errorf("drained connection retained checkpoint=%+v error=%v", finalCheckpoint, err)
				}
			} else if err != nil || finalCheckpoint.ConnectionGeneration != 2 ||
				finalCheckpoint.MachineSnapshot.Attempt.Summary.State != test.wantAttempt {
				t.Errorf("fenced checkpoint generation=%d attempt=%s error=%v, want 2/%s", finalCheckpoint.ConnectionGeneration,
					finalCheckpoint.MachineSnapshot.Attempt.Summary.State, err, test.wantAttempt)
			}
		})
	}
}
