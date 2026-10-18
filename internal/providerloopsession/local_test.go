package providerloopsession

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// Pure gate/context tests, not factory, SQL, runtime or HTTP evidence.
func TestLocalSessionBeginIntersectsCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, cancelBase := context.WithTimeout(t.Context(), time.Hour)
		defer cancelBase()
		caller, cancelCaller := context.WithTimeout(t.Context(), time.Minute)
		defer cancelCaller()
		v := &LocalV1{state: &localState{gate: make(chan struct{}, 1), ctx: base, caller: t.Context()}}
		_, call, finish, err := v.begin(caller)
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		want, _ := caller.Deadline()
		if got, ok := call.Deadline(); !ok || !got.Equal(want) {
			t.Fatal("per-call deadline not structurally propagated")
		}
		time.Sleep(time.Minute) // explicit virtual expiry, not readiness
		synctest.Wait()
		// The same deadline also starts the caller's cancellation callback.
		// Either timer may win; cancellation cause is not a scheduling contract.
		if call.Err() == nil || base.Err() != nil {
			t.Fatalf("caller expiry did not isolate operation: call=%v base=%v", call.Err(), base.Err())
		}
	})
}

func TestLocalSessionBeginSynchronouslyChecksConstructorCaller(t *testing.T) {
	caller, cancel := context.WithCancel(t.Context())
	v := &LocalV1{state: &localState{gate: make(chan struct{}, 1), ctx: t.Context(), caller: caller}}
	cancel()
	// No asynchronous hook or scheduler wait: cancellation itself must deny entry.
	if _, _, finish, err := v.begin(t.Context()); err == nil {
		finish()
		t.Fatal("cancelled constructor caller admitted operation")
	}
	if len(v.state.gate) != 0 {
		t.Fatal("failed begin retained gate")
	}
}
