//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNativePreparedCloseRetainsUnknownInputCustody(t *testing.T) {
	history, media, profile := nativeStageFixtureV2(t, []string{"png"})
	stage, err := stageNativeInputV2(history, media, profile)
	if err != nil {
		t.Fatal(err)
	}
	stage.dispatched = true // Controlled fake observation, never a real dispatch.
	slot := &preparedResultSlot{stage: history, finalized: true}
	owner := &PreparedAttachedWorkerRunnerV1{slot: slot}
	if err := owner.Close(context.Background()); !errors.Is(err, ErrContract) || owner.slot != slot {
		t.Fatalf("Close discarded unknown native custody: %v", err)
	}
	if stage.verify(context.Background(), profile, media) != nil {
		t.Fatal("Close released held input files on unknown handback")
	}
	// Even an incorrectly set clean scalar cannot override unknown handback.
	slot.stageClean = true
	if err := owner.clearSlotLocked(); !errors.Is(err, ErrContract) || owner.slot != slot {
		t.Fatalf("clean scalar discarded unknown original custody: %v", err)
	}
	stage.quiesced = true // Fake complete cleanup permits exact owned removal.
	if err := history.remove(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); err != nil || owner.slot != nil {
		t.Fatalf("clean original custody could not retire: %v", err)
	}
}

func TestNativePreparedConcurrentCloseReportsUnknownCustody(t *testing.T) {
	history, media, profile := nativeStageFixtureV2(t, nil)
	stage, err := stageNativeInputV2(history, media, profile)
	if err != nil {
		t.Fatal(err)
	}
	stage.dispatched = true // Controlled fake observation, not a process.
	cancelled := make(chan struct{})
	slot := &preparedResultSlot{stage: history, done: make(chan struct{}), cancel: func() { close(cancelled) }}
	owner := &PreparedAttachedWorkerRunnerV1{slot: slot}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- owner.Close(ctx) }()
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("Close did not cancel its original slot")
	}
	owner.mu.Lock()
	slot.finalized = true
	close(slot.done)
	owner.mu.Unlock()
	select {
	case err := <-result:
		if !errors.Is(err, ErrContract) || owner.slot != slot {
			t.Fatalf("waiting Close dropped unknown custody: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not join original completion")
	}
	if stage.verify(context.Background(), profile, media) != nil {
		t.Fatal("concurrent Close released original input files")
	}
}
