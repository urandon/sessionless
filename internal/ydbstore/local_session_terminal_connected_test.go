package ydbstore_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// Actual original session/Source/Store/issuer/body adapter and owned HTTP peer.
// SQL/funding/response content are fixtures, not a real model or native YDB.
// No capsule PUT, durable retirement/erasure or canonical terminal is claimed.
func TestLocalSessionTerminalConnectedOriginalFreeze(t *testing.T) {
	session, seed, peer, _ := newCompactionFactorySession(t, "short original task")
	result, err := session.RunModelStep(t.Context())
	if err != nil || !result.Model.SendAttempted || result.Compaction != nil {
		t.Fatal("actual original MODEL response required before terminal", err)
	}
	before := seed.Stats()
	h, err := session.PrepareTerminal(t.Context())
	if err != nil || h == nil {
		t.Fatal("same original idle Keeper/frontier freeze", err)
	}
	d, err := h.Deadline()
	if err != nil || !d.After(time.Now()) {
		t.Fatal("original terminal deadline", err)
	}
	if _, err := json.Marshal(h); err == nil {
		t.Fatal("transient terminal owner serialized")
	}
	copyHandle := *h
	if _, err := copyHandle.Deadline(); err == nil {
		t.Fatal("copy acquired original terminal owner")
	}
	copySession := *session
	if h2, err := copySession.PrepareTerminal(context.Background()); h2 != nil || err == nil {
		t.Fatal("copied session renewed terminal capture")
	}
	if next, err := copySession.RunModelStep(t.Context()); err == nil || next.Model.SendAttempted || next.Compaction != nil {
		t.Fatal("frozen session performed another operation")
	}
	if seed.Stats() != before {
		t.Fatal("terminal capture created another canonical physical effect")
	}
	if got, _ := h.Deadline(); got != d {
		t.Fatal("copied/repeated caller renewed original window")
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("terminal freeze closed borrowed Source", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal("original bounded cleanup", err)
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("terminal close stopped borrowed model/Source", err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.failure != "" || !reflect.DeepEqual(peer.kinds, []string{"sessionless.history.v1"}) {
		t.Fatal("freeze produced unexpected HTTP effects", peer.failure)
	}
}

func TestLocalSessionTerminalConnectedRequiresCompletedModel(t *testing.T) {
	session, seed, _, _ := newCompactionFactorySession(t, "short original task")
	before := seed.Stats()
	if h, err := session.PrepareTerminal(t.Context()); h != nil || err == nil {
		t.Fatal("unstarted loop produced prepared terminal owner")
	}
	if seed.Stats() != before {
		t.Fatal("invalid terminal request performed an effect")
	}
	// Invalid precondition does not silently consume a valid future invocation.
	if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted {
		t.Fatal("invalid precondition poisoned original session", err)
	}
	if _, err := session.PrepareTerminal(t.Context()); err != nil {
		t.Fatal("completed actual MODEL could not freeze", err)
	}
}
