package ydbstore_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Original session/keeper/Store/body/issuer and owned HTTP peer, fixture SQL and
// funding. Prepared refs are not PUT/readback/retirement/canonical success.
func TestLocalSessionTerminalOutputConnected(t *testing.T) {
	for _, compact := range []bool{false, true} {
		name := "fitting"
		text := "short original answer"
		steps := 1
		var limits []ydbstore.LocalCompactionConnectedLimits
		if compact {
			name, text, steps = "two_compactions", strings.Repeat("x", 4096), 3
			limits = append(limits, ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
		}
		t.Run(name, func(t *testing.T) {
			session, seed, peer, blobs := newCompactionFactorySession(t, text, limits...)
			for i := 0; i < steps; i++ {
				if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted {
					peer.mu.Lock()
					kinds, sizes, failure := append([]string(nil), peer.kinds...), append([]int(nil), peer.sizes...), peer.failure
					peer.mu.Unlock()
					blobs.mu.Lock()
					objects, retained := len(blobs.objects), 0
					for key := range blobs.objects {
						if strings.Contains(key, "/derived-context-v1/") {
							retained++
						}
					}
					blobs.mu.Unlock()
					t.Fatalf("original step%d err=%v attempted=%t compaction=%t stats=%+v kinds=%v sizes=%v objects=%d retained=%d peer=%s", i, err, result.Model.SendAttempted, result.Compaction != nil, seed.Stats(), kinds, sizes, objects, retained, failure)
				}
			}
			h, err := session.PrepareTerminal(t.Context())
			if err != nil {
				t.Fatal("original session freeze", err)
			}
			before := seed.Stats()
			blobs.mu.Lock()
			count := len(blobs.objects)
			blobs.mu.Unlock()
			p, err := h.PrepareOutput(t.Context())
			if err != nil || p == nil {
				t.Fatal("actual original session output producer", err)
			}
			drafts, err := p.Drafts()
			plan, pe := p.Plan()
			if err != nil || pe != nil || len(drafts) != 1 || drafts[0].Kind != domain.SessionEventAssistantMessage || drafts[0].DisplayText != text || plan.Validate() != nil {
				t.Fatal("original final MODEL canonical projection", err, pe)
			}
			blobs.mu.Lock()
			unchanged := len(blobs.objects) == count
			blobs.mu.Unlock()
			if !unchanged || seed.Stats() != before {
				t.Fatal("pure planning published object or terminal SQL")
			}
			copyHandle := *h
			if _, err := copyHandle.PrepareOutput(context.Background()); err == nil {
				t.Fatal("copy retook original output")
			}
			if _, err := h.PrepareOutput(t.Context()); err == nil {
				t.Fatal("repeated caller renewed output planning")
			}
			if err := session.Close(); err != nil {
				t.Fatal("original session join", err)
			}
			if _, err := p.Plan(); err == nil {
				t.Fatal("joined close retained private prepared buffers")
			}
			peer.mu.Lock()
			kinds, failure := append([]string(nil), peer.kinds...), peer.failure
			peer.mu.Unlock()
			want := []string{"sessionless.history.v1"}
			if compact {
				want = append(want, "sessionless.compaction-input.v1", "sessionless.history.v1", "sessionless.compaction-input.v1", "sessionless.history.v1")
			}
			if failure != "" || !reflect.DeepEqual(kinds, want) {
				t.Fatal("output added provider effects", failure, kinds)
			}
		})
	}
}
