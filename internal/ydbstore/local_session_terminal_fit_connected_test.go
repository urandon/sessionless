package ydbstore_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The real original session, Store, codecs, body adapter, keeper and HTTP peer
// are connected; SQL, storage and funding are fixtures. No real model is used.
// Denial must precede even the summarizer request PUT and operation reserve.
func TestLocalSessionTerminalFitDeniesCompactionBeforeEffects(t *testing.T) {
	session, seed, peer, blobs := newCompactionFactorySession(t, strings.Repeat("x", 24000), ydbstore.LocalCompactionConnectedLimits{InputTokens: 32768, NetworkOperation: 5 * time.Second})
	completed := 0
	for step := 0; step < 6; step++ {
		before := seed.Stats()
		blobs.mu.Lock()
		objects, stored := len(blobs.objects), blobs.bytes
		blobs.mu.Unlock()
		peer.mu.Lock()
		calls := len(peer.kinds)
		peer.mu.Unlock()
		result, err := session.RunModelStep(t.Context())
		if err == nil {
			if !result.Model.SendAttempted {
				t.Fatalf("step %d succeeded without original MODEL effect", step)
			}
			completed++
			continue
		}
		if !errors.Is(err, ydbstore.ErrTerminalCapacity) || errors.Is(err, configuredinference.ErrContextNoFit) {
			t.Fatalf("step %d: want distinct terminal-capacity denial, got %v; completed=%d stats=%+v", step, err, completed, seed.Stats())
		}
		if completed != 2 || calls != completed || result.Model.SendAttempted || result.Compaction != nil || seed.Stats() != before {
			t.Fatalf("capacity denial emitted/reserved operation: step=%d completed=%d result=%+v before=%+v after=%+v", step, completed, result, before, seed.Stats())
		}
		blobs.mu.Lock()
		unchanged := len(blobs.objects) == objects && blobs.bytes == stored
		blobs.mu.Unlock()
		peer.mu.Lock()
		noHTTP, failure := len(peer.kinds) == calls, peer.failure
		peer.mu.Unlock()
		if !unchanged || !noHTTP || failure != "" {
			t.Fatalf("capacity denial wrote bodies or sent HTTP: storage_unchanged=%t http_unchanged=%t peer=%s", unchanged, noHTTP, failure)
		}
		// Runtime failure is sticky: wrapper reuse cannot turn capacity denial
		// into another generation or a send under renewed admission.
		if again, e := session.RunModelStep(t.Context()); e == nil || again.Model.SendAttempted || again.Compaction != nil || seed.Stats() != before {
			t.Fatalf("denied original session regained effects: result=%+v err=%v stats=%+v", again, e, seed.Stats())
		}
		return
	}
	t.Fatal("bounded fixture never reached prospective terminal-capacity denial")
}
