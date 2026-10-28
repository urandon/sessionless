package ydbstore_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Invalid worker-loop callers must not touch a socket, reserve another parent
// or cancel an original direct/worker owner. All authority comes from the same
// real factory/Store and existing controlled SQL fixture, never a fake executor.
func TestLocalSessionWorkerLoopGuards(t *testing.T) {
	for _, mode := range []string{"unentered", "direct", "closed"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			server.Config.ReadHeaderTimeout = time.Second
			server.Config.ReadTimeout, server.Config.WriteTimeout = 2*time.Second, 2*time.Second
			t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
			server.Start()
			session, seed, blobs, _, effects := newWorkerEntrySession(t, server.URL+"/v1")
			switch mode {
			case "direct":
				if _, err := session.Start(t.Context(), seed.Request); err != nil {
					t.Fatal("original direct entry", err)
				}
			case "closed":
				if err := session.Close(); err != nil {
					t.Fatal("original close", err)
				}
			}
			before := seed.Stats()
			for range 2 {
				result, err := session.RunWorkerLoop(t.Context())
				if err == nil || result.Model.SendAttempted || result.Tool != nil || result.Compaction != nil || seed.Stats() != before || calls.Load() != 0 {
					t.Fatalf("invalid %s worker loop acquired effects: err=%v calls=%d stats=%+v", mode, err, calls.Load(), seed.Stats())
				}
			}
			if mode == "closed" {
				return
			}
			if mode == "unentered" {
				fresh, err := effects.ReserveAttemptEffect(t.Context(), seed.Request)
				if err != nil {
					t.Fatal("rejected premature call damaged original reserve", err)
				}
				if _, err := session.EnterWorkerEffect(t.Context(), fresh); err != nil {
					t.Fatal("rejected premature call damaged original worker handoff", err)
				}
			}
			assertWorkerEntryWritable(t, session, seed, blobs)
		})
	}
}
