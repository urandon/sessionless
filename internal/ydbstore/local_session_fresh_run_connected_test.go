package ydbstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// The recorder observes actual requests at the owned peer. It cannot approve
// history, a profile, a summary or an effect. No external model is contacted.
type freshRunPeer struct {
	peer *sessionCompactionPeer
	mu   sync.Mutex
	last []byte
}

func (p *freshRunPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	_ = r.Body.Close()
	if err != nil || len(body) > 1<<20 {
		http.Error(w, "bounded peer", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	clear(p.last)
	p.last = bytes.Clone(body)
	p.mu.Unlock()
	r.Body = io.NopCloser(bytes.NewReader(body))
	p.peer.ServeHTTP(w, r)
}

type freshRunBlobs struct {
	ports.BlobStore
	oldRun           domain.RunID
	corruptCanonical domain.BlobRef
	mu               sync.Mutex
	oldBodyReads     int
}

func (b *freshRunBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if b.corruptCanonical.Key != "" && ref == b.corruptCanonical {
		return io.NopCloser(strings.NewReader(`{"schema":"sessionless.assistant-message.v1","summary":"changed canonical answer","artifact_manifest_id":"wrong"}`)), nil
	}
	if strings.HasPrefix(ref.Key, domain.SessionRunObjectPrefix(tenant, "session-test", b.oldRun)) && strings.Contains(ref.Key, "/attempt-operations-v1/") {
		b.mu.Lock()
		b.oldBodyReads++
		b.mu.Unlock()
		return nil, errors.New("original erased bodies unavailable to new Run")
	}
	return b.BlobStore.Open(ctx, tenant, ref)
}

// Real original Run A -> two generations -> capsule/retire/erasure -> canonical
// success -> close all old owners -> NEW Source/Store/Run B -> real MODEL HTTP.
// SQL and object storage are cooperative fixtures, not native YDB/S3 or model
// fidelity. No old private c/accounting/issuer or TTL rows are passed to B.
func TestLocalSessionFreshRunAfterErasureConnected(t *testing.T) {
	const finalText = "terminal answer after two original compactions"
	for _, scenario := range []struct {
		name, fault, terminalText string
		denied, corruptCanonical  bool
		steps                     int
		cachedMarker              bool
		cachedLegacy              bool
	}{
		{name: "success", terminalText: finalText},
		{name: "a0_success", terminalText: finalText, steps: 1},
		{name: "a0_cached_discovery_marker", terminalText: finalText, steps: 1, cachedMarker: true},
		{name: "a0_cached_legacy_run", terminalText: finalText, steps: 1, cachedLegacy: true},
		{name: "a0_omitted_custody", fault: "a0_omitted_custody", terminalText: finalText, steps: 1, denied: true},
		{name: "a0_stripped_tag", fault: "stripped_tag", terminalText: finalText, steps: 1, denied: true},
		{name: "a0_stripped_tag_omitted_custody", fault: "stripped_tag_omitted_custody", terminalText: finalText, steps: 1, denied: true},
		{name: "a0_missing_marker", fault: "missing_marker", terminalText: finalText, steps: 1, denied: true},
		{name: "a0_unknown_marker", fault: "unknown_marker", terminalText: finalText, steps: 1, denied: true},
		{name: "a0_oversize_marker", fault: "oversize_marker", terminalText: finalText, steps: 1, denied: true},
		{name: "a0_status_mismatch", fault: "status_mismatch", terminalText: finalText, steps: 1, denied: true},
		{name: "whitespace_final", terminalText: " \n" + finalText + "\t "},
		{name: "marker", fault: "marker", terminalText: finalText, denied: true},
		{name: "missing_evidence", fault: "missing_evidence", terminalText: finalText, denied: true},
		{name: "missing_run", fault: "missing_run", terminalText: finalText, denied: true},
		{name: "omitted_custody", fault: "omitted_custody", terminalText: finalText, denied: true},
		{name: "canonical_payload_mismatch", terminalText: finalText, denied: true, corruptCanonical: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			steps := scenario.steps
			if steps == 0 {
				steps = 3
			}
			peer := &sessionCompactionPeer{modelText: strings.Repeat("x", 4096)}
			recorder := &freshRunPeer{peer: peer}
			server := httptest.NewUnstartedServer(recorder)
			server.Config.ReadHeaderTimeout = 10 * time.Second
			server.Config.ReadTimeout, server.Config.WriteTimeout = 20*time.Second, 20*time.Second
			server.Config.MaxHeaderBytes = 16 << 10
			t.Cleanup(func() {
				server.CloseClientConnections()
				server.Close()
				recorder.mu.Lock()
				clear(recorder.last)
				recorder.mu.Unlock()
			})
			server.Start()
			blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
			old := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs, ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
			completeSQL := old.InstallTerminalCompletionSQL(t)
			blobs.seed = old
			a, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: old.Source, DB: old.DB, StoreOptions: old.Options, Prepare: old.Prepare, Operate: old.Operate, Backend: old.Backend, Substrate: old.Substrate, Blobs: blobs})
			if err != nil {
				t.Fatal("original A factory", err)
			}
			t.Cleanup(func() {
				if e := a.Close(); e != nil {
					t.Error(e)
				}
			})
			if _, err = a.Start(t.Context(), old.Request); err != nil {
				t.Fatal("original A start", err)
			}
			for i := 0; i < steps; i++ {
				if i == steps-1 {
					peer.mu.Lock()
					peer.modelText = scenario.terminalText
					peer.mu.Unlock()
				}
				if result, e := a.RunModelStep(t.Context()); e != nil || !result.Model.SendAttempted {
					t.Fatalf("A step%d err=%v attempted=%t", i, e, result.Model.SendAttempted)
				}
			}
			h, p := prepareTerminalPublication(t, a, blobs.terminalSessionBlobs)
			if err = h.PublishOutput(t.Context()); err != nil {
				t.Fatal("A retained publication", err)
			}
			erased, err := h.FinishCleanup(t.Context())
			if err != nil {
				t.Fatal("A actual cleanup", err)
			}
			if err = erased.Complete(t.Context()); err != nil {
				t.Fatal("A canonical success", err)
			}
			prior := completeSQL.Snapshot(t)
			if prior.Finalizations != 1 || len(prior.Events) != 1 || prior.Run.TerminalCustody == nil {
				t.Fatal("A genuine terminal custody missing")
			}
			if steps == 1 {
				if prior.Run.DerivedContext != nil {
					t.Fatal("A0 unexpectedly compacted")
				}
			} else if prior.Run.DerivedContext == nil || len(prior.Run.DerivedContext.Inventory) != steps-1 {
				t.Fatalf("A retained generation inventory missing: steps=%d", steps)
			}
			if err = a.Close(); err != nil {
				t.Fatal(err)
			}
			if err = old.Source.Close(); err != nil {
				t.Fatal(err)
			}
			p.Clear()
			blobs.mu.Lock()
			for key := range blobs.objects {
				if strings.Contains(key, "/attempt-operations-v1/") {
					blobs.mu.Unlock()
					t.Fatal("old ordinary body survived actual erasure")
				}
			}
			blobs.mu.Unlock()
			// Reuse only immutable STORAGE and original installation files. This
			// wrapper intentionally has no terminal writer/old seed callbacks.
			freshBlobs := &freshRunBlobs{BlobStore: blobs.sessionCompactionBlobs, oldRun: prior.Run.ID}
			fresh, historySQL := ydbstore.NewFreshRunAfterTerminalSeed(t, old, completeSQL, freshBlobs)
			if scenario.fault != "" {
				historySQL.Corrupt(t, scenario.fault)
			}
			if scenario.corruptCanonical {
				freshBlobs.corruptCanonical = prior.Events[0].Payload
			}
			if fresh.Source == old.Source || fresh.Request.RunID == old.Request.RunID || fresh.Request.AttemptID == old.Request.AttemptID || fresh.Request.LeaseID == old.Request.LeaseID || fresh.Request.PhysicalInvocationClaimID == old.Request.PhysicalInvocationClaimID {
				t.Fatal("new original identities were reused")
			}
			b, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: fresh.Source, DB: fresh.DB, StoreOptions: fresh.Options, Prepare: fresh.Prepare, Operate: fresh.Operate, Backend: fresh.Backend, Substrate: fresh.Substrate, Blobs: freshBlobs})
			if err != nil {
				t.Fatal("new B factory", err)
			}
			t.Cleanup(func() {
				if e := b.Close(); e != nil {
					t.Error(e)
				}
			})
			if _, err = b.Start(t.Context(), fresh.Request); err != nil {
				t.Fatal("new B start", err)
			}
			before := fresh.Stats()
			peer.mu.Lock()
			calls := len(peer.kinds)
			peer.mu.Unlock()
			result, err := b.RunModelStep(t.Context())
			if historySQL.TTLReads != 0 || (!scenario.corruptCanonical && historySQL.HistoryReads == 0) {
				t.Fatal("history skipped or used removed old TTL state")
			}
			freshBlobs.mu.Lock()
			oldReads := freshBlobs.oldBodyReads
			freshBlobs.mu.Unlock()
			if oldReads != 0 {
				t.Fatal("new Run attempted erased original requests")
			}
			if scenario.denied {
				assertFreshRunHistoryDenied(t, err, result.Model.SendAttempted, before, fresh.Stats(), peer, calls)
				return
			}
			if err != nil || !result.Model.SendAttempted || result.Compaction != nil {
				t.Fatalf("new B actual historical consumer err=%v attempted=%t compacted=%t stats=%+v", err, result.Model.SendAttempted, result.Compaction != nil, fresh.Stats())
			}
			recorder.mu.Lock()
			wire := bytes.Clone(recorder.last)
			recorder.mu.Unlock()
			defer clear(wire)
			var request struct {
				Messages []struct{ Role, Content string }
			}
			if json.Unmarshal(wire, &request) != nil || len(request.Messages) != 2 {
				t.Fatal("B exact actual request missing")
			}
			history := request.Messages[1].Content
			if strings.Count(history, finalText) != 1 || !strings.Contains(history, "continue the original task; preserve all constraints") || !strings.Contains(history, "original task with constraints") || (steps > 1 && !strings.Contains(history, "derived_summary")) {
				t.Fatal("B duplicated/lost terminal projection, task, protected payload or retained summary")
			}
			peer.mu.Lock()
			afterCalls := len(peer.kinds)
			failure := peer.failure
			peer.mu.Unlock()
			if calls != 2*steps-1 || afterCalls != calls+1 || failure != "" {
				t.Fatalf("old effects replayed calls=%d/%d peer=%s", calls, afterCalls, failure)
			}
			if scenario.cachedMarker || scenario.cachedLegacy {
				// A and B's first MODEL above are genuine positives. B's next
				// preparation first captures an internally matching history, then
				// the keyed SQL fixture changes discovery only. This reaches the
				// same prepared source's cached-history reserve/send rejoin rather
				// than a first-capture nil-history denial masking the regression.
				before := fresh.Stats()
				fault := "cached_discovery_marker"
				if scenario.cachedLegacy {
					fault = "cached_legacy_run"
				}
				historySQL.Corrupt(t, fault)
				result, err := b.RunModelStep(t.Context())
				if historySQL.DiscoveryChanges+historySQL.LegacyChanges != 1 || historySQL.TTLReads != 0 {
					t.Fatalf("cached history fault not exercised exactly once: marker=%d legacy=%d TTL=%d", historySQL.DiscoveryChanges, historySQL.LegacyChanges, historySQL.TTLReads)
				}
				assertFreshRunHistoryDenied(t, err, result.Model.SendAttempted, before, fresh.Stats(), peer, afterCalls)
			}
		})
	}
}

func assertFreshRunHistoryDenied(t *testing.T, err error, attempted bool, before, after ydbstore.LocalSessionConnectedStats, peer *sessionCompactionPeer, calls int) {
	t.Helper()
	peer.mu.Lock()
	afterCalls := len(peer.kinds)
	peer.mu.Unlock()
	if err == nil || attempted || before != after || calls != afterCalls {
		t.Fatalf("corrupt history authorized effects: err=%v attempted=%t stats=%+v/%+v calls=%d/%d", err, attempted, before, after, calls, afterCalls)
	}
}
