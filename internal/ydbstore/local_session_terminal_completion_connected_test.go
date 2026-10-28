package ydbstore_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func newCompletionSession(t *testing.T, twoCompactions bool) (*providerloopsession.LocalV1, *ydbstore.LocalTerminalPublicationSessionConnectedSeed, *ydbstore.LocalTerminalCompletionSQLFixture, *sessionCompactionPeer, *cleanupSessionBlobs) {
	t.Helper()
	peer := &sessionCompactionPeer{modelText: "original terminal completion answer"}
	var limits []ydbstore.LocalCompactionConnectedLimits
	if twoCompactions {
		peer.modelText = strings.Repeat("x", 4096)
		limits = []ydbstore.LocalCompactionConnectedLimits{{InputTokens: 8192, NetworkOperation: 5 * time.Second}}
	}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout = 20 * time.Second
	server.Config.WriteTimeout = 20 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	blobs := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", blobs, limits...)
	fixture := seed.InstallTerminalCompletionSQL(t)
	blobs.seed = seed
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal("original completion factory", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("original completion join", err)
		}
	})
	if _, err := session.Start(t.Context(), seed.Request); err != nil {
		t.Fatal("original completion Start", err)
	}
	return session, seed, fixture, peer, blobs
}

// Real factory/HTTP/issuer/Store/keeper/bridge/physical body ownership. SQL and
// object storage are cooperative fixtures, not native YDB/S3/live model proof.
func TestLocalSessionTerminalCompletionConnected(t *testing.T) {
	for _, mode := range []string{"success", "two_compactions", "source_closed_before_complete", "cancelled_after_cleanup", "source_closed_after_commit", "buffers_cleared_after_commit", "session_closed_after_commit", "lost_ack_persisted", "lost_ack_nonpersisted"} {
		t.Run(mode, func(t *testing.T) {
			session, seed, fixture, peer, blobs := newCompletionSession(t, mode == "two_compactions")
			steps := 1
			if mode == "two_compactions" {
				steps = 3
			}
			for i := 0; i < steps; i++ {
				if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted {
					t.Logf("original MODEL step%d: %v", i, err)
					t.Logf("compaction_failure: %s", localTerminalFailureDiagnostic(t, result, seed.LocalSessionConnectedSeed))
					t.Fatalf("original MODEL step%d: %v", i, err)
				}
			}
			h, p := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
			if err := h.PublishOutput(t.Context()); err != nil {
				t.Fatal("original output publication", err)
			}
			cleanup, err := h.FinishCleanup(t.Context())
			if err != nil || cleanup == nil {
				t.Fatal("original cleanup before canonical terminal", err)
			}
			plan := seed.TerminalPublicationRun(t)
			if mode == "source_closed_before_complete" {
				if err := seed.Source.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "cancelled_after_cleanup" {
				fixture.RequestCancellation(t)
			}
			fixture.ObserveCompletion()
			if mode == "lost_ack_persisted" {
				fixture.LoseNextCompletionACK(t, "ambiguous")
			}
			if mode == "lost_ack_nonpersisted" {
				fixture.LoseNextCompletionACK(t, "rollback")
			}
			stats := seed.Stats()
			blobs.stateMu.Lock()
			puts := blobs.capsulePuts + blobs.eventPuts
			blobs.stateMu.Unlock()
			blobs.cleanupMu.Lock()
			deletes := blobs.deletes
			blobs.cleanupMu.Unlock()
			if err := cleanup.Complete(t.Context()); err != nil {
				snap := fixture.Snapshot(t)
				trace := fixture.Trace() // Preserve ACTUAL failing call before read-only diagnostics.
				diagnostic := fixture.DiagnoseCleanup(t, cleanup)
				t.Fatalf("same canonical completion: err=%v run=%s attempt=%s quota=%s events=%d finalizations=%d mutations=%d phases=%+v sql=%+v", err, snap.Run.Status, snap.Attempt.Status, snap.Reservation.Status, len(snap.Events), snap.Finalizations, snap.Mutations, diagnostic, trace)
			}
			got := fixture.Snapshot(t)
			encodedRun, encodeErr := json.Marshal(got.Run)
			if encodeErr != nil || len(encodedRun) > 32<<10 {
				t.Fatal("actual complete Run exceeds common physical bound", len(encodedRun), encodeErr)
			}
			if strings.HasPrefix(mode, "lost_ack_") && got.LostACKs != 1 {
				t.Fatal("lost ACK did not hit actual terminal mutation commit")
			}
			if got.Finalizations != 1 || len(got.Events) != 1 || got.LeasePresent || got.ActiveRuns != 0 || got.ActiveRun != "" || got.ActiveReservation != "" {
				t.Fatalf("incomplete canonical/scheduler mutation: finals=%d events=%d lease=%t active=%d run=%q reservation=%q", got.Finalizations, len(got.Events), got.LeasePresent, got.ActiveRuns, got.ActiveRun, got.ActiveReservation)
			}
			if !domain.EqualRunDerivedContextV1(plan.DerivedContext, got.Run.DerivedContext) || got.Run.TerminalCustody == nil || got.Run.TerminalCustody.Plan != plan.TerminalCustody.Plan {
				t.Fatal("canonical completion dropped/retargeted retained metadata")
			}
			if mode == "two_compactions" && (got.Run.DerivedContext == nil || len(got.Run.DerivedContext.Inventory) != 2 || got.Run.DerivedContext.Head == nil) {
				t.Fatal("actual two-generation retained closure not preserved")
			}
			if mode == "cancelled_after_cleanup" {
				if got.Run.Status != domain.RunCancelled || got.Attempt.Status != domain.AttemptCancelled || got.Reservation.Status != domain.ReservationReleased || got.Events[0].Kind != domain.SessionEventSystemNotice || got.Run.TerminalCustody.Correspondence != nil || got.Run.TerminalCustody.LocalEvidence != nil || got.EvidenceRecord != "" {
					t.Fatalf("cancellation became success: run=%s attempt=%s quota=%s event=%s evidence=%t", got.Run.Status, got.Attempt.Status, got.Reservation.Status, got.Events[0].Kind, got.EvidenceRecord != "")
				}
				blobs.mu.Lock()
				notice := append([]byte(nil), blobs.objects[string(got.Events[0].TenantID)+"\x00"+got.Events[0].Payload.Key]...)
				blobs.mu.Unlock()
				var n struct {
					Schema    string `json:"schema"`
					Code      string `json:"code"`
					Cancelled bool   `json:"cancelled"`
				}
				if json.Unmarshal(notice, &n) != nil || n.Schema != "sessionless.run-terminal-notice.v1" || n.Code != "cancelled" || !n.Cancelled {
					t.Fatal("not the original fixed cancellation notice")
				}
			} else {
				if got.Run.Status != domain.RunSucceeded || got.Attempt.Status != domain.AttemptSucceeded || got.Reservation.Status != domain.ReservationCommitted || got.Events[0].Kind != domain.SessionEventAssistantMessage || got.Run.TerminalCustody.Correspondence == nil || got.Run.TerminalCustody.LocalEvidence == nil || got.Manifest.ID == "" {
					t.Fatalf("missing canonical success: run=%s attempt=%s quota=%s event=%s manifest=%q", got.Run.Status, got.Attempt.Status, got.Reservation.Status, got.Events[0].Kind, got.Manifest.ID)
				}
				var evidence domain.ExecutionEvidenceV2
				if json.Unmarshal([]byte(got.EvidenceRecord), &evidence) != nil || evidence.Kind != domain.ExecutionEvidenceLocalV1 || evidence.ManagedV1 != nil || evidence.LocalV1 == nil || evidence.LocalV1.Validate() != nil || !reflect.DeepEqual(evidence.LocalV1, got.Run.TerminalCustody.LocalEvidence) {
					t.Fatal("not exact genuine local V2 evidence")
				}
				if got.Run.TerminalCustody.Correspondence.TerminalAssistantEventID != got.Events[0].ID || got.Manifest.RunID != got.Run.ID {
					t.Fatal("canonical assistant/manifest correspondence mismatch")
				}
			}
			switch mode {
			case "source_closed_after_commit":
				if err := seed.Source.Close(); err != nil {
					t.Fatal(err)
				}
			case "buffers_cleared_after_commit":
				p.Clear()
			case "session_closed_after_commit":
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := cleanup.Complete(t.Context()); err != nil {
				t.Fatal("exact read-only terminal replay", err)
			}
			replay := fixture.Snapshot(t)
			blobs.stateMu.Lock()
			afterPuts := blobs.capsulePuts + blobs.eventPuts
			blobs.stateMu.Unlock()
			blobs.cleanupMu.Lock()
			afterDeletes := blobs.deletes
			blobs.cleanupMu.Unlock()
			if !reflect.DeepEqual(got, replay) || seed.Stats() != stats || afterPuts != puts || afterDeletes != deletes {
				t.Fatal("terminal replay repeated canonical/provider/storage effects")
			}
			copyCleanup := *cleanup
			if err := copyCleanup.Complete(t.Context()); err == nil {
				t.Fatal("copied cleanup owner acquired completion")
			}
			if _, err := p.Drafts(); mode == "buffers_cleared_after_commit" || mode == "session_closed_after_commit" {
				if err == nil {
					t.Fatal("actual buffer clear did not revoke access")
				}
			} else if err != nil {
				t.Fatal("successful terminal ownership unexpectedly cleared", err)
			}
			peer.mu.Lock()
			calls, failure := len(peer.kinds), peer.failure
			peer.mu.Unlock()
			wantCalls := 1
			if mode == "two_compactions" {
				wantCalls = 5
			}
			if calls != wantCalls || failure != "" {
				t.Fatal("completion/replay issued model call", calls, failure)
			}
		})
	}
}

func TestLocalSessionTerminalCompletionReplayConflictsConnected(t *testing.T) {
	for _, mode := range []string{"missing_capsule", "corrupt_capsule", "missing_payload", "corrupt_payload", "missing_marker", "corrupt_marker", "missing_evidence", "corrupt_evidence", "missing_event", "missing_index", "corrupt_event", "missing_manifest", "changed_custody"} {
		t.Run(mode, func(t *testing.T) {
			session, seed, fixture, peer, blobs := newCompletionSession(t, false)
			if _, err := session.RunModelStep(t.Context()); err != nil {
				t.Fatal(err)
			}
			h, _ := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
			if err := h.PublishOutput(t.Context()); err != nil {
				t.Fatal(err)
			}
			cleanup, err := h.FinishCleanup(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			fixture.ObserveCompletion()
			if err := cleanup.Complete(t.Context()); err != nil {
				t.Fatal("original first commit", err, fixture.Trace())
			}
			committed := fixture.Snapshot(t)
			if strings.Contains(mode, "capsule") || strings.Contains(mode, "payload") {
				ref := committed.Run.TerminalCustody.Plan.Capsule
				if strings.Contains(mode, "payload") {
					ref = committed.Events[0].Payload
				}
				key := string(ref.TenantID) + "\x00" + ref.Key
				blobs.mu.Lock()
				if strings.HasPrefix(mode, "missing_") {
					delete(blobs.objects, key)
				} else {
					body := bytes.Clone(blobs.objects[key])
					if len(body) == 0 {
						blobs.mu.Unlock()
						t.Fatal("missing original retained bytes")
					}
					body[0] ^= 1
					blobs.objects[key] = body
				}
				blobs.mu.Unlock()
			} else {
				fixture.TamperReplay(t, mode)
			}
			before := fixture.Snapshot(t)
			stats := seed.Stats()
			blobs.stateMu.Lock()
			puts := blobs.capsulePuts + blobs.eventPuts
			blobs.stateMu.Unlock()
			blobs.cleanupMu.Lock()
			deletes := blobs.deletes
			blobs.cleanupMu.Unlock()
			blobs.mu.Lock()
			objects := make(map[string][]byte, len(blobs.objects))
			for key, body := range blobs.objects {
				objects[key] = bytes.Clone(body)
			}
			blobs.mu.Unlock()
			err = cleanup.Complete(t.Context())
			if err == nil {
				t.Fatal("corrupted/missing terminal dependency was repaired or accepted", mode)
			}
			if !strings.Contains(mode, "capsule") && !strings.Contains(mode, "payload") && !errors.Is(err, ydbstore.ErrRunFinalizationConflict) {
				t.Fatal("durable replay mismatch not explicit conflict", mode, err)
			}
			if !reflect.DeepEqual(before, fixture.Snapshot(t)) || stats != seed.Stats() {
				t.Fatal("failed replay mutated canonical/provider state", mode)
			}
			blobs.stateMu.Lock()
			afterPuts := blobs.capsulePuts + blobs.eventPuts
			blobs.stateMu.Unlock()
			blobs.cleanupMu.Lock()
			afterDeletes := blobs.deletes
			blobs.cleanupMu.Unlock()
			blobs.mu.Lock()
			equal := reflect.DeepEqual(objects, blobs.objects)
			blobs.mu.Unlock()
			if afterPuts != puts || afterDeletes != deletes || !equal {
				t.Fatal("failed replay repeated storage or repaired retained bytes", mode)
			}
			peer.mu.Lock()
			calls := len(peer.kinds)
			peer.mu.Unlock()
			if calls != 1 {
				t.Fatal("conflict resent MODEL", calls)
			}
		})
	}
}

func TestLocalSessionTerminalCompletionSourceClearOverlapConnected(t *testing.T) {
	session, seed, fixture, peer, blobs := newCompletionSession(t, false)
	if _, err := session.RunModelStep(t.Context()); err != nil {
		t.Fatal(err)
	}
	h, _ := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
	if err := h.PublishOutput(t.Context()); err != nil {
		t.Fatal(err)
	}
	cleanup, err := h.FinishCleanup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixture.ObserveCompletion()
	before, stats := fixture.Snapshot(t), seed.Stats()
	blobs.stateMu.Lock()
	puts := blobs.capsulePuts + blobs.eventPuts
	blobs.stateMu.Unlock()
	blobs.cleanupMu.Lock()
	deletes := blobs.deletes
	blobs.cleanupMu.Unlock()
	blobs.mu.Lock()
	objects := make(map[string][]byte, len(blobs.objects))
	for key, body := range blobs.objects {
		objects[key] = bytes.Clone(body)
	}
	blobs.mu.Unlock()
	err = fixture.ClearSourceOverlappingCompletion(t, cleanup)
	after := fixture.Snapshot(t)
	if err == nil {
		if after.Run.Status != domain.RunSucceeded || after.Attempt.Status != domain.AttemptSucceeded || after.Reservation.Status != domain.ReservationCommitted || after.Finalizations != 1 || len(after.Events) != 1 || after.Events[0].Kind != domain.SessionEventAssistantMessage || after.Run.TerminalCustody == nil || after.Run.TerminalCustody.Correspondence == nil || after.EvidenceRecord == "" || after.LeasePresent || after.ActiveRuns != 0 {
			t.Fatal("clear race produced partial canonical success")
		}
		if err := cleanup.Complete(t.Context()); err != nil {
			t.Fatal("actual committed clear-race replay", err)
		}
	} else {
		if !reflect.DeepEqual(before, after) {
			t.Fatal("denied clear race changed canonical state", err)
		}
		if err := cleanup.Complete(t.Context()); err == nil {
			t.Fatal("cleared uncommitted source retried into success")
		}
	}
	if !reflect.DeepEqual(after, fixture.Snapshot(t)) || stats != seed.Stats() {
		t.Fatal("clear-race replay repeated canonical/provider effects")
	}
	blobs.stateMu.Lock()
	afterPuts := blobs.capsulePuts + blobs.eventPuts
	blobs.stateMu.Unlock()
	blobs.cleanupMu.Lock()
	afterDeletes := blobs.deletes
	blobs.cleanupMu.Unlock()
	blobs.mu.Lock()
	equal := reflect.DeepEqual(objects, blobs.objects)
	blobs.mu.Unlock()
	if afterPuts != puts || afterDeletes != deletes || !equal {
		t.Fatal("clear race repeated physical writes/deletes")
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("capture.Clear closed borrowed original Source", err)
	}
	peer.mu.Lock()
	calls := len(peer.kinds)
	peer.mu.Unlock()
	if calls != 1 {
		t.Fatal("clear race resent MODEL", calls)
	}
}

func TestLocalSessionTerminalCompletionPrecommitCurrentnessDenialsConnected(t *testing.T) {
	for _, mode := range []string{"lease_fence", "missing_lease", "session_deletion", "changed_plan", "changed_derived", "changed_attempt", "changed_quota"} {
		t.Run(mode, func(t *testing.T) {
			two := mode == "changed_derived"
			session, seed, fixture, peer, blobs := newCompletionSession(t, two)
			steps := 1
			if two {
				steps = 3
			}
			for i := 0; i < steps; i++ {
				if _, err := session.RunModelStep(t.Context()); err != nil {
					t.Fatalf("original MODEL step%d: %v", i, err)
				}
			}
			h, _ := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
			if err := h.PublishOutput(t.Context()); err != nil {
				t.Fatal(err)
			}
			cleanup, err := h.FinishCleanup(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			fixture.TamperBeforeFirstCommit(t, mode)
			fixture.ObserveCompletion()
			before, stats := fixture.Snapshot(t), seed.Stats()
			blobs.stateMu.Lock()
			puts := blobs.capsulePuts + blobs.eventPuts
			blobs.stateMu.Unlock()
			blobs.cleanupMu.Lock()
			deletes := blobs.deletes
			blobs.cleanupMu.Unlock()
			blobs.mu.Lock()
			objects := make(map[string][]byte, len(blobs.objects))
			for key, body := range blobs.objects {
				objects[key] = bytes.Clone(body)
			}
			blobs.mu.Unlock()
			for attempt := 0; attempt < 2; attempt++ {
				if err := cleanup.Complete(t.Context()); err == nil {
					t.Fatal("changed original current state authorized first commit", mode, attempt)
				}
				got := fixture.Snapshot(t)
				if !reflect.DeepEqual(before, got) || got.Finalizations != 0 || len(got.Events) != 0 || got.Mutations != 0 || got.EvidenceRecord != "" || got.Manifest.ID != "" || fixture.Trace().ExecAttempts != 0 {
					t.Fatal("currentness denial reached canonical mutation or repaired DATA", mode, attempt, fixture.Trace())
				}
			}
			if stats != seed.Stats() {
				t.Fatal("currentness denial repeated provider/operation effect", mode)
			}
			blobs.stateMu.Lock()
			afterPuts := blobs.capsulePuts + blobs.eventPuts
			blobs.stateMu.Unlock()
			blobs.cleanupMu.Lock()
			afterDeletes := blobs.deletes
			blobs.cleanupMu.Unlock()
			blobs.mu.Lock()
			equal := reflect.DeepEqual(objects, blobs.objects)
			blobs.mu.Unlock()
			if afterPuts != puts || afterDeletes != deletes || !equal {
				t.Fatal("currentness denial wrote/deleted physical DATA", mode)
			}
			peer.mu.Lock()
			calls := len(peer.kinds)
			peer.mu.Unlock()
			want := 1
			if two {
				want = 5
			}
			if calls != want {
				t.Fatal("currentness denial resent MODEL", mode, calls, want)
			}
		})
	}
}
