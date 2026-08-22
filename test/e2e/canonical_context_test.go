//go:build e2elocal

package e2e

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

const capturedContextArtifact = "context-history.jsonl"

func TestCanonicalContextSnapshotTailMatchesReplayAndFallsBackFromCorruption(t *testing.T) {
	if os.Getenv("SESSIONLESS_E2E") != "1" {
		t.Skip("set SESSIONLESS_E2E=1 and start the local stand")
	}
	slice := newLocalSlice(t)
	defer slice.close()
	slice.reset()

	base := time.Now().UTC().UnixMilli()
	chatID := base*2 + 301

	seed := slice.postMessage(base+301, chatID, "seed canonical snapshot history")
	slice.setConnectionReady(seed)
	slice.waitRunStatus(seed, domain.RunQueued)
	slice.runWorker(nil)
	slice.waitRunStatus(seed, domain.RunSucceeded)
	snapshot := slice.ensureCanonicalSnapshot(seed)

	snapshotTail := slice.postMessage(base+302, chatID, "materialize snapshot plus tail")
	slice.waitRunStatus(snapshotTail, domain.RunQueued)
	loaded := slice.loadedWorkerJob(snapshotTail)
	slice.assertPinnedSnapshot(loaded, snapshot)
	if loaded.Job.Limits.MaxArtifacts < 2 {
		t.Fatalf("admitted max artifacts = %d, need one deterministic and one context capture artifact", loaded.Job.Limits.MaxArtifacts)
	}
	expectedSnapshotTail, replayInput := slice.eventOnlyContext(loaded)
	restoreCovered := slice.hideCoveredEventPayloads(replayInput.Events, snapshot.ThroughSequence)
	defer restoreCovered()

	slice.runWorker(map[string]string{
		"DETERMINISTIC_HARNESS_CAPTURE_CONTEXT_HISTORY": "true",
	})
	slice.waitRunStatus(snapshotTail, domain.RunSucceeded)
	actualSnapshotTail := slice.contextHistoryArtifact(snapshotTail)
	if !bytes.Equal(actualSnapshotTail, expectedSnapshotTail) {
		t.Fatal("snapshot-plus-tail history differs byte-for-byte from canonical event-only replay")
	}
	restoreCovered()

	fallback := slice.postMessage(base+303, chatID, "fall back from corrupt snapshot")
	slice.waitRunStatus(fallback, domain.RunQueued)
	fallbackJob := slice.loadedWorkerJob(fallback)
	pinned := slice.pinnedSnapshot(fallbackJob)
	if pinned.Version != snapshot.Version {
		t.Fatalf("fallback pinned snapshot version = %d, want expected latest version %d", pinned.Version, snapshot.Version)
	}
	if fallbackJob.Job.Limits.MaxArtifacts < 2 {
		t.Fatalf("fallback admitted max artifacts = %d, need one deterministic and one context capture artifact", fallbackJob.Job.Limits.MaxArtifacts)
	}
	expectedFallback, _ := slice.eventOnlyContext(fallbackJob)
	restoreSnapshot := slice.corruptSnapshotObject(pinned)
	defer restoreSnapshot()

	slice.runWorker(map[string]string{
		"DETERMINISTIC_HARNESS_CAPTURE_CONTEXT_HISTORY": "true",
	})
	slice.waitRunStatus(fallback, domain.RunSucceeded)
	actualFallback := slice.contextHistoryArtifact(fallback)
	if !bytes.Equal(actualFallback, expectedFallback) {
		t.Fatal("corrupt-snapshot fallback history differs byte-for-byte from canonical event-only replay")
	}
	restoreSnapshot()
}

func (slice *localSlice) ensureCanonicalSnapshot(ref runRef) domain.SessionSnapshot {
	slice.t.Helper()
	events, err := slice.state.ListSessionHistory(slice.ctx, ref.TenantID, ref.SessionID, 0, 512)
	if err != nil {
		slice.t.Fatal(err)
	}
	if len(events) == 0 {
		slice.t.Fatal("cannot create a snapshot for empty canonical history")
	}
	through := events[len(events)-1].Sequence
	snapshots, err := slice.state.ListSessionSnapshots(slice.ctx, ref.TenantID, ref.SessionID, 0, 32)
	if err != nil {
		slice.t.Fatal(err)
	}
	version := uint64(1)
	if len(snapshots) != 0 {
		latest := snapshots[len(snapshots)-1]
		if latest.ThroughSequence == through {
			return latest
		}
		if latest.ThroughSequence > through {
			slice.t.Fatalf("latest snapshot sequence %d exceeds canonical history boundary %d", latest.ThroughSequence, through)
		}
		version = latest.Version + 1
	}
	loaded := slice.loadedWorkerJob(ref)
	builder, err := sessioncontext.NewSnapshotBuilder(slice.state, slice.blobs)
	if err != nil {
		slice.t.Fatal(err)
	}
	snapshot, err := builder.Create(slice.ctx, sessioncontext.SnapshotRequest{
		TenantID: ref.TenantID, SessionID: ref.SessionID, Version: version,
		ThroughSequence: through,
		MaxEvents:       loaded.Job.Limits.EffectiveMaxContextEvents(),
		MaxBytes:        loaded.Job.Limits.MaxContextBytes,
	})
	if err != nil {
		slice.t.Fatal(err)
	}
	return snapshot
}

func (slice *localSlice) loadedWorkerJob(ref runRef) ports.WorkerJobState {
	slice.t.Helper()
	loaded, found, err := slice.state.LoadWorkerJob(slice.ctx, ref.TenantID, ref.RunID)
	if err != nil {
		slice.t.Fatal(err)
	}
	if !found {
		slice.t.Fatalf("worker job for run %s not found", ref.RunID)
	}
	return loaded
}

func (slice *localSlice) assertPinnedSnapshot(loaded ports.WorkerJobState, snapshot domain.SessionSnapshot) {
	slice.t.Helper()
	window := loaded.Job.ContextWindow
	if window == nil || window.SnapshotVersion == nil {
		slice.t.Fatalf("worker job %s did not pin a snapshot: %+v", loaded.Run.ID, window)
	}
	if *window.SnapshotVersion != snapshot.Version || window.AfterSequence != snapshot.ThroughSequence {
		slice.t.Fatalf("worker job %s pinned window %+v, want snapshot version=%d through=%d",
			loaded.Run.ID, window, snapshot.Version, snapshot.ThroughSequence)
	}
	if window.ThroughSequence <= snapshot.ThroughSequence {
		slice.t.Fatalf("worker job %s trigger boundary %d does not extend snapshot coverage %d",
			loaded.Run.ID, window.ThroughSequence, snapshot.ThroughSequence)
	}
	boundary, err := slice.state.ListSessionHistory(
		slice.ctx, loaded.Run.TenantID, loaded.Run.SessionID, window.ThroughSequence-1, 1,
	)
	if err != nil {
		slice.t.Fatal(err)
	}
	if len(boundary) != 1 || boundary[0].Sequence != window.ThroughSequence ||
		boundary[0].ID != loaded.Run.TriggerEventID {
		slice.t.Fatalf("worker job %s boundary = %+v, want trigger event %s at sequence %d",
			loaded.Run.ID, boundary, loaded.Run.TriggerEventID, window.ThroughSequence)
	}
}

func (slice *localSlice) pinnedSnapshot(loaded ports.WorkerJobState) domain.SessionSnapshot {
	slice.t.Helper()
	if loaded.Job.ContextWindow == nil || loaded.Job.ContextWindow.SnapshotVersion == nil {
		slice.t.Fatalf("worker job %s did not pin a snapshot", loaded.Run.ID)
	}
	wanted := *loaded.Job.ContextWindow.SnapshotVersion
	snapshots, err := slice.state.ListSessionSnapshots(
		slice.ctx, loaded.Run.TenantID, loaded.Run.SessionID, 0, 32,
	)
	if err != nil {
		slice.t.Fatal(err)
	}
	if len(snapshots) == 0 || snapshots[len(snapshots)-1].Version != wanted {
		slice.t.Fatalf("pinned snapshot version %d is not the latest catalog entry: %+v", wanted, snapshots)
	}
	for _, snapshot := range snapshots {
		if snapshot.Version == wanted {
			if loaded.Job.ContextWindow.AfterSequence != snapshot.ThroughSequence {
				slice.t.Fatalf("pinned after sequence %d differs from snapshot coverage %d",
					loaded.Job.ContextWindow.AfterSequence, snapshot.ThroughSequence)
			}
			return snapshot
		}
	}
	slice.t.Fatalf("pinned snapshot version %d not found", wanted)
	return domain.SessionSnapshot{}
}

func (slice *localSlice) eventOnlyContext(loaded ports.WorkerJobState) ([]byte, domain.SessionContextInput) {
	slice.t.Helper()
	window := loaded.Job.ContextWindow
	if window == nil {
		slice.t.Fatalf("worker job %s has no canonical context window", loaded.Run.ID)
	}
	input, err := slice.state.LoadWorkerContext(slice.ctx, ports.WorkerContextRequest{
		TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
		TriggerEventID: loaded.Run.TriggerEventID, ThroughSequence: window.ThroughSequence,
		MaxEvents: loaded.Job.Limits.EffectiveMaxContextEvents(),
	})
	if err != nil {
		slice.t.Fatal(err)
	}
	if input.Snapshot != nil {
		slice.t.Fatal("event-only context unexpectedly selected a snapshot")
	}
	var history []byte
	for _, event := range input.Events {
		line, err := sessioncontext.EncodeRecord(event, slice.readBlob(event.Payload))
		if err != nil {
			slice.t.Fatal(err)
		}
		history = append(history, line...)
	}
	return history, input
}

type savedBlob struct {
	ref  domain.BlobRef
	body []byte
}

func (slice *localSlice) hideCoveredEventPayloads(events []domain.SessionEvent, through uint64) func() {
	slice.t.Helper()
	saved := make([]savedBlob, 0, through)
	for _, event := range events {
		if event.Sequence > through {
			break
		}
		saved = append(saved, savedBlob{ref: event.Payload, body: slice.readBlob(event.Payload)})
	}
	if uint64(len(saved)) != through {
		slice.t.Fatalf("hidden covered payloads = %d, want %d", len(saved), through)
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		restoreCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		restoredAll := true
		for _, item := range saved {
			stored, err := slice.blobs.Put(restoreCtx, item.ref.TenantID, item.ref.Key, bytes.NewReader(item.body))
			if err != nil {
				slice.t.Errorf("restore event payload %s: %v", item.ref.Key, err)
				restoredAll = false
				continue
			}
			if stored != item.ref {
				slice.t.Errorf("restored event payload ref = %+v, want %+v", stored, item.ref)
				restoredAll = false
			}
		}
		restored = restoredAll
	}
	slice.t.Cleanup(restore)
	for _, item := range saved {
		if err := slice.blobs.Delete(slice.ctx, item.ref.TenantID, item.ref); err != nil {
			slice.t.Fatal(err)
		}
	}
	return restore
}

func (slice *localSlice) corruptSnapshotObject(snapshot domain.SessionSnapshot) func() {
	slice.t.Helper()
	original := slice.readBlob(snapshot.Payload)
	if len(original) == 0 {
		slice.t.Fatal("cannot corrupt an empty snapshot object")
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		restoreCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		stored, err := slice.blobs.Put(
			restoreCtx, snapshot.TenantID, snapshot.Payload.Key, bytes.NewReader(original),
		)
		if err != nil {
			slice.t.Errorf("restore snapshot object %s: %v", snapshot.Payload.Key, err)
			return
		}
		if stored != snapshot.Payload {
			slice.t.Errorf("restored snapshot ref = %+v, want %+v", stored, snapshot.Payload)
			return
		}
		restored = true
	}
	slice.t.Cleanup(restore)
	corrupt := append([]byte(nil), original...)
	corrupt[len(corrupt)/2] ^= 0xff
	stored, err := slice.blobs.Put(slice.ctx, snapshot.TenantID, snapshot.Payload.Key, bytes.NewReader(corrupt))
	if err != nil {
		slice.t.Fatal(err)
	}
	if stored.Key != snapshot.Payload.Key || stored == snapshot.Payload {
		slice.t.Fatalf("corrupt snapshot ref = %+v, want same key and changed immutable metadata", stored)
	}
	return restore
}

func (slice *localSlice) contextHistoryArtifact(ref runRef) []byte {
	slice.t.Helper()
	manifest := slice.outputManifest(ref)
	for _, artifact := range manifest.Artifacts {
		if artifact.Name == capturedContextArtifact {
			return slice.readBlob(artifact.Blob)
		}
	}
	slice.t.Fatalf("captured context artifact not found in manifest %+v", manifest)
	return nil
}
