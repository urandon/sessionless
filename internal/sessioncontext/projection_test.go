package sessioncontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestProjectCanonicalUsesExactCodecHistoryAndSnapshotBoundary(t *testing.T) {
	owner := domain.UserID("projection-owner")
	payload := []byte(`{"version":1,"text":"hello"}`)
	digest := sha256.Sum256(payload)
	event := domain.SessionEvent{ID: "projection-event", TenantID: "projection-tenant", SessionID: "projection-session", Sequence: 1,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, IdempotencyKey: "projection-idempotency", CreatedAt: time.Unix(1, 0).UTC(),
		Payload: domain.BlobRef{TenantID: "projection-tenant", Key: domain.SessionEventObjectPrefix("projection-tenant", "projection-session", "projection-event") + "payload.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:])}}
	job := domain.WorkerJob{TenantID: event.TenantID, SessionID: event.SessionID, TriggerEventID: event.ID,
		ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, Limits: domain.ProductLimits{MaxContextBytes: 1 << 20, MaxContextEvents: 4, MaxToolEvents: 1, MaxToolEventBytes: 1024, MaxArtifacts: 1}}
	proof := &CanonicalProof{Input: domain.SessionContextInput{TenantID: event.TenantID, SessionID: event.SessionID, Events: []domain.SessionEvent{event}}, EventBodies: [][]byte{payload}}
	want, err := EncodeRecord(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	got, refs, err := ProjectCanonical(job, proof, 1<<20)
	if err != nil || !bytes.Equal(got, want) || len(refs) != 0 {
		t.Fatalf("canonical replay: err=%v history=%q want=%q refs=%d", err, got, want, len(refs))
	}
	compressed, _, err := EncodeSnapshot([]EventPayload{{Event: event, Payload: payload}})
	if err != nil {
		t.Fatal(err)
	}
	version := uint64(2)
	snapshotDigest := sha256.Sum256(compressed)
	snapshot := domain.SessionSnapshot{ID: "projection-snapshot", TenantID: event.TenantID, SessionID: event.SessionID,
		Version: version, ThroughSequence: 1, EventCount: 1, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(want)), CreatedAt: time.Unix(2, 0).UTC(),
		Payload: domain.BlobRef{TenantID: event.TenantID, Key: domain.SessionSnapshotObjectKey(event.TenantID, event.SessionID, version), Size: int64(len(compressed)), SHA256: hex.EncodeToString(snapshotDigest[:])}}
	job.ContextWindow = &domain.SessionContextWindow{SnapshotVersion: &version, AfterSequence: 1, ThroughSequence: 1}
	proof = &CanonicalProof{Input: domain.SessionContextInput{TenantID: event.TenantID, SessionID: event.SessionID, Snapshot: &snapshot}, SnapshotBytes: compressed}
	got, _, err = ProjectCanonical(job, proof, 1<<20)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("snapshot replay: err=%v history=%q want=%q", err, got, want)
	}
	job.TriggerEventID = "different-trigger"
	if _, _, err := ProjectCanonical(job, proof, 1<<20); err == nil {
		t.Fatal("snapshot-only trigger mismatch accepted")
	}
	job.TriggerEventID = event.ID
	compressed[0] ^= 1
	if _, _, err := ProjectCanonical(job, proof, 1<<20); err == nil {
		t.Fatal("tampered compressed snapshot accepted")
	}
}
