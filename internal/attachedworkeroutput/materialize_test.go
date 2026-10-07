package attachedworkeroutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type memoryBlobs struct{ values map[string][]byte }

func (blobs *memoryBlobs) Put(_ context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	content, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	digest := sha256.Sum256(content)
	blobs.values[key] = bytes.Clone(content)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(content)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func (blobs *memoryBlobs) Open(_ context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ref.TenantID != tenant {
		return nil, ErrCandidateInvalid
	}
	value, ok := blobs.values[ref.Key]
	if !ok {
		return nil, ErrCandidateInvalid
	}
	return io.NopCloser(bytes.NewReader(value)), nil
}

func (blobs *memoryBlobs) Delete(_ context.Context, _ domain.TenantID, ref domain.BlobRef) error {
	delete(blobs.values, ref.Key)
	return nil
}

func outputFixture(t *testing.T) (ports.WorkerJobState, *memoryBlobs, domain.LeaseID, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	loaded := ports.WorkerJobState{}
	loaded.Run = domain.Run{
		ID: "run-a", TenantID: "tenant-a", SessionID: "session-a", TriggerEventID: "trigger-a",
		SubscriptionConnectionID: "subscription-a", Status: domain.RunRunning,
		IdempotencyKey: "run-key-a", CreatedAt: at, UpdatedAt: at,
	}
	loaded.Attempt = domain.Attempt{
		ID: "attempt-a", TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		Number: 1, Status: domain.AttemptRunning, CreatedAt: at, UpdatedAt: at,
	}
	loaded.Reservation = domain.QuotaReservation{
		ID: "reservation-a", TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID,
		SubscriptionConnectionID: loaded.Run.SubscriptionConnectionID, Status: domain.ReservationHeld,
		CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at,
	}
	owner := domain.UserID("owner-a")
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		loaded.Run.TenantID, owner, loaded.Run.ID, loaded.Attempt.ID, loaded.Run.SubscriptionConnectionID, at,
	)
	if err != nil {
		t.Fatal(err)
	}
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: owner, WorkerID: "worker-a",
		CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability-a")),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy-a"))),
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding := authority.HarnessBinding
	binding.ExecutionPlacementDigest = string(placementDigest)
	blobs := &memoryBlobs{values: make(map[string][]byte)}
	contextKey := domain.SessionRunObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID, loaded.Run.ID) + "context.json"
	contextRef, err := blobs.Put(context.Background(), loaded.Run.TenantID, contextKey, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Job = domain.WorkerJob{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, SessionID: loaded.Run.SessionID,
		TriggerEventID: loaded.Run.TriggerEventID, AttemptID: loaded.Attempt.ID,
		ReservationID: loaded.Reservation.ID, InputManifestID: "input-manifest-a",
		ContextSnapshot: contextRef, CredentialOwnerUserID: owner,
		ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute,
			MaxTurns: 4, MaxInputBytes: 1024, MaxContextBytes: 2048, MaxContextEvents: 16,
			MaxArtifacts: 4, MaxToolEvents: 8, MaxToolEventBytes: 4096,
		},
		DeliveryChat: domain.TelegramChatRef{TenantID: loaded.Run.TenantID, ChatID: 1}, ReplyToMessageID: 1, CreatedAt: at,
	}
	if err := loaded.Job.ValidateForRun(loaded.Run); err != nil {
		t.Fatalf("attached-worker fixture: %v", err)
	}
	return loaded, blobs, "lease-a", at.Add(time.Second)
}

func TestMaterializeServerOwnsCanonicalSuccessAndVerifiesArtifact(t *testing.T) {
	loaded, blobs, lease, at := outputFixture(t)
	sourceKey := candidatePrefix(loaded) + "artifact-a"
	source, err := blobs.Put(context.Background(), loaded.Run.TenantID, sourceKey, strings.NewReader("owner-a artifact"))
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "  ready  ", Artifacts: []ArtifactCandidate{{Name: "answer.txt", MediaType: "text/plain", Source: source}}}
	first, err := Materialize(context.Background(), blobs, loaded, lease, 7, candidate, at)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Materialize(context.Background(), blobs, loaded, lease, 7, candidate, at)
	if err != nil {
		t.Fatal(err)
	}
	if first.Completion == nil || first.Failure != nil || len(first.Completion.Events) != 1 ||
		len(first.Completion.Manifest.Artifacts) != 1 || first.Completion.Events[0].DisplayText != "ready" ||
		first.Completion.Events[0].ID != second.Completion.Events[0].ID ||
		first.Completion.Events[0].Payload != second.Completion.Events[0].Payload ||
		first.Completion.Manifest.ID != second.Completion.Manifest.ID ||
		first.Completion.Manifest.Artifacts[0].Blob.Key == source.Key ||
		first.Completion.Manifest.Artifacts[0].Blob.SHA256 != source.SHA256 {
		t.Errorf("server-owned canonical output mismatch: first=%+v second=%+v", first, second)
	}
}

func TestMaterializeRejectsForeignAndCorruptArtifactBeforeCanonicalWrite(t *testing.T) {
	loaded, blobs, lease, at := outputFixture(t)
	source, err := blobs.Put(context.Background(), loaded.Run.TenantID, candidatePrefix(loaded)+"artifact-a", strings.NewReader("original"))
	if err != nil {
		t.Fatal(err)
	}
	baseline := Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "ready", Artifacts: []ArtifactCandidate{{Name: "answer.txt", MediaType: "text/plain", Source: source}}}
	for _, tc := range []struct {
		name   string
		change func(*Candidate)
	}{
		{name: "foreign owner run", change: func(c *Candidate) {
			c.Artifacts[0].Source.Key = "tenants/tenant-a/sessions/session-b/runs/run-b/attached-worker/candidates/attempt-b/artifact"
		}},
		{name: "foreign tenant", change: func(c *Candidate) { c.Artifacts[0].Source.TenantID = "tenant-b" }},
		{name: "oversized", change: func(c *Candidate) { c.Artifacts[0].Source.Size = 1 << 20 }},
		{name: "invalid name", change: func(c *Candidate) { c.Artifacts[0].Name = "../escape" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := baseline
			candidate.Artifacts = append([]ArtifactCandidate(nil), baseline.Artifacts...)
			tc.change(&candidate)
			if _, err := Materialize(context.Background(), blobs, loaded, lease, 7, candidate, at); !errors.Is(err, ErrCandidateInvalid) {
				t.Errorf("unsafe candidate error=%v, want %v", err, ErrCandidateInvalid)
			}
		})
	}
	blobs.values[source.Key] = []byte("changed in source bucket")
	if _, err := Materialize(context.Background(), blobs, loaded, lease, 7, baseline, at); !errors.Is(err, ErrCandidateInvalid) {
		t.Errorf("source digest drift error=%v, want %v", err, ErrCandidateInvalid)
	}
}

func TestMaterializeFailureHasOneServerOwnedNotice(t *testing.T) {
	loaded, blobs, lease, at := outputFixture(t)
	for _, status := range []domain.AttachedWorkerTerminalStatus{
		domain.AttachedWorkerTerminalFailed, domain.AttachedWorkerTerminalCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			materialization, err := Materialize(context.Background(), blobs, loaded, lease, 7, Candidate{Status: status, FailureCode: "test_provider_failed"}, at)
			if err != nil {
				t.Fatal(err)
			}
			if materialization.Completion != nil || materialization.Failure == nil ||
				materialization.Failure.Cancelled != (status == domain.AttachedWorkerTerminalCancelled) ||
				len(materialization.Failure.Events) != 1 ||
				materialization.Failure.Events[0].Kind != domain.SessionEventSystemNotice {
				t.Errorf("failure was not one canonical notice: %+v", materialization)
			}
		})
	}
}
