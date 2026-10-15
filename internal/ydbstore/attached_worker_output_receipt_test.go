package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestAttemptOutputUploadEmptyCanonicalObjectPlannedBeforeRemotePut(t *testing.T) {
	t.Parallel()
	upstream := &receiptEmptyObjectTestStore{}
	planned := &plannedReceiptBlobs{upstream: upstream}
	digest := sha256.Sum256(nil)
	want := domain.BlobRef{TenantID: "tenant-a", Key: "tenants/tenant-a/sessions/session-a/runs/run-a/artifacts/sha256/" + hex.EncodeToString(digest[:]), SHA256: hex.EncodeToString(digest[:])}
	ref, err := planned.Put(t.Context(), want.TenantID, want.Key, bytes.NewReader(nil))
	if err != nil || ref != want || len(planned.items) != 1 || planned.total != 0 || upstream.puts != 0 {
		t.Fatalf("empty object must be planned without remote I/O: ref=%+v items=%d bytes=%d puts=%d err=%v", ref, len(planned.items), planned.total, upstream.puts, err)
	}
	if replay, err := planned.Put(t.Context(), want.TenantID, want.Key, bytes.NewReader(nil)); err != nil || replay != want || len(planned.items) != 1 {
		t.Fatalf("same empty object plan must deduplicate: ref=%+v items=%d err=%v", replay, len(planned.items), err)
	}
	reader, err := planned.Open(t.Context(), want.TenantID, want)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(body) != 0 || upstream.opens != 0 {
		t.Fatalf("planned empty read: bytes=%d opens=%d read=%v close=%v", len(body), upstream.opens, readErr, closeErr)
	}
	if err := planned.Commit(t.Context()); err != nil || upstream.puts != 1 || upstream.opens != 1 || upstream.ref != want {
		t.Fatalf("commit empty object with verified readback: ref=%+v puts=%d opens=%d err=%v", upstream.ref, upstream.puts, upstream.opens, err)
	}
}

func TestAttemptOutputUploadEmptyCanonicalObjectRejectsInvalidAndCorrupt(t *testing.T) {
	t.Parallel()
	t.Run("foreign key", func(t *testing.T) {
		planned := &plannedReceiptBlobs{}
		_, err := planned.Put(t.Context(), "tenant-a", "tenants/tenant-b/empty", bytes.NewReader(nil))
		if !errors.Is(err, attachedworkeroutput.ErrCandidateInvalid) || len(planned.items) != 0 {
			t.Fatalf("invalid empty ref accepted: items=%d err=%v", len(planned.items), err)
		}
	})
	for _, name := range []string{"wrong returned ref", "nonempty readback", "put error"} {
		t.Run(name, func(t *testing.T) {
			cause := errors.New("test-owned object failure")
			upstream := &receiptEmptyObjectTestStore{mode: name, cause: cause}
			planned := &plannedReceiptBlobs{upstream: upstream}
			if _, err := planned.Put(t.Context(), "tenant-a", "tenants/tenant-a/empty", bytes.NewReader(nil)); err != nil {
				t.Fatal(err)
			}
			err := planned.Commit(t.Context())
			if !errors.Is(err, attachedworkeroutput.ErrCandidateInvalid) || upstream.puts != 1 {
				t.Fatalf("invalid empty commit: mode=%s puts=%d err=%v", name, upstream.puts, err)
			}
			if name == "put error" && !errors.Is(err, cause) {
				t.Fatalf("lost original Put error: %v", err)
			}
		})
	}
}

func TestAttemptOutputUploadEmptyCanonicalObjectPreservesIOCause(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"plan read error", "open error", "readback error", "close error"} {
		t.Run(name, func(t *testing.T) {
			cause := errors.New("test-owned " + name)
			upstream := &receiptEmptyObjectTestStore{mode: name, cause: cause}
			planned := &plannedReceiptBlobs{upstream: upstream}
			var body io.Reader = bytes.NewReader(nil)
			if name == "plan read error" {
				body = receiptEmptyObjectTestReader{readErr: cause}
			}
			_, err := planned.Put(t.Context(), "tenant-a", "tenants/tenant-a/empty", body)
			if name == "plan read error" {
				if !errors.Is(err, cause) || len(planned.items) != 0 || upstream.puts != 0 {
					t.Fatalf("planning read cause lost or planned despite failure: items=%d puts=%d err=%v", len(planned.items), upstream.puts, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := planned.Commit(t.Context()); !errors.Is(err, cause) || upstream.puts != 1 || upstream.opens != 1 {
				t.Fatalf("commit I/O cause lost: mode=%s puts=%d opens=%d err=%v", name, upstream.puts, upstream.opens, err)
			}
		})
	}
}

func TestAttemptOutputUploadEmptyTextMaterializeUsesActualPlanner(t *testing.T) {
	// This composes actual Materialize + actual planner + in-memory Commit.
	// It does not execute the separate YDB receipt transaction or source upload.
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	loaded := ports.WorkerJobState{}
	loaded.Run = domain.Run{ID: "empty-run", TenantID: "empty-tenant", SessionID: "empty-session", TriggerEventID: "empty-trigger", SubscriptionConnectionID: "empty-subscription", Status: domain.RunRunning, IdempotencyKey: "empty-run-key", CreatedAt: at, UpdatedAt: at}
	loaded.Attempt = domain.Attempt{ID: "empty-attempt", TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, Number: 1, Status: domain.AttemptRunning, CreatedAt: at, UpdatedAt: at}
	loaded.Reservation = domain.QuotaReservation{ID: "empty-reservation", TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, SubscriptionConnectionID: loaded.Run.SubscriptionConnectionID, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at}
	owner := domain.UserID("empty-owner")
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(loaded.Run.TenantID, owner, loaded.Run.ID, loaded.Attempt.ID, loaded.Run.SubscriptionConnectionID, at)
	if err != nil {
		t.Fatal(err)
	}
	placement := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: owner, WorkerID: "empty-worker", CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("empty-capability")), PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("empty-policy")))}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding := authority.HarnessBinding
	binding.ExecutionPlacementDigest = string(placementDigest)
	prefix := domain.SessionRunObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID, loaded.Run.ID)
	contextDigest := sha256.Sum256([]byte("{}"))
	loaded.Job = domain.WorkerJob{TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, SessionID: loaded.Run.SessionID, TriggerEventID: loaded.Run.TriggerEventID, AttemptID: loaded.Attempt.ID, ReservationID: loaded.Reservation.ID, InputManifestID: "empty-input", ContextSnapshot: domain.BlobRef{TenantID: loaded.Run.TenantID, Key: prefix + "context.json", Size: 2, SHA256: hex.EncodeToString(contextDigest[:])}, CredentialOwnerUserID: owner, ExecutionPlacementV2: placement, HarnessBinding: binding, Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 4, MaxInputBytes: 1024, MaxContextBytes: 2048, MaxContextEvents: 16, MaxArtifacts: 4, MaxToolEvents: 8, MaxToolEventBytes: 4096}, DeliveryChat: domain.TelegramChatRef{TenantID: loaded.Run.TenantID, ChatID: 1}, ReplyToMessageID: 1, CreatedAt: at}
	upstream := &receiptEmptyObjectTestStore{}
	source, err := upstream.Put(t.Context(), loaded.Run.TenantID, prefix+"attached-worker/candidates/empty-attempt/empty", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	planned := &plannedReceiptBlobs{upstream: upstream}
	candidate := attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "An empty file was requested.", Artifacts: []attachedworkeroutput.ArtifactCandidate{{Name: "empty.txt", MediaType: "text/plain", Source: source}}}
	materialized, err := attachedworkeroutput.Materialize(t.Context(), planned, loaded, "empty-lease", 7, candidate, at.Add(time.Second))
	if err != nil || materialized.Completion == nil || len(materialized.Completion.Manifest.Artifacts) != 1 || len(materialized.Completion.Events) != 1 || len(planned.items) != 2 || upstream.puts != 1 {
		t.Fatalf("Materialize must plan artifact and answer event without new remote Put: items=%d puts=%d completion=%+v err=%v", len(planned.items), upstream.puts, materialized.Completion, err)
	}
	artifact := materialized.Completion.Manifest.Artifacts[0]
	if artifact.Name != "empty.txt" || artifact.MediaType != "text/plain" || artifact.Blob.Size != 0 || artifact.Blob.SHA256 != source.SHA256 || artifact.Blob.Key == source.Key {
		t.Fatalf("canonical empty artifact changed: %+v", artifact)
	}
	if err := planned.Commit(t.Context()); err != nil || upstream.puts != 3 {
		t.Fatalf("actual planner Commit must put both planned objects: puts=%d err=%v", upstream.puts, err)
	}
	if content, exists := upstream.values[artifact.Blob.Key]; !exists || len(content) != 0 {
		t.Fatalf("canonical empty object absent: exists=%t bytes=%d", exists, len(content))
	}
}

// Test-owned in-memory sink. No provider, storage service or database is used.
type receiptEmptyObjectTestStore struct {
	puts, opens int
	mode        string
	cause       error
	ref         domain.BlobRef
	values      map[string][]byte
}

func (s *receiptEmptyObjectTestStore) Put(_ context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	s.puts++
	content, err := io.ReadAll(io.LimitReader(body, 1025))
	if err != nil || len(content) > 1024 {
		return domain.BlobRef{}, attachedworkeroutput.ErrCandidateInvalid
	}
	if s.mode == "put error" {
		return domain.BlobRef{}, s.cause
	}
	digest := sha256.Sum256(content)
	s.ref = domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(content)), SHA256: hex.EncodeToString(digest[:])}
	if s.values == nil {
		s.values = make(map[string][]byte)
	}
	s.values[key] = bytes.Clone(content)
	ref := s.ref
	if s.mode == "wrong returned ref" {
		ref.Key += "-wrong"
	}
	return ref, nil
}

func (s *receiptEmptyObjectTestStore) Open(_ context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	s.opens++
	if s.mode == "open error" {
		return nil, s.cause
	}
	content, exists := s.values[ref.Key]
	if !exists || tenant != ref.TenantID {
		return nil, attachedworkeroutput.ErrCandidateInvalid
	}
	if s.mode == "nonempty readback" {
		content = []byte("x")
	}
	if s.mode == "readback error" {
		return receiptEmptyObjectTestReader{readErr: s.cause}, nil
	}
	if s.mode == "close error" {
		return receiptEmptyObjectTestReader{closeErr: s.cause}, nil
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

type receiptEmptyObjectTestReader struct{ readErr, closeErr error }

func (r receiptEmptyObjectTestReader) Read([]byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return 0, io.EOF
}
func (r receiptEmptyObjectTestReader) Close() error { return r.closeErr }

func (*receiptEmptyObjectTestStore) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return attachedworkeroutput.ErrCandidateInvalid
}

func TestAttachedWorkerOutputReceiptStatusAdmissible(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	attempt := domain.AttachedWorkerAttemptV1{
		State:          domain.AttachedWorkerAttemptClaimed,
		LeaseExpiresAt: base.Add(time.Minute),
	}
	for _, tc := range []struct {
		name           string
		state          domain.AttachedWorkerAttemptState
		status         domain.AttachedWorkerTerminalStatus
		cancelRevision uint64
		cancelDeadline time.Time
		at             time.Time
		want           bool
	}{
		{name: "claimed success", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, at: base, want: true},
		{name: "claimed failure", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalFailed, at: base, want: true},
		{name: "claimed cancelled", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalCancelled, at: base},
		{name: "claimed invalid status", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalStatus("unknown"), at: base},
		{name: "claimed at zero time", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded},
		{name: "claimed at lease expiry", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, at: base.Add(time.Minute)},
		{name: "claimed with cancel revision", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, at: base},
		{name: "claimed with cancel deadline", state: domain.AttachedWorkerAttemptClaimed, status: domain.AttachedWorkerTerminalSucceeded, cancelDeadline: base.Add(10 * time.Second), at: base},
		{name: "cancel requested before ack deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second), want: true},
		{name: "cancel requested at ack deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(10 * time.Second)},
		{name: "cancel requested without revision", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second)},
		{name: "cancel requested without deadline", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, at: base.Add(5 * time.Second)},
		{name: "cancel requested cannot succeed", state: domain.AttachedWorkerAttemptCancelRequested, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(5 * time.Second)},
		{name: "cancel acknowledged after ack deadline", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second), want: true},
		{name: "cancel acknowledged cannot succeed", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalSucceeded, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second)},
		{name: "cancel acknowledged without revision", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelDeadline: base.Add(10 * time.Second), at: base.Add(30 * time.Second)},
		{name: "cancel acknowledged without deadline", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, at: base.Add(30 * time.Second)},
		{name: "expired lease", state: domain.AttachedWorkerAttemptCancelAcknowledged, status: domain.AttachedWorkerTerminalCancelled, cancelRevision: 1, cancelDeadline: base.Add(10 * time.Second), at: base.Add(time.Minute)},
		{name: "fenced", state: domain.AttachedWorkerAttemptFencedUnknown, status: domain.AttachedWorkerTerminalFailed, at: base},
		{name: "terminal pending", state: domain.AttachedWorkerAttemptTerminalPending, status: domain.AttachedWorkerTerminalSucceeded, at: base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := attempt
			candidate.State = tc.state
			candidate.CancelRevision = tc.cancelRevision
			candidate.CancelDeadline = tc.cancelDeadline
			if got := attachedWorkerOutputReceiptStatusAdmissible(candidate, tc.status, tc.at); got != tc.want {
				t.Errorf("receipt admissible state=%s status=%s at=%s: got %t, want %t", tc.state, tc.status, tc.at.Format(time.RFC3339), got, tc.want)
			}
		})
	}
}

func TestAttachedWorkerOutputReceiptRequiresCurrentBearerAndExactOwnerHead(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.October, 7, 10, 0, 0, 0, time.UTC)
	secret := domain.DigestAttachedWorkerConnectionSecret([]byte("receipt-secret"))
	capability := domain.DigestAttachedWorkerCapability([]byte("receipt-capability"))
	contextDigest := domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("receipt-context")))
	policy := domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("receipt-policy")))
	fence, err := domain.NewAttachedWorkerFenceTokenV1("tenant-a", "owner-a", "worker-collision", "run-a", "attempt-a", "lease-a", 5)
	if err != nil {
		t.Fatal(err)
	}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "worker-collision",
		ConnectionID: "connection-a", PresentedSecretDigest: secret,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		RunID: "run-a", AttemptID: "attempt-a", AttemptSequence: 1,
		LeaseID: "lease-a", LeaseGeneration: 5, FenceToken: fence,
		LeaseExpiresAtUnixMicro: at.Add(time.Minute).UnixMicro(),
		ContextDigest:           contextDigest, CapabilityDigest: capability, PolicyDigest: policy,
	}
	worker := domain.AttachedWorker{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: request.WorkerID,
		DesiredState:         domain.AttachedWorkerDesiredActive,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
	}
	connection := domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ID: request.ConnectionID, SecretDigest: secret, State: domain.AttachedWorkerConnectionOnline,
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		AuthExpiresAt: at.Add(time.Hour), PresenceExpiresAt: at.Add(time.Minute),
	}
	attempt := domain.AttachedWorkerAttemptV1{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ConnectionID: request.ConnectionID, EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ExecutionConnectionID: request.ConnectionID, ExecutionConnectionGeneration: 3,
		RunID: request.RunID, AttemptID: request.AttemptID, LeaseID: request.LeaseID,
		LeaseGeneration: 5, FenceToken: fence, LeaseExpiresAt: at.Add(time.Minute),
		ContextDigest: contextDigest, CapabilityDigest: capability, PolicyDigest: policy,
		State: domain.AttachedWorkerAttemptClaimed, Revision: 7,
	}
	if err := validateAttachedWorkerSealedInputAuthorization(request); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		status domain.AttachedWorkerTerminalStatus
		change func(*ports.AttachedWorkerSealedInputAuthorization, *domain.AttachedWorker, *domain.AttachedWorkerConnection, *domain.AttachedWorkerAttemptV1)
		want   bool
	}{
		{name: "current success", status: domain.AttachedWorkerTerminalSucceeded, want: true},
		{name: "foreign owner", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.OwnerUserID = "owner-b"
		}},
		{name: "wrong bearer", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("stolen"))
		}},
		{name: "rotated connection", status: domain.AttachedWorkerTerminalSucceeded, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, c *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			c.ID = "connection-b"
		}},
		{name: "rebound current connection cannot originate output", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, w *domain.AttachedWorker, c *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			r.ConnectionID, c.ID, a.ConnectionID = "connection-b", "connection-b", "connection-b"
			r.ConnectionGeneration, w.ConnectionGeneration, c.ConnectionGeneration, a.ConnectionGeneration = 4, 4, 4, 4
		}},
		{name: "stale generation", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.ConnectionGeneration++
		}},
		{name: "stale fence", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.LeaseGeneration++
		}},
		{name: "capability bait and switch", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			r.CapabilityDigest = domain.DigestAttachedWorkerCapability([]byte("changed"))
		}},
		{name: "revoked", status: domain.AttachedWorkerTerminalSucceeded, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, w *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, _ *domain.AttachedWorkerAttemptV1) {
			w.DesiredState = domain.AttachedWorkerDesiredRevoked
		}},
		{name: "cancel cannot become success", status: domain.AttachedWorkerTerminalSucceeded, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			a.State, a.CancelRevision, a.CancelDeadline = domain.AttachedWorkerAttemptCancelAcknowledged, 1, at.Add(time.Second)
		}},
		{name: "cancelled after acknowledged deadline", status: domain.AttachedWorkerTerminalCancelled, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			a.State, a.CancelRevision, a.CancelDeadline = domain.AttachedWorkerAttemptCancelAcknowledged, 1, at.Add(-time.Second)
		}, want: true},
		{name: "fenced", status: domain.AttachedWorkerTerminalFailed, change: func(_ *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			a.State = domain.AttachedWorkerAttemptFencedUnknown
		}},
		{name: "attempt changed", status: domain.AttachedWorkerTerminalSucceeded, change: func(r *ports.AttachedWorkerSealedInputAuthorization, _ *domain.AttachedWorker, _ *domain.AttachedWorkerConnection, a *domain.AttachedWorkerAttemptV1) {
			r.ExpectedAttemptRevision = a.Revision
			a.Revision++
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, w, c, a := request, worker, connection, attempt
			if tc.change != nil {
				tc.change(&r, &w, &c, &a)
			}
			if got := attachedWorkerOutputReceiptAuthorized(r, tc.status, at, w, c, a); got != tc.want {
				t.Errorf("receipt authority status=%s state=%s revision=%d: got %t, want %t", tc.status, a.State, a.Revision, got, tc.want)
			}
		})
	}
}
