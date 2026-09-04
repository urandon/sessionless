package attachedworkerux

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type readStore struct {
	workers      []domain.AttachedWorker
	connection   *domain.AttachedWorkerConnection
	manifest     *domain.AttachedWorkerCapabilityManifest
	attempt      *domain.AttachedWorkerAttemptV1
	messages     []domain.AttachedWorkerAttemptMessageV1
	loadOverride *domain.AttachedWorker
	listOverride []domain.AttachedWorker
	err          error
	reads        int
	messageReads int
}

func (store *readStore) LoadAttachedWorker(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	store.reads++
	if store.err != nil {
		return domain.AttachedWorker{}, false, store.err
	}
	if store.loadOverride != nil {
		return *store.loadOverride, true, nil
	}
	for _, worker := range store.workers {
		if worker.TenantID == tenant && worker.OwnerUserID == owner && worker.ID == workerID {
			return worker, true, nil
		}
	}
	return domain.AttachedWorker{}, false, nil
}

func (store *readStore) ListAttachedWorkers(_ context.Context, tenant domain.TenantID, owner domain.UserID, after domain.AttachedWorkerID, limit uint64) ([]domain.AttachedWorker, error) {
	store.reads++
	if store.err != nil {
		return nil, store.err
	}
	if store.listOverride != nil {
		return append([]domain.AttachedWorker(nil), store.listOverride...), nil
	}
	workers := make([]domain.AttachedWorker, 0, len(store.workers))
	for _, worker := range store.workers {
		if worker.TenantID == tenant && worker.OwnerUserID == owner && worker.ID > after {
			workers = append(workers, worker)
		}
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	if uint64(len(workers)) > limit {
		workers = workers[:limit]
	}
	return workers, nil
}

func (store *readStore) LoadAttachedWorkerConnection(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	store.reads++
	if store.err != nil {
		return domain.AttachedWorkerConnection{}, false, store.err
	}
	if store.connection != nil && store.connection.TenantID == tenant && store.connection.OwnerUserID == owner && store.connection.WorkerID == workerID {
		return *store.connection, true, nil
	}
	return domain.AttachedWorkerConnection{}, false, nil
}

func (store *readStore) LoadAttachedWorkerCapabilityManifest(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID, digest domain.AttachedWorkerCapabilityDigest) (domain.AttachedWorkerCapabilityManifest, bool, error) {
	store.reads++
	if store.err != nil {
		return domain.AttachedWorkerCapabilityManifest{}, false, store.err
	}
	if store.manifest != nil && store.manifest.TenantID == tenant && store.manifest.OwnerUserID == owner && store.manifest.WorkerID == workerID && store.manifest.Digest == digest {
		return *store.manifest, true, nil
	}
	return domain.AttachedWorkerCapabilityManifest{}, false, nil
}

func (store *readStore) LoadAttachedWorkerAttempt(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorkerAttemptV1, bool, error) {
	store.reads++
	if store.err != nil {
		return domain.AttachedWorkerAttemptV1{}, false, store.err
	}
	if store.attempt != nil && store.attempt.TenantID == tenant && store.attempt.OwnerUserID == owner && store.attempt.WorkerID == workerID {
		return *store.attempt, true, nil
	}
	return domain.AttachedWorkerAttemptV1{}, false, nil
}

func (store *readStore) ListAttachedWorkerAttemptMessages(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID, attemptID domain.AttemptID) ([]domain.AttachedWorkerAttemptMessageV1, error) {
	store.reads++
	store.messageReads++
	if store.err != nil {
		return nil, store.err
	}
	result := make([]domain.AttachedWorkerAttemptMessageV1, 0, len(store.messages))
	for _, message := range store.messages {
		if message.TenantID == tenant && message.OwnerUserID == owner && message.WorkerID == workerID && message.AttemptID == attemptID {
			result = append(result, message)
		}
	}
	return result, nil
}

func testWorker(id domain.AttachedWorkerID) domain.AttachedWorker {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	return domain.AttachedWorker{
		TenantID: "tenant-1", OwnerUserID: "owner-1", ID: id, DisplayName: "Local worker",
		IdentityPublicKey: bytes.Repeat([]byte{7}, ed25519.PublicKeySize), EnrollmentGeneration: 2,
		ConnectionGeneration: 3, DesiredState: domain.AttachedWorkerDesiredActive,
		ObservedState: domain.AttachedWorkerObservedOnline, Revision: 4, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Minute),
	}
}

func testCapability(t *testing.T, worker domain.AttachedWorker, evaluatedAt time.Time) (domain.AttachedWorkerConnection, domain.AttachedWorkerCapabilityManifest) {
	t.Helper()
	protocolManifest := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, Revision: 5,
		ProtocolOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: attachedworkerprotocol.ProtocolVersionV1, Maximum: attachedworkerprotocol.ProtocolVersionV1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		OperatingSystem: "darwin", Architecture: "arm64", BuildID: "build-1",
		HarnessName: "sessionless", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: bytes.Repeat([]byte{9}, 32),
		IsolationEvidence:       []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationProcessBoundary},
		Features:                []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation}, MaxConcurrentAttempts: 1,
	}
	digestBytes, err := attachedworkerprotocol.ManifestDigestV1(protocolManifest)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(protocolManifest)
	if err != nil {
		t.Fatal(err)
	}
	digest := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digestBytes))
	identityDigest := domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey)
	connectedAt := evaluatedAt.Add(-time.Hour)
	connection := domain.AttachedWorkerConnection{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ID: "wcn-1", ActivationChallengeID: "wch-1", EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: worker.ConnectionGeneration, ProtocolVersion: uint32(attachedworkerprotocol.ProtocolVersionV1),
		CapabilityDigest: digest, SecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("secret")),
		ChannelBinding: domain.NewAttachedWorkerChannelBinding(bytes.Repeat([]byte{4}, 32)), ManifestRevision: protocolManifest.Revision,
		ManifestIdentityKey: identityDigest, ManifestSignature: bytes.Repeat([]byte{3}, ed25519.SignatureSize),
		ManifestObservedAt: connectedAt.Add(time.Minute), State: domain.AttachedWorkerConnectionOnline,
		PlatformSequence: 3, WorkerSequence: 3, PlatformAck: 3, WorkerAck: 3,
		ProtocolSnapshot: []byte{1}, ConnectedAt: connectedAt, LastCheckpointAt: evaluatedAt.Add(-20 * time.Minute),
		PresenceExpiresAt: evaluatedAt.Add(-time.Microsecond), AuthExpiresAt: evaluatedAt.Add(time.Hour), Revision: 6,
	}
	manifest := domain.AttachedWorkerCapabilityManifest{
		Version: domain.AttachedWorkerCapabilityManifestVersionV1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID,
		WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, ManifestRevision: protocolManifest.Revision,
		Digest: digest, ProtocolVersion: uint32(attachedworkerprotocol.ProtocolVersionV1), IdentityKeyDigest: identityDigest,
		ManifestPayload: payload,
	}
	return connection, manifest
}

func testAttempt(t *testing.T, worker domain.AttachedWorker, state domain.AttachedWorkerAttemptState) domain.AttachedWorkerAttemptV1 {
	t.Helper()
	now := time.Date(2026, 8, 25, 11, 55, 0, 0, time.UTC)
	fence, err := domain.NewAttachedWorkerFenceTokenV1(worker.TenantID, worker.OwnerUserID, worker.ID, "run-1", "attempt-1", "lease-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	attempt := domain.AttachedWorkerAttemptV1{
		Version: domain.AttachedWorkerAttemptVersionV1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID,
		WorkerID: worker.ID, ConnectionID: "wcn-1", RunID: "run-1", AttemptID: "attempt-1",
		ReservationID: "reservation-1", LeaseID: "lease-1", LeaseGeneration: 7, FenceToken: fence,
		EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration,
		ContextDigest:    domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context"))),
		CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))),
		State:            state, PlatformAttemptSequence: 2, WorkerAttemptSequence: 2,
		LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now.Add(time.Minute), Revision: 3,
	}
	switch state {
	case domain.AttachedWorkerAttemptCancelAcknowledged:
		attempt.CancelRevision, attempt.CancelDeadline = 1, now.Add(10*time.Minute)
	case domain.AttachedWorkerAttemptTerminalCommitted:
		attempt.TerminalSequence, attempt.TerminalStatus = 2, domain.AttachedWorkerTerminalSucceeded
		attempt.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(domain.DigestAttachedWorkerCapability([]byte("evidence")))
	}
	if err := attempt.Validate(); err != nil {
		t.Fatalf("attempt fixture: %v", err)
	}
	return attempt
}

func testAttemptMessage(t *testing.T, attempt domain.AttachedWorkerAttemptV1, kind domain.AttachedWorkerAttemptMessageKind, createdAt time.Time) domain.AttachedWorkerAttemptMessageV1 {
	t.Helper()
	binding := attachedworkerprotocol.AttemptBindingV1{
		RunID: string(attempt.RunID), AttemptID: string(attempt.AttemptID), LeaseID: string(attempt.LeaseID),
		LeaseGeneration: attempt.LeaseGeneration, FenceToken: string(attempt.FenceToken), ExpiresAtUnixMicro: attempt.LeaseExpiresAt.UnixMicro(),
		ContextDigest: mustDecodeHex(t, string(attempt.ContextDigest)), CapabilityDigest: mustDecodeHex(t, string(attempt.CapabilityDigest)),
		PolicyDigest: mustDecodeHex(t, string(attempt.PolicyDigest)),
	}
	message := domain.AttachedWorkerAttemptMessageV1{
		Version: domain.AttachedWorkerAttemptMessageVersionV1, TenantID: attempt.TenantID, OwnerUserID: attempt.OwnerUserID,
		WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, ConnectionGeneration: attempt.ConnectionGeneration,
		CreatedAt: createdAt,
	}
	frame := attachedworkerprotocol.FrameV1{
		Version: attachedworkerprotocol.ProtocolVersionV1, MessageID: "message-occurrence", WorkerID: string(attempt.WorkerID),
		EnrollmentGeneration: attempt.EnrollmentGeneration, ConnectionGeneration: attempt.ConnectionGeneration, Ack: 1,
	}
	switch kind {
	case domain.AttachedWorkerAttemptMessageCancelRequested:
		message.Direction, message.Kind = domain.AttachedWorkerAttemptPlatformToWorker, kind
		message.AttemptSequence, message.EnvelopeSequence, message.OperationDeadline = 3, 4, attempt.CancelDeadline
		frame.Sequence, frame.Kind = message.EnvelopeSequence, attachedworkerprotocol.MessageCancel
		frame.Cancel = &attachedworkerprotocol.CancelV1{Binding: binding, AttemptSequence: message.AttemptSequence, CancelRevision: attempt.CancelRevision, Code: attachedworkerprotocol.CancelRequested}
	case domain.AttachedWorkerAttemptMessageCancelAcknowledged:
		message.Direction, message.Kind = domain.AttachedWorkerAttemptWorkerToPlatform, kind
		message.AttemptSequence, message.EnvelopeSequence = 3, 4
		frame.Sequence, frame.Kind = message.EnvelopeSequence, attachedworkerprotocol.MessageCancelAck
		frame.CancelAck = &attachedworkerprotocol.CancelAckV1{Binding: binding, AttemptSequence: message.AttemptSequence, CancelRevision: attempt.CancelRevision}
	case domain.AttachedWorkerAttemptMessageTerminalCommitted:
		message.Direction, message.Kind = domain.AttachedWorkerAttemptPlatformToWorker, kind
		message.AttemptSequence, message.EnvelopeSequence = attempt.PlatformAttemptSequence, 5
		message.MaterializationReservationID, message.ExecutionConnectionID = attempt.ReservationID, attempt.ConnectionID
		frame.Sequence, frame.Kind = message.EnvelopeSequence, attachedworkerprotocol.MessageTerminalAck
		frame.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
			Binding: binding, AttemptSequence: message.AttemptSequence, TerminalSequence: attempt.TerminalSequence,
			Status: attachedworkerprotocol.TerminalStatus(attempt.TerminalStatus), Result: testTerminalResult(t, attempt.TerminalStatus),
			EvidenceDigest: mustDecodeHex(t, string(attempt.TerminalEvidenceDigest)),
		}
	case domain.AttachedWorkerAttemptMessageTerminal:
		message.Direction, message.Kind = domain.AttachedWorkerAttemptWorkerToPlatform, kind
		message.AttemptSequence, message.EnvelopeSequence = attempt.WorkerAttemptSequence, 4
		message.MaterializationReservationID, message.ExecutionConnectionID = attempt.ReservationID, attempt.ConnectionID
		frame.Sequence, frame.Kind = message.EnvelopeSequence, attachedworkerprotocol.MessageTerminal
		frame.Terminal = &attachedworkerprotocol.TerminalV1{
			Binding: binding, AttemptSequence: message.AttemptSequence, TerminalSequence: attempt.TerminalSequence,
			Status: attachedworkerprotocol.TerminalStatus(attempt.TerminalStatus), Result: testTerminalResult(t, attempt.TerminalStatus),
			EvidenceDigest: mustDecodeHex(t, string(attempt.TerminalEvidenceDigest)),
		}
	default:
		t.Fatalf("unsupported occurrence message kind %q", kind)
	}
	payload, err := attachedworkerprotocol.EncodeBatchV1(attachedworkerprotocol.BatchV1{Version: frame.Version, Frames: []attachedworkerprotocol.FrameV1{frame}})
	if err != nil {
		t.Fatalf("encode occurrence frame: %v", err)
	}
	fingerprint, err := attachedworkerprotocol.AttemptFrameFingerprintV1(frame)
	if err != nil {
		t.Fatalf("fingerprint occurrence frame: %v", err)
	}
	message.Payload = payload
	message.Fingerprint = domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(fingerprint))
	if err := message.Validate(); err != nil {
		t.Fatalf("message fixture: %v", err)
	}
	return message
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func testTerminalResult(t *testing.T, status domain.AttachedWorkerTerminalStatus) attachedworkerprotocol.TerminalResult {
	t.Helper()
	switch status {
	case domain.AttachedWorkerTerminalSucceeded:
		return attachedworkerprotocol.TerminalResultCompleted
	case domain.AttachedWorkerTerminalFailed:
		return attachedworkerprotocol.TerminalResultFailed
	case domain.AttachedWorkerTerminalCancelled:
		return attachedworkerprotocol.TerminalResultCancelled
	default:
		t.Fatalf("unsupported terminal status %q", status)
		return ""
	}
}

func mutateOccurrenceFrame(t *testing.T, message domain.AttachedWorkerAttemptMessageV1, mutate func(*attachedworkerprotocol.FrameV1)) domain.AttachedWorkerAttemptMessageV1 {
	t.Helper()
	batch, err := attachedworkerprotocol.DecodeBatchV1(message.Payload)
	if err != nil || len(batch.Frames) != 1 {
		t.Fatalf("decode message fixture: %v", err)
	}
	mutate(&batch.Frames[0])
	payload, err := attachedworkerprotocol.EncodeBatchV1(batch)
	if err != nil {
		t.Fatalf("encode mutated message fixture: %v", err)
	}
	fingerprint, err := attachedworkerprotocol.AttemptFrameFingerprintV1(batch.Frames[0])
	if err != nil {
		t.Fatalf("fingerprint mutated message fixture: %v", err)
	}
	message.Payload = payload
	message.Fingerprint = domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(fingerprint))
	return message
}

func TestGetSeparatesAuthorityEvidenceAndFreshness(t *testing.T) {
	t.Parallel()
	evaluatedAt := time.Date(2026, 8, 25, 12, 0, 0, 123456789, time.UTC)
	worker := testWorker("worker-1")
	connection, manifest := testCapability(t, worker, evaluatedAt)
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptCancelAcknowledged)
	requestedAt := attempt.CreatedAt.Add(30 * time.Second)
	acknowledgedAt := requestedAt.Add(15 * time.Second)
	messages := []domain.AttachedWorkerAttemptMessageV1{
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelRequested, requestedAt),
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelAcknowledged, acknowledgedAt),
	}
	if err := worker.Validate(); err != nil {
		t.Fatalf("worker fixture: %v", err)
	}
	if err := connection.Validate(); err != nil {
		t.Fatalf("connection fixture: %v", err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest fixture: %v", err)
	}
	store := &readStore{workers: []domain.AttachedWorker{worker}, connection: &connection, manifest: &manifest, attempt: &attempt, messages: messages}
	service, err := NewService(store, func() time.Time { return evaluatedAt })
	if err != nil {
		t.Fatal(err)
	}
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if model.EvaluatedAt.Nanosecond()%1000 != 0 || model.Worker.ObservedState != "online" || model.Connectivity.State != "online" || model.Connectivity.Freshness != FreshnessExpired {
		t.Fatalf("current truth/freshness collapsed: %+v", model)
	}
	if model.Capability.State != "advertised" || model.AdmissionPreview.State != "not_evaluated" || model.Resource.Quota.State != "unknown" {
		t.Fatalf("evidence became authorization: %+v", model)
	}
	if model.Readiness.Isolation.ConfigurationState != "unsupported" || len(model.Readiness.Isolation.AdvertisedEvidence) != 1 {
		t.Fatalf("advertisement upgraded isolation support: %+v", model.Readiness.Isolation)
	}
	if model.Execution.CancelRequest.State != "requested" || model.Execution.CancelAcknowledgement.State != "acknowledged" || model.Execution.ProcessObservation.State != "unknown" || model.Execution.CanonicalTerminal.State != "none" {
		t.Fatalf("execution facts collapsed: %+v", model.Execution)
	}
	if !model.Execution.CancelRequest.RequestedAt.Equal(requestedAt) || !model.Execution.CancelAcknowledgement.AcknowledgedAt.Equal(acknowledgedAt) {
		t.Fatalf("durable cancellation occurrence times lost: %+v", model.Execution)
	}
	for _, action := range model.Governance.AvailableActions {
		if action.Enabled || action.ReasonCode != ActionUnavailableControlContract {
			t.Fatalf("unaccepted control enabled: %+v", action)
		}
	}
	if store.reads != 5 {
		t.Fatalf("unexpected read count %d", store.reads)
	}
}

func TestGetCollapsesForeignOwnerAndMissing(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	service, err := NewService(&readStore{workers: []domain.AttachedWorker{worker}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, locator := range map[string][2]string{
		"foreign owner": {"owner-2", string(worker.ID)},
		"missing":       {string(worker.OwnerUserID), "worker-missing"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := service.Get(context.Background(), worker.TenantID, domain.UserID(locator[0]), domain.AttachedWorkerID(locator[1]))
			if !errors.Is(err, ErrNotFound) || err.Error() != ErrNotFound.Error() {
				t.Fatalf("public owner oracle: %v", err)
			}
		})
	}
}

func TestMissingConnectionDoesNotRewriteObservedTruth(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}}, nil)
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if model.Worker.ObservedState != "online" || model.Connectivity.State != "unknown" || containsReason(model.ObservationWarnings, ReasonWorkerOffline) {
		t.Fatalf("missing observation rewrote current truth: %+v", model)
	}
}

func TestBackendDetailsAreSanitized(t *testing.T) {
	t.Parallel()
	service, _ := NewService(&readStore{err: errors.New("provider token from backend")}, nil)
	_, err := service.Get(context.Background(), "tenant-1", "owner-1", "worker-1")
	if !errors.Is(err, ErrBackend) || strings.Contains(err.Error(), "provider") || strings.Contains(err.Error(), "token") {
		t.Fatalf("backend detail escaped: %v", err)
	}
}

func TestStoreScopeAndOrderingCorruptionFailClosed(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	foreign := worker
	foreign.OwnerUserID = "owner-2"
	service, _ := NewService(&readStore{loadOverride: &foreign}, nil)
	if _, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID); !errors.Is(err, ErrBackend) {
		t.Fatalf("foreign row escaped point-load scope: %v", err)
	}
	service, _ = NewService(&readStore{listOverride: []domain.AttachedWorker{testWorker("worker-2"), testWorker("worker-1")}}, nil)
	if _, err := service.List(context.Background(), worker.TenantID, worker.OwnerUserID, "", 2); !errors.Is(err, ErrBackend) {
		t.Fatalf("unordered page escaped store contract: %v", err)
	}
}

func TestListUsesBoundedOwnerScopedPagination(t *testing.T) {
	t.Parallel()
	workers := []domain.AttachedWorker{testWorker("worker-3"), testWorker("worker-1"), testWorker("worker-2")}
	service, _ := NewService(&readStore{workers: workers}, func() time.Time { return time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC) })
	result, err := service.List(context.Background(), "tenant-1", "owner-1", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 2 || !result.HasMore || result.NextWorkerID != "worker-2" || result.Items[0].Worker.WorkerID != "worker-1" {
		t.Fatalf("bad page: %+v", result)
	}
	for _, item := range result.Items {
		if !item.EvaluatedAt.Equal(result.EvaluatedAt) {
			t.Fatalf("summary lost its page-time provenance: page=%s item=%s", result.EvaluatedAt, item.EvaluatedAt)
		}
	}
	if _, err := service.List(context.Background(), "tenant-1", "owner-1", "", MaxListLimitV1+1); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unbounded limit accepted: %v", err)
	}
}

func TestListUsesAttemptHeadWithoutLoadingOccurrenceLedger(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptCancelAcknowledged)
	store := &readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt}
	service, _ := NewService(store, nil)
	page, err := service.List(context.Background(), worker.TenantID, worker.OwnerUserID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ExecutionState != string(domain.AttachedWorkerAttemptCancelAcknowledged) {
		t.Fatalf("summary lost authoritative attempt head: %+v", page)
	}
	if store.messageReads != 0 {
		t.Fatalf("summary loaded raw occurrence ledger %d times", store.messageReads)
	}
}

func TestDiagnosticsDoNotInventObservedStateTime(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}}, nil)
	diagnostics, err := service.Diagnostics(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range diagnostics.Facts {
		if fact.Code == "observed_state" {
			if !fact.ObservedAt.IsZero() {
				t.Fatalf("worker updated_at was misreported as observed-state transition time: %s", fact.ObservedAt)
			}
			return
		}
	}
	t.Fatal("observed_state diagnostic fact missing")
}

func TestStoredCapabilityCorruptionFailsClosed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	worker := testWorker("worker-1")
	connection, manifest := testCapability(t, worker, now)
	manifest.ManifestPayload = []byte(`{}`)
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, connection: &connection, manifest: &manifest}, func() time.Time { return now })
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if !errors.Is(err, ErrBackend) || model.Version != 0 {
		t.Fatalf("corrupt authority escaped as public truth: model=%+v err=%v", model, err)
	}
}

func TestTerminalCommitDoesNotClaimProcessStopped(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptTerminalCommitted)
	committedAt := attempt.UpdatedAt
	messages := []domain.AttachedWorkerAttemptMessageV1{
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageTerminal, committedAt.Add(-time.Second)),
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageTerminalCommitted, committedAt),
	}
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt, messages: messages}, nil)
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if model.Execution.WorkerTerminal.State != "received" || model.Execution.CanonicalTerminal.State != "committed" || model.Execution.ProcessObservation.State != "unknown" {
		t.Fatalf("terminal/process authorities collapsed: %+v", model.Execution)
	}
	if !model.Execution.CanonicalTerminal.CommittedAt.Equal(committedAt) {
		t.Fatalf("durable terminal commit time lost: %+v", model.Execution.CanonicalTerminal)
	}
}

func TestTerminalWithCancelRevisionDoesNotFabricateCancelAck(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptTerminalCommitted)
	attempt.CancelRevision = 2
	attempt.CancelDeadline = attempt.CreatedAt.Add(10 * time.Minute)
	if err := attempt.Validate(); err != nil {
		t.Fatal(err)
	}
	messages := []domain.AttachedWorkerAttemptMessageV1{
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelRequested, attempt.CreatedAt.Add(time.Second)),
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageTerminal, attempt.UpdatedAt.Add(-time.Second)),
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageTerminalCommitted, attempt.UpdatedAt),
	}
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt, messages: messages}, nil)
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if model.Execution.CancelRequest.State != "requested" || model.Execution.CancelAcknowledgement.State != "unknown" || model.Execution.CanonicalTerminal.State != "committed" {
		t.Fatalf("retained cancel revision fabricated acknowledgement: %+v", model.Execution)
	}
}

func TestRetiredCancellationUsesOnlyDurableAcknowledgementEvidence(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptCancelAcknowledged)
	attempt.State = domain.AttachedWorkerAttemptRetired
	if err := attempt.Validate(); err != nil {
		t.Fatal(err)
	}
	requested := testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelRequested, attempt.CreatedAt.Add(time.Second))
	acknowledged := testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelAcknowledged, attempt.CreatedAt.Add(2*time.Second))

	for name, messages := range map[string][]domain.AttachedWorkerAttemptMessageV1{
		"acknowledged":   {requested, acknowledged},
		"unacknowledged": {requested},
	} {
		t.Run(name, func(t *testing.T) {
			service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt, messages: messages}, nil)
			model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
			if err != nil {
				t.Fatal(err)
			}
			if name == "acknowledged" {
				if model.Execution.CancelAcknowledgement.State != "acknowledged" ||
					!model.Execution.CancelAcknowledgement.AcknowledgedAt.Equal(acknowledged.CreatedAt) {
					t.Fatalf("retirement discarded durable CancelAck: %+v", model.Execution.CancelAcknowledgement)
				}
			} else if model.Execution.CancelAcknowledgement.State != "unknown" ||
				!model.Execution.CancelAcknowledgement.AcknowledgedAt.IsZero() {
				t.Fatalf("retirement fabricated CancelAck: %+v", model.Execution.CancelAcknowledgement)
			}
		})
	}
}

func TestEstablishedExecutionWithoutAuthoritativeOccurrenceFailsClosed(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptCancelAcknowledged)
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt}, nil)
	if _, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID); !errors.Is(err, ErrBackend) {
		t.Fatalf("missing durable occurrence was exposed as established truth: %v", err)
	}

	messages := []domain.AttachedWorkerAttemptMessageV1{
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelRequested, attempt.CreatedAt.Add(time.Second)),
		testAttemptMessage(t, attempt, domain.AttachedWorkerAttemptMessageCancelAcknowledged, attempt.CreatedAt.Add(2*time.Second)),
	}
	messages[1].ConnectionGeneration++
	service, _ = NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt, messages: messages}, nil)
	if _, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID); !errors.Is(err, ErrBackend) {
		t.Fatalf("cross-generation occurrence escaped: %v", err)
	}
}

func TestOccurrenceEvidenceMustExactlyBindTheAttemptHead(t *testing.T) {
	t.Parallel()
	worker := testWorker("worker-1")
	terminalAttempt := testAttempt(t, worker, domain.AttachedWorkerAttemptTerminalCommitted)
	terminalReceived := testAttemptMessage(t, terminalAttempt, domain.AttachedWorkerAttemptMessageTerminal, terminalAttempt.UpdatedAt.Add(-time.Second))
	terminal := testAttemptMessage(t, terminalAttempt, domain.AttachedWorkerAttemptMessageTerminalCommitted, terminalAttempt.UpdatedAt)
	cancelAttempt := testAttempt(t, worker, domain.AttachedWorkerAttemptCancelAcknowledged)
	requested := testAttemptMessage(t, cancelAttempt, domain.AttachedWorkerAttemptMessageCancelRequested, cancelAttempt.CreatedAt.Add(20*time.Second))
	acknowledged := testAttemptMessage(t, cancelAttempt, domain.AttachedWorkerAttemptMessageCancelAcknowledged, cancelAttempt.CreatedAt.Add(30*time.Second))
	cancelledTerminalAttempt := terminalAttempt
	cancelledTerminalAttempt.CancelRevision = 1
	cancelledTerminalAttempt.CancelDeadline = cancelledTerminalAttempt.CreatedAt.Add(50 * time.Second)
	cancelledTerminalAttempt.TerminalStatus = domain.AttachedWorkerTerminalCancelled
	if err := cancelledTerminalAttempt.Validate(); err != nil {
		t.Fatal(err)
	}
	cancelledRequest := testAttemptMessage(t, cancelledTerminalAttempt, domain.AttachedWorkerAttemptMessageCancelRequested, cancelledTerminalAttempt.CreatedAt.Add(10*time.Second))
	cancelledTerminal := testAttemptMessage(t, cancelledTerminalAttempt, domain.AttachedWorkerAttemptMessageTerminal, cancelledTerminalAttempt.CreatedAt.Add(30*time.Second))
	cancelledCommit := testAttemptMessage(t, cancelledTerminalAttempt, domain.AttachedWorkerAttemptMessageTerminalCommitted, cancelledTerminalAttempt.CreatedAt.Add(40*time.Second))
	lateCancelAck := testAttemptMessage(t, cancelledTerminalAttempt, domain.AttachedWorkerAttemptMessageCancelAcknowledged, cancelledTerminalAttempt.CreatedAt.Add(50*time.Second))

	tests := map[string]struct {
		attempt  domain.AttachedWorkerAttemptV1
		messages []domain.AttachedWorkerAttemptMessageV1
	}{
		"terminal sequence": {
			attempt: terminalAttempt,
			messages: []domain.AttachedWorkerAttemptMessageV1{terminalReceived, mutateOccurrenceFrame(t, terminal, func(frame *attachedworkerprotocol.FrameV1) {
				frame.TerminalAck.TerminalSequence++
			})},
		},
		"terminal evidence": {
			attempt: terminalAttempt,
			messages: []domain.AttachedWorkerAttemptMessageV1{terminalReceived, mutateOccurrenceFrame(t, terminal, func(frame *attachedworkerprotocol.FrameV1) {
				frame.TerminalAck.EvidenceDigest = bytes.Repeat([]byte{8}, 32)
			})},
		},
		"terminal reservation": {
			attempt: terminalAttempt,
			messages: func() []domain.AttachedWorkerAttemptMessageV1 {
				divergent := terminal
				divergent.MaterializationReservationID = "reservation-other"
				return []domain.AttachedWorkerAttemptMessageV1{terminalReceived, divergent}
			}(),
		},
		"cancel revision": {
			attempt: cancelAttempt,
			messages: []domain.AttachedWorkerAttemptMessageV1{requested, mutateOccurrenceFrame(t, acknowledged, func(frame *attachedworkerprotocol.FrameV1) {
				frame.CancelAck.CancelRevision++
			})},
		},
		"cancel chronology": {
			attempt: cancelAttempt,
			messages: []domain.AttachedWorkerAttemptMessageV1{requested, func() domain.AttachedWorkerAttemptMessageV1 {
				before := acknowledged
				before.CreatedAt = requested.CreatedAt.Add(-time.Second)
				return before
			}()},
		},
		"after head update": {
			attempt: terminalAttempt,
			messages: func() []domain.AttachedWorkerAttemptMessageV1 {
				future := terminal
				future.CreatedAt = terminalAttempt.UpdatedAt.Add(time.Microsecond)
				return []domain.AttachedWorkerAttemptMessageV1{terminalReceived, future}
			}(),
		},
		"terminal before evidence": {
			attempt: terminalAttempt,
			messages: func() []domain.AttachedWorkerAttemptMessageV1 {
				early := terminal
				early.CreatedAt = terminalReceived.CreatedAt.Add(-time.Microsecond)
				return []domain.AttachedWorkerAttemptMessageV1{terminalReceived, early}
			}(),
		},
		"terminal before cancel acknowledgement": {
			attempt:  cancelledTerminalAttempt,
			messages: []domain.AttachedWorkerAttemptMessageV1{cancelledRequest, cancelledTerminal, cancelledCommit, lateCancelAck},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &test.attempt, messages: test.messages}, nil)
			if _, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID); !errors.Is(err, ErrBackend) {
				t.Fatalf("divergent occurrence escaped as authoritative time: %v", err)
			}
		})
	}
}

func TestPublicReadDTOsExcludeSensitiveAuthority(t *testing.T) {
	t.Parallel()
	for _, root := range []reflect.Type{
		reflect.TypeOf(AttachedWorkerUXReadModelV1{}), reflect.TypeOf(AttachedWorkerListV1{}), reflect.TypeOf(AttachedWorkerDiagnosticsV1{}),
		reflect.TypeOf(ActionPlanRequestV1{}), reflect.TypeOf(ActionPlanV1{}), reflect.TypeOf(ActionApplyV1{}), reflect.TypeOf(ActionOperationV1{}),
	} {
		walkJSONFields(t, root, map[string]bool{})
	}
	if strings.Contains(string(ActionUnavailableNotFound), "owner") {
		t.Fatal("owner oracle entered public error vocabulary")
	}
}

func TestActionInputEnvelopesCarryNoCallerAuthority(t *testing.T) {
	t.Parallel()
	assertJSONFields(t, reflect.TypeOf(ActionPlanRequestV1{}), []string{"action", "version"})
	assertJSONFields(t, reflect.TypeOf(ActionApplyV1{}), []string{"confirmation", "idempotency_key", "plan_id", "version"})
}

func TestUnknownFactsOmitZeroTimesAndQuotaZeroRemainsObservable(t *testing.T) {
	t.Parallel()
	unknown, err := json.Marshal(AttachedWorkerUXReadModelV1{
		Version: ReadModelVersionV1, EvaluatedAt: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		Worker: WorkerV1{WorkerID: "wrk_test", DisplayName: "Test", Revision: 1,
			EnrollmentGeneration: 1, DesiredState: "active", ObservedState: "offline",
			CreatedAt: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)},
		Identity: IdentityV1{Algorithm: "ed25519", Fingerprint: "sha256:fixture", EnrollmentState: "consumed"},
		Readiness: ReadinessV1{
			DaemonObservation: DaemonObservationV1{State: "unknown", Source: "unavailable", Freshness: FreshnessUnknown},
			LastDaemonFailure: LastFailureV1{State: "unknown"}, CredentialState: "unknown",
			Isolation: IsolationV1{ConfigurationState: "unsupported", AdvertisedEvidence: []string{}, VerificationState: "unsupported"},
		},
		Connectivity:        ConnectivityV1{State: "unknown", Freshness: FreshnessUnknown, LastFailure: LastFailureV1{State: "unknown"}},
		Capability:          CapabilityV1{State: "unknown", Harness: HarnessV1{}, IsolationEvidence: []string{}, Features: []string{}},
		AdmissionPreview:    AdmissionPreviewV1{State: "not_evaluated"},
		ObservationWarnings: []ReasonCodeV1{},
		Resource:            ResourceV1{State: "unknown", CredentialState: "unknown", EntitlementState: "unknown", Quota: QuotaObservationV1{State: "unknown"}},
		Execution:           emptyExecution(), Governance: GovernanceV1{AdmissionControl: "unavailable", RemoteErase: "not_requested", AvailableActions: disabledActions()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(unknown, []byte("0001-01-01")) || bytes.Contains(unknown, []byte(`"remaining"`)) {
		t.Fatalf("unknown DTO fabricated a timestamp or quota value: %s", unknown)
	}
	zero := uint64(0)
	knownZero, err := json.Marshal(QuotaObservationV1{State: "observed", Remaining: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(knownZero, []byte(`"remaining":"0"`)) {
		t.Fatalf("known zero quota disappeared: %s", knownZero)
	}
	maximum, err := json.Marshal(WorkerV1{
		WorkerID: "wrk_max", DisplayName: "Max", Revision: ^uint64(0),
		EnrollmentGeneration: ^uint64(0), ConnectionGeneration: ^uint64(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(maximum, []byte(`"18446744073709551615"`)) != 3 {
		t.Fatalf("uint64 authority lost browser precision: %s", maximum)
	}
}

func TestStablePublicCatalogHasNoDuplicatesOrOwnerOracle(t *testing.T) {
	t.Parallel()
	reasons := []ReasonCodeV1{
		ReasonWorkerNotActive, ReasonWorkerRevoked, ReasonWorkerDraining, ReasonWorkerOffline,
		ReasonConnectionAttaching, ReasonConnectionSuperseded, ReasonPresenceExpired, ReasonAuthenticationExpired,
		ReasonProtocolIncompatible, ReasonCapabilityMissing, ReasonCapabilityStale, ReasonCapabilityMismatch,
		ReasonPolicyMismatch, ReasonIsolationUnsupported, ReasonIsolationUnverified, ReasonCredentialUnavailable,
		ReasonCredentialReauthRequired, ReasonEntitlementUnknown, ReasonEntitlementInactive, ReasonQuotaUnknown,
		ReasonQuotaZero, ReasonQuotaExhausted, ReasonCapacityBusy, ReasonAttemptActive, ReasonAttemptAmbiguous,
		ReasonControlContractUnavailable, ReasonBackendUnavailable,
	}
	actions := []ActionCodeV1{
		ActionCreateEnrollment, ActionConsumeEnrollment, ActionRename, ActionRotateIdentity, ActionPauseAdmission,
		ActionResumeAdmission, ActionDrain, ActionRevoke, ActionRequestCancel, ActionReconnectRemediation,
		ActionReauthRemediation, ActionCheckUpdate, ActionLogout, ActionUninstallPlan,
	}
	actionReasons := []ActionUnavailableCodeV1{
		ActionUnavailableNotFound, ActionUnavailableStaleRevision, ActionUnavailableStaleGeneration,
		ActionUnavailableInvalidState, ActionUnavailableActiveAttempt, ActionUnavailableAmbiguousAttempt,
		ActionUnavailableAwaitingAcknowledgement, ActionUnavailableAlreadyApplied,
		ActionUnavailableUnsupportedPlatform, ActionUnavailableFeatureDisabled,
		ActionUnavailableControlContract, ActionUnavailableConfirmationRequired, ActionUnavailableOperationInProgress,
	}
	assertUniqueCatalog(t, reasons)
	assertUniqueCatalog(t, actions)
	assertUniqueCatalog(t, actionReasons)
	for _, value := range append(append(stringSlice(reasons), stringSlice(actions)...), stringSlice(actionReasons)...) {
		if strings.Contains(value, "owner") || strings.Contains(value, "force") || strings.Contains(value, "token") {
			t.Fatalf("unsafe public catalog value %q", value)
		}
	}
}

func containsReason(values []ReasonCodeV1, target ReasonCodeV1) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func assertUniqueCatalog[T ~string](t *testing.T, values []T) {
	t.Helper()
	seen := map[T]struct{}{}
	for _, value := range values {
		if value == "" {
			t.Fatal("empty catalog value")
		}
		if _, ok := seen[value]; ok {
			t.Fatalf("duplicate catalog value %q", value)
		}
		seen[value] = struct{}{}
	}
}

func stringSlice[T ~string](values []T) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, string(value))
	}
	return result
}

func assertJSONFields(t *testing.T, value reflect.Type, expected []string) {
	t.Helper()
	actual := make([]string, 0, value.NumField())
	for index := 0; index < value.NumField(); index++ {
		actual = append(actual, strings.Split(value.Field(index).Tag.Get("json"), ",")[0])
	}
	sort.Strings(actual)
	sort.Strings(expected)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("%s JSON fields = %v, want %v", value, actual, expected)
	}
}

func walkJSONFields(t *testing.T, value reflect.Type, seen map[string]bool) {
	t.Helper()
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		value = value.Elem()
	}
	if value.PkgPath() == "time" || value.Kind() != reflect.Struct || seen[value.PkgPath()+"."+value.Name()] {
		return
	}
	seen[value.PkgPath()+"."+value.Name()] = true
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")[0]
		for _, forbidden := range []string{"tenant", "owner", "secret", "bearer", "proof", "signature", "public_key", "channel_binding", "protocol_snapshot", "raw_frame", "prompt", "result", "provider_error", "path", "nonce"} {
			if strings.Contains(tag, forbidden) {
				t.Fatalf("%s exposes forbidden JSON field %q", value, tag)
			}
		}
		walkJSONFields(t, field.Type, seen)
	}
}
