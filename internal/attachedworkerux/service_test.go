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
	loadOverride *domain.AttachedWorker
	listOverride []domain.AttachedWorker
	err          error
	reads        int
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

func TestGetSeparatesAuthorityEvidenceAndFreshness(t *testing.T) {
	t.Parallel()
	evaluatedAt := time.Date(2026, 8, 25, 12, 0, 0, 123456789, time.UTC)
	worker := testWorker("worker-1")
	connection, manifest := testCapability(t, worker, evaluatedAt)
	attempt := testAttempt(t, worker, domain.AttachedWorkerAttemptCancelAcknowledged)
	if err := worker.Validate(); err != nil {
		t.Fatalf("worker fixture: %v", err)
	}
	if err := connection.Validate(); err != nil {
		t.Fatalf("connection fixture: %v", err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest fixture: %v", err)
	}
	store := &readStore{workers: []domain.AttachedWorker{worker}, connection: &connection, manifest: &manifest, attempt: &attempt}
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
	for _, action := range model.Governance.AvailableActions {
		if action.Enabled || action.ReasonCode != ActionUnavailableFeatureDisabled {
			t.Fatalf("unaccepted control enabled: %+v", action)
		}
	}
	if store.reads != 4 {
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
	if _, err := service.List(context.Background(), "tenant-1", "owner-1", "", MaxListLimitV1+1); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("unbounded limit accepted: %v", err)
	}
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
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt}, nil)
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if model.Execution.WorkerTerminal.State != "received" || model.Execution.CanonicalTerminal.State != "committed" || model.Execution.ProcessObservation.State != "unknown" {
		t.Fatalf("terminal/process authorities collapsed: %+v", model.Execution)
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
	service, _ := NewService(&readStore{workers: []domain.AttachedWorker{worker}, attempt: &attempt}, nil)
	model, err := service.Get(context.Background(), worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil {
		t.Fatal(err)
	}
	if model.Execution.CancelRequest.State != "requested" || model.Execution.CancelAcknowledgement.State != "unknown" || model.Execution.CanonicalTerminal.State != "committed" {
		t.Fatalf("retained cancel revision fabricated acknowledgement: %+v", model.Execution)
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
	assertUniqueCatalog(t, reasons)
	assertUniqueCatalog(t, actions)
	for _, value := range append(stringSlice(reasons), stringSlice(actions)...) {
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
