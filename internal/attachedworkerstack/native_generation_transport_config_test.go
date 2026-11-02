package attachedworkerstack_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const joinedGenerationScenarioTimeout = 2 * time.Minute
const joinedGenerationReservationHold = 3 * time.Minute

// Fresh-admission fixture DATA only. Keep the original sampled DB instant,
// scope and job/runtime limits; this cannot renew an existing reservation.
func joinedGenerationAdmissionRequest(tenant domain.TenantID, outbox domain.DispatchOutbox, ordinal uint64, at time.Time) ports.DispatchAdmissionRequest {
	return ports.DispatchAdmissionRequest{TenantID: tenant, OutboxID: outbox.ID, RunID: outbox.RunID, AttemptID: outbox.AttemptID, ReservationID: domain.QuotaReservationID(fmt.Sprintf("joined-reservation-%d", ordinal)), Now: at, HoldUntil: at.Add(joinedGenerationReservationHold), Limits: domain.ProductLimits{MaxTenantQueueDepth: 16, MaxActiveRuns: 2, MaxRuntime: 30 * time.Second, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 16, MaxArtifacts: 16, MaxToolEvents: 1, MaxToolEventBytes: 1024}, Workload: domain.WorkloadShape{Runtime: 30 * time.Second, Turns: 1, InputBytes: 4096, ContextBytes: 4096, Artifacts: 5}}
}

func TestNativeGenerationJoinedReservationTiming(t *testing.T) {
	at := time.Date(2026, 10, 29, 10, 11, 12, 345000, time.UTC)
	outbox := domain.DispatchOutbox{ID: "dispatch-original", RunID: "run-original", AttemptID: "attempt-original"}
	got := joinedGenerationAdmissionRequest("tenant-original", outbox, 7, at)
	if joinedGenerationScenarioTimeout != 2*time.Minute || !got.HoldUntil.After(at.Add(joinedGenerationScenarioTimeout)) {
		t.Fatalf("non-expiry fixture hold=%s must outlive original scenario=%s", got.HoldUntil.Sub(at), joinedGenerationScenarioTimeout)
	}
	want := ports.DispatchAdmissionRequest{TenantID: "tenant-original", OutboxID: outbox.ID, RunID: outbox.RunID, AttemptID: outbox.AttemptID, ReservationID: "joined-reservation-7", Now: at, HoldUntil: at.Add(3 * time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 16, MaxActiveRuns: 2, MaxRuntime: 30 * time.Second, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 16, MaxArtifacts: 16, MaxToolEvents: 1, MaxToolEventBytes: 1024}, Workload: domain.WorkloadShape{Runtime: 30 * time.Second, Turns: 1, InputBytes: 4096, ContextBytes: 4096, Artifacts: 5}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fresh admission DATA changed beyond exact hold interval: got=%+v want=%+v", got, want)
	}
	if at.Add(time.Minute).After(at.Add(joinedGenerationScenarioTimeout)) {
		t.Fatal("causal old one-minute hold unexpectedly outlives the scenario")
	}
}

// Shared fixture DATA: the checkpoint is the platform's fifteen-minute minimum,
// so presence must outlive it. This does not change any invocation/operation,
// publication, lease, test or CI deadline and creates no runtime authority.
func joinedGenerationTransportConfig(ids ports.IDGenerator, audience string, offer attachedworkerprotocol.VersionOfferV1) attachedworkertransport.ServiceConfig {
	return attachedworkertransport.ServiceConfig{IDs: ids, Audience: audience, PlatformOffer: offer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: time.Minute,
		ChallengeRetention: time.Hour, PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval}
}

func TestNativeGenerationJoinedTransportConfig(t *testing.T) {
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	config := joinedGenerationTransportConfig(testkit.NewSequenceIDGenerator("config-only"), "sessionless:attached-worker:v1", offer)
	// Constructor validation must perform no Store I/O. This unconfigured
	// concrete Store has no database or usable authority; the returned service
	// is never used to authorize, enroll, exchange or finalize work.
	store := new(ydbstore.Store)
	if service, err := attachedworkertransport.NewReceiptFinalizingService(config, store, store); err != nil || service == nil {
		t.Fatal("joined fixture must pass the real transport constructor offline", err)
	}
	before := config
	before.PresenceTTL = 10 * time.Minute
	if service, err := attachedworkertransport.NewReceiptFinalizingService(before, store, store); service != nil || !errors.Is(err, attachedworkertransport.ErrTransportConfig) {
		t.Fatalf("original presence below checkpoint must be rejected: %v", err)
	}
	if config.CheckpointInterval != attachedworkertransport.MinimumHeartbeatInterval || config.PresenceTTL <= config.CheckpointInterval || config.PresenceTTL > ports.AttachedWorkerMaxPresenceTTL || config.AuthTTL <= config.PresenceTTL {
		t.Fatal("fixture weakened original transport timing relations")
	}
}
