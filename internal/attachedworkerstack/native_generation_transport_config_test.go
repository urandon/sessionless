package attachedworkerstack_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

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
