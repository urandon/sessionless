package attachedworkertransport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestConnectionSecretsAndBearersAreOpaqueAndRedacted(t *testing.T) {
	if _, err := ParseConnectionSecret(make([]byte, connectionSecretBytes)); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("zero secret error = %v", err)
	}
	secret, err := ParseConnectionSecret(bytes.Repeat([]byte{0x41}, connectionSecretBytes))
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := NewConnectionBearer("tenant-a", "owner-a", "wrk_test", "wcn_test", secret)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(secret) != "[REDACTED]" || fmt.Sprintf("%#v", bearer) != "[REDACTED]" {
		t.Fatal("secret formatting leaked")
	}
	encoded, err := json.Marshal(bearer)
	if err != nil || string(encoded) != `"[REDACTED]"` {
		t.Fatalf("bearer JSON = %s, %v", encoded, err)
	}
	parsed, err := ParseConnectionBearer(bearer.Bytes())
	if err != nil || !bytes.Equal(parsed.secret.Bytes(), secret.Bytes()) {
		t.Fatalf("bearer round trip failed: %v", err)
	}
	tampered := bearer.Bytes()
	tampered[len(tampered)-1] = '!'
	if _, err := ParseConnectionBearer(tampered); err == nil {
		t.Fatal("tampered bearer parsed")
	}
}

func TestAW03RejectsBrokerUntilOutboundStateIsTransactional(t *testing.T) {
	_, store, _, _ := newTransportFixture(t)
	_, err := NewService(ServiceConfig{
		IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1", PlatformOffer: testOffer(),
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: 5 * time.Minute,
		ChallengeRetention: time.Hour, PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: MinimumHeartbeatInterval,
	}, store, transportBrokerFunc(func(context.Context, domain.AttachedWorkerConnection, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return nil, nil
	}))
	if !errors.Is(err, ErrTransportConfig) {
		t.Fatalf("non-nil AW03 broker error = %v", err)
	}
}

func TestServiceConfigMirrorsStoreDurationBounds(t *testing.T) {
	_, store, _, _ := newTransportFixture(t)
	valid := ServiceConfig{
		IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1", PlatformOffer: testOffer(),
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1},
		ChallengeLifetime:   ports.AttachedWorkerMaxChallengeLifetime,
		ChallengeRetention:  ports.AttachedWorkerMaxChallengeRetention,
		PresenceTTL:         ports.AttachedWorkerMaxPresenceTTL, AuthTTL: ports.AttachedWorkerMaxAuthTTL,
		CheckpointInterval: ports.AttachedWorkerMaxCheckpointInterval,
	}
	if _, err := NewService(valid, store, nil); err != nil {
		t.Fatalf("max-boundary config rejected: %v", err)
	}
	belowMinimum := valid
	belowMinimum.CheckpointInterval = MinimumHeartbeatInterval - time.Nanosecond
	if _, err := NewService(belowMinimum, store, nil); !errors.Is(err, ErrTransportConfig) {
		t.Fatalf("subminimum checkpoint error = %v", err)
	}
	for name, mutate := range map[string]func(*ServiceConfig){
		"challenge":  func(value *ServiceConfig) { value.ChallengeLifetime++ },
		"retention":  func(value *ServiceConfig) { value.ChallengeRetention++ },
		"presence":   func(value *ServiceConfig) { value.PresenceTTL++ },
		"auth":       func(value *ServiceConfig) { value.AuthTTL++ },
		"checkpoint": func(value *ServiceConfig) { value.CheckpointInterval++ },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := NewService(candidate, store, nil); !errors.Is(err, ErrTransportConfig) {
				t.Fatalf("oversized config error = %v", err)
			}
		})
	}
}

func TestInitialAttachLostResponsesReconcileAcrossTwoPhasePresence(t *testing.T) {
	service, store, worker, privateKey := newTransportFixture(t)
	request := signedChallengeRequest(t, worker, privateKey)
	grant, err := service.IssueChallenge(context.Background(), worker.TenantID, worker.OwnerUserID, request)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Frame.MessageID != attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1) ||
		grant.Frame.Sequence != 1 || grant.Frame.Ack != 1 {
		t.Fatalf("non-canonical challenge frame: %#v", grant.Frame)
	}

	secret, _ := ParseConnectionSecret(bytes.Repeat([]byte{0x52}, connectionSecretBytes))
	manifest := testCapabilityManifest(worker)
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	channel := ConnectionChannelBinding(grant.Challenge.ID, grant.Challenge.WorkerNonceDigest, grant.Challenge.PlatformNonceDigest, secret.Digest())
	channelBytes, _ := decodeChannelBinding(channel)
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
		IdentityPublicKey: worker.IdentityPublicKey, EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: grant.Challenge.TargetConnectionGeneration, Version: attachedworkerprotocol.ProtocolVersionV1,
		ChannelBinding: channelBytes,
	}
	attach := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: grant.Challenge.TargetConnectionGeneration, Sequence: 2, Ack: 1,
		Kind: attachedworkerprotocol.MessageAttach,
		Attach: &attachedworkerprotocol.AttachV1{
			WorkerOffer: request.Hello.Hello.Offer, PlatformOffer: grant.Frame.Challenge.PlatformOffer,
			SelectedVersion: 1, WorkerNonce: request.Hello.Hello.WorkerNonce,
			PlatformNonce: grant.Frame.Challenge.PlatformNonce, CapabilityDigest: capabilityDigest,
		},
	}
	if err := attachedworkerprotocol.SignAttachV1(privateKey, auth, &attach); err != nil {
		t.Fatal(err)
	}
	activation, err := service.Activate(context.Background(), worker.TenantID, worker.OwnerUserID, ActivateRequest{
		ChallengeID: grant.Challenge.ID, ConnectionSecretDigest: secret.Digest(), Attach: attach,
	})
	if err != nil {
		t.Fatal(err)
	}
	if activation.Accepted.Kind != attachedworkerprotocol.MessageAttachAccepted || activation.Accepted.Sequence != 2 || activation.Accepted.Ack != 2 ||
		activation.Accepted.MessageID != attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2) ||
		activation.Connection.State != domain.AttachedWorkerConnectionAttaching || !activation.Connection.PresenceExpiresAt.IsZero() {
		t.Fatalf("activation grant violated two-phase contract: %#v", activation)
	}
	activationReplay, err := service.Activate(context.Background(), worker.TenantID, worker.OwnerUserID, ActivateRequest{
		ChallengeID: grant.Challenge.ID, ConnectionSecretDigest: secret.Digest(), Attach: attach,
	})
	if err != nil || activationReplay.Connection.ID != activation.Connection.ID || !reflect.DeepEqual(activationReplay.Accepted, activation.Accepted) {
		t.Fatalf("lost activation response was not reconciled: %#v, %v", activationReplay, err)
	}
	divergentSecret, _ := ParseConnectionSecret(bytes.Repeat([]byte{0x53}, connectionSecretBytes))
	divergentAttach := attach
	divergentAttach.Attach = &attachedworkerprotocol.AttachV1{
		WorkerOffer: cloneOffer(attach.Attach.WorkerOffer), PlatformOffer: cloneOffer(attach.Attach.PlatformOffer),
		SelectedVersion: attach.Attach.SelectedVersion, WorkerNonce: append([]byte(nil), attach.Attach.WorkerNonce...),
		PlatformNonce: append([]byte(nil), attach.Attach.PlatformNonce...), CapabilityDigest: append([]byte(nil), attach.Attach.CapabilityDigest...),
	}
	divergentChannel := ConnectionChannelBinding(grant.Challenge.ID, grant.Challenge.WorkerNonceDigest, grant.Challenge.PlatformNonceDigest, divergentSecret.Digest())
	divergentChannelBytes, _ := decodeChannelBinding(divergentChannel)
	divergentAuth := auth
	divergentAuth.ChannelBinding = divergentChannelBytes
	if err := attachedworkerprotocol.SignAttachV1(privateKey, divergentAuth, &divergentAttach); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(context.Background(), worker.TenantID, worker.OwnerUserID, ActivateRequest{
		ChallengeID: grant.Challenge.ID, ConnectionSecretDigest: divergentSecret.Digest(), Attach: divergentAttach,
	}); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("divergent activation replay error = %v", err)
	}
	if store.activationCalls != 3 {
		t.Fatalf("cryptographically valid divergent activation was not store-rejected: calls=%d", store.activationCalls)
	}

	manifestFrame := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: grant.Challenge.TargetConnectionGeneration, Sequence: 3, Ack: 2,
		Kind:     attachedworkerprotocol.MessageManifest,
		Manifest: &attachedworkerprotocol.ManifestV1{Manifest: manifest, Digest: capabilityDigest},
	}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, auth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	bearer, _ := NewConnectionBearer(worker.TenantID, worker.OwnerUserID, worker.ID, activation.Connection.ID, secret)
	if response, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{manifestFrame}}); err != nil || response != nil {
		t.Fatalf("manifest exchange = %#v, %v", response, err)
	}
	if response, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{manifestFrame}}); err != nil || response != nil {
		t.Fatalf("lost manifest response was not reconciled: %#v, %v", response, err)
	}
	divergentManifest := manifestFrame
	divergentManifest.Manifest = &attachedworkerprotocol.ManifestV1{
		Manifest: divergentManifest.Manifest.Manifest,
		Digest:   append([]byte(nil), divergentManifest.Manifest.Digest...), Signature: append([]byte(nil), divergentManifest.Manifest.Signature...),
	}
	divergentManifest.Manifest.Signature[0] ^= 0xff
	if _, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{divergentManifest}}); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("divergent manifest replay error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.connection.State != domain.AttachedWorkerConnectionOnline || store.connection.WorkerSequence != 3 || store.acceptCalls != 2 || store.authorizeCalls != 0 {
		t.Fatalf("manifest did not atomically establish presence: %#v", store.connection)
	}
	store.mu.Unlock()
	heartbeat := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 4),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: grant.Challenge.TargetConnectionGeneration, Sequence: 4, Ack: 2,
		Kind:      attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: store.now.UnixMicro(), Available: true},
	}
	if response, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{heartbeat}}); err != nil || response != nil {
		t.Fatalf("heartbeat exchange = %#v, %v", response, err)
	}
	if response, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{heartbeat}}); err != nil || response != nil {
		t.Fatalf("lost heartbeat response was not reconciled: %#v, %v", response, err)
	}
	manifestSequenceHeartbeat := heartbeat
	manifestSequenceHeartbeat.Sequence = 3
	manifestSequenceHeartbeat.MessageID = attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3)
	if _, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{manifestSequenceHeartbeat}}); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("heartbeat replay over durable manifest sequence error = %v", err)
	}
	divergentGenerationHeartbeat := heartbeat
	divergentGenerationHeartbeat.ConnectionGeneration++
	if _, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{divergentGenerationHeartbeat}}); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("cross-generation heartbeat replay error = %v", err)
	}
	unsupported := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 5),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: grant.Challenge.TargetConnectionGeneration, Sequence: 5, Ack: 2,
		Kind: attachedworkerprotocol.MessageError, Error: &attachedworkerprotocol.ErrorV1{Code: attachedworkerprotocol.ErrorProtocolViolation},
	}
	if _, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{unsupported}}); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("unsupported pre-AW04 frame error = %v", err)
	}
	store.mu.Lock()
	if store.authorizeCalls != 2 {
		t.Fatal("unsupported pre-AW04 frame reached store authorization")
	}
}

func TestChallengeScopeAndProofFailClosedBeforeMutation(t *testing.T) {
	service, store, worker, privateKey := newTransportFixture(t)
	request := signedChallengeRequest(t, worker, privateKey)
	request.Proof[0] ^= 0xff
	if _, err := service.IssueChallenge(context.Background(), worker.TenantID, worker.OwnerUserID, request); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("bad proof error = %v", err)
	}
	if store.createCalls != 0 {
		t.Fatal("bad proof reached challenge mutation")
	}
	request = signedChallengeRequest(t, worker, privateKey)
	if _, err := service.IssueChallenge(context.Background(), worker.TenantID, "owner-b", request); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("cross-owner error = %v", err)
	}
	if store.createCalls != 0 {
		t.Fatal("cross-owner request reached challenge mutation")
	}
	request = signedChallengeRequest(t, worker, privateKey)
	request.ExpectedAudience = "sessionless:other-environment:v1"
	proof, err := SignChallengeRequest(privateKey, worker.TenantID, worker.OwnerUserID, worker, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Proof = proof
	if _, err := service.IssueChallenge(context.Background(), worker.TenantID, worker.OwnerUserID, request); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("cross-audience error = %v", err)
	}
	if store.createCalls != 0 {
		t.Fatal("cross-audience request reached challenge mutation")
	}
}

func TestReconnectIsRejectedBeforeChallengeUntilAuthoritativeSnapshotExists(t *testing.T) {
	service, store, worker, privateKey := newTransportFixture(t)
	store.worker.ConnectionGeneration = 1
	worker.ConnectionGeneration = 1
	request := signedChallengeRequest(t, worker, privateKey)
	request.Purpose = domain.AttachedWorkerAttachReconnect
	proof, err := SignChallengeRequest(privateKey, worker.TenantID, worker.OwnerUserID, worker, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Proof = proof
	if _, err := service.IssueChallenge(context.Background(), worker.TenantID, worker.OwnerUserID, request); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("reconnect error = %v", err)
	}
	if store.createCalls != 0 {
		t.Fatal("unsupported reconnect left a dangling challenge")
	}
}

func newTransportFixture(t *testing.T) (*Service, *transportMemoryStore, domain.AttachedWorker, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	worker := domain.AttachedWorker{
		TenantID: "tenant-a", OwnerUserID: "owner-a", ID: "wrk_test", DisplayName: "test worker",
		IdentityPublicKey: publicKey, EnrollmentGeneration: 1, ConnectionGeneration: 0,
		DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOffline,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	store := &transportMemoryStore{worker: worker, now: now}
	service, err := NewService(ServiceConfig{
		IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1",
		PlatformOffer: testOffer(), ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1},
		ChallengeLifetime: 5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour, CheckpointInterval: MinimumHeartbeatInterval,
		Random: bytes.NewReader(bytes.Repeat([]byte{0x35}, 128)),
	}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, worker, privateKey
}

func signedChallengeRequest(t *testing.T, worker domain.AttachedWorker, privateKey ed25519.PrivateKey) IssueChallengeRequest {
	t.Helper()
	request := IssueChallengeRequest{
		WorkerID: worker.ID, ExpectedAudience: "sessionless:attached-worker:v1",
		ExpectedWorkerRevision: worker.Revision, Purpose: domain.AttachedWorkerAttachInitial,
		Hello: attachedworkerprotocol.FrameV1{
			Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1),
			WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
			ConnectionGeneration: worker.ConnectionGeneration + 1, Sequence: 1, Kind: attachedworkerprotocol.MessageHello,
			Hello: &attachedworkerprotocol.HelloV1{Offer: testOffer(), WorkerNonce: bytes.Repeat([]byte{0x23}, 32)},
		},
	}
	proof, err := SignChallengeRequest(privateKey, worker.TenantID, worker.OwnerUserID, worker, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Proof = proof
	return request
}

func testOffer() attachedworkerprotocol.VersionOfferV1 {
	return attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
}

func testCapabilityManifest(worker domain.AttachedWorker) attachedworkerprotocol.CapabilityManifestV1 {
	return attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, Revision: 1, ProtocolOffer: testOffer(),
		OperatingSystem: "darwin", Architecture: "arm64", BuildID: "build-1", HarnessName: "codex",
		HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: bytes.Repeat([]byte{0x73}, 32),
		IsolationEvidence:       []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationNetworkBoundary, attachedworkerprotocol.IsolationProcessBoundary},
		Features:                []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect},
		MaxConcurrentAttempts:   1,
	}
}

type transportIDs struct{}

type transportBrokerFunc func(context.Context, domain.AttachedWorkerConnection, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)

func (broker transportBrokerFunc) Exchange(ctx context.Context, connection domain.AttachedWorkerConnection, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	return broker(ctx, connection, batch)
}

func (transportIDs) NewID(_ context.Context, kind ports.IDKind) (string, error) {
	switch kind {
	case ports.IDAttachedWorkerChallenge:
		return "wch_test", nil
	case ports.IDAttachedWorkerConnection:
		return "wcn_test", nil
	default:
		return "", errors.New("unsupported ID")
	}
}

type transportMemoryStore struct {
	mu                sync.Mutex
	now               time.Time
	worker            domain.AttachedWorker
	challenge         domain.AttachedWorkerAttachChallenge
	connection        domain.AttachedWorkerConnection
	activationRequest ports.AttachedWorkerConnectionActivation
	manifestRequest   ports.AttachedWorkerManifestAcceptance
	authorizeRequest  ports.AttachedWorkerExchangeAuthorization
	createCalls       int
	activationCalls   int
	acceptCalls       int
	authorizeCalls    int
}

func (store *transportMemoryStore) CreateAttachedWorkerAttachChallenge(_ context.Context, request ports.AttachedWorkerChallengeCreate) (domain.AttachedWorkerAttachChallenge, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.createCalls++
	store.challenge = domain.AttachedWorkerAttachChallenge{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: request.ChallengeID, WorkerID: request.WorkerID,
		ConnectionID: request.ConnectionID, Purpose: request.Purpose, Audience: request.Audience,
		ExpectedWorkerRevision: request.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: request.ExpectedEnrollmentGeneration,
		ExpectedConnectionGeneration: request.ExpectedConnectionGeneration, TargetConnectionGeneration: request.ExpectedConnectionGeneration + 1,
		WorkerProtocolMinimum: request.WorkerProtocolMinimum, WorkerProtocolMaximum: request.WorkerProtocolMaximum,
		WorkerProtocolVersions: request.WorkerProtocolVersions, PlatformProtocolMinimum: request.PlatformProtocolMinimum,
		PlatformProtocolMaximum: request.PlatformProtocolMaximum, PlatformProtocolVersions: request.PlatformProtocolVersions,
		SelectedProtocolVersion: request.SelectedProtocolVersion, WorkerNonceDigest: request.WorkerNonceDigest,
		PlatformNonceDigest: request.PlatformNonceDigest, CreatedAt: store.now, ExpiresAt: store.now.Add(request.Lifetime),
		RetainUntil: store.now.Add(request.Retention), Revision: 1,
	}
	return store.challenge, nil
}

func (store *transportMemoryStore) LoadAttachedWorkerAttachChallenge(_ context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID, challenge domain.AttachedWorkerChallengeID) (domain.AttachedWorkerAttachChallenge, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	found := store.challenge.TenantID == tenant && store.challenge.OwnerUserID == owner && store.challenge.WorkerID == worker && store.challenge.ID == challenge
	return store.challenge, found, nil
}

func (store *transportMemoryStore) ActivateAttachedWorkerConnection(_ context.Context, request ports.AttachedWorkerConnectionActivation) (ports.AttachedWorkerConnectionResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.activationCalls++
	if !store.challenge.ConsumedAt.IsZero() {
		if request == store.activationRequest {
			return ports.AttachedWorkerConnectionResult{Status: ports.AttachedWorkerConnectionActivated, Connection: store.connection}, nil
		}
		return ports.AttachedWorkerConnectionResult{Status: ports.AttachedWorkerConnectionConsumed}, nil
	}
	store.activationRequest = request
	store.worker.ConnectionGeneration++
	store.worker.Revision++
	store.worker.UpdatedAt = store.now
	store.challenge.ConsumedAt = store.now
	store.challenge.Revision++
	store.connection = domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ID: store.challenge.ConnectionID, ActivationChallengeID: request.ChallengeID,
		EnrollmentGeneration: request.ExpectedEnrollmentGeneration, ConnectionGeneration: request.ExpectedConnectionGeneration + 1,
		ProtocolVersion: store.challenge.SelectedProtocolVersion, CapabilityDigest: request.ExpectedCapabilityDigest,
		SecretDigest: request.ConnectionSecretDigest, ChannelBinding: request.ChannelBinding,
		State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1,
		ConnectedAt: store.now, AuthExpiresAt: store.now.Add(request.AuthTTL), Revision: 1,
	}
	return ports.AttachedWorkerConnectionResult{Status: ports.AttachedWorkerConnectionActivated, Connection: store.connection}, nil
}

func (store *transportMemoryStore) AcceptAttachedWorkerManifest(_ context.Context, request ports.AttachedWorkerManifestAcceptance) (ports.AttachedWorkerAuthorizationResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.acceptCalls++
	if store.connection.Revision == request.ExpectedConnectionRevision+1 && store.worker.Revision == request.ExpectedWorkerRevision+1 {
		if reflect.DeepEqual(request, store.manifestRequest) {
			return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: store.connection, Checkpointed: true}, nil
		}
		return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionConflict}, nil
	}
	if request.ExpectedConnectionRevision != store.connection.Revision || request.PresentedSecretDigest != store.connection.SecretDigest {
		return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionDenied}, nil
	}
	canonicalDigest := domain.DigestAttachedWorkerCapability(request.Capability.CanonicalManifest)
	if canonicalDigest != request.Capability.Digest {
		return ports.AttachedWorkerAuthorizationResult{}, errors.New("canonical manifest digest mismatch")
	}
	store.manifestRequest = request
	store.worker.ObservedState = domain.AttachedWorkerObservedOnline
	store.worker.Revision++
	store.worker.UpdatedAt = store.now
	store.connection.State = domain.AttachedWorkerConnectionOnline
	store.connection.ManifestRevision = request.Capability.ManifestRevision
	store.connection.ManifestIdentityKey = request.Capability.IdentityKeyDigest
	store.connection.ManifestSignature = append([]byte(nil), request.Capability.Signature...)
	store.connection.ManifestObservedAt = store.now
	store.connection.PlatformSequence, store.connection.WorkerSequence = request.PlatformSequence, request.WorkerSequence
	store.connection.PlatformAck, store.connection.WorkerAck = request.PlatformAck, request.WorkerAck
	store.connection.LastCheckpointAt = store.now
	store.connection.PresenceExpiresAt = store.now.Add(request.PresenceTTL)
	store.connection.Revision++
	return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: store.connection, Checkpointed: true}, nil
}

func (store *transportMemoryStore) LoadAttachedWorkerConnection(_ context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	found := store.connection.TenantID == tenant && store.connection.OwnerUserID == owner && store.connection.WorkerID == worker
	return store.connection, found, nil
}

func (store *transportMemoryStore) AuthorizeAttachedWorkerExchange(_ context.Context, request ports.AttachedWorkerExchangeAuthorization) (ports.AttachedWorkerAuthorizationResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.authorizeCalls++
	if store.connection.Revision == request.ExpectedConnectionRevision+1 {
		if request == store.authorizeRequest {
			return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: store.connection, Checkpointed: true}, nil
		}
		return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionConflict}, nil
	}
	if request.ExpectedConnectionRevision != store.connection.Revision || request.PresentedSecretDigest != store.connection.SecretDigest {
		return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionDenied}, nil
	}
	store.authorizeRequest = request
	store.connection.PlatformSequence, store.connection.WorkerSequence = request.PlatformSequence, request.WorkerSequence
	store.connection.PlatformAck, store.connection.WorkerAck = request.PlatformAck, request.WorkerAck
	store.connection.LastCheckpointAt = store.now
	store.connection.PresenceExpiresAt = store.now.Add(request.PresenceTTL)
	store.connection.Revision++
	return ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: store.connection, Checkpointed: true}, nil
}

func (*transportMemoryStore) ListExpiredAttachedWorkerPresence(context.Context, uint32, time.Time, ports.AttachedWorkerPresenceCursor, uint64) ([]domain.AttachedWorkerPresenceExpiry, error) {
	return nil, nil
}
func (*transportMemoryStore) ExpireAttachedWorkerPresence(context.Context, domain.AttachedWorkerPresenceExpiry) (bool, error) {
	return false, nil
}
func (*transportMemoryStore) CreateAttachedWorkerEnrollment(context.Context, domain.AttachedWorkerEnrollment, domain.AttachedWorkerAuditEvent) error {
	return nil
}
func (*transportMemoryStore) LoadAttachedWorkerEnrollment(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerEnrollmentID) (domain.AttachedWorkerEnrollment, bool, error) {
	return domain.AttachedWorkerEnrollment{}, false, nil
}
func (*transportMemoryStore) ClaimAttachedWorkerEnrollment(context.Context, ports.AttachedWorkerClaimMutation) (ports.AttachedWorkerClaimResult, error) {
	return ports.AttachedWorkerClaimResult{}, nil
}
func (store *transportMemoryStore) LoadAttachedWorker(_ context.Context, tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.worker, store.worker.TenantID == tenant && store.worker.OwnerUserID == owner && store.worker.ID == worker, nil
}
func (*transportMemoryStore) ListAttachedWorkers(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID, uint64) ([]domain.AttachedWorker, error) {
	return nil, nil
}
func (*transportMemoryStore) CompareAndSwapAttachedWorker(context.Context, ports.AttachedWorkerCASMutation) (bool, error) {
	return false, nil
}
func (*transportMemoryStore) RevokeAttachedWorker(context.Context, ports.AttachedWorkerRevokeMutation) (bool, error) {
	return false, nil
}
func (*transportMemoryStore) ListAttachedWorkerAuditEvents(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID, uint64, uint64) ([]domain.AttachedWorkerAuditEvent, error) {
	return nil, nil
}

var _ ports.AttachedWorkerTransportStore = (*transportMemoryStore)(nil)
