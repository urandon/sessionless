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

func TestAW04AcceptsOnlyTransactionalAttemptBroker(t *testing.T) {
	_, store, _, _ := newTransportFixture(t)
	_, err := NewService(ServiceConfig{
		IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1", PlatformOffer: testOffer(),
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: 5 * time.Minute,
		ChallengeRetention: time.Hour, PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: MinimumHeartbeatInterval,
	}, store, transportBrokerFunc(func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
		return ports.AttachedWorkerAttemptResult{}, nil
	}))
	if err != nil {
		t.Fatalf("transactional AW04 broker error = %v", err)
	}
}

func TestAW04ActiveHeartbeatRequiresBrokerAndPollsStrictPlatformFrame(t *testing.T) {
	for _, kind := range []attachedworkerprotocol.MessageKind{attachedworkerprotocol.MessageLeaseOffer, attachedworkerprotocol.MessageCancel, attachedworkerprotocol.MessageTerminalAck} {
		t.Run(string(kind), func(t *testing.T) {
			fixture := newReadyTransportFixture(t)
			active := uint32(0)
			var result ports.AttachedWorkerAttemptResult
			if kind == attachedworkerprotocol.MessageLeaseOffer {
				// The scheduler has already persisted and advanced the connection
				// snapshot to Offered, but the worker has not received the frame.
				// Its idle/available heartbeat must still reach broker polling.
				result = fixture.makeOffered(t)
			} else {
				fixture.makeClaimed(t)
				active = 1
				result = fixture.pollResult(t, kind)
			}
			broker := transportAttemptBroker{poll: func(context.Context, ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
				return result, nil
			}}
			service := newTransportServiceWithBroker(t, fixture.store, broker)
			response, err := service.Exchange(context.Background(), fixture.bearer, fixture.heartbeat(active))
			if err != nil || response == nil || len(response.Frames) != 1 || response.Frames[0].Kind != kind {
				t.Fatalf("response=%#v err=%v", response, err)
			}
		})
	}

	t.Run("disabled broker rejects active presence", func(t *testing.T) {
		fixture := newReadyTransportFixture(t)
		fixture.makeClaimed(t)
		if _, err := fixture.service.Exchange(context.Background(), fixture.bearer, fixture.heartbeat(1)); !errors.Is(err, ErrTransportUnauthorized) {
			t.Fatalf("active heartbeat without broker error=%v", err)
		}
		if fixture.store.authorizeCalls != 0 {
			t.Fatal("disabled active heartbeat reached presence mutation")
		}
	})
	for _, status := range []ports.AttachedWorkerExecutionStatus{ports.AttachedWorkerExecutionNotFound, ports.AttachedWorkerExecutionApplied} {
		t.Run("idle_"+string(status), func(t *testing.T) {
			fixture := newReadyTransportFixture(t)
			fixture.makeClaimed(t)
			broker := transportAttemptBroker{poll: func(context.Context, ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
				return ports.AttachedWorkerAttemptResult{Status: status}, nil
			}}
			response, err := newTransportServiceWithBroker(t, fixture.store, broker).Exchange(context.Background(), fixture.bearer, fixture.heartbeat(1))
			if err != nil || response != nil {
				t.Fatalf("idle response=%#v err=%v", response, err)
			}
		})
	}
}

func TestAW04PollRejectsMalformedCrossBindingAndStaleOutbound(t *testing.T) {
	for name, mutate := range map[string]func(*ports.AttachedWorkerAttemptResult){
		"malformed":        func(result *ports.AttachedWorkerAttemptResult) { result.Outbound.Payload = []byte(`{"version":1`) },
		"stale generation": func(result *ports.AttachedWorkerAttemptResult) { result.Outbound.ConnectionGeneration++ },
		"cross binding": func(result *ports.AttachedWorkerAttemptResult) {
			batch, _ := attachedworkerprotocol.DecodeBatchV1(result.Outbound.Payload)
			binding := batch.Frames[0].LeaseOffer.Binding
			binding.AttemptID = "attempt-other"
			batch.Frames[0].LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: binding, AttemptSequence: 1}
			result.Outbound.Payload, _ = attachedworkerprotocol.EncodeBatchV1(batch)
		},
		"multiple frames": func(result *ports.AttachedWorkerAttemptResult) {
			batch, _ := attachedworkerprotocol.DecodeBatchV1(result.Outbound.Payload)
			batch.Frames = append(batch.Frames, batch.Frames[0])
			batch.Frames[1].Sequence++
			batch.Frames[1].MessageID = attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, batch.Frames[1].Sequence)
			result.Outbound.Payload, _ = attachedworkerprotocol.EncodeBatchV1(batch)
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newReadyTransportFixture(t)
			result := fixture.pollResult(t, attachedworkerprotocol.MessageLeaseOffer)
			mutate(&result)
			broker := transportAttemptBroker{poll: func(context.Context, ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
				return result, nil
			}}
			if _, err := newTransportServiceWithBroker(t, fixture.store, broker).Exchange(context.Background(), fixture.bearer, fixture.heartbeat(0)); !errors.Is(err, ErrTransportUnauthorized) {
				t.Fatalf("invalid outbound error=%v", err)
			}
		})
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
	unsupportedImplementationSet := valid
	unsupportedImplementationSet.ImplementedVersions = []attachedworkerprotocol.ProtocolVersion{1, 2}
	if _, err := NewService(unsupportedImplementationSet, store, nil); !errors.Is(err, ErrTransportConfig) {
		t.Fatalf("non-v1 implementation set error = %v", err)
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
	divergentHeartbeat := heartbeat
	divergentHeartbeat.Heartbeat = &attachedworkerprotocol.HeartbeatV1{
		ObservedAtUnixMicro: heartbeat.Heartbeat.ObservedAtUnixMicro + 1, Available: true,
	}
	if _, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{divergentHeartbeat}}); !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("divergent same-sequence heartbeat error = %v", err)
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

type transportBrokerFunc func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error)

func (broker transportBrokerFunc) ExchangeAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
	return broker(ctx, request)
}

func (broker transportBrokerFunc) PollAttachedWorkerAttempt(context.Context, ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
	return ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionNotFound}, nil
}

type transportAttemptBroker struct {
	poll     func(context.Context, ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error)
	exchange func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error)
}

func (broker transportAttemptBroker) PollAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
	if broker.poll == nil {
		return ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionNotFound}, nil
	}
	return broker.poll(ctx, request)
}
func (broker transportAttemptBroker) ExchangeAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
	if broker.exchange == nil {
		return ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionNotFound}, nil
	}
	return broker.exchange(ctx, request)
}

type readyTransportFixture struct {
	service    *Service
	store      *transportMemoryStore
	worker     domain.AttachedWorker
	bearer     ConnectionBearer
	connection domain.AttachedWorkerConnection
	secret     ConnectionSecret
}

func newTransportServiceWithBroker(t *testing.T, store *transportMemoryStore, broker AttemptBroker) *Service {
	t.Helper()
	service, err := NewService(ServiceConfig{IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1", PlatformOffer: testOffer(),
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: 5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour, CheckpointInterval: MinimumHeartbeatInterval,
		Random: bytes.NewReader(bytes.Repeat([]byte{0x35}, 128))}, store, broker)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func newReadyTransportFixture(t *testing.T) readyTransportFixture {
	t.Helper()
	service, store, worker, privateKey := newTransportFixture(t)
	request := signedChallengeRequest(t, worker, privateKey)
	grant, err := service.IssueChallenge(context.Background(), worker.TenantID, worker.OwnerUserID, request)
	if err != nil {
		t.Fatal(err)
	}
	secret, _ := ParseConnectionSecret(bytes.Repeat([]byte{0x62}, connectionSecretBytes))
	manifest := testCapabilityManifest(worker)
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	channel := ConnectionChannelBinding(grant.Challenge.ID, grant.Challenge.WorkerNonceDigest, grant.Challenge.PlatformNonceDigest, secret.Digest())
	channelBytes, _ := decodeChannelBinding(channel)
	auth := attachedworkerprotocol.AuthContextV1{TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
		IdentityPublicKey: worker.IdentityPublicKey, EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: grant.Challenge.TargetConnectionGeneration,
		Version: 1, ChannelBinding: channelBytes}
	attach := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: grant.Challenge.TargetConnectionGeneration,
		Sequence: 2, Ack: 1, Kind: attachedworkerprotocol.MessageAttach,
		Attach: &attachedworkerprotocol.AttachV1{WorkerOffer: request.Hello.Hello.Offer, PlatformOffer: grant.Frame.Challenge.PlatformOffer,
			SelectedVersion: 1, WorkerNonce: request.Hello.Hello.WorkerNonce, PlatformNonce: grant.Frame.Challenge.PlatformNonce, CapabilityDigest: capabilityDigest}}
	if err := attachedworkerprotocol.SignAttachV1(privateKey, auth, &attach); err != nil {
		t.Fatal(err)
	}
	activation, err := service.Activate(context.Background(), worker.TenantID, worker.OwnerUserID, ActivateRequest{ChallengeID: grant.Challenge.ID, ConnectionSecretDigest: secret.Digest(), Attach: attach})
	if err != nil {
		t.Fatal(err)
	}
	manifestFrame := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: grant.Challenge.TargetConnectionGeneration,
		Sequence: 3, Ack: 2, Kind: attachedworkerprotocol.MessageManifest, Manifest: &attachedworkerprotocol.ManifestV1{Manifest: manifest, Digest: capabilityDigest}}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, auth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	bearer, _ := NewConnectionBearer(worker.TenantID, worker.OwnerUserID, worker.ID, activation.Connection.ID, secret)
	if response, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{manifestFrame}}); err != nil || response != nil {
		t.Fatalf("manifest setup response=%#v err=%v", response, err)
	}
	return readyTransportFixture{service: service, store: store, worker: worker, bearer: bearer, connection: store.connection, secret: secret}
}

func (fixture readyTransportFixture) heartbeat(active uint32) attachedworkerprotocol.BatchV1 {
	sequence := fixture.connection.WorkerSequence + 1
	frame := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence),
		WorkerID: string(fixture.worker.ID), EnrollmentGeneration: fixture.connection.EnrollmentGeneration, ConnectionGeneration: fixture.connection.ConnectionGeneration,
		Sequence: sequence, Ack: fixture.connection.PlatformSequence, Kind: attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: fixture.store.now.UnixMicro(), Available: active == 0, ActiveAttempts: active}}
	return attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}}
}

func (fixture *readyTransportFixture) makeClaimed(t *testing.T) {
	t.Helper()
	offerResult := fixture.makeOffered(t)
	config, _, snapshot, err := fixture.service.loadConnectionProtocolState(context.Background(), fixture.bearer, fixture.connection)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := attachedworkerprotocol.DecodeBatchV1(offerResult.Outbound.Payload)
	if err != nil {
		t.Fatal(err)
	}
	offer := batch.Frames[0]
	binding := offer.LeaseOffer.Binding
	claim := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, fixture.connection.WorkerSequence+1),
		WorkerID: string(fixture.worker.ID), EnrollmentGeneration: fixture.connection.EnrollmentGeneration, ConnectionGeneration: fixture.connection.ConnectionGeneration,
		Sequence: fixture.connection.WorkerSequence + 1, Ack: offer.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: binding, AttemptSequence: 1}}
	snapshot, err = attachedworkerprotocol.ApplyMachineFrameV1(config, snapshot, attachedworkerprotocol.DirectionWorkerToPlatform, claim, fixture.store.now.UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	accepted := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, offer.Sequence+1),
		WorkerID: string(fixture.worker.ID), EnrollmentGeneration: fixture.connection.EnrollmentGeneration, ConnectionGeneration: fixture.connection.ConnectionGeneration,
		Sequence: offer.Sequence + 1, Ack: claim.Sequence, Kind: attachedworkerprotocol.MessageLeaseAccepted,
		LeaseAccepted: &attachedworkerprotocol.LeaseAcceptedV1{Binding: binding, AttemptSequence: 2}}
	snapshot, err = attachedworkerprotocol.ApplyMachineFrameV1(config, snapshot, attachedworkerprotocol.DirectionPlatformToWorker, accepted, fixture.store.now.UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store.connection.ProtocolSnapshot = encoded
	fixture.store.connection.PlatformSequence, fixture.store.connection.PlatformAck = snapshot.Platform.Sequence, snapshot.Platform.Ack
	fixture.store.connection.WorkerSequence, fixture.store.connection.WorkerAck = snapshot.Worker.Sequence, snapshot.Worker.Ack
	fixture.connection = fixture.store.connection
}

func (fixture *readyTransportFixture) makeOffered(t *testing.T) ports.AttachedWorkerAttemptResult {
	t.Helper()
	result := fixture.pollResult(t, attachedworkerprotocol.MessageLeaseOffer)
	batch, err := attachedworkerprotocol.DecodeBatchV1(result.Outbound.Payload)
	if err != nil {
		t.Fatal(err)
	}
	frame := batch.Frames[0]
	frame.Sequence, frame.Ack = fixture.connection.PlatformSequence+1, fixture.connection.WorkerSequence
	frame.MessageID = attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, frame.Sequence)
	batch.Frames[0] = frame
	result.Outbound.Payload, err = attachedworkerprotocol.EncodeBatchV1(batch)
	if err != nil {
		t.Fatal(err)
	}
	result.Outbound.EnvelopeSequence = frame.Sequence
	config, _, snapshot, err := fixture.service.loadConnectionProtocolState(context.Background(), fixture.bearer, fixture.connection)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = attachedworkerprotocol.ApplyMachineFrameV1(config, snapshot, attachedworkerprotocol.DirectionPlatformToWorker, frame, fixture.store.now.UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store.connection.ProtocolSnapshot = encoded
	fixture.store.connection.PlatformSequence, fixture.store.connection.PlatformAck = snapshot.Platform.Sequence, snapshot.Platform.Ack
	fixture.store.connection.WorkerSequence, fixture.store.connection.WorkerAck = snapshot.Worker.Sequence, snapshot.Worker.Ack
	fixture.connection = fixture.store.connection
	return result
}

func (fixture readyTransportFixture) pollResult(t *testing.T, kind attachedworkerprotocol.MessageKind) ports.AttachedWorkerAttemptResult {
	t.Helper()
	now := fixture.store.now
	contextDigest := domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context")))
	policyDigest := domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy")))
	fence, err := domain.NewAttachedWorkerFenceTokenV1(fixture.worker.TenantID, fixture.worker.OwnerUserID, fixture.worker.ID, "run-1", "attempt-1", "lease-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	binding := attachedworkerprotocol.AttemptBindingV1{RunID: "run-1", AttemptID: "attempt-1", LeaseID: "lease-1", LeaseGeneration: 7,
		FenceToken: string(fence), ExpiresAtUnixMicro: now.Add(time.Minute).UnixMicro(), ContextDigest: mustDecodeHex(string(contextDigest)),
		CapabilityDigest: mustDecodeHex(string(fixture.connection.CapabilityDigest)), PolicyDigest: mustDecodeHex(string(policyDigest))}
	attempt := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: fixture.worker.TenantID, OwnerUserID: fixture.worker.OwnerUserID, WorkerID: fixture.worker.ID,
		ConnectionID: fixture.connection.ID, RunID: "run-1", AttemptID: "attempt-1", ReservationID: "reservation-1", LeaseID: "lease-1",
		LeaseGeneration: 7, FenceToken: fence, EnrollmentGeneration: fixture.connection.EnrollmentGeneration, ConnectionGeneration: fixture.connection.ConnectionGeneration,
		ContextDigest: contextDigest, CapabilityDigest: fixture.connection.CapabilityDigest, PolicyDigest: policyDigest, State: domain.AttachedWorkerAttemptOffered,
		PlatformAttemptSequence: 1, LeaseExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now, Revision: 1}
	platformSequence := fixture.connection.PlatformSequence + 1
	workerSequence := fixture.connection.WorkerSequence + 1
	frame := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, platformSequence),
		WorkerID: string(fixture.worker.ID), EnrollmentGeneration: fixture.connection.EnrollmentGeneration, ConnectionGeneration: fixture.connection.ConnectionGeneration,
		Sequence: platformSequence, Ack: workerSequence, Kind: kind}
	domainKind := domain.AttachedWorkerAttemptMessageLeaseOffered
	switch kind {
	case attachedworkerprotocol.MessageLeaseOffer:
		frame.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: binding, AttemptSequence: 1}
	case attachedworkerprotocol.MessageCancel:
		frame.Cancel = &attachedworkerprotocol.CancelV1{Binding: binding, AttemptSequence: 2, CancelRevision: 1, Code: attachedworkerprotocol.CancelRequested}
		domainKind = domain.AttachedWorkerAttemptMessageCancelRequested
		attempt.State = domain.AttachedWorkerAttemptCancelRequested
		attempt.PlatformAttemptSequence, attempt.WorkerAttemptSequence, attempt.CancelRevision, attempt.CancelDeadline = 2, 1, 1, now.Add(30*time.Second)
	case attachedworkerprotocol.MessageTerminalAck:
		evidence := bytes.Repeat([]byte{0x71}, 32)
		frame.TerminalAck = &attachedworkerprotocol.TerminalAckV1{Binding: binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: evidence}
		domainKind = domain.AttachedWorkerAttemptMessageTerminalCommitted
		attempt.State = domain.AttachedWorkerAttemptTerminalCommitted
		attempt.PlatformAttemptSequence, attempt.WorkerAttemptSequence, attempt.TerminalSequence = 2, 2, 1
		attempt.TerminalStatus = domain.AttachedWorkerTerminalSucceeded
		attempt.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(domain.DigestAttachedWorkerCapability(evidence))
	default:
		t.Fatalf("unsupported poll kind %s", kind)
	}
	payload, err := attachedworkerprotocol.EncodeBatchV1(attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}})
	if err != nil {
		t.Fatal(err)
	}
	outbound := &domain.AttachedWorkerAttemptMessageV1{Version: 1, TenantID: fixture.worker.TenantID, OwnerUserID: fixture.worker.OwnerUserID,
		WorkerID: fixture.worker.ID, AttemptID: "attempt-1", Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: attempt.PlatformAttemptSequence,
		ConnectionGeneration: fixture.connection.ConnectionGeneration, EnvelopeSequence: frame.Sequence, Kind: domainKind,
		Fingerprint: domain.AttachedWorkerAttemptMessageFingerprint(domain.DigestAttachedWorkerCapability([]byte("outbound"))), Payload: payload, CreatedAt: now}
	if domainKind == domain.AttachedWorkerAttemptMessageTerminalCommitted {
		outbound.MaterializationReservationID = attempt.ReservationID
		outbound.ExecutionConnectionID = attempt.ConnectionID
	}
	return ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: attempt, Outbound: outbound}
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
		if reflect.DeepEqual(request, store.activationRequest) {
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
		ProtocolSnapshot: append([]byte(nil), request.ProtocolSnapshot...),
		State:            domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1,
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
	store.connection.ProtocolSnapshot = append([]byte(nil), request.ProtocolSnapshot...)
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
		if reflect.DeepEqual(request, store.authorizeRequest) {
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
	store.connection.ProtocolSnapshot = append([]byte(nil), request.ProtocolSnapshot...)
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
