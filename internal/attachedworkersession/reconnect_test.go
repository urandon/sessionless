package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestConnectorRestoresIdleCheckpointThroughAuthoritativeReconnect(t *testing.T) {
	fixture := newSessionFixture(t)
	initial := mustReadySession(t, fixture)
	previousConfig, previousSnapshot := sessionProtocolState(t, initial)
	checkpoint, err := initial.lease.LoadReconnectCheckpoint(context.Background())
	if err != nil || checkpoint.Revision != 1 || checkpoint.ConnectionGeneration != 1 ||
		checkpoint.MachineSnapshot.Connection != attachedworkerprotocol.ConnectionReady {
		t.Fatalf("initial checkpoint=%+v err=%v", checkpoint, err)
	}
	beforeSecret, err := initial.lease.LoadSecret(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer clearLocalSecret(&beforeSecret)
	if err := initial.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	bootstrap := &reconnectBootstrap{
		now:              fixture.bootstrap.now,
		publicKey:        ed25519.PrivateKey(fixture.secret.IdentityPrivateKey).Public().(ed25519.PublicKey),
		previousConfig:   previousConfig,
		previousSnapshot: previousSnapshot,
	}
	exchange := &fakeExchange{}
	factory := &fakeFactory{exchange: exchange}
	config := fixture.config
	config.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x61}, 32), bytes.Repeat([]byte{0x62}, 32)...))
	connector, err := New(fixture.store, bootstrap, factory, config)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := connector.Reconnect(context.Background(), ReconnectInputV1(fixture.input))
	if err != nil {
		t.Fatalf("reconnect error=%v challenge=%d activate=%d", err, bootstrap.challengeCalls.Load(), bootstrap.activateCalls.Load())
	}
	registerSessionCleanup(t, resumed)
	snapshot := resumed.Snapshot()
	if snapshot.State != StateReady || snapshot.ConnectionGeneration != 2 || snapshot.ConnectionID != "connection-002" {
		t.Fatalf("resumed snapshot=%+v", snapshot)
	}
	afterSecret, err := resumed.lease.LoadSecret(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer clearLocalSecret(&afterSecret)
	if bytes.Equal(beforeSecret.ConnectionSecret, afterSecret.ConnectionSecret) {
		t.Fatal("reconnect reused the previous connection secret")
	}
	afterCheckpoint, err := resumed.lease.LoadReconnectCheckpoint(context.Background())
	if err != nil || afterCheckpoint.Revision != 1 || afterCheckpoint.ManifestRevision != 3 ||
		afterCheckpoint.ConnectionGeneration != 2 || afterCheckpoint.ConnectionID != "connection-002" ||
		afterCheckpoint.MachineSnapshot.Connection != attachedworkerprotocol.ConnectionReady ||
		afterCheckpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptIdle {
		t.Fatalf("resumed checkpoint=%+v err=%v", afterCheckpoint, err)
	}
}

func TestReconnectCheckpointTamperingFailsBeforeNetwork(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "duplicate version",
			mutate: func(encoded []byte) []byte {
				return bytes.Replace(encoded, []byte(`{"version":1`), []byte(`{"Version":1,"version":1`), 1)
			},
		},
		{
			name: "cross owner",
			mutate: func(encoded []byte) []byte {
				return bytes.Replace(encoded, []byte(`"owner_user_id":"user-001"`), []byte(`"owner_user_id":"user-002"`), 1)
			},
		},
		{
			name: "future version",
			mutate: func(encoded []byte) []byte {
				return bytes.Replace(encoded, []byte(`{"version":1`), []byte(`{"version":2`), 1)
			},
		},
		{
			name: "truncated",
			mutate: func(encoded []byte) []byte {
				return append([]byte(nil), encoded[:len(encoded)/2]...)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSessionFixture(t)
			initial := mustReadySession(t, fixture)
			if err := initial.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filepath.Dir(fixture.manifest.OCI.CLIConfigDir), "state", attachedworkerlocal.ReconnectCheckpointFileName)
			encoded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			mutated := test.mutate(encoded)
			if bytes.Equal(encoded, mutated) {
				t.Fatal("test mutation did not change checkpoint")
			}
			if err := os.WriteFile(path, mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			bootstrap := &reconnectBootstrap{}
			config := fixture.config
			config.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x71}, 32), bytes.Repeat([]byte{0x72}, 32)...))
			connector, err := New(fixture.store, bootstrap, &fakeFactory{exchange: &fakeExchange{}}, config)
			if err != nil {
				t.Fatal(err)
			}
			_, err = connector.Reconnect(context.Background(), ReconnectInputV1(fixture.input))
			if !errors.Is(err, ErrReconciliationRequired) {
				t.Fatalf("reconnect error=%v", err)
			}
			if bootstrap.challengeCalls.Load() != 0 || bootstrap.activateCalls.Load() != 0 {
				t.Fatal("tampered checkpoint reached network")
			}
		})
	}
}

func TestActiveAttemptPersistsReconnectCheckpoint(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	snapshot := session.Snapshot()
	capabilityDigest := mustDecodeHex(t, string(snapshot.CapabilityDigest))
	binding := sessionAttemptBinding(capabilityDigest, "checkpoint-retire")
	fixture.exchange.setHandler(func(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		response := sessionPlatformBatch(snapshot, 3, 4, attachedworkerprotocol.MessageLeaseOffer)
		response.Frames[0].LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: binding, AttemptSequence: 1}
		return response, nil
	})
	response, err := session.ExchangeAction(context.Background(), ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{
		ObservedAtUnixMicro: sessionTestTime.UnixMicro(), Available: true,
	}})
	if err != nil || response == nil || response.Kind != attachedworkerprotocol.MessageLeaseOffer {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	checkpoint, err := session.lease.LoadReconnectCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("load active checkpoint: %v", err)
	}
	if checkpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptOffered ||
		checkpoint.MachineSnapshot.Attempt.Summary.Binding.AttemptID != binding.AttemptID {
		t.Fatalf("active checkpoint attempt=%+v", checkpoint.MachineSnapshot.Attempt.Summary)
	}
	recovery, err := session.ReconnectRecovery()
	if err != nil || recovery.AttemptState != attachedworkerprotocol.AttemptOffered ||
		recovery.TerminalDecision != attachedworkerprotocol.ReconnectTerminalNone || recovery.Terminal != nil {
		t.Fatalf("active recovery=%+v err=%v", recovery, err)
	}
	previousConfig, previousSnapshot := sessionProtocolState(t, session)
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	bootstrap := &reconnectBootstrap{
		now:              fixture.bootstrap.now,
		publicKey:        ed25519.PrivateKey(fixture.secret.IdentityPrivateKey).Public().(ed25519.PublicKey),
		previousConfig:   previousConfig,
		previousSnapshot: previousSnapshot,
	}
	config := fixture.config
	config.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x63}, 32), bytes.Repeat([]byte{0x64}, 32)...))
	connector, err := New(fixture.store, bootstrap, &fakeFactory{exchange: &fakeExchange{}}, config)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := connector.Reconnect(context.Background(), ReconnectInputV1(fixture.input))
	if err != nil {
		t.Fatalf("active reconnect error=%v", err)
	}
	registerSessionCleanup(t, resumed)
	recovery, err = resumed.ReconnectRecovery()
	if err != nil || recovery.AttemptState != attachedworkerprotocol.AttemptOffered ||
		recovery.TerminalDecision != attachedworkerprotocol.ReconnectTerminalNone || recovery.Terminal != nil {
		t.Fatalf("resumed active recovery=%+v err=%v", recovery, err)
	}
	resumedCheckpoint, err := resumed.lease.LoadReconnectCheckpoint(context.Background())
	if err != nil || resumedCheckpoint.ConnectionGeneration != 2 ||
		resumedCheckpoint.MachineSnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptOffered ||
		resumedCheckpoint.MachineSnapshot.Attempt.Summary.Binding.AttemptID != binding.AttemptID {
		t.Fatalf("resumed active checkpoint=%+v err=%v", resumedCheckpoint, err)
	}
}

func sessionProtocolState(t *testing.T, session *Session) (attachedworkerprotocol.MachineConfig, attachedworkerprotocol.MachineSnapshotV1) {
	t.Helper()
	session.mu.Lock()
	defer session.mu.Unlock()
	snapshot, err := session.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	config := attachedworkerprotocol.MachineConfig{
		Auth: attachedworkerprotocol.AuthContextV1{
			TenantID:             session.machineConfig.Auth.TenantID,
			OwnerUserID:          session.machineConfig.Auth.OwnerUserID,
			WorkerID:             session.machineConfig.Auth.WorkerID,
			IdentityPublicKey:    append(ed25519.PublicKey(nil), session.machineConfig.Auth.IdentityPublicKey...),
			EnrollmentGeneration: session.machineConfig.Auth.EnrollmentGeneration,
			ConnectionGeneration: session.machineConfig.Auth.ConnectionGeneration,
			Version:              session.machineConfig.Auth.Version,
			ChannelBinding:       append([]byte(nil), session.machineConfig.Auth.ChannelBinding...),
		},
		WorkerOffer:         cloneOffer(session.machineConfig.WorkerOffer),
		PlatformOffer:       cloneOffer(session.machineConfig.PlatformOffer),
		ImplementedVersions: append([]attachedworkerprotocol.ProtocolVersion(nil), session.machineConfig.ImplementedVersions...),
	}
	return config, snapshot
}

type reconnectBootstrap struct {
	now              time.Time
	publicKey        ed25519.PublicKey
	previousConfig   attachedworkerprotocol.MachineConfig
	previousSnapshot attachedworkerprotocol.MachineSnapshotV1
	challenge        domain.AttachedWorkerAttachChallenge
	challengeCalls   atomic.Int32
	activateCalls    atomic.Int32
}

func (fake *reconnectBootstrap) IssueChallenge(_ context.Context, input attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	fake.challengeCalls.Add(1)
	if input.Purpose != domain.AttachedWorkerAttachReconnect {
		return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorProtocol, false)
	}
	request := attachedworkertransport.IssueChallengeRequest{
		WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ExpectedAudience: input.ExpectedAudience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: input.Purpose, Hello: input.Hello,
	}
	transcript, err := attachedworkertransport.ChallengeRequestProofTranscriptV1(
		input.TenantLocator, input.OwnerLocator, request.WorkerID, input.Hello.EnrollmentGeneration,
		input.Hello.ConnectionGeneration-1, request,
	)
	if err != nil || !ed25519.Verify(fake.publicKey, transcript, input.Proof) {
		return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorUnauthorized, false)
	}
	protocolSnapshot, err := attachedworkerprotocol.EncodeMachineSnapshotV1(fake.previousSnapshot)
	if err != nil {
		return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorProtocol, false)
	}
	platformNonce := bytes.Repeat([]byte{0x82}, 32)
	fake.challenge = domain.AttachedWorkerAttachChallenge{
		TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, ID: "challenge-002",
		WorkerID: request.WorkerID, ConnectionID: "connection-002", Purpose: input.Purpose, Audience: input.ExpectedAudience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1, TargetConnectionGeneration: input.Hello.ConnectionGeneration,
		ExpectedConnectionID: "connection-001", ExpectedConnectionRevision: 2,
		ExpectedCapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", fake.previousSnapshot.CapabilityDigest)),
		ExpectedProtocolSnapshot: protocolSnapshot,
		WorkerProtocolMinimum:    1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
		WorkerNonceDigest:   domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce),
		PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(platformNonce),
		CreatedAt:           fake.now, ExpiresAt: fake.now.Add(time.Minute), RetainUntil: fake.now.Add(time.Hour), Revision: 1,
	}
	return &attachedworkerhttp.ChallengeResponseV1{Challenge: fake.challenge, Frame: attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
		WorkerID: input.Hello.WorkerID, EnrollmentGeneration: input.Hello.EnrollmentGeneration,
		ConnectionGeneration: input.Hello.ConnectionGeneration, Sequence: 1, Ack: 1,
		Kind: attachedworkerprotocol.MessageChallenge, Challenge: &attachedworkerprotocol.ChallengeV1{
			WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: testOffer(), SelectedVersion: 1,
			WorkerNonce: append([]byte(nil), input.Hello.Hello.WorkerNonce...), PlatformNonce: platformNonce,
		},
	}}, nil
}

func (fake *reconnectBootstrap) Activate(_ context.Context, input attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	fake.activateCalls.Add(1)
	channel := attachedworkertransport.ConnectionChannelBinding(
		fake.challenge.ID, fake.challenge.WorkerNonceDigest, fake.challenge.PlatformNonceDigest, input.ConnectionSecret.Digest(),
	)
	channelBytes := mustDecodeHexValue(string(channel))
	nextAuth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(fake.challenge.TenantID), OwnerUserID: string(fake.challenge.OwnerUserID),
		WorkerID: string(fake.challenge.WorkerID), IdentityPublicKey: append(ed25519.PublicKey(nil), fake.publicKey...),
		EnrollmentGeneration: fake.challenge.ExpectedEnrollmentGeneration,
		ConnectionGeneration: fake.challenge.TargetConnectionGeneration,
		Version:              input.Attach.Version, ChannelBinding: channelBytes,
	}
	if attachedworkerprotocol.VerifyReconnectV1(nextAuth, input.Attach) != nil {
		return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorUnauthorized, false)
	}
	accepted, _, err := attachedworkerprotocol.BuildReconnectAcceptedSnapshotV1(
		fake.previousConfig, fake.previousSnapshot, nextAuth, input.Attach,
	)
	if err != nil {
		return nil, attachedworkerhttp.NewExchangeError(attachedworkerhttp.ErrorUnauthorized, false)
	}
	capability := input.Attach.Reconnect.CapabilityDigest
	return &attachedworkerhttp.ActivateResponseV1{
		Connection: attachedworkerhttp.ActivateConnectionV1{
			TenantID: fake.challenge.TenantID, OwnerUserID: fake.challenge.OwnerUserID, WorkerID: fake.challenge.WorkerID,
			ID: fake.challenge.ConnectionID, ActivationChallengeID: fake.challenge.ID,
			EnrollmentGeneration: input.Attach.EnrollmentGeneration, ConnectionGeneration: input.Attach.ConnectionGeneration,
			ProtocolVersion:  uint32(input.Attach.Version),
			CapabilityDigest: domain.AttachedWorkerCapabilityDigest(fmt.Sprintf("%x", capability)),
			SecretDigest:     input.ConnectionSecret.Digest(), ChannelBinding: channel,
			State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2,
			PlatformAck: 2, WorkerAck: 1, ConnectedAt: fake.now.Add(time.Minute),
			AuthExpiresAt: fake.now.Add(time.Hour), Revision: 1,
		},
		Accepted: accepted,
	}, nil
}

func TestReconnectCheckpointJSONContainsNoSecretFields(t *testing.T) {
	fixture := newSessionFixture(t)
	session := mustReadySession(t, fixture)
	registerSessionCleanup(t, session)
	encoded, err := os.ReadFile(filepath.Join(filepath.Dir(fixture.manifest.OCI.CLIConfigDir), "state", attachedworkerlocal.ReconnectCheckpointFileName))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if json.Unmarshal(encoded, &decoded) != nil {
		t.Fatal("checkpoint is not JSON")
	}
	for _, forbidden := range []string{"private", "connection_secret", "bearer", "credential", "request_body", "response_body"} {
		if bytes.Contains(bytes.ToLower(encoded), []byte(forbidden)) {
			t.Fatalf("checkpoint contains forbidden field %q", forbidden)
		}
	}
}
