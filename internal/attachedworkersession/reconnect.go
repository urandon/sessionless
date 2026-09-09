package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const reconnectCheckpointTimeout = 5 * time.Second

type ReconnectInputV1 struct {
	ExpectedWorkerRevision uint64
	CapabilityManifest     attachedworkerprotocol.CapabilityManifestV1
}

func (connector *Connector) Reconnect(ctx context.Context, input ReconnectInputV1) (*Session, error) {
	if connector == nil || ctx == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedInput, err := cloneConnectInput(ConnectInputV1(input))
	if err != nil {
		return nil, err
	}
	connector.mu.Lock()
	switch connector.state {
	case StateIdle:
		connector.state = StateConnecting
	case StateConnecting, StateReady:
		connector.mu.Unlock()
		return nil, ErrAlreadyOwned
	case StateReconciliationRequired:
		connector.mu.Unlock()
		return nil, ErrReconciliationRequired
	case StateFenced:
		connector.mu.Unlock()
		return nil, ErrSessionFenced
	default:
		connector.mu.Unlock()
		return nil, ErrSessionClosed
	}
	connector.mu.Unlock()

	opCtx, cancel := context.WithTimeout(ctx, connector.config.OperationTimeout)
	result := make(chan connectResult, 1)
	go func() { result <- connector.reconnect(opCtx, ownedInput) }()
	select {
	case outcome := <-result:
		operationErr := opCtx.Err()
		cancel()
		if operationErr != nil {
			connector.mark(StateReconciliationRequired)
			if outcome.session != nil {
				_ = outcome.session.abandon()
			}
			return nil, errors.Join(ErrReconciliationRequired, operationErr)
		}
		if outcome.err != nil {
			if outcome.reconciliation {
				connector.mark(StateReconciliationRequired)
			} else {
				connector.mark(StateIdle)
			}
			return nil, outcome.err
		}
		connector.mark(StateReady)
		return outcome.session, nil
	case <-opCtx.Done():
		cancel()
		connector.mark(StateReconciliationRequired)
		go func() {
			outcome := <-result
			if outcome.session != nil {
				_ = outcome.session.abandon()
			}
		}()
		return nil, errors.Join(ErrReconciliationRequired, opCtx.Err())
	}
}

func (connector *Connector) reconnect(ctx context.Context, input ConnectInputV1) (result connectResult) {
	if input.ExpectedWorkerRevision == 0 || input.CapabilityManifest.Validate() != nil {
		result.err = ErrInvalidAuthority
		return result
	}
	lease, err := connector.store.AcquireRuntime(ctx)
	if err != nil {
		result.err = sanitizeLocal(err)
		return result
	}
	owned := true
	defer func() {
		if owned {
			if closeErr := sanitizeClose(lease.Close()); closeErr != nil {
				result.err = errors.Join(result.err, closeErr)
				result.reconciliation = true
			}
		}
	}()

	localSnapshot, err := lease.LoadSnapshot(ctx)
	if err != nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
		result.reconciliation = true
		return result
	}
	secret, err := lease.LoadSecret(ctx)
	if err != nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
		result.reconciliation = true
		return result
	}
	defer clearLocalSecret(&secret)
	checkpoint, err := lease.LoadReconnectCheckpoint(ctx)
	if err != nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
		result.reconciliation = true
		return result
	}
	manifest := localSnapshot.Manifest
	if validateCapabilityAuthority(manifest, input.CapabilityManifest, connector.config.WorkerOffer) != nil ||
		checkpoint.Validate(manifest) != nil || manifest.ConnectionGeneration == 0 ||
		manifest.Revision == math.MaxUint64 || manifest.ConnectionGeneration == math.MaxUint64 {
		result.err = ErrReconciliationRequired
		result.reconciliation = true
		return result
	}
	digest, err := attachedworkerprotocol.ManifestDigestV1(input.CapabilityManifest)
	if err != nil || checkpoint.CapabilityDigest != domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)) {
		result.err = ErrInvalidAuthority
		return result
	}
	privateKey := ed25519.PrivateKey(secret.IdentityPrivateKey)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	previousConfig := attachedworkerprotocol.MachineConfig{
		Auth: attachedworkerprotocol.AuthContextV1{
			TenantID: string(manifest.TenantID), OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID),
			IdentityPublicKey: append(ed25519.PublicKey(nil), publicKey...), EnrollmentGeneration: manifest.EnrollmentGeneration,
			ConnectionGeneration: manifest.ConnectionGeneration, Version: checkpoint.ProtocolVersion,
			ChannelBinding: append([]byte(nil), checkpoint.ChannelBinding...),
		},
		WorkerOffer: cloneOffer(checkpoint.WorkerOffer), PlatformOffer: cloneOffer(checkpoint.PlatformOffer),
		ImplementedVersions: append([]attachedworkerprotocol.ProtocolVersion(nil), connector.config.ImplementedVersions...),
	}
	machine, err := attachedworkerprotocol.RestoreConformanceMachine(previousConfig, checkpoint.MachineSnapshot)
	if err != nil {
		result.err = ErrReconciliationRequired
		result.reconciliation = true
		return result
	}

	rawConnectionSecret := make([]byte, connectionKeyBytes)
	defer clearBytes(rawConnectionSecret)
	workerNonce := make([]byte, connectionKeyBytes)
	defer clearBytes(workerNonce)
	if _, err := io.ReadFull(connector.config.Random, rawConnectionSecret); err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	connectionSecret, err := attachedworkertransport.ParseConnectionSecret(rawConnectionSecret)
	if err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	if _, err := io.ReadFull(connector.config.Random, workerNonce); err != nil || allZero(workerNonce) {
		result.err = ErrConnectionSessionFailed
		return result
	}

	nextManifest := manifest
	nextManifest.Revision++
	nextManifest.ConnectionGeneration++
	nextManifest.UpdatedAt = nextTime(connector.config.Now().UTC(), manifest.UpdatedAt)
	nextSecret := secret
	nextSecret.IdentityPrivateKey = append([]byte(nil), secret.IdentityPrivateKey...)
	nextSecret.ManifestRevision = nextManifest.Revision
	nextSecret.ConnectionGeneration = nextManifest.ConnectionGeneration
	nextSecret.ConnectionSecret = append([]byte(nil), rawConnectionSecret...)
	if err := lease.Update(ctx, manifest.Revision, nextManifest, nextSecret); err != nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
		result.reconciliation = true
		clearLocalSecret(&nextSecret)
		return result
	}
	clearLocalSecret(&nextSecret)
	result.reconciliation = true
	if err := ctx.Err(); err != nil {
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}

	hello := attachedworkerprotocol.FrameV1{
		Version:   attachedworkerprotocol.ProtocolVersionV1,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1),
		WorkerID:  string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Sequence: 1, Ack: 0,
		Kind: attachedworkerprotocol.MessageHello,
		Hello: &attachedworkerprotocol.HelloV1{
			Offer: cloneOffer(connector.config.WorkerOffer), WorkerNonce: append([]byte(nil), workerNonce...),
		},
	}
	proofRequest := attachedworkertransport.IssueChallengeRequest{
		WorkerID: manifest.WorkerID, ExpectedAudience: connector.config.Audience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: domain.AttachedWorkerAttachReconnect, Hello: hello,
	}
	proof, err := attachedworkertransport.SignChallengeRequestV1(
		privateKey, publicKey, manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID,
		manifest.EnrollmentGeneration, manifest.ConnectionGeneration, proofRequest,
	)
	if err != nil {
		result.err = ErrInvalidAuthority
		return result
	}
	challengeRequest := attachedworkerhttp.ChallengeRequestV1{
		TenantLocator: manifest.TenantID, OwnerLocator: manifest.OwnerUserID,
		ExpectedAudience: connector.config.Audience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
		Purpose: domain.AttachedWorkerAttachReconnect, Hello: hello, Proof: proof,
	}
	challengeResponse, err := connector.bootstrap.IssueChallenge(ctx, challengeRequest)
	clearBytes(proof)
	if contextErr := ctx.Err(); contextErr != nil {
		result.err = errors.Join(ErrReconciliationRequired, contextErr)
		return result
	}
	if err != nil || challengeResponse == nil || !validChallenge(challengeRequest, *challengeResponse) {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeExchange(err))
		return result
	}

	selected := challengeResponse.Frame.Version
	channelBinding := attachedworkertransport.ConnectionChannelBinding(
		challengeResponse.Challenge.ID,
		domain.DigestAttachedWorkerChallenge(workerNonce),
		domain.DigestAttachedWorkerChallenge(challengeResponse.Frame.Challenge.PlatformNonce),
		connectionSecret.Digest(),
	)
	channelBytes, err := hex.DecodeString(string(channelBinding))
	if err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	defer clearBytes(channelBytes)
	nextAuth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(manifest.TenantID), OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID),
		IdentityPublicKey: append(ed25519.PublicKey(nil), publicKey...), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Version: selected,
		ChannelBinding: append([]byte(nil), channelBytes...),
	}
	claim, err := machine.BeginReconnect(nextAuth)
	if err != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	acceptance := attachedworkerprotocol.AcceptanceContextV1{ChannelBinding: nextAuth.ChannelBinding, NowUnixMicro: 1}
	if machine.Accept(attachedworkerprotocol.DirectionWorkerToPlatform, hello, acceptance) != nil ||
		machine.Accept(attachedworkerprotocol.DirectionPlatformToWorker, challengeResponse.Frame, acceptance) != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	reconnectPayload, err := attachedworkerprotocol.BuildReconnectV1(claim, attachedworkerprotocol.ReconnectNegotiationV1{
		WorkerOffer: connector.config.WorkerOffer, PlatformOffer: challengeResponse.Frame.Challenge.PlatformOffer,
		SelectedVersion: selected, WorkerNonce: workerNonce, PlatformNonce: challengeResponse.Frame.Challenge.PlatformNonce,
		CapabilityDigest: digest,
	})
	if err != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	reconnectFrame := attachedworkerprotocol.FrameV1{
		Version: selected, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Sequence: 2, Ack: 1,
		Kind: attachedworkerprotocol.MessageReconnect, Reconnect: &reconnectPayload,
	}
	if err := attachedworkerprotocol.SignReconnectV1(privateKey, nextAuth, &reconnectFrame); err != nil ||
		machine.Accept(attachedworkerprotocol.DirectionWorkerToPlatform, reconnectFrame, acceptance) != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	defer clearBytes(reconnectFrame.Reconnect.Signature)
	activation, err := connector.bootstrap.Activate(ctx, attachedworkerhttp.ActivateClientInputV1{
		TenantLocator: manifest.TenantID, OwnerLocator: manifest.OwnerUserID,
		ChallengeID: challengeResponse.Challenge.ID, ExpectedConnectionID: challengeResponse.Challenge.ConnectionID,
		ConnectionSecret: connectionSecret, Attach: reconnectFrame,
	})
	if contextErr := ctx.Err(); contextErr != nil {
		result.err = errors.Join(ErrReconciliationRequired, contextErr)
		return result
	}
	if err != nil || activation == nil ||
		!validReconnectActivation(manifest, challengeResponse.Challenge, connectionSecret, digest, reconnectFrame, *activation) ||
		!activation.Connection.AuthExpiresAt.After(connector.config.Now().UTC()) ||
		machine.Accept(attachedworkerprotocol.DirectionPlatformToWorker, activation.Accepted, acceptance) != nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeExchange(err))
		return result
	}

	machineConfig := attachedworkerprotocol.MachineConfig{
		Auth: nextAuth, WorkerOffer: cloneOffer(connector.config.WorkerOffer),
		PlatformOffer:       cloneOffer(challengeResponse.Frame.Challenge.PlatformOffer),
		ImplementedVersions: append([]attachedworkerprotocol.ProtocolVersion(nil), connector.config.ImplementedVersions...),
	}
	binding := ConnectionBindingV1{
		TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID,
		EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: nextManifest.ConnectionGeneration,
		ProtocolVersion: selected, ConnectionID: activation.Connection.ID,
		CapabilityDigest:      domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)),
		AuthenticationExpires: activation.Connection.AuthExpiresAt,
	}
	bearer, err := attachedworkertransport.NewConnectionBearer(
		manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID, activation.Connection.ID, connectionSecret,
	)
	if err != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	rawBearer := bearer.Bytes()
	exchange, err := connector.factory.Open(binding, rawBearer)
	clearBytes(rawBearer)
	if err != nil || exchange == nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeExchange(err))
		if closer, ok := exchange.(interface{ Close() error }); ok {
			result.err = errors.Join(result.err, sanitizeClose(closer.Close()))
		}
		return result
	}

	session := newOwnedSession(connector, lease, exchange, machineConfig, machine, binding, nextManifest)
	if err := ctx.Err(); err != nil {
		_ = session.abandon()
		owned = false
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}
	manifestFrame := attachedworkerprotocol.FrameV1{
		Version: selected, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Sequence: 3, Ack: 2,
		Kind:     attachedworkerprotocol.MessageManifest,
		Manifest: &attachedworkerprotocol.ManifestV1{Manifest: input.CapabilityManifest, Digest: append([]byte(nil), digest...)},
	}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, nextAuth, &manifestFrame); err != nil {
		_ = session.abandon()
		owned = false
		result.err = ErrReconciliationRequired
		return result
	}
	defer clearBytes(manifestFrame.Manifest.Signature)
	if _, err := session.exchangeWithReplay(ctx, attachedworkerprotocol.BatchV1{
		Version: selected, Frames: []attachedworkerprotocol.FrameV1{manifestFrame},
	}); err != nil {
		_ = session.abandon()
		owned = false
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}
	session.mu.Lock()
	session.state = StateReady
	session.mu.Unlock()
	owned = false
	result.session = session
	result.reconciliation = false
	return result
}

func (session *Session) persistReconnectCheckpoint(ctx context.Context, machine *attachedworkerprotocol.ConformanceMachine) error {
	if session == nil || machine == nil {
		return ErrInvalidAuthority
	}
	snapshot, err := machine.Snapshot()
	if err != nil {
		return ErrReconciliationRequired
	}
	session.mu.Lock()
	lease := session.lease
	binding := session.binding
	config := session.machineConfig
	manifestRevision := session.manifestRevision
	manifestUpdatedAt := session.manifestUpdatedAt
	revision := session.checkpointRevision
	now := session.now
	session.mu.Unlock()
	if lease == nil || now == nil {
		return ErrReconciliationRequired
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconnectCheckpointTimeout)
	defer cancel()
	if snapshot.Connection != attachedworkerprotocol.ConnectionReady && snapshot.Connection != attachedworkerprotocol.ConnectionDraining {
		if revision == 0 {
			return nil
		}
		if err := lease.RetireReconnectCheckpoint(cleanupCtx, revision); err != nil {
			return errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
		}
		session.mu.Lock()
		session.checkpointRevision = 0
		session.mu.Unlock()
		return nil
	}
	checkpointedAt := nextTime(now().UTC(), manifestUpdatedAt)
	checkpoint := attachedworkerlocal.ReconnectCheckpointV1{
		Version: attachedworkerlocal.ReconnectCheckpointVersionV1, Revision: revision + 1,
		ManifestRevision: manifestRevision, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		WorkerID: binding.WorkerID, EnrollmentGeneration: binding.EnrollmentGeneration,
		ConnectionGeneration: binding.ConnectionGeneration, ProtocolVersion: binding.ProtocolVersion,
		ConnectionID: binding.ConnectionID, CapabilityDigest: binding.CapabilityDigest,
		AuthenticationExpires: binding.AuthenticationExpires, ChannelBinding: append([]byte(nil), config.Auth.ChannelBinding...),
		WorkerOffer: cloneOffer(config.WorkerOffer), PlatformOffer: cloneOffer(config.PlatformOffer),
		MachineSnapshot: snapshot, CheckpointedAt: checkpointedAt,
	}
	if err := lease.PersistReconnectCheckpoint(cleanupCtx, revision, checkpoint); err != nil {
		return errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
	}
	session.mu.Lock()
	session.checkpointRevision = checkpoint.Revision
	session.mu.Unlock()
	return nil
}

func newOwnedSession(
	connector *Connector,
	lease localRuntimeLease,
	exchange ExchangePort,
	machineConfig attachedworkerprotocol.MachineConfig,
	machine *attachedworkerprotocol.ConformanceMachine,
	binding ConnectionBindingV1,
	manifest attachedworkerlocal.ManifestV1,
) *Session {
	session := &Session{
		state: StateConnecting, binding: binding, machineConfig: machineConfig, machine: machine,
		exchange: exchange, lease: lease, operationTimeout: connector.config.OperationTimeout,
		operationGate: make(chan struct{}, 1), now: connector.config.Now,
		maxExchangeAttempts: connector.config.MaxExchangeAttempts,
		retryInitialBackoff: connector.config.RetryInitialBackoff, retryMaxBackoff: connector.config.RetryMaxBackoff,
		retryJitter: &exchangeRetryJitter{state: connector.config.retrySeed}, retryWait: connector.config.retryWait,
		manifestRevision: manifest.Revision, manifestUpdatedAt: manifest.UpdatedAt,
	}
	session.operationGate <- struct{}{}
	return session
}

func validReconnectActivation(
	manifest attachedworkerlocal.ManifestV1,
	challenge domain.AttachedWorkerAttachChallenge,
	secret attachedworkertransport.ConnectionSecret,
	capabilityDigest []byte,
	reconnect attachedworkerprotocol.FrameV1,
	response attachedworkerhttp.ActivateResponseV1,
) bool {
	if reconnect.Reconnect == nil || response.Accepted.ReconnectAccepted == nil {
		return false
	}
	connection := response.Connection
	accepted := response.Accepted
	expectedCapability := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest))
	expectedBinding := attachedworkertransport.ConnectionChannelBinding(
		challenge.ID, domain.DigestAttachedWorkerChallenge(reconnect.Reconnect.WorkerNonce),
		domain.DigestAttachedWorkerChallenge(reconnect.Reconnect.PlatformNonce), secret.Digest(),
	)
	return accepted.Validate() == nil && accepted.Kind == attachedworkerprotocol.MessageReconnectAccepted &&
		connection.TenantID == manifest.TenantID && connection.OwnerUserID == manifest.OwnerUserID &&
		connection.WorkerID == manifest.WorkerID && connection.ID == challenge.ConnectionID &&
		connection.ActivationChallengeID == challenge.ID && connection.EnrollmentGeneration == manifest.EnrollmentGeneration &&
		connection.ConnectionGeneration == challenge.TargetConnectionGeneration &&
		connection.ProtocolVersion == uint32(reconnect.Version) && connection.CapabilityDigest == expectedCapability &&
		connection.SecretDigest == secret.Digest() && connection.ChannelBinding == expectedBinding &&
		connection.State == domain.AttachedWorkerConnectionAttaching &&
		connection.PlatformSequence == 2 && connection.WorkerSequence == 2 &&
		connection.PlatformAck == 2 && connection.WorkerAck == 1 &&
		!connection.ConnectedAt.IsZero() && connection.AuthExpiresAt.After(connection.ConnectedAt) && connection.Revision != 0 &&
		accepted.Version == reconnect.Version && accepted.WorkerID == reconnect.WorkerID &&
		accepted.EnrollmentGeneration == reconnect.EnrollmentGeneration &&
		accepted.ConnectionGeneration == reconnect.ConnectionGeneration && accepted.Sequence == 2 && accepted.Ack == 2 &&
		bytes.Equal(accepted.ReconnectAccepted.CapabilityDigest, capabilityDigest)
}
