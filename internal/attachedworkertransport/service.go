package attachedworkertransport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	connectionSecretBytes       = 32
	transportNonceBytes         = 32
	connectionBearerPrefix      = "slw1."
	challengeRequestProofDomain = "sessionless.attached-worker.challenge-request.v1"
	connectionChannelBindDomain = "sessionless.attached-worker.connection-binding.v1"
	maxConnectionBearerBytes    = 1024
)

var (
	ErrTransportUnauthorized = errors.New("attached worker transport is unauthorized")
	ErrTransportConflict     = errors.New("attached worker transport conflicts with authoritative state")
	ErrTransportBackend      = errors.New("attached worker transport backend failed")
	ErrTransportConfig       = errors.New("attached worker transport configuration is invalid")
)

type ServiceConfig struct {
	IDs                 ports.IDGenerator
	Random              io.Reader
	Audience            string
	PlatformOffer       attachedworkerprotocol.VersionOfferV1
	ImplementedVersions []attachedworkerprotocol.ProtocolVersion
	ChallengeLifetime   time.Duration
	ChallengeRetention  time.Duration
	PresenceTTL         time.Duration
	AuthTTL             time.Duration
	CheckpointInterval  time.Duration
}

type FrameBroker interface {
	Exchange(context.Context, domain.AttachedWorkerConnection, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)
}

type Service struct {
	ids                ports.IDGenerator
	random             io.Reader
	audience           string
	platformOffer      attachedworkerprotocol.VersionOfferV1
	implemented        []attachedworkerprotocol.ProtocolVersion
	challengeLifetime  time.Duration
	challengeRetention time.Duration
	presenceTTL        time.Duration
	authTTL            time.Duration
	checkpointInterval time.Duration
	store              ports.AttachedWorkerTransportStore
}

func NewService(config ServiceConfig, store ports.AttachedWorkerTransportStore, broker FrameBroker) (*Service, error) {
	if config.IDs == nil || store == nil || broker != nil || !validAudience(config.Audience) ||
		config.ChallengeLifetime <= 0 || config.ChallengeRetention <= config.ChallengeLifetime ||
		config.PresenceTTL <= 0 || config.AuthTTL <= 0 || config.CheckpointInterval < MinimumHeartbeatInterval ||
		config.PresenceTTL <= config.CheckpointInterval || config.AuthTTL <= config.PresenceTTL ||
		config.ChallengeLifetime > ports.AttachedWorkerMaxChallengeLifetime ||
		config.ChallengeRetention > ports.AttachedWorkerMaxChallengeRetention ||
		config.PresenceTTL > ports.AttachedWorkerMaxPresenceTTL || config.AuthTTL > ports.AttachedWorkerMaxAuthTTL ||
		config.CheckpointInterval > ports.AttachedWorkerMaxCheckpointInterval {
		return nil, ErrTransportConfig
	}
	if _, err := attachedworkerprotocol.NegotiateOffers(config.PlatformOffer, config.PlatformOffer, config.ImplementedVersions); err != nil {
		return nil, ErrTransportConfig
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	return &Service{
		ids: config.IDs, random: config.Random, audience: config.Audience,
		platformOffer: cloneOffer(config.PlatformOffer), implemented: append([]attachedworkerprotocol.ProtocolVersion(nil), config.ImplementedVersions...),
		challengeLifetime: config.ChallengeLifetime, challengeRetention: config.ChallengeRetention,
		presenceTTL: config.PresenceTTL, authTTL: config.AuthTTL, checkpointInterval: config.CheckpointInterval,
		store: store,
	}, nil
}

type IssueChallengeRequest struct {
	WorkerID               domain.AttachedWorkerID
	ExpectedAudience       string
	ExpectedWorkerRevision uint64
	Purpose                domain.AttachedWorkerAttachPurpose
	Hello                  attachedworkerprotocol.FrameV1
	Proof                  []byte
}

type ChallengeGrant struct {
	Challenge domain.AttachedWorkerAttachChallenge
	Frame     attachedworkerprotocol.FrameV1
}

func (service *Service) IssueChallenge(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request IssueChallengeRequest,
) (ChallengeGrant, error) {
	if service == nil || service.store == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil ||
		request.WorkerID.Validate() != nil || request.ExpectedAudience != service.audience ||
		request.ExpectedWorkerRevision == 0 || !request.Purpose.Valid() ||
		validateSingleFrame(request.Hello) != nil || request.Hello.Kind != attachedworkerprotocol.MessageHello ||
		request.Hello.Hello == nil || len(request.Proof) != ed25519.SignatureSize {
		return ChallengeGrant{}, ErrTransportUnauthorized
	}
	worker, found, err := service.store.LoadAttachedWorker(ctx, tenantID, ownerUserID, request.WorkerID)
	if err != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	if !found || worker.Validate() != nil || worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID ||
		worker.ID != request.WorkerID || worker.Revision != request.ExpectedWorkerRevision ||
		worker.DesiredState == domain.AttachedWorkerDesiredRevoked || worker.ConnectionGeneration == math.MaxUint64 ||
		request.Hello.MessageID != attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1) ||
		request.Hello.Sequence != 1 || request.Hello.Ack != 0 ||
		request.Hello.WorkerID != string(worker.ID) || request.Hello.EnrollmentGeneration != worker.EnrollmentGeneration ||
		request.Hello.ConnectionGeneration != worker.ConnectionGeneration+1 {
		return ChallengeGrant{}, ErrTransportUnauthorized
	}
	if (request.Purpose == domain.AttachedWorkerAttachInitial) != (worker.ConnectionGeneration == 0) {
		return ChallengeGrant{}, ErrTransportUnauthorized
	}
	// AW-03 has no durable attempt snapshot yet. Reject reconnect before
	// creating a single-use challenge; AW-04 will enable it with authoritative
	// reconciliation rather than trusting the worker's claim.
	if request.Purpose == domain.AttachedWorkerAttachReconnect {
		return ChallengeGrant{}, ErrTransportUnauthorized
	}
	transcript, err := ChallengeRequestProofTranscript(tenantID, ownerUserID, worker, request)
	if err != nil || !ed25519.Verify(ed25519.PublicKey(worker.IdentityPublicKey), transcript, request.Proof) {
		return ChallengeGrant{}, ErrTransportUnauthorized
	}
	selected, err := attachedworkerprotocol.NegotiateOffers(service.platformOffer, request.Hello.Hello.Offer, service.implemented)
	if err != nil {
		return ChallengeGrant{}, ErrTransportUnauthorized
	}
	challengeValue, err := service.ids.NewID(ctx, ports.IDAttachedWorkerChallenge)
	if err != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	connectionValue, err := service.ids.NewID(ctx, ports.IDAttachedWorkerConnection)
	if err != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	challengeID := domain.AttachedWorkerChallengeID(challengeValue)
	connectionID := domain.AttachedWorkerConnectionID(connectionValue)
	if challengeID.Validate() != nil || connectionID.Validate() != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	platformNonce := make([]byte, transportNonceBytes)
	if _, err := io.ReadFull(service.random, platformNonce); err != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	create := ports.AttachedWorkerChallengeCreate{
		TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: worker.ID,
		ChallengeID: challengeID, ConnectionID: connectionID, Purpose: request.Purpose, Audience: request.ExpectedAudience,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration,
		WorkerProtocolMinimum:        uint32(request.Hello.Hello.Offer.Window.Minimum),
		WorkerProtocolMaximum:        uint32(request.Hello.Hello.Offer.Window.Maximum),
		WorkerProtocolVersions:       protocolVersions(request.Hello.Hello.Offer.Supported),
		PlatformProtocolMinimum:      uint32(service.platformOffer.Window.Minimum),
		PlatformProtocolMaximum:      uint32(service.platformOffer.Window.Maximum),
		PlatformProtocolVersions:     protocolVersions(service.platformOffer.Supported),
		SelectedProtocolVersion:      uint32(selected),
		WorkerNonceDigest:            domain.DigestAttachedWorkerChallenge(request.Hello.Hello.WorkerNonce),
		PlatformNonceDigest:          domain.DigestAttachedWorkerChallenge(platformNonce),
		Lifetime:                     service.challengeLifetime, Retention: service.challengeRetention,
	}
	challenge, err := service.store.CreateAttachedWorkerAttachChallenge(ctx, create)
	if err != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	if challenge.Validate() != nil || challenge.TenantID != tenantID || challenge.OwnerUserID != ownerUserID ||
		challenge.WorkerID != worker.ID || challenge.ID != challengeID || challenge.ConnectionID != connectionID ||
		!challengeMatchesCreate(challenge, create) {
		return ChallengeGrant{}, ErrTransportBackend
	}
	frame := attachedworkerprotocol.FrameV1{
		Version: selected, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1), WorkerID: string(worker.ID),
		EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: challenge.TargetConnectionGeneration,
		Sequence: 1, Ack: 1, Kind: attachedworkerprotocol.MessageChallenge,
		Challenge: &attachedworkerprotocol.ChallengeV1{
			WorkerOffer: cloneOffer(request.Hello.Hello.Offer), PlatformOffer: cloneOffer(service.platformOffer), SelectedVersion: selected,
			WorkerNonce: cloneBytes(request.Hello.Hello.WorkerNonce), PlatformNonce: cloneBytes(platformNonce),
		},
	}
	if validateSingleFrame(frame) != nil {
		return ChallengeGrant{}, ErrTransportBackend
	}
	return ChallengeGrant{Challenge: challenge, Frame: frame}, nil
}

func ChallengeRequestProofTranscript(
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	worker domain.AttachedWorker,
	request IssueChallengeRequest,
) ([]byte, error) {
	if tenantID.Validate() != nil || ownerUserID.Validate() != nil || worker.Validate() != nil ||
		worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID || worker.ID != request.WorkerID ||
		request.ExpectedWorkerRevision == 0 || !validAudience(request.ExpectedAudience) || !request.Purpose.Valid() || validateSingleFrame(request.Hello) != nil ||
		request.Hello.Kind != attachedworkerprotocol.MessageHello || request.Hello.Hello == nil {
		return nil, ErrTransportUnauthorized
	}
	result := appendTransportField(nil, []byte(challengeRequestProofDomain))
	result = appendTransportField(result, []byte(tenantID))
	result = appendTransportField(result, []byte(ownerUserID))
	result = appendTransportField(result, []byte(worker.ID))
	result = appendTransportField(result, []byte(request.ExpectedAudience))
	result = appendTransportUint64(result, request.ExpectedWorkerRevision)
	result = appendTransportUint64(result, worker.EnrollmentGeneration)
	result = appendTransportUint64(result, worker.ConnectionGeneration)
	result = appendTransportUint64(result, uint64(request.Hello.ConnectionGeneration))
	result = appendTransportField(result, []byte(request.Purpose))
	result = appendTransportField(result, []byte(request.Hello.MessageID))
	result = appendTransportUint64(result, request.Hello.Sequence)
	result = appendTransportOffer(result, request.Hello.Hello.Offer)
	result = appendTransportField(result, request.Hello.Hello.WorkerNonce)
	return result, nil
}

func SignChallengeRequest(privateKey ed25519.PrivateKey, tenantID domain.TenantID, ownerUserID domain.UserID, worker domain.AttachedWorker, request IssueChallengeRequest) ([]byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize || !bytes.Equal(privateKey.Public().(ed25519.PublicKey), worker.IdentityPublicKey) {
		return nil, ErrTransportUnauthorized
	}
	transcript, err := ChallengeRequestProofTranscript(tenantID, ownerUserID, worker, request)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(privateKey, transcript), nil
}

type ConnectionSecret struct{ value [connectionSecretBytes]byte }

func ParseConnectionSecret(value []byte) (ConnectionSecret, error) {
	if len(value) != connectionSecretBytes {
		return ConnectionSecret{}, ErrTransportUnauthorized
	}
	var secret ConnectionSecret
	copy(secret.value[:], value)
	var nonzero byte
	for _, item := range secret.value {
		nonzero |= item
	}
	if nonzero == 0 {
		return ConnectionSecret{}, ErrTransportUnauthorized
	}
	return secret, nil
}

func (secret ConnectionSecret) Bytes() []byte { return cloneBytes(secret.value[:]) }
func (secret ConnectionSecret) Digest() domain.AttachedWorkerConnectionSecretDigest {
	return domain.DigestAttachedWorkerConnectionSecret(secret.value[:])
}
func (ConnectionSecret) String() string               { return "[REDACTED]" }
func (ConnectionSecret) GoString() string             { return "[REDACTED]" }
func (ConnectionSecret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

type ActivateRequest struct {
	ChallengeID            domain.AttachedWorkerChallengeID
	ConnectionSecretDigest domain.AttachedWorkerConnectionSecretDigest
	Attach                 attachedworkerprotocol.FrameV1
}

type ActivationGrant struct {
	Connection domain.AttachedWorkerConnection
	Accepted   attachedworkerprotocol.FrameV1
}

func (service *Service) Activate(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request ActivateRequest,
) (ActivationGrant, error) {
	if service == nil || service.store == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil ||
		request.ChallengeID.Validate() != nil || validateSingleFrame(request.Attach) != nil ||
		validateSecretDigest(request.ConnectionSecretDigest) != nil {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	workerID := domain.AttachedWorkerID(request.Attach.WorkerID)
	if workerID.Validate() != nil {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	challenge, found, err := service.store.LoadAttachedWorkerAttachChallenge(ctx, tenantID, ownerUserID, workerID, request.ChallengeID)
	if err != nil {
		return ActivationGrant{}, ErrTransportBackend
	}
	if !found || challenge.Validate() != nil || challenge.TenantID != tenantID || challenge.OwnerUserID != ownerUserID ||
		challenge.WorkerID != workerID || challenge.Audience != service.audience {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	worker, found, err := service.store.LoadAttachedWorker(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return ActivationGrant{}, ErrTransportBackend
	}
	if !found || worker.Validate() != nil || worker.DesiredState == domain.AttachedWorkerDesiredRevoked ||
		worker.EnrollmentGeneration != challenge.ExpectedEnrollmentGeneration ||
		!activationWorkerStateCanReconcile(worker, challenge) {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	if !sameActivationNegotiation(challenge, request.Attach) ||
		subtle.ConstantTimeCompare([]byte(challenge.WorkerNonceDigest), []byte(domain.DigestAttachedWorkerChallenge(activationWorkerNonce(request.Attach)))) != 1 ||
		subtle.ConstantTimeCompare([]byte(challenge.PlatformNonceDigest), []byte(domain.DigestAttachedWorkerChallenge(activationPlatformNonce(request.Attach)))) != 1 {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	channelBinding := ConnectionChannelBinding(challenge.ID, challenge.WorkerNonceDigest, challenge.PlatformNonceDigest, request.ConnectionSecretDigest)
	channelBindingBytes, err := decodeChannelBinding(channelBinding)
	if err != nil {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(tenantID), OwnerUserID: string(ownerUserID), WorkerID: string(worker.ID),
		IdentityPublicKey: ed25519.PublicKey(cloneBytes(worker.IdentityPublicKey)), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration,
		Version:              attachedworkerprotocol.ProtocolVersion(challenge.SelectedProtocolVersion), ChannelBinding: channelBindingBytes,
	}
	var accepted attachedworkerprotocol.FrameV1
	switch challenge.Purpose {
	case domain.AttachedWorkerAttachInitial:
		if request.Attach.Kind != attachedworkerprotocol.MessageAttach || attachedworkerprotocol.VerifyAttachV1(auth, request.Attach) != nil {
			return ActivationGrant{}, ErrTransportUnauthorized
		}
		accepted = attachedworkerprotocol.FrameV1{
			Version: auth.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2),
			WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration,
			ConnectionGeneration: challenge.TargetConnectionGeneration, Sequence: 2, Ack: 2,
			Kind: attachedworkerprotocol.MessageAttachAccepted,
			AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{
				WorkerOffer: cloneOffer(request.Attach.Attach.WorkerOffer), PlatformOffer: cloneOffer(request.Attach.Attach.PlatformOffer),
				SelectedVersion: request.Attach.Attach.SelectedVersion, WorkerNonce: cloneBytes(request.Attach.Attach.WorkerNonce),
				PlatformNonce: cloneBytes(request.Attach.Attach.PlatformNonce), CapabilityDigest: cloneBytes(request.Attach.Attach.CapabilityDigest),
			},
		}
	case domain.AttachedWorkerAttachReconnect:
		// Active-attempt reconnect needs the authoritative AW-04 attempt
		// snapshot. Until that store exists, reconnect is deliberately denied
		// instead of trusting the worker's claim or fabricating a snapshot.
		return ActivationGrant{}, ErrTransportUnauthorized
	default:
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	if validateInitialHandshake(service, worker, challenge, request.Attach, accepted, channelBindingBytes) != nil {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	capabilityDigest := domain.AttachedWorkerCapabilityDigest(hexDigest(activationCapabilityDigest(request.Attach)))
	expectedChallengeRevision := challenge.Revision
	if !challenge.ConsumedAt.IsZero() {
		if expectedChallengeRevision == 0 {
			return ActivationGrant{}, ErrTransportUnauthorized
		}
		expectedChallengeRevision--
	}
	result, err := service.store.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		ExpectedChallengeRevision: expectedChallengeRevision, ExpectedWorkerRevision: challenge.ExpectedWorkerRevision,
		ExpectedEnrollmentGeneration: challenge.ExpectedEnrollmentGeneration, ExpectedConnectionGeneration: challenge.ExpectedConnectionGeneration,
		PresentedWorkerNonceDigest:   domain.DigestAttachedWorkerChallenge(activationWorkerNonce(request.Attach)),
		PresentedPlatformNonceDigest: domain.DigestAttachedWorkerChallenge(activationPlatformNonce(request.Attach)),
		ConnectionSecretDigest:       request.ConnectionSecretDigest, ChannelBinding: channelBinding,
		ExpectedCapabilityDigest: capabilityDigest, AuthTTL: service.authTTL,
	})
	if err != nil {
		return ActivationGrant{}, ErrTransportBackend
	}
	if result.Status != ports.AttachedWorkerConnectionActivated || result.Connection.Validate() != nil ||
		result.Connection.TenantID != tenantID || result.Connection.OwnerUserID != ownerUserID || result.Connection.WorkerID != worker.ID ||
		result.Connection.ID != challenge.ConnectionID || result.Connection.ActivationChallengeID != challenge.ID ||
		result.Connection.EnrollmentGeneration != worker.EnrollmentGeneration || result.Connection.ConnectionGeneration != challenge.TargetConnectionGeneration ||
		result.Connection.ProtocolVersion != challenge.SelectedProtocolVersion || result.Connection.ChannelBinding != channelBinding ||
		result.Connection.SecretDigest != request.ConnectionSecretDigest || result.Connection.CapabilityDigest != capabilityDigest ||
		result.Connection.State != domain.AttachedWorkerConnectionAttaching || result.Connection.PlatformSequence != 2 ||
		result.Connection.WorkerSequence != 2 || result.Connection.PlatformAck != 2 || result.Connection.WorkerAck != 1 {
		return ActivationGrant{}, ErrTransportUnauthorized
	}
	return ActivationGrant{Connection: result.Connection, Accepted: accepted}, nil
}

func ConnectionChannelBinding(challengeID domain.AttachedWorkerChallengeID, workerNonceDigest, platformNonceDigest domain.AttachedWorkerChallengeDigest, secretDigest domain.AttachedWorkerConnectionSecretDigest) domain.AttachedWorkerChannelBinding {
	result := appendTransportField(nil, []byte(connectionChannelBindDomain))
	result = appendTransportField(result, []byte(challengeID))
	result = appendTransportField(result, []byte(workerNonceDigest))
	result = appendTransportField(result, []byte(platformNonceDigest))
	result = appendTransportField(result, []byte(secretDigest))
	digest := sha256.Sum256(result)
	return domain.NewAttachedWorkerChannelBinding(digest[:])
}

func activationWorkerStateCanReconcile(worker domain.AttachedWorker, challenge domain.AttachedWorkerAttachChallenge) bool {
	if worker.Revision == challenge.ExpectedWorkerRevision && worker.ConnectionGeneration == challenge.ExpectedConnectionGeneration {
		return true
	}
	return challenge.ExpectedWorkerRevision != math.MaxUint64 &&
		worker.Revision == challenge.ExpectedWorkerRevision+1 &&
		worker.ConnectionGeneration == challenge.TargetConnectionGeneration
}

func validateInitialHandshake(service *Service, worker domain.AttachedWorker, challenge domain.AttachedWorkerAttachChallenge, attach, accepted attachedworkerprotocol.FrameV1, channelBinding []byte) error {
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
		IdentityPublicKey: cloneBytes(worker.IdentityPublicKey), EnrollmentGeneration: worker.EnrollmentGeneration,
		ConnectionGeneration: challenge.TargetConnectionGeneration,
		Version:              attachedworkerprotocol.ProtocolVersion(challenge.SelectedProtocolVersion), ChannelBinding: cloneBytes(channelBinding),
	}
	machine, err := attachedworkerprotocol.NewConformanceMachine(attachedworkerprotocol.MachineConfig{
		Auth: auth, WorkerOffer: protocolOffer(challenge.WorkerProtocolMinimum, challenge.WorkerProtocolMaximum, challenge.WorkerProtocolVersions),
		PlatformOffer:       protocolOffer(challenge.PlatformProtocolMinimum, challenge.PlatformProtocolMaximum, challenge.PlatformProtocolVersions),
		ImplementedVersions: append([]attachedworkerprotocol.ProtocolVersion(nil), service.implemented...),
	})
	if err != nil {
		return err
	}
	hello := attachedworkerprotocol.FrameV1{
		Version: auth.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: challenge.TargetConnectionGeneration,
		Sequence: 1, Ack: 0, Kind: attachedworkerprotocol.MessageHello,
		Hello: &attachedworkerprotocol.HelloV1{Offer: protocolOffer(challenge.WorkerProtocolMinimum, challenge.WorkerProtocolMaximum, challenge.WorkerProtocolVersions), WorkerNonce: cloneBytes(activationWorkerNonce(attach))},
	}
	challengeFrame := attachedworkerprotocol.FrameV1{
		Version: auth.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: challenge.TargetConnectionGeneration,
		Sequence: 1, Ack: 1, Kind: attachedworkerprotocol.MessageChallenge,
		Challenge: &attachedworkerprotocol.ChallengeV1{
			WorkerOffer:     protocolOffer(challenge.WorkerProtocolMinimum, challenge.WorkerProtocolMaximum, challenge.WorkerProtocolVersions),
			PlatformOffer:   protocolOffer(challenge.PlatformProtocolMinimum, challenge.PlatformProtocolMaximum, challenge.PlatformProtocolVersions),
			SelectedVersion: auth.Version, WorkerNonce: cloneBytes(activationWorkerNonce(attach)), PlatformNonce: cloneBytes(activationPlatformNonce(attach)),
		},
	}
	acceptance := attachedworkerprotocol.AcceptanceContextV1{ChannelBinding: cloneBytes(channelBinding), NowUnixMicro: 1}
	for _, item := range []struct {
		direction attachedworkerprotocol.Direction
		frame     attachedworkerprotocol.FrameV1
	}{{attachedworkerprotocol.DirectionWorkerToPlatform, hello}, {attachedworkerprotocol.DirectionPlatformToWorker, challengeFrame}, {attachedworkerprotocol.DirectionWorkerToPlatform, attach}, {attachedworkerprotocol.DirectionPlatformToWorker, accepted}} {
		if err := machine.Accept(item.direction, item.frame, acceptance); err != nil {
			return err
		}
	}
	return nil
}

type ConnectionBearer struct {
	tenantID     domain.TenantID
	ownerUserID  domain.UserID
	workerID     domain.AttachedWorkerID
	connectionID domain.AttachedWorkerConnectionID
	secret       ConnectionSecret
}

func NewConnectionBearer(tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID, connectionID domain.AttachedWorkerConnectionID, secret ConnectionSecret) (ConnectionBearer, error) {
	if tenantID.Validate() != nil || ownerUserID.Validate() != nil || workerID.Validate() != nil || connectionID.Validate() != nil {
		return ConnectionBearer{}, ErrTransportUnauthorized
	}
	return ConnectionBearer{tenantID: tenantID, ownerUserID: ownerUserID, workerID: workerID, connectionID: connectionID, secret: secret}, nil
}

func ParseConnectionBearer(value []byte) (ConnectionBearer, error) {
	if len(value) <= len(connectionBearerPrefix) || len(value) > maxConnectionBearerBytes || !bytes.HasPrefix(value, []byte(connectionBearerPrefix)) {
		return ConnectionBearer{}, ErrTransportUnauthorized
	}
	payload, err := base64.RawURLEncoding.DecodeString(string(value[len(connectionBearerPrefix):]))
	if err != nil {
		return ConnectionBearer{}, ErrTransportUnauthorized
	}
	fields, err := decodeBearerFields(payload, 5)
	if err != nil || len(fields[4]) != connectionSecretBytes {
		return ConnectionBearer{}, ErrTransportUnauthorized
	}
	secret, err := ParseConnectionSecret(fields[4])
	if err != nil {
		return ConnectionBearer{}, ErrTransportUnauthorized
	}
	return NewConnectionBearer(domain.TenantID(fields[0]), domain.UserID(fields[1]), domain.AttachedWorkerID(fields[2]), domain.AttachedWorkerConnectionID(fields[3]), secret)
}

func (bearer ConnectionBearer) Bytes() []byte {
	payload := appendTransportField(nil, []byte(bearer.tenantID))
	payload = appendTransportField(payload, []byte(bearer.ownerUserID))
	payload = appendTransportField(payload, []byte(bearer.workerID))
	payload = appendTransportField(payload, []byte(bearer.connectionID))
	payload = appendTransportField(payload, bearer.secret.value[:])
	return []byte(connectionBearerPrefix + base64.RawURLEncoding.EncodeToString(payload))
}
func (ConnectionBearer) String() string               { return "[REDACTED]" }
func (ConnectionBearer) GoString() string             { return "[REDACTED]" }
func (ConnectionBearer) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

// ExchangeBearer is the adapter seam for HTTP bearer bytes. The HTTP package
// remains independent and can pass its redacted BearerToken.Bytes() here.
func (service *Service) ExchangeBearer(ctx context.Context, rawBearer []byte, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	bearer, err := ParseConnectionBearer(rawBearer)
	if err != nil {
		return nil, ErrTransportUnauthorized
	}
	return service.Exchange(ctx, bearer, batch)
}

func (service *Service) Exchange(ctx context.Context, bearer ConnectionBearer, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if service == nil || service.store == nil || validateBatch(batch) != nil || len(batch.Frames) == 0 {
		return nil, ErrTransportUnauthorized
	}
	connection, found, err := service.store.LoadAttachedWorkerConnection(ctx, bearer.tenantID, bearer.ownerUserID, bearer.workerID)
	if err != nil {
		return nil, ErrTransportBackend
	}
	if !found || connection.Validate() != nil || connection.ID != bearer.connectionID ||
		batch.Version != attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion) {
		return nil, ErrTransportUnauthorized
	}
	if len(batch.Frames) == 1 && batch.Frames[0].Kind == attachedworkerprotocol.MessageManifest {
		return service.acceptManifestExchange(ctx, bearer, connection, batch)
	}
	if connection.State != domain.AttachedWorkerConnectionOnline && connection.State != domain.AttachedWorkerConnectionDraining {
		return nil, ErrTransportUnauthorized
	}
	// AW-03 accepts one presence observation per transaction. Besides bounding
	// write amplification, this makes an ambiguous-response retry have one
	// exact envelope target that the store can reconcile.
	if len(batch.Frames) != 1 {
		return nil, ErrTransportUnauthorized
	}
	last := batch.Frames[0]
	replay := last.Sequence >= 4 && last.Sequence == connection.WorkerSequence && last.Ack == connection.WorkerAck
	for index, frame := range batch.Frames {
		// AW-03 persists presence only. Attempt/control frames remain disabled
		// until AW-04 can run them through the authoritative conformance state
		// machine; advancing their envelope sequence here would poison replay.
		if frame.Kind != attachedworkerprotocol.MessageHeartbeat || frame.Heartbeat == nil || frame.Heartbeat.ActiveAttempts != 0 ||
			(connection.State == domain.AttachedWorkerConnectionDraining && frame.Heartbeat.Available) ||
			frame.WorkerID != string(bearer.workerID) || frame.EnrollmentGeneration != connection.EnrollmentGeneration ||
			frame.ConnectionGeneration != connection.ConnectionGeneration || frame.Version != batch.Version ||
			frame.MessageID != attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, frame.Sequence) ||
			(!replay && frame.Sequence != connection.WorkerSequence+uint64(index)+1) || (replay && frame.Sequence != connection.WorkerSequence) || frame.Ack < connection.WorkerAck ||
			frame.Ack > connection.PlatformSequence {
			return nil, ErrTransportUnauthorized
		}
	}
	expectedConnectionRevision := connection.Revision
	if replay {
		if expectedConnectionRevision == 0 {
			return nil, ErrTransportUnauthorized
		}
		expectedConnectionRevision--
	}
	authorized, err := service.store.AuthorizeAttachedWorkerExchange(ctx, ports.AttachedWorkerExchangeAuthorization{
		TenantID: bearer.tenantID, OwnerUserID: bearer.ownerUserID, WorkerID: bearer.workerID,
		ConnectionID: bearer.connectionID, ConnectionGeneration: connection.ConnectionGeneration,
		PresentedSecretDigest: bearer.secret.Digest(), ExpectedConnectionRevision: expectedConnectionRevision,
		PlatformSequence: connection.PlatformSequence, WorkerSequence: last.Sequence,
		PlatformAck: connection.PlatformAck, WorkerAck: last.Ack,
		CheckpointInterval: service.checkpointInterval, PresenceTTL: service.presenceTTL,
	})
	if err != nil {
		return nil, ErrTransportBackend
	}
	if authorized.Status != ports.AttachedWorkerConnectionAuthorized || authorized.Connection.Validate() != nil ||
		authorized.Connection.TenantID != bearer.tenantID || authorized.Connection.OwnerUserID != bearer.ownerUserID ||
		authorized.Connection.WorkerID != bearer.workerID || authorized.Connection.ID != bearer.connectionID ||
		authorized.Connection.ConnectionGeneration != connection.ConnectionGeneration ||
		authorized.Connection.EnrollmentGeneration != connection.EnrollmentGeneration ||
		authorized.Connection.ProtocolVersion != connection.ProtocolVersion || authorized.Connection.SecretDigest != connection.SecretDigest ||
		authorized.Connection.ChannelBinding != connection.ChannelBinding || authorized.Connection.CapabilityDigest != connection.CapabilityDigest ||
		authorized.Connection.PlatformSequence != connection.PlatformSequence || authorized.Connection.WorkerSequence != last.Sequence ||
		authorized.Connection.PlatformAck != connection.PlatformAck || authorized.Connection.WorkerAck != last.Ack {
		return nil, ErrTransportUnauthorized
	}
	// AW-03 is presence-only and intentionally returns 204. AW-04 must add a
	// transactional outbound watermark/conformance boundary before any broker
	// can return platform frames.
	return nil, nil
}

func (service *Service) acceptManifestExchange(ctx context.Context, bearer ConnectionBearer, connection domain.AttachedWorkerConnection, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if len(batch.Frames) != 1 {
		return nil, ErrTransportUnauthorized
	}
	frame := batch.Frames[0]
	if frame.Kind != attachedworkerprotocol.MessageManifest || frame.Manifest == nil || frame.Sequence != 3 || frame.Ack != 2 ||
		frame.MessageID != attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3) ||
		frame.WorkerID != string(connection.WorkerID) || frame.EnrollmentGeneration != connection.EnrollmentGeneration ||
		frame.ConnectionGeneration != connection.ConnectionGeneration || frame.Version != batch.Version ||
		!bytes.Equal(frame.Manifest.Digest, mustDecodeHex(string(connection.CapabilityDigest))) {
		return nil, ErrTransportUnauthorized
	}
	if connection.State != domain.AttachedWorkerConnectionAttaching && connection.State != domain.AttachedWorkerConnectionOnline && connection.State != domain.AttachedWorkerConnectionDraining {
		return nil, ErrTransportUnauthorized
	}
	worker, found, err := service.store.LoadAttachedWorker(ctx, bearer.tenantID, bearer.ownerUserID, bearer.workerID)
	if err != nil {
		return nil, ErrTransportBackend
	}
	channelBinding, err := decodeChannelBinding(connection.ChannelBinding)
	if err != nil || !found || worker.Validate() != nil || worker.DesiredState == domain.AttachedWorkerDesiredRevoked ||
		worker.TenantID != bearer.tenantID || worker.OwnerUserID != bearer.ownerUserID || worker.ID != bearer.workerID ||
		worker.EnrollmentGeneration != connection.EnrollmentGeneration || worker.ConnectionGeneration != connection.ConnectionGeneration {
		return nil, ErrTransportUnauthorized
	}
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(bearer.tenantID), OwnerUserID: string(bearer.ownerUserID), WorkerID: string(bearer.workerID),
		IdentityPublicKey: cloneBytes(worker.IdentityPublicKey), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Version: batch.Version, ChannelBinding: channelBinding,
	}
	if attachedworkerprotocol.VerifyManifestV1(auth, frame) != nil {
		return nil, ErrTransportUnauthorized
	}
	payload, err := json.Marshal(frame.Manifest.Manifest)
	if err != nil || len(payload) == 0 || len(payload) > 32<<10 {
		return nil, ErrTransportUnauthorized
	}
	canonical, err := attachedworkerprotocol.CanonicalManifestBytesV1(frame.Manifest.Manifest)
	if err != nil || len(canonical) == 0 || len(canonical) > 32<<10 {
		return nil, ErrTransportUnauthorized
	}
	expectedConnectionRevision, expectedWorkerRevision := connection.Revision, worker.Revision
	if connection.State != domain.AttachedWorkerConnectionAttaching {
		if expectedConnectionRevision == 0 || expectedWorkerRevision == 0 {
			return nil, ErrTransportUnauthorized
		}
		expectedConnectionRevision--
		expectedWorkerRevision--
	}
	result, err := service.store.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: bearer.tenantID, OwnerUserID: bearer.ownerUserID, WorkerID: bearer.workerID,
		ConnectionID: connection.ID, ConnectionGeneration: connection.ConnectionGeneration,
		ExpectedConnectionRevision: expectedConnectionRevision, ExpectedWorkerRevision: expectedWorkerRevision,
		PresentedSecretDigest: bearer.secret.Digest(),
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: frame.Manifest.Manifest.Revision, Digest: connection.CapabilityDigest,
			ProtocolVersion: connection.ProtocolVersion, IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey),
			CanonicalManifest: canonical, ManifestPayload: payload, Signature: cloneBytes(frame.Manifest.Signature),
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2, PresenceTTL: service.presenceTTL,
	})
	if err != nil {
		return nil, ErrTransportBackend
	}
	expectedState := domain.AttachedWorkerConnectionOnline
	if worker.DesiredState == domain.AttachedWorkerDesiredDrain {
		expectedState = domain.AttachedWorkerConnectionDraining
	}
	if result.Status != ports.AttachedWorkerConnectionAuthorized || result.Connection.Validate() != nil ||
		result.Connection.State != expectedState || result.Connection.TenantID != bearer.tenantID ||
		result.Connection.OwnerUserID != bearer.ownerUserID || result.Connection.WorkerID != bearer.workerID ||
		result.Connection.ID != connection.ID || result.Connection.EnrollmentGeneration != connection.EnrollmentGeneration ||
		result.Connection.ConnectionGeneration != connection.ConnectionGeneration || result.Connection.SecretDigest != connection.SecretDigest ||
		result.Connection.ChannelBinding != connection.ChannelBinding || result.Connection.CapabilityDigest != connection.CapabilityDigest ||
		result.Connection.ManifestRevision != frame.Manifest.Manifest.Revision ||
		result.Connection.ManifestIdentityKey != domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey) ||
		!bytes.Equal(result.Connection.ManifestSignature, frame.Manifest.Signature) || result.Connection.ManifestObservedAt.IsZero() ||
		result.Connection.WorkerSequence != 3 || result.Connection.PlatformSequence != 2 ||
		result.Connection.WorkerAck != 2 || result.Connection.PlatformAck != 2 {
		return nil, ErrTransportUnauthorized
	}
	return nil, nil
}

func validateBatch(batch attachedworkerprotocol.BatchV1) error {
	_, err := attachedworkerprotocol.EncodeBatchV1(batch)
	return err
}

func validateSingleFrame(frame attachedworkerprotocol.FrameV1) error {
	return validateBatch(attachedworkerprotocol.BatchV1{Version: frame.Version, Frames: []attachedworkerprotocol.FrameV1{frame}})
}

func cloneOffer(value attachedworkerprotocol.VersionOfferV1) attachedworkerprotocol.VersionOfferV1 {
	return attachedworkerprotocol.VersionOfferV1{Window: value.Window, Supported: append([]attachedworkerprotocol.ProtocolVersion(nil), value.Supported...)}
}

func challengeMatchesCreate(challenge domain.AttachedWorkerAttachChallenge, create ports.AttachedWorkerChallengeCreate) bool {
	return challenge.Purpose == create.Purpose && challenge.Audience == create.Audience &&
		challenge.ExpectedWorkerRevision == create.ExpectedWorkerRevision &&
		challenge.ExpectedEnrollmentGeneration == create.ExpectedEnrollmentGeneration &&
		challenge.ExpectedConnectionGeneration == create.ExpectedConnectionGeneration &&
		challenge.TargetConnectionGeneration == create.ExpectedConnectionGeneration+1 &&
		challenge.WorkerProtocolMinimum == create.WorkerProtocolMinimum && challenge.WorkerProtocolMaximum == create.WorkerProtocolMaximum &&
		challenge.PlatformProtocolMinimum == create.PlatformProtocolMinimum && challenge.PlatformProtocolMaximum == create.PlatformProtocolMaximum &&
		challenge.SelectedProtocolVersion == create.SelectedProtocolVersion &&
		equalUint32s(challenge.WorkerProtocolVersions, create.WorkerProtocolVersions) &&
		equalUint32s(challenge.PlatformProtocolVersions, create.PlatformProtocolVersions) &&
		challenge.WorkerNonceDigest == create.WorkerNonceDigest && challenge.PlatformNonceDigest == create.PlatformNonceDigest
}

func equalUint32s(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func protocolVersions(values []attachedworkerprotocol.ProtocolVersion) []uint32 {
	result := make([]uint32, len(values))
	for index, value := range values {
		result[index] = uint32(value)
	}
	return result
}

func protocolOffer(minimum, maximum uint32, versions []uint32) attachedworkerprotocol.VersionOfferV1 {
	supported := make([]attachedworkerprotocol.ProtocolVersion, len(versions))
	for index, value := range versions {
		supported[index] = attachedworkerprotocol.ProtocolVersion(value)
	}
	return attachedworkerprotocol.VersionOfferV1{
		Window: attachedworkerprotocol.VersionWindow{
			Minimum: attachedworkerprotocol.ProtocolVersion(minimum),
			Maximum: attachedworkerprotocol.ProtocolVersion(maximum),
		},
		Supported: supported,
	}
}

func validAudience(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 256
}

func validateSecretDigest(value domain.AttachedWorkerConnectionSecretDigest) error {
	return value.Validate()
}

func decodeChannelBinding(value domain.AttachedWorkerChannelBinding) ([]byte, error) {
	if value.Validate() != nil {
		return nil, ErrTransportUnauthorized
	}
	decoded, err := hex.DecodeString(string(value))
	if err != nil || len(decoded) != sha256.Size {
		return nil, ErrTransportUnauthorized
	}
	return decoded, nil
}

func appendTransportOffer(destination []byte, offer attachedworkerprotocol.VersionOfferV1) []byte {
	destination = appendTransportUint32(destination, uint32(offer.Window.Minimum))
	destination = appendTransportUint32(destination, uint32(offer.Window.Maximum))
	destination = appendTransportUint32(destination, uint32(len(offer.Supported)))
	for _, version := range offer.Supported {
		destination = appendTransportUint32(destination, uint32(version))
	}
	return destination
}

func appendTransportField(destination, value []byte) []byte {
	destination = appendTransportUint32(destination, uint32(len(value)))
	return append(destination, value...)
}

func appendTransportUint32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendTransportUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func decodeBearerFields(payload []byte, count int) ([][]byte, error) {
	result := make([][]byte, 0, count)
	for len(payload) > 0 && len(result) < count {
		if len(payload) < 4 {
			return nil, ErrTransportUnauthorized
		}
		size := int(binary.BigEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if size <= 0 || size > len(payload) {
			return nil, ErrTransportUnauthorized
		}
		result = append(result, cloneBytes(payload[:size]))
		payload = payload[size:]
	}
	if len(result) != count || len(payload) != 0 {
		return nil, ErrTransportUnauthorized
	}
	return result, nil
}

func sameActivationNegotiation(challenge domain.AttachedWorkerAttachChallenge, frame attachedworkerprotocol.FrameV1) bool {
	workerOffer, platformOffer, selected := activationNegotiation(frame)
	return uint32(workerOffer.Window.Minimum) == challenge.WorkerProtocolMinimum && uint32(workerOffer.Window.Maximum) == challenge.WorkerProtocolMaximum &&
		uint32(platformOffer.Window.Minimum) == challenge.PlatformProtocolMinimum && uint32(platformOffer.Window.Maximum) == challenge.PlatformProtocolMaximum &&
		uint32(selected) == challenge.SelectedProtocolVersion && equalProtocolVersions(workerOffer.Supported, challenge.WorkerProtocolVersions) &&
		equalProtocolVersions(platformOffer.Supported, challenge.PlatformProtocolVersions)
}

func activationNegotiation(frame attachedworkerprotocol.FrameV1) (attachedworkerprotocol.VersionOfferV1, attachedworkerprotocol.VersionOfferV1, attachedworkerprotocol.ProtocolVersion) {
	if frame.Attach != nil {
		return frame.Attach.WorkerOffer, frame.Attach.PlatformOffer, frame.Attach.SelectedVersion
	}
	if frame.Reconnect != nil {
		return frame.Reconnect.WorkerOffer, frame.Reconnect.PlatformOffer, frame.Reconnect.SelectedVersion
	}
	return attachedworkerprotocol.VersionOfferV1{}, attachedworkerprotocol.VersionOfferV1{}, 0
}

func activationWorkerNonce(frame attachedworkerprotocol.FrameV1) []byte {
	if frame.Attach != nil {
		return frame.Attach.WorkerNonce
	}
	if frame.Reconnect != nil {
		return frame.Reconnect.WorkerNonce
	}
	return nil
}
func activationPlatformNonce(frame attachedworkerprotocol.FrameV1) []byte {
	if frame.Attach != nil {
		return frame.Attach.PlatformNonce
	}
	if frame.Reconnect != nil {
		return frame.Reconnect.PlatformNonce
	}
	return nil
}
func activationCapabilityDigest(frame attachedworkerprotocol.FrameV1) []byte {
	if frame.Attach != nil {
		return frame.Attach.CapabilityDigest
	}
	if frame.Reconnect != nil {
		return frame.Reconnect.CapabilityDigest
	}
	return nil
}

func equalProtocolVersions(left []attachedworkerprotocol.ProtocolVersion, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if uint32(left[index]) != right[index] {
			return false
		}
	}
	return true
}

func hexDigest(value []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, item := range value {
		result[index*2] = digits[item>>4]
		result[index*2+1] = digits[item&15]
	}
	return string(result)
}

func mustDecodeHex(value string) []byte {
	decoded, _ := hex.DecodeString(value)
	return decoded
}

func cloneBytes(value []byte) []byte { return append([]byte(nil), value...) }
