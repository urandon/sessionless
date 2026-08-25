package attachedworkerprotocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
)

type ConnectionState string

const (
	ConnectionUnattached       ConnectionState = "unattached"
	ConnectionHello            ConnectionState = "hello"
	ConnectionChallenged       ConnectionState = "challenged"
	ConnectionAttachPending    ConnectionState = "attach_pending"
	ConnectionReconnectPending ConnectionState = "reconnect_pending"
	ConnectionAttached         ConnectionState = "attached"
	ConnectionReady            ConnectionState = "ready"
	ConnectionDraining         ConnectionState = "draining"
	ConnectionDrained          ConnectionState = "drained"
	ConnectionRevoking         ConnectionState = "revoking"
	ConnectionRevoked          ConnectionState = "revoked"
)

type AttemptState string

const (
	AttemptIdle              AttemptState = "idle"
	AttemptOffered           AttemptState = "offered"
	AttemptClaimPending      AttemptState = "claim_pending"
	AttemptClaimed           AttemptState = "claimed"
	AttemptCancelRequested   AttemptState = "cancel_requested"
	AttemptCancelAcked       AttemptState = "cancel_acked"
	AttemptFenced            AttemptState = "fenced"
	AttemptTerminalPending   AttemptState = "terminal_pending"
	AttemptTerminalCommitted AttemptState = "terminal_committed"
)

type MachineConfig struct {
	Auth                AuthContextV1
	WorkerOffer         VersionOfferV1
	PlatformOffer       VersionOfferV1
	ImplementedVersions []ProtocolVersion
}

type sequenceState struct {
	sequence    uint64
	ack         uint64
	fingerprint [sha256.Size]byte
}

type AcceptanceContextV1 struct {
	ChannelBinding []byte
	NowUnixMicro   int64
}

type attemptRecord struct {
	state                 AttemptState
	binding               AttemptBindingV1
	platform              attemptSequenceState
	worker                attemptSequenceState
	progressSequence      uint64
	cancelRevision        uint64
	cancelCode            CancelCode
	terminalSequence      uint64
	terminalStatus        TerminalStatus
	terminalResult        TerminalResult
	terminalEvidence      []byte
	pendingWorkerTerminal *terminalReplayCommitment
}

type attemptSequenceState struct {
	sequence     uint64
	fingerprints map[uint64][sha256.Size]byte
}

// terminalReplayCommitment is a bounded signed reconnect claim awaiting an
// exact replay decision. It is separate from accepted-frame fingerprints.
type terminalReplayCommitment struct {
	kind        MessageKind
	sequence    uint64
	fingerprint [sha256.Size]byte
	decision    ReconnectTerminalDecision
	terminal    TerminalV1
}

type ConformanceMachine struct {
	config                       MachineConfig
	connection                   ConnectionState
	platform                     sequenceState
	worker                       sequenceState
	hello                        HelloV1
	challenge                    ChallengeV1
	capabilityDigest             []byte
	manifest                     CapabilityManifestV1
	attempt                      attemptRecord
	drainRevision                uint64
	revoke                       RevokeV1
	reconnecting                 bool
	reconnectTarget              ConnectionState
	previousConnectionGeneration uint64
	reconnectWatermarks          ConnectionWatermarksV1
	reconnectAttempt             AttemptSummaryV1
	reconnectClaim               ReconnectSnapshotV1
}

func NewConformanceMachine(config MachineConfig) (*ConformanceMachine, error) {
	config = cloneMachineConfig(config)
	if config.Auth.Validate() != nil || config.WorkerOffer.Validate() != nil || config.PlatformOffer.Validate() != nil {
		return nil, protocolError(ErrorUnauthorized)
	}
	selected, err := NegotiateOffers(config.WorkerOffer, config.PlatformOffer, config.ImplementedVersions)
	if err != nil || selected != config.Auth.Version {
		return nil, protocolError(ErrorUnauthorized)
	}
	return &ConformanceMachine{
		config: config, connection: ConnectionUnattached,
		attempt: attemptRecord{state: AttemptIdle},
	}, nil
}

func (machine *ConformanceMachine) ConnectionState() ConnectionState { return machine.connection }
func (machine *ConformanceMachine) AttemptState() AttemptState       { return machine.attempt.state }
func (machine *ConformanceMachine) AttemptBinding() (AttemptBindingV1, bool) {
	if machine == nil || machine.attempt.state == AttemptIdle {
		return AttemptBindingV1{}, false
	}
	return cloneBinding(machine.attempt.binding), true
}

// BeginReconnect starts a new channel-bound handshake while retaining the
// bounded attempt record. It never changes the lease binding or its expiry.
func (machine *ConformanceMachine) BeginReconnect(next AuthContextV1) (ReconnectSnapshotV1, error) {
	next = cloneAuthContext(next)
	if machine == nil || next.Validate() != nil ||
		(machine.connection != ConnectionReady && machine.connection != ConnectionDraining) ||
		next.TenantID != machine.config.Auth.TenantID || next.OwnerUserID != machine.config.Auth.OwnerUserID ||
		next.WorkerID != machine.config.Auth.WorkerID || !bytes.Equal(next.IdentityPublicKey, machine.config.Auth.IdentityPublicKey) ||
		next.EnrollmentGeneration != machine.config.Auth.EnrollmentGeneration || next.Version != machine.config.Auth.Version ||
		!machine.hasFeature(FeatureReconnect) || machine.config.Auth.ConnectionGeneration == math.MaxUint64 ||
		next.ConnectionGeneration != machine.config.Auth.ConnectionGeneration+1 {
		return ReconnectSnapshotV1{}, protocolError(ErrorUnauthorized)
	}
	machine.previousConnectionGeneration = machine.config.Auth.ConnectionGeneration
	machine.reconnectWatermarks = machine.currentWatermarks()
	machine.reconnectAttempt = machine.currentAttemptSummary()
	machine.reconnectTarget = machine.connection
	machine.config.Auth = next
	machine.connection = ConnectionUnattached
	machine.platform, machine.worker = sequenceState{}, sequenceState{}
	machine.hello, machine.challenge = HelloV1{}, ChallengeV1{}
	machine.reconnecting = true
	return machine.SnapshotForReconnect()
}

func (machine *ConformanceMachine) SnapshotForReconnect() (ReconnectSnapshotV1, error) {
	if machine == nil || !machine.reconnecting {
		return ReconnectSnapshotV1{}, protocolError(ErrorProtocolViolation)
	}
	return sealReconnectSnapshot(ReconnectSnapshotV1{
		PreviousConnectionGeneration: machine.previousConnectionGeneration,
		Watermarks:                   machine.reconnectWatermarks,
		Attempt:                      machine.reconnectAttempt,
		PendingTerminalReplay:        machine.pendingTerminalForSnapshot(),
	}), nil
}

func MessageIDV1(direction Direction, sequence uint64) string {
	prefix := "w"
	if direction == DirectionPlatformToWorker {
		prefix = "p"
	}
	return fmt.Sprintf("%s-%020d", prefix, sequence)
}

// Accept verifies authoritative scope and channel binding before applying one
// deterministic protocol transition. It performs no I/O.
func (machine *ConformanceMachine) Accept(direction Direction, frame FrameV1, acceptance AcceptanceContextV1) error {
	if machine == nil || !validDirection(direction) || machine.config.Auth.Validate() != nil ||
		!bytes.Equal(acceptance.ChannelBinding, machine.config.Auth.ChannelBinding) || acceptance.NowUnixMicro <= 0 ||
		frame.Validate() != nil || frame.Version != machine.config.Auth.Version ||
		frame.WorkerID != machine.config.Auth.WorkerID ||
		frame.EnrollmentGeneration != machine.config.Auth.EnrollmentGeneration ||
		frame.ConnectionGeneration != machine.config.Auth.ConnectionGeneration {
		return protocolError(ErrorUnauthorized)
	}
	if frame.MessageID != MessageIDV1(direction, frame.Sequence) {
		return protocolError(ErrorProtocolViolation)
	}
	sender, peer := &machine.worker, &machine.platform
	if direction == DirectionPlatformToWorker {
		sender, peer = &machine.platform, &machine.worker
	}
	fingerprint := frameFingerprint(frame)
	if frame.Sequence == sender.sequence {
		if sender.sequence != 0 && sender.fingerprint == fingerprint {
			return nil
		}
		return protocolError(ErrorConflict)
	}
	if sender.sequence == math.MaxUint64 || frame.Sequence != sender.sequence+1 ||
		frame.Ack < sender.ack || frame.Ack > peer.sequence {
		return protocolError(ErrorSequenceMismatch)
	}
	if err := machine.apply(direction, frame, acceptance.NowUnixMicro); err != nil {
		return err
	}
	sender.sequence, sender.ack, sender.fingerprint = frame.Sequence, frame.Ack, fingerprint
	return nil
}

func (machine *ConformanceMachine) apply(direction Direction, frame FrameV1, nowUnixMicro int64) error {
	switch frame.Kind {
	case MessageHello:
		return machine.acceptHello(direction, frame)
	case MessageChallenge:
		return machine.acceptChallenge(direction, frame)
	case MessageAttach:
		return machine.acceptAttach(direction, frame)
	case MessageAttachAccepted:
		return machine.acceptAttachAccepted(direction, frame)
	case MessageReconnect:
		return machine.acceptReconnect(direction, frame)
	case MessageReconnectAccepted:
		return machine.acceptReconnectAccepted(direction, frame)
	case MessageManifest:
		return machine.acceptManifest(direction, frame)
	case MessageHeartbeat:
		return machine.acceptHeartbeat(direction, frame)
	case MessageDrain:
		return machine.acceptDrain(direction, frame)
	case MessageDrained:
		return machine.acceptDrained(direction, frame)
	case MessageRevoke:
		return machine.acceptRevoke(direction, frame)
	case MessageRevoked:
		return machine.acceptRevoked(direction, frame)
	case MessageLeaseOffer, MessageLeaseClaim, MessageLeaseAccepted, MessageProgress,
		MessageCancel, MessageCancelAck, MessageTerminal, MessageTerminalAck:
		return machine.acceptAttempt(direction, frame, nowUnixMicro)
	case MessageError:
		return nil
	default:
		return protocolError(ErrorInvalidTransition)
	}
}

func (machine *ConformanceMachine) acceptHello(direction Direction, frame FrameV1) error {
	if direction != DirectionWorkerToPlatform || machine.connection != ConnectionUnattached ||
		!sameVersionOffer(frame.Hello.Offer, machine.config.WorkerOffer) {
		return protocolError(ErrorInvalidTransition)
	}
	machine.hello = cloneHello(*frame.Hello)
	machine.connection = ConnectionHello
	return nil
}

func (machine *ConformanceMachine) acceptChallenge(direction Direction, frame FrameV1) error {
	if direction != DirectionPlatformToWorker || machine.connection != ConnectionHello ||
		!sameVersionOffer(frame.Challenge.WorkerOffer, machine.hello.Offer) ||
		!sameVersionOffer(frame.Challenge.PlatformOffer, machine.config.PlatformOffer) ||
		!bytes.Equal(frame.Challenge.WorkerNonce, machine.hello.WorkerNonce) {
		return protocolError(ErrorInvalidTransition)
	}
	selected, err := NegotiateOffers(frame.Challenge.WorkerOffer, frame.Challenge.PlatformOffer, machine.config.ImplementedVersions)
	if err != nil || selected != frame.Challenge.SelectedVersion || selected != machine.config.Auth.Version {
		return protocolError(ErrorUnsupportedVersion)
	}
	machine.challenge = cloneChallenge(*frame.Challenge)
	machine.connection = ConnectionChallenged
	return nil
}

func (machine *ConformanceMachine) acceptAttach(direction Direction, frame FrameV1) error {
	if direction != DirectionWorkerToPlatform || machine.connection != ConnectionChallenged || machine.reconnecting ||
		!negotiationMatchesChallenge(frame.Attach.WorkerOffer, frame.Attach.PlatformOffer, frame.Attach.SelectedVersion,
			frame.Attach.WorkerNonce, frame.Attach.PlatformNonce, machine.challenge) ||
		VerifyAttachV1(machine.config.Auth, frame) != nil {
		return protocolError(ErrorUnauthorized)
	}
	machine.capabilityDigest = append([]byte(nil), frame.Attach.CapabilityDigest...)
	machine.connection = ConnectionAttachPending
	return nil
}

func (machine *ConformanceMachine) acceptAttachAccepted(direction Direction, frame FrameV1) error {
	if direction != DirectionPlatformToWorker || machine.connection != ConnectionAttachPending ||
		!acceptedMatchesChallenge(AttachAcceptedV1(*frame.AttachAccepted), machine.challenge, machine.capabilityDigest) {
		return protocolError(ErrorUnauthorized)
	}
	machine.connection = ConnectionAttached
	return nil
}

func (machine *ConformanceMachine) acceptReconnect(direction Direction, frame FrameV1) error {
	workerClaim := sealReconnectSnapshot(ReconnectSnapshotV1{
		PreviousConnectionGeneration: frame.Reconnect.PreviousConnectionGeneration,
		Watermarks:                   frame.Reconnect.PreviousWatermarks, Attempt: frame.Reconnect.AttemptSummary,
		PendingTerminalReplay: frame.Reconnect.PendingTerminalReplay,
	})
	authoritative, snapshotErr := machine.SnapshotForReconnect()
	if direction != DirectionWorkerToPlatform || machine.connection != ConnectionChallenged || !machine.reconnecting ||
		frame.Reconnect.PreviousConnectionGeneration != machine.previousConnectionGeneration ||
		frame.Reconnect.PreviousConnectionGeneration == math.MaxUint64 ||
		frame.Reconnect.PreviousConnectionGeneration+1 != machine.config.Auth.ConnectionGeneration ||
		!negotiationMatchesChallenge(frame.Reconnect.WorkerOffer, frame.Reconnect.PlatformOffer,
			frame.Reconnect.SelectedVersion, frame.Reconnect.WorkerNonce, frame.Reconnect.PlatformNonce, machine.challenge) ||
		VerifyReconnectV1(machine.config.Auth, frame) != nil ||
		snapshotErr != nil || !reconnectClaimsCompatible(workerClaim, authoritative) ||
		(len(machine.capabilityDigest) != 0 && !bytes.Equal(frame.Reconnect.CapabilityDigest, machine.capabilityDigest)) {
		return protocolError(ErrorUnauthorized)
	}
	machine.capabilityDigest = append([]byte(nil), frame.Reconnect.CapabilityDigest...)
	machine.reconnectClaim = cloneReconnectSnapshot(workerClaim)
	machine.connection = ConnectionReconnectPending
	return nil
}

func (machine *ConformanceMachine) acceptReconnectAccepted(direction Direction, frame FrameV1) error {
	authoritative := sealReconnectSnapshot(ReconnectSnapshotV1{
		PreviousConnectionGeneration: machine.previousConnectionGeneration,
		Watermarks:                   frame.ReconnectAccepted.AuthoritativeWatermarks,
		Attempt:                      frame.ReconnectAccepted.AuthoritativeAttempt,
		PendingTerminalReplay:        frame.ReconnectAccepted.AuthoritativePendingTerminalReplay,
	})
	negotiation := ReconnectNegotiationV1{
		WorkerOffer: frame.ReconnectAccepted.WorkerOffer, PlatformOffer: frame.ReconnectAccepted.PlatformOffer,
		SelectedVersion: frame.ReconnectAccepted.SelectedVersion, WorkerNonce: frame.ReconnectAccepted.WorkerNonce,
		PlatformNonce: frame.ReconnectAccepted.PlatformNonce, CapabilityDigest: frame.ReconnectAccepted.CapabilityDigest,
	}
	expected, buildErr := BuildReconnectAcceptedV1(authoritative, machine.reconnectClaim, negotiation)
	if direction != DirectionPlatformToWorker || machine.connection != ConnectionReconnectPending ||
		!acceptedMatchesChallenge(AttachAcceptedV1{
			WorkerOffer: frame.ReconnectAccepted.WorkerOffer, PlatformOffer: frame.ReconnectAccepted.PlatformOffer,
			SelectedVersion: frame.ReconnectAccepted.SelectedVersion, WorkerNonce: frame.ReconnectAccepted.WorkerNonce,
			PlatformNonce: frame.ReconnectAccepted.PlatformNonce, CapabilityDigest: frame.ReconnectAccepted.CapabilityDigest,
		}, machine.challenge, machine.capabilityDigest) ||
		buildErr != nil || !sameReconnectAccepted(expected, *frame.ReconnectAccepted) {
		return protocolError(ErrorUnauthorized)
	}
	if err := machine.installReplayCommitment(authoritative, machine.reconnectClaim, frame.ReconnectAccepted.ReplayPlan); err != nil {
		return err
	}
	machine.applyAttemptSummary(frame.ReconnectAccepted.AuthoritativeAttempt)
	machine.connection = ConnectionAttached
	return nil
}

func (machine *ConformanceMachine) acceptManifest(direction Direction, frame FrameV1) error {
	manifest := frame.Manifest.Manifest
	if direction != DirectionWorkerToPlatform || machine.connection != ConnectionAttached ||
		!bytes.Equal(frame.Manifest.Digest, machine.capabilityDigest) || manifest.WorkerID != machine.config.Auth.WorkerID ||
		manifest.EnrollmentGeneration != machine.config.Auth.EnrollmentGeneration ||
		!sameVersionOffer(manifest.ProtocolOffer, machine.config.WorkerOffer) || VerifyManifestV1(machine.config.Auth, frame) != nil {
		return protocolError(ErrorUnauthorized)
	}
	machine.manifest = cloneManifest(manifest)
	if machine.reconnecting {
		machine.connection = machine.reconnectTarget
		machine.reconnecting = false
	} else {
		machine.connection = ConnectionReady
	}
	return nil
}

func (machine *ConformanceMachine) acceptHeartbeat(direction Direction, frame FrameV1) error {
	if direction != DirectionWorkerToPlatform ||
		(machine.connection != ConnectionReady && machine.connection != ConnectionDraining) {
		return protocolError(ErrorInvalidTransition)
	}
	active := uint32(0)
	if machine.attempt.state != AttemptIdle && machine.attempt.state != AttemptTerminalCommitted {
		active = 1
	}
	if frame.Heartbeat.ActiveAttempts != active ||
		(frame.Heartbeat.Available && (machine.connection != ConnectionReady || active != 0)) {
		return protocolError(ErrorConflict)
	}
	return nil
}

func (machine *ConformanceMachine) acceptDrain(direction Direction, frame FrameV1) error {
	if direction != DirectionPlatformToWorker || machine.connection != ConnectionReady {
		return protocolError(ErrorInvalidTransition)
	}
	machine.drainRevision = frame.Drain.Revision
	machine.connection = ConnectionDraining
	return nil
}

func (machine *ConformanceMachine) acceptDrained(direction Direction, frame FrameV1) error {
	if direction != DirectionWorkerToPlatform || machine.connection != ConnectionDraining ||
		frame.Drained.Revision != machine.drainRevision ||
		(machine.attempt.state != AttemptIdle && machine.attempt.state != AttemptTerminalCommitted) {
		return protocolError(ErrorInvalidTransition)
	}
	machine.connection = ConnectionDrained
	return nil
}

func (machine *ConformanceMachine) acceptRevoke(direction Direction, frame FrameV1) error {
	if direction != DirectionPlatformToWorker || machine.connection == ConnectionUnattached ||
		machine.connection == ConnectionRevoked || machine.config.Auth.EnrollmentGeneration == math.MaxUint64 ||
		machine.config.Auth.ConnectionGeneration == math.MaxUint64 ||
		frame.Revoke.NextEnrollmentGeneration != machine.config.Auth.EnrollmentGeneration+1 ||
		frame.Revoke.NextConnectionGeneration != machine.config.Auth.ConnectionGeneration+1 {
		return protocolError(ErrorInvalidTransition)
	}
	machine.revoke = *frame.Revoke
	if machine.attempt.state != AttemptIdle && machine.attempt.state != AttemptTerminalCommitted {
		machine.attempt.state = AttemptFenced
		machine.attempt.cancelCode = CancelFenced
		machine.attempt.terminalSequence, machine.attempt.terminalStatus = 0, ""
		machine.attempt.terminalResult, machine.attempt.terminalEvidence = "", nil
		if machine.attempt.cancelRevision == 0 {
			machine.attempt.cancelRevision = 1
		}
	}
	machine.connection = ConnectionRevoking
	return nil
}

func (machine *ConformanceMachine) acceptRevoked(direction Direction, frame FrameV1) error {
	if direction != DirectionWorkerToPlatform || machine.connection != ConnectionRevoking ||
		(machine.attempt.state != AttemptIdle && machine.attempt.state != AttemptTerminalCommitted && machine.attempt.state != AttemptFenced) ||
		*frame.Revoked != RevokedV1(machine.revoke) {
		return protocolError(ErrorInvalidTransition)
	}
	machine.connection = ConnectionRevoked
	return nil
}

func (machine *ConformanceMachine) acceptAttempt(direction Direction, frame FrameV1, nowUnixMicro int64) error {
	binding, sequence := attemptPayload(frame)
	fingerprint := attemptFingerprint(frame)
	sender := &machine.attempt.worker
	if direction == DirectionPlatformToWorker {
		sender = &machine.attempt.platform
	}
	if sequence <= sender.sequence {
		if sequence != 0 && sender.fingerprints[sequence] == fingerprint {
			return nil
		}
		return protocolError(ErrorConflict)
	}
	if direction == DirectionWorkerToPlatform && machine.attempt.pendingWorkerTerminal != nil &&
		sequence == machine.attempt.pendingWorkerTerminal.sequence {
		if machine.attempt.pendingWorkerTerminal.kind != frame.Kind || machine.attempt.pendingWorkerTerminal.fingerprint != fingerprint {
			return protocolError(ErrorConflict)
		}
		if machine.attempt.pendingWorkerTerminal.decision == ReconnectTerminalDiscard {
			return protocolError(ErrorProtocolViolation)
		}
	}
	if sender.sequence == math.MaxUint64 || sequence != sender.sequence+1 {
		return protocolError(ErrorSequenceMismatch)
	}
	if machine.attempt.state != AttemptIdle && !sameAttemptBinding(binding, machine.attempt.binding) {
		return protocolError(ErrorBindingMismatch)
	}
	if !bytes.Equal(binding.CapabilityDigest, machine.capabilityDigest) {
		return protocolError(ErrorBindingMismatch)
	}
	if attemptRequiresValidLease(frame.Kind) && nowUnixMicro >= binding.ExpiresAtUnixMicro {
		return protocolError(ErrorConflict)
	}
	if err := machine.applyAttemptTransition(direction, frame, binding); err != nil {
		return err
	}
	if sender.fingerprints == nil {
		sender.fingerprints = make(map[uint64][sha256.Size]byte, MaxAttemptMessages)
	}
	sender.sequence, sender.fingerprints[sequence] = sequence, fingerprint
	if direction == DirectionWorkerToPlatform && machine.attempt.pendingWorkerTerminal != nil &&
		machine.attempt.pendingWorkerTerminal.sequence == sequence {
		machine.attempt.pendingWorkerTerminal = nil
	}
	return nil
}

func attemptRequiresValidLease(kind MessageKind) bool {
	switch kind {
	case MessageLeaseOffer, MessageLeaseClaim, MessageLeaseAccepted, MessageProgress, MessageTerminal, MessageTerminalAck:
		return true
	default:
		return false
	}
}

func (machine *ConformanceMachine) applyAttemptTransition(direction Direction, frame FrameV1, binding AttemptBindingV1) error {
	switch frame.Kind {
	case MessageLeaseOffer:
		if direction != DirectionPlatformToWorker || machine.connection != ConnectionReady || machine.attempt.state != AttemptIdle ||
			frame.LeaseOffer.AttemptSequence != 1 {
			return protocolError(ErrorInvalidTransition)
		}
		machine.attempt.binding, machine.attempt.state = cloneBinding(binding), AttemptOffered
	case MessageLeaseClaim:
		if direction != DirectionWorkerToPlatform || machine.attempt.state != AttemptOffered {
			return protocolError(ErrorInvalidTransition)
		}
		machine.attempt.state = AttemptClaimPending
	case MessageLeaseAccepted:
		if direction != DirectionPlatformToWorker || machine.attempt.state != AttemptClaimPending {
			return protocolError(ErrorInvalidTransition)
		}
		machine.attempt.state = AttemptClaimed
	case MessageProgress:
		if direction != DirectionWorkerToPlatform || !machine.hasFeature(FeatureProgress) ||
			(machine.attempt.state != AttemptClaimed && machine.attempt.state != AttemptCancelRequested && machine.attempt.state != AttemptCancelAcked) ||
			machine.attempt.progressSequence == math.MaxUint64 ||
			frame.Progress.ProgressSequence != machine.attempt.progressSequence+1 {
			return protocolError(ErrorInvalidTransition)
		}
		machine.attempt.progressSequence = frame.Progress.ProgressSequence
	case MessageCancel:
		if direction != DirectionPlatformToWorker || !machine.hasFeature(FeatureCancellation) ||
			(machine.attempt.state != AttemptClaimPending && machine.attempt.state != AttemptClaimed && machine.attempt.state != AttemptTerminalPending) ||
			frame.Cancel.CancelRevision != 1 {
			return protocolError(ErrorInvalidTransition)
		}
		machine.attempt.cancelRevision, machine.attempt.cancelCode = frame.Cancel.CancelRevision, frame.Cancel.Code
		machine.attempt.terminalSequence, machine.attempt.terminalStatus = 0, ""
		machine.attempt.terminalResult, machine.attempt.terminalEvidence = "", nil
		if frame.Cancel.Code == CancelFenced {
			machine.attempt.state = AttemptFenced
		} else {
			machine.attempt.state = AttemptCancelRequested
		}
	case MessageCancelAck:
		if direction != DirectionWorkerToPlatform || !machine.hasFeature(FeatureCancellation) ||
			(machine.attempt.state != AttemptCancelRequested && machine.attempt.state != AttemptFenced) ||
			frame.CancelAck.CancelRevision != machine.attempt.cancelRevision {
			return protocolError(ErrorInvalidTransition)
		}
		if machine.attempt.state != AttemptFenced {
			machine.attempt.state = AttemptCancelAcked
		}
	case MessageTerminal:
		if direction != DirectionWorkerToPlatform ||
			(machine.attempt.state != AttemptClaimed && machine.attempt.state != AttemptCancelRequested &&
				machine.attempt.state != AttemptCancelAcked && machine.attempt.state != AttemptFenced) ||
			frame.Terminal.TerminalSequence != 1 ||
			(frame.Terminal.Status == TerminalCancelled && machine.attempt.cancelRevision == 0) {
			return protocolError(ErrorInvalidTransition)
		}
		if machine.attempt.cancelRevision != 0 && frame.Terminal.Status != TerminalCancelled {
			if machine.attempt.cancelCode == CancelFenced || machine.connection == ConnectionRevoking || machine.connection == ConnectionRevoked {
				return protocolError(ErrorInvalidTransition)
			}
			// A requested cancellation deterministically wins a crossed
			// non-cancelled outcome. The envelope is consumed, but the
			// outcome is deliberately not made committable.
			return nil
		}
		machine.attempt.terminalSequence = frame.Terminal.TerminalSequence
		machine.attempt.terminalStatus = frame.Terminal.Status
		machine.attempt.terminalResult = frame.Terminal.Result
		machine.attempt.terminalEvidence = append([]byte(nil), frame.Terminal.EvidenceDigest...)
		if machine.attempt.cancelCode != CancelFenced && machine.connection != ConnectionRevoking && machine.connection != ConnectionRevoked {
			machine.attempt.state = AttemptTerminalPending
		}
	case MessageTerminalAck:
		if direction != DirectionPlatformToWorker || machine.attempt.state != AttemptTerminalPending ||
			machine.connection == ConnectionRevoking || machine.connection == ConnectionRevoked ||
			frame.TerminalAck.TerminalSequence != machine.attempt.terminalSequence ||
			frame.TerminalAck.Status != machine.attempt.terminalStatus ||
			frame.TerminalAck.Result != machine.attempt.terminalResult ||
			!bytes.Equal(frame.TerminalAck.EvidenceDigest, machine.attempt.terminalEvidence) {
			return protocolError(ErrorConflict)
		}
		machine.attempt.state = AttemptTerminalCommitted
	default:
		return protocolError(ErrorInvalidTransition)
	}
	return nil
}

func (machine *ConformanceMachine) EraseCommittedAttempt() error {
	if machine == nil || machine.attempt.state != AttemptTerminalCommitted {
		return protocolError(ErrorInvalidTransition)
	}
	machine.attempt = attemptRecord{state: AttemptIdle}
	return nil
}

func attemptPayload(frame FrameV1) (AttemptBindingV1, uint64) {
	switch frame.Kind {
	case MessageLeaseOffer:
		return frame.LeaseOffer.Binding, frame.LeaseOffer.AttemptSequence
	case MessageLeaseClaim:
		return frame.LeaseClaim.Binding, frame.LeaseClaim.AttemptSequence
	case MessageLeaseAccepted:
		return frame.LeaseAccepted.Binding, frame.LeaseAccepted.AttemptSequence
	case MessageProgress:
		return frame.Progress.Binding, frame.Progress.AttemptSequence
	case MessageCancel:
		return frame.Cancel.Binding, frame.Cancel.AttemptSequence
	case MessageCancelAck:
		return frame.CancelAck.Binding, frame.CancelAck.AttemptSequence
	case MessageTerminal:
		return frame.Terminal.Binding, frame.Terminal.AttemptSequence
	case MessageTerminalAck:
		return frame.TerminalAck.Binding, frame.TerminalAck.AttemptSequence
	default:
		return AttemptBindingV1{}, 0
	}
}

func frameFingerprint(frame FrameV1) [sha256.Size]byte {
	encoded, _ := json.Marshal(frame)
	return sha256.Sum256(encoded)
}

// attemptFingerprint deliberately excludes the connection envelope. Exact
// attempt-message replay can therefore be acknowledged after reconnect while
// a divergent payload at the same attempt sequence is still a conflict.
func attemptFingerprint(frame FrameV1) [sha256.Size]byte {
	var payload any
	switch frame.Kind {
	case MessageLeaseOffer:
		payload = frame.LeaseOffer
	case MessageLeaseClaim:
		payload = frame.LeaseClaim
	case MessageLeaseAccepted:
		payload = frame.LeaseAccepted
	case MessageProgress:
		payload = frame.Progress
	case MessageCancel:
		payload = frame.Cancel
	case MessageCancelAck:
		payload = frame.CancelAck
	case MessageTerminal:
		payload = frame.Terminal
	case MessageTerminalAck:
		payload = frame.TerminalAck
	}
	encoded, _ := json.Marshal(struct {
		Kind    MessageKind `json:"kind"`
		Payload any         `json:"payload"`
	}{Kind: frame.Kind, Payload: payload})
	return sha256.Sum256(encoded)
}

func sameVersionOffer(left, right VersionOfferV1) bool {
	if left.Window != right.Window || len(left.Supported) != len(right.Supported) {
		return false
	}
	for index := range left.Supported {
		if left.Supported[index] != right.Supported[index] {
			return false
		}
	}
	return true
}

func negotiationMatchesChallenge(
	workerOffer, platformOffer VersionOfferV1,
	selected ProtocolVersion,
	workerNonce, platformNonce []byte,
	challenge ChallengeV1,
) bool {
	return sameVersionOffer(workerOffer, challenge.WorkerOffer) && sameVersionOffer(platformOffer, challenge.PlatformOffer) &&
		selected == challenge.SelectedVersion && bytes.Equal(workerNonce, challenge.WorkerNonce) &&
		bytes.Equal(platformNonce, challenge.PlatformNonce)
}

func acceptedMatchesChallenge(accepted AttachAcceptedV1, challenge ChallengeV1, capabilityDigest []byte) bool {
	return negotiationMatchesChallenge(accepted.WorkerOffer, accepted.PlatformOffer, accepted.SelectedVersion,
		accepted.WorkerNonce, accepted.PlatformNonce, challenge) && bytes.Equal(accepted.CapabilityDigest, capabilityDigest)
}

func sameAttemptBinding(left, right AttemptBindingV1) bool {
	return left.RunID == right.RunID && left.AttemptID == right.AttemptID && left.LeaseID == right.LeaseID &&
		left.LeaseGeneration == right.LeaseGeneration && left.FenceToken == right.FenceToken &&
		left.ExpiresAtUnixMicro == right.ExpiresAtUnixMicro && bytes.Equal(left.ContextDigest, right.ContextDigest) &&
		bytes.Equal(left.CapabilityDigest, right.CapabilityDigest) && bytes.Equal(left.PolicyDigest, right.PolicyDigest)
}

func cloneHello(value HelloV1) HelloV1 {
	value.Offer.Supported = append([]ProtocolVersion(nil), value.Offer.Supported...)
	value.WorkerNonce = append([]byte(nil), value.WorkerNonce...)
	return value
}

func cloneChallenge(value ChallengeV1) ChallengeV1 {
	value.WorkerOffer.Supported = append([]ProtocolVersion(nil), value.WorkerOffer.Supported...)
	value.PlatformOffer.Supported = append([]ProtocolVersion(nil), value.PlatformOffer.Supported...)
	value.WorkerNonce = append([]byte(nil), value.WorkerNonce...)
	value.PlatformNonce = append([]byte(nil), value.PlatformNonce...)
	return value
}

func cloneManifest(value CapabilityManifestV1) CapabilityManifestV1 {
	value.ProtocolOffer.Supported = append([]ProtocolVersion(nil), value.ProtocolOffer.Supported...)
	value.HarnessExecutableDigest = append([]byte(nil), value.HarnessExecutableDigest...)
	value.IsolationEvidence = append([]IsolationEvidenceV1(nil), value.IsolationEvidence...)
	value.Features = append([]ProtocolFeatureV1(nil), value.Features...)
	return value
}

func cloneBinding(value AttemptBindingV1) AttemptBindingV1 {
	value.ContextDigest = append([]byte(nil), value.ContextDigest...)
	value.CapabilityDigest = append([]byte(nil), value.CapabilityDigest...)
	value.PolicyDigest = append([]byte(nil), value.PolicyDigest...)
	return value
}

func (machine *ConformanceMachine) currentWatermarks() ConnectionWatermarksV1 {
	return ConnectionWatermarksV1{
		PlatformSequence: machine.platform.sequence, WorkerSequence: machine.worker.sequence,
		PlatformAck: machine.platform.ack, WorkerAck: machine.worker.ack,
	}
}

func (machine *ConformanceMachine) currentAttemptSummary() AttemptSummaryV1 {
	summary := AttemptSummaryV1{
		State: machine.attempt.state, Binding: cloneBinding(machine.attempt.binding),
		PlatformSequence: machine.attempt.platform.sequence, WorkerSequence: machine.attempt.worker.sequence,
		ProgressSequence: machine.attempt.progressSequence, CancelRevision: machine.attempt.cancelRevision,
		CancelCode: machine.attempt.cancelCode, TerminalSequence: machine.attempt.terminalSequence,
		TerminalStatus: machine.attempt.terminalStatus, TerminalResult: machine.attempt.terminalResult,
		TerminalEvidenceDigest: append([]byte(nil), machine.attempt.terminalEvidence...),
	}
	return sealAttemptSummary(summary)
}

func (machine *ConformanceMachine) applyAttemptSummary(summary AttemptSummaryV1) {
	machine.attempt.state = summary.State
	machine.attempt.binding = cloneBinding(summary.Binding)
	machine.attempt.platform.sequence = summary.PlatformSequence
	machine.attempt.worker.sequence = summary.WorkerSequence
	machine.attempt.progressSequence = summary.ProgressSequence
	machine.attempt.cancelRevision = summary.CancelRevision
	machine.attempt.cancelCode = summary.CancelCode
	machine.attempt.terminalSequence = summary.TerminalSequence
	machine.attempt.terminalStatus = summary.TerminalStatus
	machine.attempt.terminalResult = summary.TerminalResult
	machine.attempt.terminalEvidence = append([]byte(nil), summary.TerminalEvidenceDigest...)
	pruneAttemptFingerprints(&machine.attempt.platform, summary.PlatformSequence)
	pruneAttemptFingerprints(&machine.attempt.worker, summary.WorkerSequence)
}

func pruneAttemptFingerprints(state *attemptSequenceState, through uint64) {
	for sequence := range state.fingerprints {
		if sequence > through {
			delete(state.fingerprints, sequence)
		}
	}
}

func (machine *ConformanceMachine) installReplayCommitment(
	authoritative ReconnectSnapshotV1,
	workerClaim ReconnectSnapshotV1,
	plan ReplayPlanV1,
) error {
	if plan.TerminalDecision == ReconnectTerminalCommitted {
		machine.attempt.pendingWorkerTerminal = nil
		return nil
	}
	if plan.TerminalDecision == ReconnectTerminalNone {
		if machine.attempt.pendingWorkerTerminal != nil &&
			(machine.attempt.pendingWorkerTerminal.sequence <= authoritative.Attempt.WorkerSequence ||
				authoritative.Attempt.TerminalSequence > 0) {
			machine.attempt.pendingWorkerTerminal = nil
		}
		return nil
	}
	terminal := reconnectPendingTerminal(workerClaim)
	if terminal == nil {
		return protocolError(ErrorConflict)
	}
	next := &terminalReplayCommitment{
		kind: MessageTerminal, sequence: terminal.AttemptSequence, decision: plan.TerminalDecision,
		fingerprint: attemptFingerprint(FrameV1{Kind: MessageTerminal, Terminal: terminal}), terminal: *cloneTerminal(terminal),
	}
	if machine.attempt.pendingWorkerTerminal != nil &&
		(machine.attempt.pendingWorkerTerminal.sequence != next.sequence ||
			machine.attempt.pendingWorkerTerminal.fingerprint != next.fingerprint) {
		return protocolError(ErrorConflict)
	}
	machine.attempt.pendingWorkerTerminal = next
	return nil
}

func (machine *ConformanceMachine) pendingTerminalForSnapshot() *TerminalV1 {
	if machine.attempt.pendingWorkerTerminal == nil {
		return nil
	}
	return cloneTerminal(&machine.attempt.pendingWorkerTerminal.terminal)
}

func (machine *ConformanceMachine) hasFeature(feature ProtocolFeatureV1) bool {
	for _, candidate := range machine.manifest.Features {
		if candidate == feature {
			return true
		}
	}
	return false
}

func cloneAuthContext(auth AuthContextV1) AuthContextV1 {
	auth.IdentityPublicKey = append([]byte(nil), auth.IdentityPublicKey...)
	auth.ChannelBinding = append([]byte(nil), auth.ChannelBinding...)
	return auth
}

func cloneVersionOffer(offer VersionOfferV1) VersionOfferV1 {
	offer.Supported = append([]ProtocolVersion(nil), offer.Supported...)
	return offer
}

func cloneMachineConfig(config MachineConfig) MachineConfig {
	config.Auth = cloneAuthContext(config.Auth)
	config.WorkerOffer = cloneVersionOffer(config.WorkerOffer)
	config.PlatformOffer = cloneVersionOffer(config.PlatformOffer)
	config.ImplementedVersions = append([]ProtocolVersion(nil), config.ImplementedVersions...)
	return config
}

func sameReconnectAccepted(left, right ReconnectAcceptedV1) bool {
	return sameVersionOffer(left.WorkerOffer, right.WorkerOffer) && sameVersionOffer(left.PlatformOffer, right.PlatformOffer) &&
		left.SelectedVersion == right.SelectedVersion && bytes.Equal(left.WorkerNonce, right.WorkerNonce) &&
		bytes.Equal(left.PlatformNonce, right.PlatformNonce) && bytes.Equal(left.CapabilityDigest, right.CapabilityDigest) &&
		left.AuthoritativeWatermarks == right.AuthoritativeWatermarks &&
		sameAttemptSummary(left.AuthoritativeAttempt, right.AuthoritativeAttempt) &&
		sameOptionalTerminal(left.AuthoritativePendingTerminalReplay, right.AuthoritativePendingTerminalReplay) &&
		left.ReplayPlan == right.ReplayPlan
}

func sameOptionalTerminal(left, right *TerminalV1) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return sameTerminal(*left, *right)
}
