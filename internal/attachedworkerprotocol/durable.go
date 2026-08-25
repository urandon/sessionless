package attachedworkerprotocol

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MachineSnapshotVersionV1 uint32 = 1
	machineSnapshotDomainV1         = "sessionless.attached-worker.machine-snapshot.v1"
	machineConfigDomainV1           = "sessionless.attached-worker.machine-config.v1"
	machineFenceDomainV1            = "sessionless.attached-worker.fence.v1\x00"
)

// LeaseOfferAuthorityV1 is the store-authoritative input to a lease offer.
// LeaseGeneration is the numeric fence allocated transactionally by the
// store; the wire-visible opaque FenceToken, all sequences, acknowledgements,
// worker authority, and capability digest are derived by the reducer.
type LeaseOfferAuthorityV1 struct {
	RunID              string `json:"run_id"`
	AttemptID          string `json:"attempt_id"`
	LeaseID            string `json:"lease_id"`
	LeaseGeneration    uint64 `json:"lease_generation"`
	NowUnixMicro       int64  `json:"now_unix_micro"`
	ExpiresAtUnixMicro int64  `json:"expires_at_unix_micro"`
	ContextDigest      []byte `json:"context_digest"`
	PolicyDigest       []byte `json:"policy_digest"`
}

// CancelAuthorityV1 is selected by the authoritative store transaction.
// Revision is currently exactly one; the reducer derives all envelope,
// attempt-binding, and sequence fields from the durable machine snapshot.
type CancelAuthorityV1 struct {
	Revision     uint64     `json:"revision"`
	Code         CancelCode `json:"code"`
	NowUnixMicro int64      `json:"now_unix_micro"`
}

type LeaseAcceptedAuthorityV1 struct {
	NowUnixMicro int64 `json:"now_unix_micro"`
}

type TerminalAckAuthorityV1 struct {
	NowUnixMicro int64 `json:"now_unix_micro"`
}

// MachineEnvelopeSnapshotV1 is one direction of the connection envelope.
// Only the latest fingerprint is needed because connection-envelope replay is
// limited to the immediately current sequence.
type MachineEnvelopeSnapshotV1 struct {
	Sequence    uint64 `json:"sequence"`
	Ack         uint64 `json:"ack"`
	Fingerprint []byte `json:"fingerprint"`
}

type MachineAttemptFingerprintV1 struct {
	Sequence    uint64 `json:"sequence"`
	Fingerprint []byte `json:"fingerprint"`
}

// MachineAttemptDirectionSnapshotV1 retains the bounded attempt replay ledger.
// Fingerprints are strictly ordered so persistence has one canonical shape.
type MachineAttemptDirectionSnapshotV1 struct {
	Sequence     uint64                        `json:"sequence"`
	Fingerprints []MachineAttemptFingerprintV1 `json:"fingerprints"`
}

type MachinePendingTerminalSnapshotV1 struct {
	Kind        MessageKind               `json:"kind"`
	Sequence    uint64                    `json:"sequence"`
	Fingerprint []byte                    `json:"fingerprint"`
	Decision    ReconnectTerminalDecision `json:"decision"`
	Terminal    TerminalV1                `json:"terminal"`
}

type MachineAttemptSnapshotV1 struct {
	Summary               AttemptSummaryV1                  `json:"summary"`
	Platform              MachineAttemptDirectionSnapshotV1 `json:"platform"`
	Worker                MachineAttemptDirectionSnapshotV1 `json:"worker"`
	PendingWorkerTerminal *MachinePendingTerminalSnapshotV1 `json:"pending_worker_terminal,omitempty"`
}

// MachineReconnectStateV1 contains the authoritative pre-reconnect state and,
// once received, the bounded worker claim. Current authentication is supplied
// only by MachineConfig during restore.
type MachineReconnectStateV1 struct {
	Target                       ConnectionState        `json:"target"`
	PreviousConnectionGeneration uint64                 `json:"previous_connection_generation"`
	Watermarks                   ConnectionWatermarksV1 `json:"watermarks"`
	Attempt                      AttemptSummaryV1       `json:"attempt"`
	Claim                        *ReconnectSnapshotV1   `json:"claim,omitempty"`
}

// MachineSnapshotV1 is the bounded, versioned persistence form of one
// ConformanceMachine. ConfigurationDigest pins the authoritative MachineConfig
// without duplicating tenant, owner, key, channel, or negotiation configuration
// into mutable protocol state.
type MachineSnapshotV1 struct {
	Version             uint32                    `json:"version"`
	ConfigurationDigest []byte                    `json:"configuration_digest"`
	Connection          ConnectionState           `json:"connection"`
	Platform            MachineEnvelopeSnapshotV1 `json:"platform"`
	Worker              MachineEnvelopeSnapshotV1 `json:"worker"`
	Hello               *HelloV1                  `json:"hello,omitempty"`
	Challenge           *ChallengeV1              `json:"challenge,omitempty"`
	CapabilityDigest    []byte                    `json:"capability_digest"`
	Manifest            *CapabilityManifestV1     `json:"manifest,omitempty"`
	Attempt             MachineAttemptSnapshotV1  `json:"attempt"`
	DrainRevision       uint64                    `json:"drain_revision"`
	Revoke              *RevokeV1                 `json:"revoke,omitempty"`
	Reconnect           *MachineReconnectStateV1  `json:"reconnect,omitempty"`
	Digest              []byte                    `json:"digest"`
}

func (snapshot MachineSnapshotV1) Clone() MachineSnapshotV1 {
	return cloneMachineSnapshot(snapshot)
}

func (snapshot MachineSnapshotV1) Validate() error {
	if validateMachineSnapshotContent(snapshot) != nil || !validBytes(snapshot.Digest, sha256.Size) {
		return protocolError(ErrorMalformedFrame)
	}
	digest, err := machineSnapshotDigestUnchecked(snapshot)
	if err != nil || subtle.ConstantTimeCompare(digest, snapshot.Digest) != 1 {
		return protocolError(ErrorConflict)
	}
	return nil
}

// CanonicalMachineSnapshotBytesV1 returns the domain-separated bytes covered
// by MachineSnapshotV1.Digest. The Digest field itself is excluded.
func CanonicalMachineSnapshotBytesV1(snapshot MachineSnapshotV1) ([]byte, error) {
	if validateMachineSnapshotContent(snapshot) != nil {
		return nil, protocolError(ErrorMalformedFrame)
	}
	return canonicalMachineSnapshotBytesUnchecked(snapshot), nil
}

func MachineSnapshotDigestV1(snapshot MachineSnapshotV1) ([]byte, error) {
	if validateMachineSnapshotContent(snapshot) != nil {
		return nil, protocolError(ErrorMalformedFrame)
	}
	return machineSnapshotDigestUnchecked(snapshot)
}

// AttemptFrameFingerprintV1 is the connection-envelope-independent replay
// fingerprint used by ConformanceMachine for attempt messages.
func AttemptFrameFingerprintV1(frame FrameV1) ([]byte, error) {
	if frame.Validate() != nil || !isAttemptMessage(frame.Kind) {
		return nil, protocolError(ErrorInvalidFrame)
	}
	fingerprint := attemptFingerprint(frame)
	return append([]byte(nil), fingerprint[:]...), nil
}

// BuildInitialAttachSnapshotV1 reconstructs the exact initial conformance
// state from the signed Attach and the platform's canonical AttachAccepted.
// Hello and Challenge carry only transcript fields already bound by Attach;
// their envelope positions are fixed by the v1 initial handshake.
func BuildInitialAttachSnapshotV1(
	config MachineConfig,
	signedAttach FrameV1,
	accepted FrameV1,
) (MachineSnapshotV1, error) {
	if signedAttach.Kind != MessageAttach || signedAttach.Attach == nil ||
		accepted.Kind != MessageAttachAccepted || accepted.AttachAccepted == nil {
		return MachineSnapshotV1{}, protocolError(ErrorUnauthorized)
	}
	machine, err := NewConformanceMachine(config)
	if err != nil {
		return MachineSnapshotV1{}, protocolError(ErrorUnauthorized)
	}
	hello := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionWorkerToPlatform, 1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: 1, Ack: 0, Kind: MessageHello,
		Hello: &HelloV1{Offer: cloneVersionOffer(signedAttach.Attach.WorkerOffer),
			WorkerNonce: append([]byte(nil), signedAttach.Attach.WorkerNonce...)},
	}
	challenge := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, 1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: 1, Ack: 1, Kind: MessageChallenge,
		Challenge: &ChallengeV1{
			WorkerOffer: cloneVersionOffer(signedAttach.Attach.WorkerOffer), PlatformOffer: cloneVersionOffer(signedAttach.Attach.PlatformOffer),
			SelectedVersion: signedAttach.Attach.SelectedVersion, WorkerNonce: append([]byte(nil), signedAttach.Attach.WorkerNonce...),
			PlatformNonce: append([]byte(nil), signedAttach.Attach.PlatformNonce...),
		},
	}
	acceptance := AcceptanceContextV1{ChannelBinding: append([]byte(nil), config.Auth.ChannelBinding...), NowUnixMicro: 1}
	for _, transition := range []struct {
		direction Direction
		frame     FrameV1
	}{
		{DirectionWorkerToPlatform, hello}, {DirectionPlatformToWorker, challenge},
		{DirectionWorkerToPlatform, signedAttach}, {DirectionPlatformToWorker, accepted},
	} {
		if err := machine.Accept(transition.direction, transition.frame, acceptance); err != nil {
			return MachineSnapshotV1{}, protocolError(ErrorUnauthorized)
		}
	}
	if machine.connection != ConnectionAttached {
		return MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return machine.Snapshot()
}

// ApplyMachineFrameV1 is the persistence reducer for an already-built exact
// frame. Stores use it to atomically advance the canonical snapshot together
// with connection/attempt indexes; they must not update watermarks in parallel.
func ApplyMachineFrameV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
	direction Direction,
	frame FrameV1,
	nowUnixMicro int64,
) (MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil {
		return MachineSnapshotV1{}, err
	}
	if err := machine.Accept(direction, frame, AcceptanceContextV1{
		ChannelBinding: config.Auth.ChannelBinding, NowUnixMicro: nowUnixMicro,
	}); err != nil {
		return MachineSnapshotV1{}, err
	}
	return machine.Snapshot()
}

// EncodeMachineSnapshotV1 returns the single canonical JSON value used for
// bounded persistence. Decode rejects unknown fields, duplicate/case-colliding
// keys, nulls, non-canonical numbers, excessive depth, and trailing values.
func EncodeMachineSnapshotV1(snapshot MachineSnapshotV1) ([]byte, error) {
	if snapshot.Validate() != nil {
		return nil, protocolError(ErrorMalformedFrame)
	}
	snapshot = normalizeMachineSnapshotJSONSlices(snapshot.Clone())
	encoded, err := json.Marshal(snapshot)
	if err != nil || len(encoded) > MaxBatchBytes || preflightMachineSnapshotJSON(encoded) != nil {
		if len(encoded) > MaxBatchBytes {
			return nil, protocolError(ErrorFrameTooLarge)
		}
		return nil, protocolError(ErrorMalformedFrame)
	}
	return encoded, nil
}

func normalizeMachineSnapshotJSONSlices(snapshot MachineSnapshotV1) MachineSnapshotV1 {
	if snapshot.Platform.Fingerprint == nil {
		snapshot.Platform.Fingerprint = []byte{}
	}
	if snapshot.Worker.Fingerprint == nil {
		snapshot.Worker.Fingerprint = []byte{}
	}
	if snapshot.CapabilityDigest == nil {
		snapshot.CapabilityDigest = []byte{}
	}
	if snapshot.Attempt.Platform.Fingerprints == nil {
		snapshot.Attempt.Platform.Fingerprints = []MachineAttemptFingerprintV1{}
	}
	if snapshot.Attempt.Worker.Fingerprints == nil {
		snapshot.Attempt.Worker.Fingerprints = []MachineAttemptFingerprintV1{}
	}
	normalizeMachineSnapshotAttemptSummaryJSON(&snapshot.Attempt.Summary)
	if snapshot.Reconnect != nil {
		normalizeMachineSnapshotAttemptSummaryJSON(&snapshot.Reconnect.Attempt)
		if snapshot.Reconnect.Claim != nil {
			normalizeMachineSnapshotAttemptSummaryJSON(&snapshot.Reconnect.Claim.Attempt)
		}
	}
	return snapshot
}

func normalizeMachineSnapshotAttemptSummaryJSON(summary *AttemptSummaryV1) {
	if summary.Binding.ContextDigest == nil {
		summary.Binding.ContextDigest = []byte{}
	}
	if summary.Binding.CapabilityDigest == nil {
		summary.Binding.CapabilityDigest = []byte{}
	}
	if summary.Binding.PolicyDigest == nil {
		summary.Binding.PolicyDigest = []byte{}
	}
	if summary.TerminalEvidenceDigest == nil {
		summary.TerminalEvidenceDigest = []byte{}
	}
}

func DecodeMachineSnapshotV1(encoded []byte) (MachineSnapshotV1, error) {
	if len(encoded) == 0 || len(encoded) > MaxBatchBytes {
		if len(encoded) > MaxBatchBytes {
			return MachineSnapshotV1{}, protocolError(ErrorFrameTooLarge)
		}
		return MachineSnapshotV1{}, protocolError(ErrorMalformedFrame)
	}
	if !utf8.Valid(encoded) || bytes.HasPrefix(encoded, []byte{0xef, 0xbb, 0xbf}) ||
		preflightMachineSnapshotJSON(encoded) != nil {
		return MachineSnapshotV1{}, protocolError(ErrorMalformedFrame)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var snapshot MachineSnapshotV1
	if err := decoder.Decode(&snapshot); err != nil {
		return MachineSnapshotV1{}, protocolError(ErrorMalformedFrame)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) || snapshot.Validate() != nil {
		return MachineSnapshotV1{}, protocolError(ErrorMalformedFrame)
	}
	return snapshot.Clone(), nil
}

// BuildLeaseOfferTransitionV1 is the canonical persistence reducer for a new
// attached-worker attempt. It restores the one conformance machine, derives a
// frame without caller-controlled sequence or opaque fence fields, applies the
// existing transition logic, and returns the exact post-offer snapshot that
// must be committed atomically with the lease.
func BuildLeaseOfferTransitionV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
	authority LeaseOfferAuthorityV1,
) (FrameV1, MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil || machine.connection != ConnectionReady || machine.attempt.state != AttemptIdle ||
		authority.NowUnixMicro <= 0 || authority.ExpiresAtUnixMicro <= authority.NowUnixMicro ||
		machine.platform.sequence == math.MaxUint64 || machine.attempt.platform.sequence == math.MaxUint64 {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	binding := AttemptBindingV1{
		RunID: authority.RunID, AttemptID: authority.AttemptID, LeaseID: authority.LeaseID,
		LeaseGeneration:    authority.LeaseGeneration,
		FenceToken:         opaqueLeaseFenceTokenV1(config.Auth, authority),
		ExpiresAtUnixMicro: authority.ExpiresAtUnixMicro,
		ContextDigest:      append([]byte(nil), authority.ContextDigest...),
		CapabilityDigest:   append([]byte(nil), machine.capabilityDigest...),
		PolicyDigest:       append([]byte(nil), authority.PolicyDigest...),
	}
	attemptSequence := machine.attempt.platform.sequence + 1
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence+1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence + 1,
		Ack: machine.worker.sequence, Kind: MessageLeaseOffer,
		LeaseOffer: &LeaseOfferV1{Binding: binding, AttemptSequence: attemptSequence},
	}
	if frame.Validate() != nil || machine.Accept(DirectionPlatformToWorker, frame, AcceptanceContextV1{
		ChannelBinding: config.Auth.ChannelBinding, NowUnixMicro: authority.NowUnixMicro,
	}) != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	post, err := machine.Snapshot()
	if err != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return frame, post, nil
}

// BuildCancelTransitionV1 builds a normal cancellation or authoritative
// fence without accepting a caller-supplied frame. Cancellation deliberately
// remains applicable at the exact lease-expiry boundary and afterwards: a
// deadline transaction must be able to fence work after execution authority
// has expired.
func BuildCancelTransitionV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
	authority CancelAuthorityV1,
) (FrameV1, MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil || authority.Revision != 1 ||
		(authority.Code != CancelRequested && authority.Code != CancelFenced) || authority.NowUnixMicro <= 0 ||
		machine.platform.sequence == math.MaxUint64 || machine.attempt.platform.sequence == math.MaxUint64 {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	if machine.attempt.cancelRevision != 0 {
		if machine.attempt.cancelRevision != authority.Revision || machine.attempt.cancelCode != authority.Code {
			return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
		}
		frame, exact := exactPersistedCancelFrame(config, machine)
		if !exact {
			return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
		}
		return frame, snapshot.Clone(), nil
	}
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence+1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence + 1,
		Ack: machine.worker.sequence, Kind: MessageCancel,
		Cancel: &CancelV1{
			Binding: cloneBinding(machine.attempt.binding), AttemptSequence: machine.attempt.platform.sequence + 1,
			CancelRevision: authority.Revision, Code: authority.Code,
		},
	}
	if frame.Validate() != nil || machine.Accept(DirectionPlatformToWorker, frame, AcceptanceContextV1{
		ChannelBinding: config.Auth.ChannelBinding, NowUnixMicro: authority.NowUnixMicro,
	}) != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	post, err := machine.Snapshot()
	if err != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return frame, post, nil
}

// BuildLeaseAcceptedTransitionV1 derives the platform acknowledgement after
// the store has atomically applied an exact LeaseClaim and started the
// canonical job. Lease validity is exclusive at NowUnixMicro.
func BuildLeaseAcceptedTransitionV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
	authority LeaseAcceptedAuthorityV1,
) (FrameV1, MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil || authority.NowUnixMicro <= 0 || machine.platform.sequence == math.MaxUint64 ||
		machine.attempt.platform.sequence == math.MaxUint64 {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	if machine.attempt.state == AttemptClaimed {
		frame, exact := exactPersistedLeaseAcceptedFrame(config, machine)
		if !exact {
			return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
		}
		return frame, snapshot.Clone(), nil
	}
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence+1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence + 1,
		Ack: machine.worker.sequence, Kind: MessageLeaseAccepted,
		LeaseAccepted: &LeaseAcceptedV1{Binding: cloneBinding(machine.attempt.binding),
			AttemptSequence: machine.attempt.platform.sequence + 1},
	}
	if frame.Validate() != nil || machine.Accept(DirectionPlatformToWorker, frame, AcceptanceContextV1{
		ChannelBinding: config.Auth.ChannelBinding, NowUnixMicro: authority.NowUnixMicro,
	}) != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	post, err := machine.Snapshot()
	if err != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return frame, post, nil
}

// BuildTerminalAckTransitionV1 derives acknowledgement exclusively from the
// accepted terminal tuple in the snapshot. No result/evidence input is
// caller-controlled, and lease validity is exclusive at NowUnixMicro.
func BuildTerminalAckTransitionV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
	authority TerminalAckAuthorityV1,
) (FrameV1, MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil || authority.NowUnixMicro <= 0 || machine.platform.sequence == math.MaxUint64 ||
		machine.attempt.platform.sequence == math.MaxUint64 {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	if machine.attempt.state == AttemptTerminalCommitted {
		frame, exact := exactPersistedTerminalAckFrame(config, machine)
		if !exact {
			return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
		}
		return frame, snapshot.Clone(), nil
	}
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence+1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence + 1,
		Ack: machine.worker.sequence, Kind: MessageTerminalAck,
		TerminalAck: &TerminalAckV1{
			Binding: cloneBinding(machine.attempt.binding), AttemptSequence: machine.attempt.platform.sequence + 1,
			TerminalSequence: machine.attempt.terminalSequence, Status: machine.attempt.terminalStatus,
			Result: machine.attempt.terminalResult, EvidenceDigest: append([]byte(nil), machine.attempt.terminalEvidence...),
		},
	}
	if frame.Validate() != nil || machine.Accept(DirectionPlatformToWorker, frame, AcceptanceContextV1{
		ChannelBinding: config.Auth.ChannelBinding, NowUnixMicro: authority.NowUnixMicro,
	}) != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	post, err := machine.Snapshot()
	if err != nil {
		return FrameV1{}, MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return frame, post, nil
}

// RetireCommittedAttemptV1 removes the completed attempt from the canonical
// machine only after the worker has acknowledged the latest platform frame.
// The connection envelope and its replay fingerprints remain intact, so an
// exact duplicate heartbeat is still idempotent while the worker becomes
// eligible for a distinct next lease offer.
func RetireCommittedAttemptV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
) (MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil || machine.attempt.state != AttemptTerminalCommitted ||
		machine.worker.ack < machine.platform.sequence {
		return MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	if err := machine.EraseCommittedAttempt(); err != nil {
		return MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	post, err := machine.Snapshot()
	if err != nil {
		return MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return post, nil
}

// RetirePreClaimCancelledAttemptV1 removes an attempt after the durable
// authority has established that its lease was cancelled before claim and the
// worker has acknowledged that cancellation. The protocol snapshot alone
// cannot distinguish pre-claim from post-claim cancellation, so callers must
// make that distinction from their canonical attempt record before invoking
// this reducer.
func RetirePreClaimCancelledAttemptV1(
	config MachineConfig,
	snapshot MachineSnapshotV1,
) (MachineSnapshotV1, error) {
	machine, err := RestoreConformanceMachine(config, snapshot)
	if err != nil || machine.attempt.state != AttemptCancelAcked {
		return MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	machine.attempt = attemptRecord{state: AttemptIdle}
	post, err := machine.Snapshot()
	if err != nil {
		return MachineSnapshotV1{}, protocolError(ErrorConflict)
	}
	return post, nil
}

func exactPersistedCancelFrame(config MachineConfig, machine *ConformanceMachine) (FrameV1, bool) {
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence,
		Ack: machine.platform.ack, Kind: MessageCancel,
		Cancel: &CancelV1{Binding: cloneBinding(machine.attempt.binding), AttemptSequence: machine.attempt.platform.sequence,
			CancelRevision: machine.attempt.cancelRevision, Code: machine.attempt.cancelCode},
	}
	return exactPersistedPlatformAttemptFrame(machine, frame)
}

func exactPersistedLeaseAcceptedFrame(config MachineConfig, machine *ConformanceMachine) (FrameV1, bool) {
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence,
		Ack: machine.platform.ack, Kind: MessageLeaseAccepted,
		LeaseAccepted: &LeaseAcceptedV1{Binding: cloneBinding(machine.attempt.binding),
			AttemptSequence: machine.attempt.platform.sequence},
	}
	return exactPersistedPlatformAttemptFrame(machine, frame)
}

func exactPersistedTerminalAckFrame(config MachineConfig, machine *ConformanceMachine) (FrameV1, bool) {
	frame := FrameV1{
		Version: config.Auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, machine.platform.sequence),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration,
		ConnectionGeneration: config.Auth.ConnectionGeneration, Sequence: machine.platform.sequence,
		Ack: machine.platform.ack, Kind: MessageTerminalAck,
		TerminalAck: &TerminalAckV1{
			Binding: cloneBinding(machine.attempt.binding), AttemptSequence: machine.attempt.platform.sequence,
			TerminalSequence: machine.attempt.terminalSequence, Status: machine.attempt.terminalStatus,
			Result: machine.attempt.terminalResult, EvidenceDigest: append([]byte(nil), machine.attempt.terminalEvidence...),
		},
	}
	return exactPersistedPlatformAttemptFrame(machine, frame)
}

func exactPersistedPlatformAttemptFrame(machine *ConformanceMachine, frame FrameV1) (FrameV1, bool) {
	if machine == nil || frame.Validate() != nil || frame.Sequence == 0 ||
		machine.platform.fingerprint != frameFingerprint(frame) {
		return FrameV1{}, false
	}
	fingerprint, found := machine.attempt.platform.fingerprints[attemptSequence(frame)]
	if !found || fingerprint != attemptFingerprint(frame) {
		return FrameV1{}, false
	}
	return frame, true
}

func attemptSequence(frame FrameV1) uint64 {
	_, sequence := attemptPayload(frame)
	return sequence
}

func opaqueLeaseFenceTokenV1(auth AuthContextV1, authority LeaseOfferAuthorityV1) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(machineFenceDomainV1))
	for _, value := range []string{auth.TenantID, auth.OwnerUserID, auth.WorkerID,
		authority.RunID, authority.AttemptID, authority.LeaseID} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	var numeric [8]byte
	binary.BigEndian.PutUint64(numeric[:], authority.LeaseGeneration)
	_, _ = hash.Write(numeric[:])
	return hex.EncodeToString(hash.Sum(nil))
}

func (machine *ConformanceMachine) Snapshot() (MachineSnapshotV1, error) {
	if machine == nil {
		return MachineSnapshotV1{}, protocolError(ErrorProtocolViolation)
	}
	configurationDigest, err := machineConfigDigestV1(machine.config)
	if err != nil {
		return MachineSnapshotV1{}, err
	}
	snapshot := MachineSnapshotV1{
		Version: MachineSnapshotVersionV1, ConfigurationDigest: configurationDigest,
		Connection: machine.connection,
		Platform:   snapshotEnvelope(machine.platform), Worker: snapshotEnvelope(machine.worker),
		CapabilityDigest: append([]byte(nil), machine.capabilityDigest...),
		Attempt: MachineAttemptSnapshotV1{
			Summary: machine.currentAttemptSummary(), Platform: snapshotAttemptDirection(machine.attempt.platform),
			Worker: snapshotAttemptDirection(machine.attempt.worker),
		},
		DrainRevision: machine.drainRevision,
	}
	if machine.hello.Validate() == nil {
		hello := cloneHello(machine.hello)
		snapshot.Hello = &hello
	}
	if machine.challenge.Validate() == nil {
		challenge := cloneChallenge(machine.challenge)
		snapshot.Challenge = &challenge
	}
	if machine.manifest.Validate() == nil {
		manifest := cloneManifest(machine.manifest)
		snapshot.Manifest = &manifest
	}
	if machine.revoke.Validate() == nil {
		revoke := machine.revoke
		snapshot.Revoke = &revoke
	}
	if commitment := machine.attempt.pendingWorkerTerminal; commitment != nil {
		snapshot.Attempt.PendingWorkerTerminal = &MachinePendingTerminalSnapshotV1{
			Kind: commitment.kind, Sequence: commitment.sequence,
			Fingerprint: append([]byte(nil), commitment.fingerprint[:]...), Decision: commitment.decision,
			Terminal: *cloneTerminal(&commitment.terminal),
		}
	}
	if machine.reconnecting {
		reconnect := &MachineReconnectStateV1{
			Target: machine.reconnectTarget, PreviousConnectionGeneration: machine.previousConnectionGeneration,
			Watermarks: machine.reconnectWatermarks, Attempt: cloneAttemptSummary(machine.reconnectAttempt),
		}
		if machine.reconnectClaim.Validate() == nil {
			claim := cloneReconnectSnapshot(machine.reconnectClaim)
			reconnect.Claim = &claim
		}
		snapshot.Reconnect = reconnect
	}
	digest, err := MachineSnapshotDigestV1(snapshot)
	if err != nil {
		return MachineSnapshotV1{}, err
	}
	snapshot.Digest = digest
	return cloneMachineSnapshot(snapshot), nil
}

func RestoreConformanceMachine(config MachineConfig, snapshot MachineSnapshotV1) (*ConformanceMachine, error) {
	snapshot = cloneMachineSnapshot(snapshot)
	if snapshot.Validate() != nil {
		return nil, protocolError(ErrorConflict)
	}
	configurationDigest, err := machineConfigDigestV1(config)
	if err != nil || subtle.ConstantTimeCompare(configurationDigest, snapshot.ConfigurationDigest) != 1 {
		return nil, protocolError(ErrorUnauthorized)
	}
	machine, err := NewConformanceMachine(config)
	if err != nil || validateMachineSnapshotAgainstConfig(snapshot, machine.config) != nil {
		return nil, protocolError(ErrorUnauthorized)
	}
	machine.connection = snapshot.Connection
	machine.platform = restoreEnvelope(snapshot.Platform)
	machine.worker = restoreEnvelope(snapshot.Worker)
	if snapshot.Hello != nil {
		machine.hello = cloneHello(*snapshot.Hello)
	}
	if snapshot.Challenge != nil {
		machine.challenge = cloneChallenge(*snapshot.Challenge)
	}
	machine.capabilityDigest = append([]byte(nil), snapshot.CapabilityDigest...)
	if snapshot.Manifest != nil {
		machine.manifest = cloneManifest(*snapshot.Manifest)
	}
	machine.applyAttemptSummary(snapshot.Attempt.Summary)
	machine.attempt.platform = restoreAttemptDirection(snapshot.Attempt.Platform)
	machine.attempt.worker = restoreAttemptDirection(snapshot.Attempt.Worker)
	if pending := snapshot.Attempt.PendingWorkerTerminal; pending != nil {
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], pending.Fingerprint)
		machine.attempt.pendingWorkerTerminal = &terminalReplayCommitment{
			kind: pending.Kind, sequence: pending.Sequence, fingerprint: fingerprint,
			decision: pending.Decision, terminal: *cloneTerminal(&pending.Terminal),
		}
	}
	machine.drainRevision = snapshot.DrainRevision
	if snapshot.Revoke != nil {
		machine.revoke = *snapshot.Revoke
	}
	if snapshot.Reconnect != nil {
		machine.reconnecting = true
		machine.reconnectTarget = snapshot.Reconnect.Target
		machine.previousConnectionGeneration = snapshot.Reconnect.PreviousConnectionGeneration
		machine.reconnectWatermarks = snapshot.Reconnect.Watermarks
		machine.reconnectAttempt = cloneAttemptSummary(snapshot.Reconnect.Attempt)
		if snapshot.Reconnect.Claim != nil {
			machine.reconnectClaim = cloneReconnectSnapshot(*snapshot.Reconnect.Claim)
		}
	}
	restored, err := machine.Snapshot()
	if err != nil || subtle.ConstantTimeCompare(restored.Digest, snapshot.Digest) != 1 {
		return nil, protocolError(ErrorConflict)
	}
	return machine, nil
}

func validateMachineSnapshotContent(snapshot MachineSnapshotV1) error {
	if snapshot.Version != MachineSnapshotVersionV1 || !validBytes(snapshot.ConfigurationDigest, sha256.Size) ||
		!validMachineConnectionState(snapshot.Connection) || validateEnvelopeSnapshot(snapshot.Platform) != nil ||
		validateEnvelopeSnapshot(snapshot.Worker) != nil || snapshot.Platform.Ack > snapshot.Worker.Sequence ||
		snapshot.Worker.Ack > snapshot.Platform.Sequence || snapshot.Attempt.Summary.Validate() != nil ||
		validateAttemptDirectionSnapshot(snapshot.Attempt.Platform) != nil ||
		validateAttemptDirectionSnapshot(snapshot.Attempt.Worker) != nil ||
		snapshot.Attempt.Summary.PlatformSequence != snapshot.Attempt.Platform.Sequence ||
		snapshot.Attempt.Summary.WorkerSequence != snapshot.Attempt.Worker.Sequence {
		return protocolError(ErrorMalformedFrame)
	}
	if snapshot.Hello != nil && snapshot.Hello.Validate() != nil || snapshot.Challenge != nil && snapshot.Challenge.Validate() != nil ||
		snapshot.Manifest != nil && snapshot.Manifest.Validate() != nil || snapshot.Revoke != nil && snapshot.Revoke.Validate() != nil {
		return protocolError(ErrorMalformedFrame)
	}
	if snapshot.Challenge != nil && (snapshot.Hello == nil ||
		!sameVersionOffer(snapshot.Challenge.WorkerOffer, snapshot.Hello.Offer) ||
		!bytes.Equal(snapshot.Challenge.WorkerNonce, snapshot.Hello.WorkerNonce)) {
		return protocolError(ErrorMalformedFrame)
	}
	if snapshot.Manifest != nil {
		manifestDigest, err := ManifestDigestV1(*snapshot.Manifest)
		if err != nil || !bytes.Equal(manifestDigest, snapshot.CapabilityDigest) {
			return protocolError(ErrorMalformedFrame)
		}
	}
	if len(snapshot.CapabilityDigest) != 0 && !validBytes(snapshot.CapabilityDigest, sha256.Size) {
		return protocolError(ErrorMalformedFrame)
	}
	if snapshot.Attempt.Summary.State != AttemptIdle {
		if snapshot.Manifest == nil || !bytes.Equal(snapshot.Attempt.Summary.Binding.CapabilityDigest, snapshot.CapabilityDigest) {
			return protocolError(ErrorMalformedFrame)
		}
	}
	if validatePendingTerminalSnapshot(snapshot.Attempt.PendingWorkerTerminal, snapshot.Attempt) != nil {
		return protocolError(ErrorMalformedFrame)
	}
	if snapshot.Reconnect != nil && validateMachineReconnectState(*snapshot.Reconnect, snapshot.Attempt.PendingWorkerTerminal) != nil {
		return protocolError(ErrorMalformedFrame)
	}
	hasRevoke := snapshot.Revoke != nil
	expectsRevoke := snapshot.Connection == ConnectionRevoking || snapshot.Connection == ConnectionRevoked
	if hasRevoke != expectsRevoke {
		return protocolError(ErrorMalformedFrame)
	}
	if snapshot.Connection == ConnectionDraining || snapshot.Connection == ConnectionDrained {
		if snapshot.DrainRevision == 0 {
			return protocolError(ErrorMalformedFrame)
		}
	}
	if snapshot.Connection == ConnectionUnattached && snapshot.Reconnect == nil {
		if snapshot.Platform.Sequence != 0 || snapshot.Worker.Sequence != 0 || snapshot.Hello != nil || snapshot.Challenge != nil ||
			len(snapshot.CapabilityDigest) != 0 || snapshot.Manifest != nil || snapshot.Attempt.Summary.State != AttemptIdle ||
			snapshot.DrainRevision != 0 || snapshot.Revoke != nil {
			return protocolError(ErrorMalformedFrame)
		}
	}
	switch snapshot.Connection {
	case ConnectionHello:
		if snapshot.Hello == nil || snapshot.Challenge != nil {
			return protocolError(ErrorMalformedFrame)
		}
	case ConnectionChallenged:
		if snapshot.Hello == nil || snapshot.Challenge == nil {
			return protocolError(ErrorMalformedFrame)
		}
	case ConnectionAttachPending:
		if snapshot.Reconnect != nil || snapshot.Hello == nil || snapshot.Challenge == nil ||
			!validBytes(snapshot.CapabilityDigest, sha256.Size) || snapshot.Manifest != nil {
			return protocolError(ErrorMalformedFrame)
		}
	case ConnectionReconnectPending:
		if snapshot.Reconnect == nil || snapshot.Reconnect.Claim == nil || snapshot.Hello == nil || snapshot.Challenge == nil ||
			snapshot.Manifest == nil || !validBytes(snapshot.CapabilityDigest, sha256.Size) {
			return protocolError(ErrorMalformedFrame)
		}
	case ConnectionAttached:
		if snapshot.Hello == nil || snapshot.Challenge == nil || !validBytes(snapshot.CapabilityDigest, sha256.Size) ||
			(snapshot.Reconnect == nil) != (snapshot.Manifest == nil) {
			return protocolError(ErrorMalformedFrame)
		}
	case ConnectionReady, ConnectionDraining, ConnectionDrained:
		if snapshot.Reconnect != nil || snapshot.Hello == nil || snapshot.Challenge == nil || snapshot.Manifest == nil ||
			!validBytes(snapshot.CapabilityDigest, sha256.Size) {
			return protocolError(ErrorMalformedFrame)
		}
	}
	return nil
}

func validateMachineSnapshotAgainstConfig(snapshot MachineSnapshotV1, config MachineConfig) error {
	if snapshot.Hello != nil && !sameVersionOffer(snapshot.Hello.Offer, config.WorkerOffer) {
		return protocolError(ErrorUnauthorized)
	}
	if snapshot.Challenge != nil {
		if !sameVersionOffer(snapshot.Challenge.WorkerOffer, config.WorkerOffer) ||
			!sameVersionOffer(snapshot.Challenge.PlatformOffer, config.PlatformOffer) ||
			snapshot.Challenge.SelectedVersion != config.Auth.Version || snapshot.Hello == nil ||
			!bytes.Equal(snapshot.Challenge.WorkerNonce, snapshot.Hello.WorkerNonce) {
			return protocolError(ErrorUnauthorized)
		}
	}
	if snapshot.Manifest != nil {
		digest, err := ManifestDigestV1(*snapshot.Manifest)
		if err != nil || snapshot.Manifest.WorkerID != config.Auth.WorkerID ||
			snapshot.Manifest.EnrollmentGeneration != config.Auth.EnrollmentGeneration ||
			!sameVersionOffer(snapshot.Manifest.ProtocolOffer, config.WorkerOffer) ||
			!bytes.Equal(digest, snapshot.CapabilityDigest) {
			return protocolError(ErrorUnauthorized)
		}
	}
	if snapshot.Revoke != nil && (config.Auth.EnrollmentGeneration == math.MaxUint64 || config.Auth.ConnectionGeneration == math.MaxUint64 ||
		snapshot.Revoke.NextEnrollmentGeneration != config.Auth.EnrollmentGeneration+1 ||
		snapshot.Revoke.NextConnectionGeneration != config.Auth.ConnectionGeneration+1) {
		return protocolError(ErrorUnauthorized)
	}
	if snapshot.Reconnect != nil && (config.Auth.ConnectionGeneration == 0 ||
		snapshot.Reconnect.PreviousConnectionGeneration == math.MaxUint64 ||
		snapshot.Reconnect.PreviousConnectionGeneration+1 != config.Auth.ConnectionGeneration) {
		return protocolError(ErrorUnauthorized)
	}
	return nil
}

func validateEnvelopeSnapshot(snapshot MachineEnvelopeSnapshotV1) error {
	if snapshot.Sequence == 0 {
		if snapshot.Ack != 0 || len(snapshot.Fingerprint) != 0 {
			return protocolError(ErrorMalformedFrame)
		}
		return nil
	}
	if !validBytes(snapshot.Fingerprint, sha256.Size) {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func validateAttemptDirectionSnapshot(snapshot MachineAttemptDirectionSnapshotV1) error {
	if snapshot.Sequence > MaxAttemptMessages || len(snapshot.Fingerprints) > MaxAttemptMessages {
		return protocolError(ErrorMalformedFrame)
	}
	var previous uint64
	for _, item := range snapshot.Fingerprints {
		if item.Sequence == 0 || item.Sequence <= previous || item.Sequence > snapshot.Sequence ||
			!validBytes(item.Fingerprint, sha256.Size) {
			return protocolError(ErrorMalformedFrame)
		}
		previous = item.Sequence
	}
	return nil
}

func validatePendingTerminalSnapshot(pending *MachinePendingTerminalSnapshotV1, attempt MachineAttemptSnapshotV1) error {
	if pending == nil {
		return nil
	}
	if pending.Kind != MessageTerminal || pending.Terminal.Validate() != nil || pending.Sequence == 0 ||
		pending.Sequence != pending.Terminal.AttemptSequence ||
		(pending.Decision != ReconnectTerminalReplay && pending.Decision != ReconnectTerminalDiscard) ||
		!sameAttemptBinding(pending.Terminal.Binding, attempt.Summary.Binding) || !validBytes(pending.Fingerprint, sha256.Size) {
		return protocolError(ErrorMalformedFrame)
	}
	expected := attemptFingerprint(FrameV1{Kind: MessageTerminal, Terminal: &pending.Terminal})
	if subtle.ConstantTimeCompare(expected[:], pending.Fingerprint) != 1 {
		return protocolError(ErrorConflict)
	}
	if pending.Decision == ReconnectTerminalReplay && pending.Sequence <= attempt.Worker.Sequence ||
		pending.Decision == ReconnectTerminalDiscard && pending.Sequence > attempt.Worker.Sequence {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func validateMachineReconnectState(reconnect MachineReconnectStateV1, pending *MachinePendingTerminalSnapshotV1) error {
	if reconnect.Target != ConnectionReady && reconnect.Target != ConnectionDraining ||
		reconnect.PreviousConnectionGeneration == 0 || reconnect.Watermarks.Validate() != nil || reconnect.Attempt.Validate() != nil {
		return protocolError(ErrorMalformedFrame)
	}
	if reconnect.Claim != nil {
		if reconnect.Claim.Validate() != nil || reconnect.Claim.PreviousConnectionGeneration != reconnect.PreviousConnectionGeneration {
			return protocolError(ErrorMalformedFrame)
		}
		authoritative := sealReconnectSnapshot(ReconnectSnapshotV1{
			PreviousConnectionGeneration: reconnect.PreviousConnectionGeneration,
			Watermarks:                   reconnect.Watermarks,
			Attempt:                      reconnect.Attempt,
		})
		if pending != nil {
			authoritative.PendingTerminalReplay = cloneTerminal(&pending.Terminal)
			authoritative = sealReconnectSnapshot(authoritative)
		}
		if !reconnectClaimsCompatible(*reconnect.Claim, authoritative) {
			return protocolError(ErrorConflict)
		}
	}
	return nil
}

func validMachineConnectionState(state ConnectionState) bool {
	switch state {
	case ConnectionUnattached, ConnectionHello, ConnectionChallenged, ConnectionAttachPending,
		ConnectionReconnectPending, ConnectionAttached, ConnectionReady, ConnectionDraining,
		ConnectionDrained, ConnectionRevoking, ConnectionRevoked:
		return true
	default:
		return false
	}
}

func isAttemptMessage(kind MessageKind) bool {
	switch kind {
	case MessageLeaseOffer, MessageLeaseClaim, MessageLeaseAccepted, MessageProgress,
		MessageCancel, MessageCancelAck, MessageTerminal, MessageTerminalAck:
		return true
	default:
		return false
	}
}

func machineConfigDigestV1(config MachineConfig) ([]byte, error) {
	config = cloneMachineConfig(config)
	if config.Auth.Validate() != nil || config.WorkerOffer.Validate() != nil || config.PlatformOffer.Validate() != nil {
		return nil, protocolError(ErrorUnauthorized)
	}
	selected, err := NegotiateOffers(config.WorkerOffer, config.PlatformOffer, config.ImplementedVersions)
	if err != nil || selected != config.Auth.Version {
		return nil, protocolError(ErrorUnauthorized)
	}
	result := appendCanonicalField(nil, []byte(machineConfigDomainV1))
	result = appendCanonicalField(result, []byte(config.Auth.TenantID))
	result = appendCanonicalField(result, []byte(config.Auth.OwnerUserID))
	result = appendCanonicalField(result, []byte(config.Auth.WorkerID))
	result = appendCanonicalField(result, config.Auth.IdentityPublicKey)
	result = appendCanonicalUint64(result, config.Auth.EnrollmentGeneration)
	result = appendCanonicalUint64(result, config.Auth.ConnectionGeneration)
	result = appendCanonicalUint32(result, uint32(config.Auth.Version))
	result = appendCanonicalField(result, config.Auth.ChannelBinding)
	result = appendVersionOffer(result, config.WorkerOffer)
	result = appendVersionOffer(result, config.PlatformOffer)
	result = appendCanonicalUint32(result, uint32(len(config.ImplementedVersions)))
	for _, version := range config.ImplementedVersions {
		result = appendCanonicalUint32(result, uint32(version))
	}
	digest := sha256.Sum256(result)
	return append([]byte(nil), digest[:]...), nil
}

func machineSnapshotDigestUnchecked(snapshot MachineSnapshotV1) ([]byte, error) {
	digest := sha256.Sum256(canonicalMachineSnapshotBytesUnchecked(snapshot))
	return append([]byte(nil), digest[:]...), nil
}

func canonicalMachineSnapshotBytesUnchecked(snapshot MachineSnapshotV1) []byte {
	result := appendCanonicalField(nil, []byte(machineSnapshotDomainV1))
	result = appendCanonicalUint32(result, snapshot.Version)
	result = appendCanonicalField(result, snapshot.ConfigurationDigest)
	result = appendCanonicalField(result, []byte(snapshot.Connection))
	result = appendEnvelopeSnapshot(result, snapshot.Platform)
	result = appendEnvelopeSnapshot(result, snapshot.Worker)
	result = appendOptionalHello(result, snapshot.Hello)
	result = appendOptionalChallenge(result, snapshot.Challenge)
	result = appendCanonicalField(result, snapshot.CapabilityDigest)
	if snapshot.Manifest == nil {
		result = appendCanonicalUint32(result, 0)
	} else {
		manifest, _ := CanonicalManifestBytesV1(*snapshot.Manifest)
		result = appendCanonicalUint32(result, 1)
		result = appendCanonicalField(result, manifest)
	}
	result = appendCanonicalField(result, snapshot.Attempt.Summary.Digest)
	result = appendAttemptDirectionSnapshot(result, snapshot.Attempt.Platform)
	result = appendAttemptDirectionSnapshot(result, snapshot.Attempt.Worker)
	if pending := snapshot.Attempt.PendingWorkerTerminal; pending == nil {
		result = appendCanonicalUint32(result, 0)
	} else {
		result = appendCanonicalUint32(result, 1)
		result = appendCanonicalField(result, []byte(pending.Kind))
		result = appendCanonicalUint64(result, pending.Sequence)
		result = appendCanonicalField(result, pending.Fingerprint)
		result = appendCanonicalField(result, []byte(pending.Decision))
	}
	result = appendCanonicalUint64(result, snapshot.DrainRevision)
	if snapshot.Revoke == nil {
		result = appendCanonicalUint32(result, 0)
	} else {
		result = appendCanonicalUint32(result, 1)
		result = appendCanonicalUint64(result, snapshot.Revoke.Revision)
		result = appendCanonicalUint64(result, snapshot.Revoke.NextEnrollmentGeneration)
		result = appendCanonicalUint64(result, snapshot.Revoke.NextConnectionGeneration)
	}
	if reconnect := snapshot.Reconnect; reconnect == nil {
		result = appendCanonicalUint32(result, 0)
	} else {
		result = appendCanonicalUint32(result, 1)
		result = appendCanonicalField(result, []byte(reconnect.Target))
		result = appendCanonicalUint64(result, reconnect.PreviousConnectionGeneration)
		result = appendMachineSnapshotWatermarks(result, reconnect.Watermarks)
		result = appendCanonicalField(result, reconnect.Attempt.Digest)
		if reconnect.Claim == nil {
			result = appendCanonicalUint32(result, 0)
		} else {
			result = appendCanonicalUint32(result, 1)
			result = appendCanonicalField(result, reconnect.Claim.Digest)
		}
	}
	return result
}

func appendEnvelopeSnapshot(result []byte, snapshot MachineEnvelopeSnapshotV1) []byte {
	result = appendCanonicalUint64(result, snapshot.Sequence)
	result = appendCanonicalUint64(result, snapshot.Ack)
	return appendCanonicalField(result, snapshot.Fingerprint)
}

func appendAttemptDirectionSnapshot(result []byte, snapshot MachineAttemptDirectionSnapshotV1) []byte {
	result = appendCanonicalUint64(result, snapshot.Sequence)
	result = appendCanonicalUint32(result, uint32(len(snapshot.Fingerprints)))
	for _, item := range snapshot.Fingerprints {
		result = appendCanonicalUint64(result, item.Sequence)
		result = appendCanonicalField(result, item.Fingerprint)
	}
	return result
}

func appendOptionalHello(result []byte, hello *HelloV1) []byte {
	if hello == nil {
		return appendCanonicalUint32(result, 0)
	}
	result = appendCanonicalUint32(result, 1)
	result = appendVersionOffer(result, hello.Offer)
	return appendCanonicalField(result, hello.WorkerNonce)
}

func appendOptionalChallenge(result []byte, challenge *ChallengeV1) []byte {
	if challenge == nil {
		return appendCanonicalUint32(result, 0)
	}
	result = appendCanonicalUint32(result, 1)
	result = appendVersionOffer(result, challenge.WorkerOffer)
	result = appendVersionOffer(result, challenge.PlatformOffer)
	result = appendCanonicalUint32(result, uint32(challenge.SelectedVersion))
	result = appendCanonicalField(result, challenge.WorkerNonce)
	return appendCanonicalField(result, challenge.PlatformNonce)
}

func appendMachineSnapshotWatermarks(result []byte, watermarks ConnectionWatermarksV1) []byte {
	result = appendCanonicalUint64(result, watermarks.PlatformSequence)
	result = appendCanonicalUint64(result, watermarks.WorkerSequence)
	result = appendCanonicalUint64(result, watermarks.PlatformAck)
	return appendCanonicalUint64(result, watermarks.WorkerAck)
}

func snapshotEnvelope(state sequenceState) MachineEnvelopeSnapshotV1 {
	snapshot := MachineEnvelopeSnapshotV1{Sequence: state.sequence, Ack: state.ack}
	if state.sequence != 0 {
		snapshot.Fingerprint = append([]byte(nil), state.fingerprint[:]...)
	}
	return snapshot
}

func snapshotAttemptDirection(state attemptSequenceState) MachineAttemptDirectionSnapshotV1 {
	snapshot := MachineAttemptDirectionSnapshotV1{Sequence: state.sequence}
	sequences := make([]uint64, 0, len(state.fingerprints))
	for sequence := range state.fingerprints {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(left, right int) bool { return sequences[left] < sequences[right] })
	for _, sequence := range sequences {
		fingerprint := state.fingerprints[sequence]
		snapshot.Fingerprints = append(snapshot.Fingerprints, MachineAttemptFingerprintV1{
			Sequence: sequence, Fingerprint: append([]byte(nil), fingerprint[:]...),
		})
	}
	return snapshot
}

func restoreEnvelope(snapshot MachineEnvelopeSnapshotV1) sequenceState {
	state := sequenceState{sequence: snapshot.Sequence, ack: snapshot.Ack}
	copy(state.fingerprint[:], snapshot.Fingerprint)
	return state
}

func restoreAttemptDirection(snapshot MachineAttemptDirectionSnapshotV1) attemptSequenceState {
	state := attemptSequenceState{sequence: snapshot.Sequence}
	if len(snapshot.Fingerprints) != 0 {
		state.fingerprints = make(map[uint64][sha256.Size]byte, len(snapshot.Fingerprints))
	}
	for _, item := range snapshot.Fingerprints {
		var fingerprint [sha256.Size]byte
		copy(fingerprint[:], item.Fingerprint)
		state.fingerprints[item.Sequence] = fingerprint
	}
	return state
}

func cloneMachineSnapshot(snapshot MachineSnapshotV1) MachineSnapshotV1 {
	snapshot.ConfigurationDigest = append([]byte(nil), snapshot.ConfigurationDigest...)
	snapshot.Platform.Fingerprint = append([]byte(nil), snapshot.Platform.Fingerprint...)
	snapshot.Worker.Fingerprint = append([]byte(nil), snapshot.Worker.Fingerprint...)
	if snapshot.Hello != nil {
		hello := cloneHello(*snapshot.Hello)
		snapshot.Hello = &hello
	}
	if snapshot.Challenge != nil {
		challenge := cloneChallenge(*snapshot.Challenge)
		snapshot.Challenge = &challenge
	}
	snapshot.CapabilityDigest = append([]byte(nil), snapshot.CapabilityDigest...)
	if snapshot.Manifest != nil {
		manifest := cloneManifest(*snapshot.Manifest)
		snapshot.Manifest = &manifest
	}
	snapshot.Attempt.Summary = cloneAttemptSummary(snapshot.Attempt.Summary)
	snapshot.Attempt.Platform = cloneAttemptDirectionSnapshot(snapshot.Attempt.Platform)
	snapshot.Attempt.Worker = cloneAttemptDirectionSnapshot(snapshot.Attempt.Worker)
	if snapshot.Attempt.PendingWorkerTerminal != nil {
		pending := *snapshot.Attempt.PendingWorkerTerminal
		pending.Fingerprint = append([]byte(nil), pending.Fingerprint...)
		pending.Terminal = *cloneTerminal(&pending.Terminal)
		snapshot.Attempt.PendingWorkerTerminal = &pending
	}
	if snapshot.Revoke != nil {
		revoke := *snapshot.Revoke
		snapshot.Revoke = &revoke
	}
	if snapshot.Reconnect != nil {
		reconnect := *snapshot.Reconnect
		reconnect.Attempt = cloneAttemptSummary(reconnect.Attempt)
		if reconnect.Claim != nil {
			claim := cloneReconnectSnapshot(*reconnect.Claim)
			reconnect.Claim = &claim
		}
		snapshot.Reconnect = &reconnect
	}
	snapshot.Digest = append([]byte(nil), snapshot.Digest...)
	return snapshot
}

func cloneAttemptDirectionSnapshot(snapshot MachineAttemptDirectionSnapshotV1) MachineAttemptDirectionSnapshotV1 {
	items := make([]MachineAttemptFingerprintV1, len(snapshot.Fingerprints))
	for index, item := range snapshot.Fingerprints {
		items[index] = MachineAttemptFingerprintV1{Sequence: item.Sequence, Fingerprint: append([]byte(nil), item.Fingerprint...)}
	}
	snapshot.Fingerprints = items
	return snapshot
}

func preflightMachineSnapshotJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanMachineSnapshotJSONValue(decoder, 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func scanMachineSnapshotJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return protocolError(ErrorMalformedFrame)
	}
	token, err := decoder.Token()
	if err != nil {
		return protocolError(ErrorMalformedFrame)
	}
	switch token := token.(type) {
	case nil:
		return protocolError(ErrorMalformedFrame)
	case string:
		// Snapshot byte strings include base64-encoded bounded manifests and
		// replay ledgers. The total 64KiB envelope is the tighter persistence
		// bound; object keys retain canonicalJSONKey's narrow limit.
		if len(token) > MaxBatchBytes {
			return protocolError(ErrorMalformedFrame)
		}
		return nil
	case json.Number:
		if !canonicalUnsignedJSONNumber.MatchString(token.String()) {
			return protocolError(ErrorMalformedFrame)
		}
		return nil
	case bool:
		return nil
	case json.Delim:
		switch token {
		case '{':
			return scanMachineSnapshotJSONObject(decoder, depth)
		case '[':
			return scanMachineSnapshotJSONArray(decoder, depth)
		default:
			return protocolError(ErrorMalformedFrame)
		}
	default:
		return protocolError(ErrorMalformedFrame)
	}
}

func scanMachineSnapshotJSONObject(decoder *json.Decoder, depth int) error {
	seen := make(map[string]struct{}, maxJSONObjectMembers)
	members := 0
	for decoder.More() {
		members++
		if members > maxJSONObjectMembers {
			return protocolError(ErrorMalformedFrame)
		}
		token, err := decoder.Token()
		if err != nil {
			return protocolError(ErrorMalformedFrame)
		}
		key, ok := token.(string)
		if !ok || !canonicalJSONKey(key) {
			return protocolError(ErrorMalformedFrame)
		}
		folded := strings.ToLower(key)
		if _, duplicate := seen[folded]; duplicate {
			return protocolError(ErrorMalformedFrame)
		}
		seen[folded] = struct{}{}
		if err := scanMachineSnapshotJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func scanMachineSnapshotJSONArray(decoder *json.Decoder, depth int) error {
	items := 0
	for decoder.More() {
		items++
		if items > MaxAttemptMessages {
			return protocolError(ErrorMalformedFrame)
		}
		if err := scanMachineSnapshotJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim(']') {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}
