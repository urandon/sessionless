package attachedworkerprotocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
)

type protocolFixture struct {
	private       ed25519.PrivateKey
	auth          AuthContextV1
	workerOffer   VersionOfferV1
	platformOffer VersionOfferV1
	manifest      CapabilityManifestV1
	digest        []byte
	binding       AttemptBindingV1
	workerSeq     uint64
	platformSeq   uint64
}

func newProtocolFixture(t *testing.T) *protocolFixture {
	t.Helper()
	seed := bytes.Repeat([]byte{0x11}, ed25519.SeedSize)
	private := ed25519.NewKeyFromSeed(seed)
	workerOffer := VersionOfferV1{Window: VersionWindow{Minimum: 1, Maximum: 3}, Supported: []ProtocolVersion{1, 3}}
	platformOffer := VersionOfferV1{Window: VersionWindow{Minimum: 1, Maximum: 3}, Supported: []ProtocolVersion{1, 2}}
	manifest := CapabilityManifestV1{
		WorkerID: "worker-1", EnrollmentGeneration: 4, Revision: 7, ProtocolOffer: workerOffer,
		OperatingSystem: "linux", Architecture: "arm64", BuildID: "build-1",
		HarnessName: "sessionless", HarnessVersion: "1.0.0", HarnessSurface: HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: digestByte(0x21),
		IsolationEvidence:       []IsolationEvidenceV1{IsolationFilesystemBoundary, IsolationNetworkBoundary, IsolationProcessBoundary},
		Features:                []ProtocolFeatureV1{FeatureCancellation, FeatureProgress, FeatureReconnect}, MaxConcurrentAttempts: 1,
	}
	digest, err := ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return &protocolFixture{
		private: private, workerOffer: workerOffer, platformOffer: platformOffer, manifest: manifest, digest: digest,
		auth: AuthContextV1{
			TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1",
			IdentityPublicKey: private.Public().(ed25519.PublicKey), EnrollmentGeneration: 4,
			ConnectionGeneration: 9, Version: ProtocolVersionV1, ChannelBinding: digestByte(0x31),
		},
		binding: AttemptBindingV1{
			RunID: "run-1", AttemptID: "attempt-1", LeaseID: "lease-1", LeaseGeneration: 2,
			FenceToken: "fence-1", ExpiresAtUnixMicro: 1_900_000_000_000_000,
			ContextDigest: digestByte(0x41), CapabilityDigest: digest, PolicyDigest: digestByte(0x51),
		},
	}
}

func digestByte(value byte) []byte { return bytes.Repeat([]byte{value}, sha256.Size) }

func (fixture *protocolFixture) frame(direction Direction, kind MessageKind) FrameV1 {
	if direction == DirectionWorkerToPlatform {
		fixture.workerSeq++
		return FrameV1{Version: fixture.auth.Version, MessageID: MessageIDV1(direction, fixture.workerSeq), WorkerID: fixture.auth.WorkerID,
			EnrollmentGeneration: fixture.auth.EnrollmentGeneration, ConnectionGeneration: fixture.auth.ConnectionGeneration,
			Sequence: fixture.workerSeq, Ack: fixture.platformSeq, Kind: kind}
	}
	fixture.platformSeq++
	return FrameV1{Version: fixture.auth.Version, MessageID: MessageIDV1(direction, fixture.platformSeq), WorkerID: fixture.auth.WorkerID,
		EnrollmentGeneration: fixture.auth.EnrollmentGeneration, ConnectionGeneration: fixture.auth.ConnectionGeneration,
		Sequence: fixture.platformSeq, Ack: fixture.workerSeq, Kind: kind}
}

func (fixture *protocolFixture) attachMachine(t *testing.T) *ConformanceMachine {
	t.Helper()
	machine, err := NewConformanceMachine(MachineConfig{Auth: fixture.auth, WorkerOffer: fixture.workerOffer,
		PlatformOffer: fixture.platformOffer, ImplementedVersions: []ProtocolVersion{1}})
	if err != nil {
		t.Fatal(err)
	}
	workerNonce, platformNonce := digestByte(0x61), digestByte(0x71)
	hello := fixture.frame(DirectionWorkerToPlatform, MessageHello)
	hello.Hello = &HelloV1{Offer: fixture.workerOffer, WorkerNonce: workerNonce}
	acceptOK(t, machine, DirectionWorkerToPlatform, hello, fixture.auth.ChannelBinding)
	challenge := fixture.frame(DirectionPlatformToWorker, MessageChallenge)
	challenge.Challenge = &ChallengeV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	acceptOK(t, machine, DirectionPlatformToWorker, challenge, fixture.auth.ChannelBinding)
	attach := fixture.frame(DirectionWorkerToPlatform, MessageAttach)
	attach.Attach = &AttachV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce,
		CapabilityDigest: fixture.digest, Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignAttachV1(fixture.private, fixture.auth, &attach); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, attach, fixture.auth.ChannelBinding)
	accepted := fixture.frame(DirectionPlatformToWorker, MessageAttachAccepted)
	accepted.AttachAccepted = &AttachAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest}
	acceptOK(t, machine, DirectionPlatformToWorker, accepted, fixture.auth.ChannelBinding)
	manifest := fixture.frame(DirectionWorkerToPlatform, MessageManifest)
	manifest.Manifest = &ManifestV1{Manifest: fixture.manifest, Digest: fixture.digest, Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignManifestV1(fixture.private, fixture.auth, &manifest); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, manifest, fixture.auth.ChannelBinding)
	return machine
}

func (fixture *protocolFixture) claimMachine(t *testing.T, deliverAccepted bool) *ConformanceMachine {
	t.Helper()
	machine := fixture.attachMachine(t)
	offer := fixture.frame(DirectionPlatformToWorker, MessageLeaseOffer)
	offer.LeaseOffer = &LeaseOfferV1{Binding: fixture.binding, AttemptSequence: 1}
	acceptOK(t, machine, DirectionPlatformToWorker, offer, fixture.auth.ChannelBinding)
	claim := fixture.frame(DirectionWorkerToPlatform, MessageLeaseClaim)
	claim.LeaseClaim = &LeaseClaimV1{Binding: fixture.binding, AttemptSequence: 1}
	acceptOK(t, machine, DirectionWorkerToPlatform, claim, fixture.auth.ChannelBinding)
	accepted := fixture.frame(DirectionPlatformToWorker, MessageLeaseAccepted)
	accepted.LeaseAccepted = &LeaseAcceptedV1{Binding: fixture.binding, AttemptSequence: 2}
	if deliverAccepted {
		acceptOK(t, machine, DirectionPlatformToWorker, accepted, fixture.auth.ChannelBinding)
	}
	return machine
}

func reconnectPair(t *testing.T, authoritative, lagging *ConformanceMachine, fixture *protocolFixture) {
	t.Helper()
	nextAuth := fixture.auth
	nextAuth.ConnectionGeneration++
	nextAuth.ChannelBinding = digestByte(0x39)
	if err := authoritative.BeginReconnect(nextAuth); err != nil {
		t.Fatalf("authoritative reconnect: %v", err)
	}
	if err := lagging.BeginReconnect(nextAuth); err != nil {
		t.Fatalf("lagging reconnect: %v", err)
	}
	fixture.auth, fixture.workerSeq, fixture.platformSeq = nextAuth, 0, 0
	workerNonce, platformNonce := digestByte(0x63), digestByte(0x73)
	hello := fixture.frame(DirectionWorkerToPlatform, MessageHello)
	hello.Hello = &HelloV1{Offer: fixture.workerOffer, WorkerNonce: workerNonce}
	challenge := fixture.frame(DirectionPlatformToWorker, MessageChallenge)
	challenge.Challenge = &ChallengeV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	for _, machine := range []*ConformanceMachine{authoritative, lagging} {
		acceptOK(t, machine, DirectionWorkerToPlatform, hello, nextAuth.ChannelBinding)
		acceptOK(t, machine, DirectionPlatformToWorker, challenge, nextAuth.ChannelBinding)
	}
	reconnect := fixture.frame(DirectionWorkerToPlatform, MessageReconnect)
	reconnect.Reconnect = &ReconnectV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, PreviousConnectionGeneration: nextAuth.ConnectionGeneration - 1,
		WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest,
		PreviousWatermarks: lagging.reconnectWatermarks, AttemptSummary: lagging.reconnectAttempt,
		Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignReconnectV1(fixture.private, nextAuth, &reconnect); err != nil {
		t.Fatal(err)
	}
	for _, machine := range []*ConformanceMachine{authoritative, lagging} {
		acceptOK(t, machine, DirectionWorkerToPlatform, reconnect, nextAuth.ChannelBinding)
	}
	reconciled := fixture.frame(DirectionPlatformToWorker, MessageReconnectAccepted)
	reconciled.ReconnectAccepted = &ReconnectAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest,
		AuthoritativeWatermarks: authoritative.reconnectWatermarks, AuthoritativeAttempt: authoritative.reconnectAttempt}
	for _, machine := range []*ConformanceMachine{authoritative, lagging} {
		acceptOK(t, machine, DirectionPlatformToWorker, reconciled, nextAuth.ChannelBinding)
	}
	manifest := fixture.frame(DirectionWorkerToPlatform, MessageManifest)
	manifest.Manifest = &ManifestV1{Manifest: fixture.manifest, Digest: fixture.digest, Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignManifestV1(fixture.private, nextAuth, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, machine := range []*ConformanceMachine{authoritative, lagging} {
		acceptOK(t, machine, DirectionWorkerToPlatform, manifest, nextAuth.ChannelBinding)
	}
}

func acceptOK(t *testing.T, machine *ConformanceMachine, direction Direction, frame FrameV1, channel []byte) {
	t.Helper()
	if err := machine.Accept(direction, frame, AcceptanceContextV1{ChannelBinding: channel, NowUnixMicro: 1_800_000_000_000_000}); err != nil {
		t.Fatalf("accept %s: %v", frame.Kind, err)
	}
}

func acceptAt(machine *ConformanceMachine, direction Direction, frame FrameV1, channel []byte, now int64) error {
	return machine.Accept(direction, frame, AcceptanceContextV1{ChannelBinding: channel, NowUnixMicro: now})
}

func requireCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) || protocolErr.Code != code || err.Error() != string(code) {
		t.Fatalf("got %v, want sanitized %s", err, code)
	}
}

func TestNegotiateOffersUsesExplicitIntersection(t *testing.T) {
	local := VersionOfferV1{Window: VersionWindow{Minimum: 1, Maximum: 5}, Supported: []ProtocolVersion{1, 3, 5}}
	peer := VersionOfferV1{Window: VersionWindow{Minimum: 2, Maximum: 5}, Supported: []ProtocolVersion{2, 3, 4}}
	selected, err := NegotiateOffers(local, peer, []ProtocolVersion{1, 2, 3, 4, 5})
	if err != nil || selected != 3 {
		t.Fatalf("selected=%d err=%v", selected, err)
	}
	peer.Supported = []ProtocolVersion{2, 4}
	_, err = NegotiateOffers(local, peer, []ProtocolVersion{1, 2, 3, 4, 5})
	requireCode(t, err, ErrorUnsupportedVersion)
	local.Supported = []ProtocolVersion{1, 1}
	_, err = NegotiateOffers(local, peer, []ProtocolVersion{1})
	requireCode(t, err, ErrorUnsupportedVersion)
	_, err = NegotiateVersion(VersionWindow{Minimum: 1, Maximum: 9}, VersionWindow{Minimum: 1, Maximum: 1}, []ProtocolVersion{1})
	requireCode(t, err, ErrorUnsupportedVersion)
}

func TestManifestDigestIsCanonicalAndSensitive(t *testing.T) {
	fixture := newProtocolFixture(t)
	first, err := ManifestDigestV1(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ManifestDigestV1(fixture.manifest)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("digest is not deterministic")
	}
	changed := fixture.manifest
	changed.BuildID = "build-2"
	third, err := ManifestDigestV1(changed)
	if err != nil || bytes.Equal(first, third) {
		t.Fatal("build evidence was not bound")
	}
	changed = fixture.manifest
	changed.Features = []ProtocolFeatureV1{FeatureProgress, FeatureCancellation}
	requireCode(t, changed.Validate(), ErrorMalformedFrame)
	changed = fixture.manifest
	changed.IsolationEvidence = append(changed.IsolationEvidence, IsolationProcessBoundary)
	requireCode(t, changed.Validate(), ErrorMalformedFrame)
}

func TestSignedTranscriptsBindAuthoritativeScopeAndEnvelope(t *testing.T) {
	fixture := newProtocolFixture(t)
	frame := fixture.frame(DirectionWorkerToPlatform, MessageAttach)
	frame.Attach = &AttachV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1,
		WorkerNonce: digestByte(1), PlatformNonce: digestByte(2), CapabilityDigest: fixture.digest,
		Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignAttachV1(fixture.private, fixture.auth, &frame); err != nil {
		t.Fatal(err)
	}
	if err := VerifyAttachV1(fixture.auth, frame); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*AuthContextV1, *FrameV1){
		func(auth *AuthContextV1, _ *FrameV1) { auth.TenantID = "tenant-2" },
		func(auth *AuthContextV1, _ *FrameV1) { auth.OwnerUserID = "owner-2" },
		func(auth *AuthContextV1, _ *FrameV1) { auth.ConnectionGeneration++ },
		func(auth *AuthContextV1, _ *FrameV1) { auth.ChannelBinding[0] ^= 1 },
		func(_ *AuthContextV1, frame *FrameV1) { frame.Ack++ },
		func(_ *AuthContextV1, frame *FrameV1) { frame.Attach.PlatformNonce[0] ^= 1 },
	}
	for index, mutate := range mutations {
		authCopy, frameCopy := fixture.auth, frame
		authCopy.ChannelBinding = append([]byte(nil), fixture.auth.ChannelBinding...)
		frameCopy.Attach = cloneAttachForTest(*frame.Attach)
		mutate(&authCopy, &frameCopy)
		requireCode(t, VerifyAttachV1(authCopy, frameCopy), ErrorUnauthorized)
		_ = index
	}
}

func TestStandaloneVerifierRejectsCrossVersionDowngrade(t *testing.T) {
	fixture := newProtocolFixture(t)
	workerOffer := VersionOfferV1{Window: VersionWindow{Minimum: 1, Maximum: 2}, Supported: []ProtocolVersion{1, 2}}
	platformOffer := VersionOfferV1{Window: VersionWindow{Minimum: 1, Maximum: 2}, Supported: []ProtocolVersion{1, 2}}
	frame := fixture.frame(DirectionWorkerToPlatform, MessageAttach)
	frame.Attach = &AttachV1{WorkerOffer: workerOffer, PlatformOffer: platformOffer, SelectedVersion: 1,
		WorkerNonce: digestByte(1), PlatformNonce: digestByte(2), CapabilityDigest: fixture.digest,
		Signature: make([]byte, ed25519.SignatureSize)}
	requireCode(t, SignAttachV1(fixture.private, fixture.auth, &frame), ErrorUnauthorized)
	frame.Version = 2
	requireCode(t, VerifyAttachV1(fixture.auth, frame), ErrorUnauthorized)
}

func cloneAttachForTest(value AttachV1) *AttachV1 {
	value.WorkerOffer.Supported = append([]ProtocolVersion(nil), value.WorkerOffer.Supported...)
	value.PlatformOffer.Supported = append([]ProtocolVersion(nil), value.PlatformOffer.Supported...)
	value.WorkerNonce = append([]byte(nil), value.WorkerNonce...)
	value.PlatformNonce = append([]byte(nil), value.PlatformNonce...)
	value.CapabilityDigest = append([]byte(nil), value.CapabilityDigest...)
	value.Signature = append([]byte(nil), value.Signature...)
	return &value
}

func TestCodecRejectsAmbiguousOrUnboundedJSON(t *testing.T) {
	fixture := newProtocolFixture(t)
	frame := fixture.frame(DirectionWorkerToPlatform, MessageHeartbeat)
	frame.Heartbeat = &HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}
	encoded, err := EncodeBatchV1(BatchV1{Version: 1, Frames: []FrameV1{frame}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeBatchV1(encoded); err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"Version":1`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"Version":1`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"unknown":0`), 1),
		append(append([]byte(nil), encoded...), []byte(` {}`)...),
		append([]byte{0xef, 0xbb, 0xbf}, encoded...),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1.0`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1e0`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":01`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":-0`), 1),
		bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":null`), 1),
	}
	for _, candidate := range cases {
		_, err := DecodeBatchV1(candidate)
		requireCode(t, err, ErrorMalformedFrame)
		if strings.Contains(err.Error(), "unknown") || strings.Contains(err.Error(), "Version") {
			t.Fatal("input-derived detail escaped")
		}
	}
	_, err = DecodeBatchV1(bytes.Repeat([]byte{'x'}, MaxBatchBytes+1))
	requireCode(t, err, ErrorFrameTooLarge)
	tooMany := BatchV1{Version: 1, Frames: make([]FrameV1, MaxBatchFrames+1)}
	_, err = EncodeBatchV1(tooMany)
	requireCode(t, err, ErrorMalformedFrame)
	oversizedString := frame
	oversizedString.MessageID = strings.Repeat("a", maxOpaqueBytes+1)
	_, err = EncodeBatchV1(BatchV1{Version: 1, Frames: []FrameV1{oversizedString}})
	requireCode(t, err, ErrorMalformedFrame)
}

func TestConformanceMachineFullAttemptAndTerminalReplay(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	heartbeat := fixture.frame(DirectionWorkerToPlatform, MessageHeartbeat)
	heartbeat.Heartbeat = &HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}
	acceptOK(t, machine, DirectionWorkerToPlatform, heartbeat, fixture.auth.ChannelBinding)
	steps := []struct {
		direction Direction
		kind      MessageKind
		set       func(*FrameV1)
		state     AttemptState
	}{
		{DirectionPlatformToWorker, MessageLeaseOffer, func(frame *FrameV1) { frame.LeaseOffer = &LeaseOfferV1{Binding: fixture.binding, AttemptSequence: 1} }, AttemptOffered},
		{DirectionWorkerToPlatform, MessageLeaseClaim, func(frame *FrameV1) { frame.LeaseClaim = &LeaseClaimV1{Binding: fixture.binding, AttemptSequence: 1} }, AttemptClaimPending},
		{DirectionPlatformToWorker, MessageLeaseAccepted, func(frame *FrameV1) {
			frame.LeaseAccepted = &LeaseAcceptedV1{Binding: fixture.binding, AttemptSequence: 2}
		}, AttemptClaimed},
		{DirectionWorkerToPlatform, MessageProgress, func(frame *FrameV1) {
			frame.Progress = &ProgressV1{Binding: fixture.binding, AttemptSequence: 2, ProgressSequence: 1, Stage: ProgressStarted}
		}, AttemptClaimed},
		{DirectionPlatformToWorker, MessageCancel, func(frame *FrameV1) {
			frame.Cancel = &CancelV1{Binding: fixture.binding, AttemptSequence: 3, CancelRevision: 1, Code: CancelRequested}
		}, AttemptCancelRequested},
		{DirectionWorkerToPlatform, MessageCancelAck, func(frame *FrameV1) {
			frame.CancelAck = &CancelAckV1{Binding: fixture.binding, AttemptSequence: 3, CancelRevision: 1}
		}, AttemptCancelAcked},
		{DirectionWorkerToPlatform, MessageTerminal, func(frame *FrameV1) {
			frame.Terminal = &TerminalV1{Binding: fixture.binding, AttemptSequence: 4, TerminalSequence: 1, Status: TerminalCancelled, Result: TerminalResultCancelled, EvidenceDigest: digestByte(0x81)}
		}, AttemptTerminalPending},
		{DirectionPlatformToWorker, MessageTerminalAck, func(frame *FrameV1) {
			frame.TerminalAck = &TerminalAckV1{Binding: fixture.binding, AttemptSequence: 4, TerminalSequence: 1, Status: TerminalCancelled, Result: TerminalResultCancelled, EvidenceDigest: digestByte(0x81)}
		}, AttemptTerminalCommitted},
	}
	var terminal FrameV1
	for _, step := range steps {
		frame := fixture.frame(step.direction, step.kind)
		step.set(&frame)
		acceptOK(t, machine, step.direction, frame, fixture.auth.ChannelBinding)
		if machine.AttemptState() != step.state {
			t.Fatalf("%s: got %s want %s", step.kind, machine.AttemptState(), step.state)
		}
		if step.kind == MessageTerminal {
			terminal = frame
		}
	}
	replay := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	replay.Terminal = terminal.Terminal
	acceptOK(t, machine, DirectionWorkerToPlatform, replay, fixture.auth.ChannelBinding)
	divergent := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	copyValue := *terminal.Terminal
	copyValue.EvidenceDigest = append([]byte(nil), copyValue.EvidenceDigest...)
	copyValue.EvidenceDigest[0] ^= 1
	divergent.Terminal = &copyValue
	requireCode(t, acceptAt(machine, DirectionWorkerToPlatform, divergent, fixture.auth.ChannelBinding, 1_800_000_000_000_000), ErrorConflict)
	if err := machine.EraseCommittedAttempt(); err != nil || machine.AttemptState() != AttemptIdle {
		t.Fatalf("erase: state=%s err=%v", machine.AttemptState(), err)
	}
}

func TestFullDuplexCrossingAllowsMonotonicStaleACKs(t *testing.T) {
	leftFixture := newProtocolFixture(t)
	left := leftFixture.attachMachine(t)
	rightFixture := newProtocolFixture(t)
	right := rightFixture.attachMachine(t)
	platform := leftFixture.frame(DirectionPlatformToWorker, MessageError)
	platform.Ack = left.platform.ack
	platform.Error = &ErrorV1{Code: ErrorConflict}
	worker := leftFixture.frame(DirectionWorkerToPlatform, MessageHeartbeat)
	worker.Ack = left.worker.ack
	worker.Heartbeat = &HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}
	acceptOK(t, left, DirectionPlatformToWorker, platform, leftFixture.auth.ChannelBinding)
	acceptOK(t, left, DirectionWorkerToPlatform, worker, leftFixture.auth.ChannelBinding)
	acceptOK(t, right, DirectionWorkerToPlatform, worker, rightFixture.auth.ChannelBinding)
	acceptOK(t, right, DirectionPlatformToWorker, platform, rightFixture.auth.ChannelBinding)
	if left.currentWatermarks() != right.currentWatermarks() {
		t.Fatalf("crossing diverged: left=%+v right=%+v", left.currentWatermarks(), right.currentWatermarks())
	}
}

func TestCrossedCancelAndProgressConverge(t *testing.T) {
	leftFixture := newProtocolFixture(t)
	left := leftFixture.claimMachine(t, true)
	rightFixture := newProtocolFixture(t)
	right := rightFixture.claimMachine(t, true)
	cancel := leftFixture.frame(DirectionPlatformToWorker, MessageCancel)
	cancel.Ack = left.platform.ack
	cancel.Cancel = &CancelV1{Binding: leftFixture.binding, AttemptSequence: 3, CancelRevision: 1, Code: CancelRequested}
	progress := leftFixture.frame(DirectionWorkerToPlatform, MessageProgress)
	progress.Ack = left.worker.ack
	progress.Progress = &ProgressV1{Binding: leftFixture.binding, AttemptSequence: 2, ProgressSequence: 1, Stage: ProgressActive}
	acceptOK(t, left, DirectionPlatformToWorker, cancel, leftFixture.auth.ChannelBinding)
	acceptOK(t, left, DirectionWorkerToPlatform, progress, leftFixture.auth.ChannelBinding)
	acceptOK(t, right, DirectionWorkerToPlatform, progress, rightFixture.auth.ChannelBinding)
	acceptOK(t, right, DirectionPlatformToWorker, cancel, rightFixture.auth.ChannelBinding)
	if !sameAttemptSummary(left.currentAttemptSummary(), right.currentAttemptSummary()) || left.AttemptState() != AttemptCancelRequested {
		t.Fatalf("crossed transition diverged: left=%+v right=%+v", left.currentAttemptSummary(), right.currentAttemptSummary())
	}
}

func TestCrossedCancelAndTerminalConvergeWithoutCommit(t *testing.T) {
	leftFixture := newProtocolFixture(t)
	left := leftFixture.claimMachine(t, true)
	rightFixture := newProtocolFixture(t)
	right := rightFixture.claimMachine(t, true)
	cancel := leftFixture.frame(DirectionPlatformToWorker, MessageCancel)
	cancel.Ack = left.platform.ack
	cancel.Cancel = &CancelV1{Binding: leftFixture.binding, AttemptSequence: 3, CancelRevision: 1, Code: CancelRequested}
	terminal := leftFixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	terminal.Ack = left.worker.ack
	terminal.Terminal = &TerminalV1{Binding: leftFixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x95)}
	acceptOK(t, left, DirectionWorkerToPlatform, terminal, leftFixture.auth.ChannelBinding)
	acceptOK(t, left, DirectionPlatformToWorker, cancel, leftFixture.auth.ChannelBinding)
	acceptOK(t, right, DirectionPlatformToWorker, cancel, rightFixture.auth.ChannelBinding)
	acceptOK(t, right, DirectionWorkerToPlatform, terminal, rightFixture.auth.ChannelBinding)
	if !sameAttemptSummary(left.currentAttemptSummary(), right.currentAttemptSummary()) ||
		left.AttemptState() != AttemptCancelRequested || left.currentAttemptSummary().TerminalSequence != 0 {
		t.Fatalf("crossed terminal became committable: left=%+v right=%+v", left.currentAttemptSummary(), right.currentAttemptSummary())
	}
}

func TestReconnectReconcilesLostLeaseAcceptedAndTerminalAck(t *testing.T) {
	t.Run("lease_accepted", func(t *testing.T) {
		authoritativeFixture := newProtocolFixture(t)
		authoritative := authoritativeFixture.claimMachine(t, true)
		laggingFixture := newProtocolFixture(t)
		lagging := laggingFixture.claimMachine(t, false)
		reconnectPair(t, authoritative, lagging, authoritativeFixture)
		if authoritative.AttemptState() != AttemptClaimed || lagging.AttemptState() != AttemptClaimed ||
			!sameAttemptSummary(authoritative.currentAttemptSummary(), lagging.currentAttemptSummary()) {
			t.Fatalf("lost accepted not reconciled: %s/%s", authoritative.AttemptState(), lagging.AttemptState())
		}
	})
	t.Run("terminal_ack", func(t *testing.T) {
		authoritativeFixture := newProtocolFixture(t)
		authoritative := authoritativeFixture.claimMachine(t, true)
		laggingFixture := newProtocolFixture(t)
		lagging := laggingFixture.claimMachine(t, true)
		for _, pair := range []struct {
			machine *ConformanceMachine
			fixture *protocolFixture
		}{{authoritative, authoritativeFixture}, {lagging, laggingFixture}} {
			terminal := pair.fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
			terminal.Terminal = &TerminalV1{Binding: pair.fixture.binding, AttemptSequence: 2, TerminalSequence: 1,
				Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x91)}
			acceptOK(t, pair.machine, DirectionWorkerToPlatform, terminal, pair.fixture.auth.ChannelBinding)
		}
		ack := authoritativeFixture.frame(DirectionPlatformToWorker, MessageTerminalAck)
		ack.TerminalAck = &TerminalAckV1{Binding: authoritativeFixture.binding, AttemptSequence: 3, TerminalSequence: 1,
			Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x91)}
		badAck := ack
		badAck.TerminalAck = &TerminalAckV1{Binding: authoritativeFixture.binding, AttemptSequence: 3, TerminalSequence: 1,
			Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x90)}
		requireCode(t, acceptAt(authoritative, DirectionPlatformToWorker, badAck, authoritativeFixture.auth.ChannelBinding,
			1_800_000_000_000_000), ErrorConflict)
		acceptOK(t, authoritative, DirectionPlatformToWorker, ack, authoritativeFixture.auth.ChannelBinding)
		reconnectPair(t, authoritative, lagging, authoritativeFixture)
		if authoritative.AttemptState() != AttemptTerminalCommitted || lagging.AttemptState() != AttemptTerminalCommitted {
			t.Fatalf("lost terminal ack not reconciled: %s/%s", authoritative.AttemptState(), lagging.AttemptState())
		}
	})
}

func TestAuthoritativeLeaseExpiryIsExclusive(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	offer := fixture.frame(DirectionPlatformToWorker, MessageLeaseOffer)
	offer.LeaseOffer = &LeaseOfferV1{Binding: fixture.binding, AttemptSequence: 1}
	requireCode(t, acceptAt(machine, DirectionPlatformToWorker, offer, fixture.auth.ChannelBinding,
		fixture.binding.ExpiresAtUnixMicro), ErrorConflict)
	acceptOK(t, machine, DirectionPlatformToWorker, offer, fixture.auth.ChannelBinding)
	claim := fixture.frame(DirectionWorkerToPlatform, MessageLeaseClaim)
	claim.LeaseClaim = &LeaseClaimV1{Binding: fixture.binding, AttemptSequence: 1}
	requireCode(t, acceptAt(machine, DirectionWorkerToPlatform, claim, fixture.auth.ChannelBinding,
		fixture.binding.ExpiresAtUnixMicro), ErrorConflict)

	progressFixture := newProtocolFixture(t)
	progressMachine := progressFixture.claimMachine(t, true)
	progress := progressFixture.frame(DirectionWorkerToPlatform, MessageProgress)
	progress.Progress = &ProgressV1{Binding: progressFixture.binding, AttemptSequence: 2, ProgressSequence: 1, Stage: ProgressActive}
	requireCode(t, acceptAt(progressMachine, DirectionWorkerToPlatform, progress, progressFixture.auth.ChannelBinding,
		progressFixture.binding.ExpiresAtUnixMicro), ErrorConflict)

	terminalFixture := newProtocolFixture(t)
	terminalMachine := terminalFixture.claimMachine(t, true)
	terminal := terminalFixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	terminal.Terminal = &TerminalV1{Binding: terminalFixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x96)}
	requireCode(t, acceptAt(terminalMachine, DirectionWorkerToPlatform, terminal, terminalFixture.auth.ChannelBinding,
		terminalFixture.binding.ExpiresAtUnixMicro), ErrorConflict)
}

func TestManifestFeatureSetGatesTransitions(t *testing.T) {
	t.Run("reconnect", func(t *testing.T) {
		fixture := newProtocolFixture(t)
		fixture.manifest.Features = []ProtocolFeatureV1{FeatureCancellation, FeatureProgress}
		fixture.digest, _ = ManifestDigestV1(fixture.manifest)
		fixture.binding.CapabilityDigest = fixture.digest
		machine := fixture.attachMachine(t)
		next := fixture.auth
		next.ConnectionGeneration++
		next.ChannelBinding = digestByte(0x37)
		requireCode(t, machine.BeginReconnect(next), ErrorUnauthorized)
	})
	t.Run("progress", func(t *testing.T) {
		fixture := newProtocolFixture(t)
		fixture.manifest.Features = []ProtocolFeatureV1{FeatureCancellation, FeatureReconnect}
		fixture.digest, _ = ManifestDigestV1(fixture.manifest)
		fixture.binding.CapabilityDigest = fixture.digest
		machine := fixture.claimMachine(t, true)
		progress := fixture.frame(DirectionWorkerToPlatform, MessageProgress)
		progress.Progress = &ProgressV1{Binding: fixture.binding, AttemptSequence: 2, ProgressSequence: 1, Stage: ProgressActive}
		requireCode(t, acceptAt(machine, DirectionWorkerToPlatform, progress, fixture.auth.ChannelBinding,
			1_800_000_000_000_000), ErrorProtocolViolation)
	})
	t.Run("cancel", func(t *testing.T) {
		fixture := newProtocolFixture(t)
		fixture.manifest.Features = []ProtocolFeatureV1{FeatureProgress, FeatureReconnect}
		fixture.digest, _ = ManifestDigestV1(fixture.manifest)
		fixture.binding.CapabilityDigest = fixture.digest
		machine := fixture.claimMachine(t, true)
		cancel := fixture.frame(DirectionPlatformToWorker, MessageCancel)
		cancel.Cancel = &CancelV1{Binding: fixture.binding, AttemptSequence: 3, CancelRevision: 1, Code: CancelRequested}
		requireCode(t, acceptAt(machine, DirectionPlatformToWorker, cancel, fixture.auth.ChannelBinding,
			1_800_000_000_000_000), ErrorProtocolViolation)
	})
}

func TestRevokeAndFencedCancelPreventLateCommit(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.claimMachine(t, true)
	revoke := fixture.frame(DirectionPlatformToWorker, MessageRevoke)
	revoke.Revoke = &RevokeV1{Revision: 1, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}
	acceptOK(t, machine, DirectionPlatformToWorker, revoke, fixture.auth.ChannelBinding)
	if machine.AttemptState() != AttemptFenced || machine.currentAttemptSummary().CancelCode != CancelFenced {
		t.Fatalf("active attempt was not fenced: %+v", machine.currentAttemptSummary())
	}
	success := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	success.Terminal = &TerminalV1{Binding: fixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x92)}
	requireCode(t, acceptAt(machine, DirectionWorkerToPlatform, success, fixture.auth.ChannelBinding,
		1_800_000_000_000_000), ErrorProtocolViolation)
	cancelled := success
	cancelled.Terminal = &TerminalV1{Binding: fixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalCancelled, Result: TerminalResultCancelled, EvidenceDigest: digestByte(0x93)}
	acceptOK(t, machine, DirectionWorkerToPlatform, cancelled, fixture.auth.ChannelBinding)
	if machine.AttemptState() != AttemptFenced {
		t.Fatalf("fenced evidence became committable: %s", machine.AttemptState())
	}
	revoked := fixture.frame(DirectionWorkerToPlatform, MessageRevoked)
	revoked.Revoked = &RevokedV1{Revision: 1, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}
	acceptOK(t, machine, DirectionWorkerToPlatform, revoked, fixture.auth.ChannelBinding)
	if machine.ConnectionState() != ConnectionRevoked || machine.AttemptState() != AttemptFenced {
		t.Fatalf("active revoke handling diverged: %s/%s", machine.ConnectionState(), machine.AttemptState())
	}
}

func TestReconnectPreservesLeaseAndAcceptsExactTerminalReplay(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	offer := fixture.frame(DirectionPlatformToWorker, MessageLeaseOffer)
	offer.LeaseOffer = &LeaseOfferV1{Binding: fixture.binding, AttemptSequence: 1}
	acceptOK(t, machine, DirectionPlatformToWorker, offer, fixture.auth.ChannelBinding)
	claim := fixture.frame(DirectionWorkerToPlatform, MessageLeaseClaim)
	claim.LeaseClaim = &LeaseClaimV1{Binding: fixture.binding, AttemptSequence: 1}
	acceptOK(t, machine, DirectionWorkerToPlatform, claim, fixture.auth.ChannelBinding)
	accepted := fixture.frame(DirectionPlatformToWorker, MessageLeaseAccepted)
	accepted.LeaseAccepted = &LeaseAcceptedV1{Binding: fixture.binding, AttemptSequence: 2}
	acceptOK(t, machine, DirectionPlatformToWorker, accepted, fixture.auth.ChannelBinding)
	terminal := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	terminal.Terminal = &TerminalV1{Binding: fixture.binding, AttemptSequence: 2, TerminalSequence: 1, Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x82)}
	acceptOK(t, machine, DirectionWorkerToPlatform, terminal, fixture.auth.ChannelBinding)

	previousConnectionGeneration := fixture.auth.ConnectionGeneration
	nextAuth := fixture.auth
	nextAuth.ConnectionGeneration++
	nextAuth.ChannelBinding = digestByte(0x32)
	if err := machine.BeginReconnect(nextAuth); err != nil {
		t.Fatal(err)
	}
	fixture.auth, fixture.workerSeq, fixture.platformSeq = nextAuth, 0, 0
	workerNonce, platformNonce := digestByte(0x62), digestByte(0x72)
	hello := fixture.frame(DirectionWorkerToPlatform, MessageHello)
	hello.Hello = &HelloV1{Offer: fixture.workerOffer, WorkerNonce: workerNonce}
	acceptOK(t, machine, DirectionWorkerToPlatform, hello, nextAuth.ChannelBinding)
	challenge := fixture.frame(DirectionPlatformToWorker, MessageChallenge)
	challenge.Challenge = &ChallengeV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	acceptOK(t, machine, DirectionPlatformToWorker, challenge, nextAuth.ChannelBinding)
	reconnect := fixture.frame(DirectionWorkerToPlatform, MessageReconnect)
	reconnect.Reconnect = &ReconnectV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, PreviousConnectionGeneration: previousConnectionGeneration,
		WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest,
		PreviousWatermarks: machine.reconnectWatermarks, AttemptSummary: machine.reconnectAttempt,
		Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignReconnectV1(fixture.private, nextAuth, &reconnect); err != nil {
		t.Fatal(err)
	}
	tamperedReconnect := reconnect
	tamperedPayload := *reconnect.Reconnect
	tamperedPayload.PreviousWatermarks.PlatformAck = 0
	tamperedReconnect.Reconnect = &tamperedPayload
	if tamperedPayload.PreviousWatermarks != reconnect.Reconnect.PreviousWatermarks {
		requireCode(t, VerifyReconnectV1(nextAuth, tamperedReconnect), ErrorUnauthorized)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, reconnect, nextAuth.ChannelBinding)
	reconnectAccepted := fixture.frame(DirectionPlatformToWorker, MessageReconnectAccepted)
	reconnectAccepted.ReconnectAccepted = &ReconnectAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest,
		AuthoritativeWatermarks: machine.reconnectWatermarks, AuthoritativeAttempt: machine.reconnectAttempt}
	acceptOK(t, machine, DirectionPlatformToWorker, reconnectAccepted, nextAuth.ChannelBinding)
	manifest := fixture.frame(DirectionWorkerToPlatform, MessageManifest)
	manifest.Manifest = &ManifestV1{Manifest: fixture.manifest, Digest: fixture.digest, Signature: make([]byte, ed25519.SignatureSize)}
	if err := SignManifestV1(fixture.private, nextAuth, &manifest); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, manifest, nextAuth.ChannelBinding)

	replay := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	replay.Terminal = terminal.Terminal
	acceptOK(t, machine, DirectionWorkerToPlatform, replay, nextAuth.ChannelBinding)
	ack := fixture.frame(DirectionPlatformToWorker, MessageTerminalAck)
	ack.TerminalAck = &TerminalAckV1{Binding: fixture.binding, AttemptSequence: 3, TerminalSequence: 1, Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x82)}
	acceptOK(t, machine, DirectionPlatformToWorker, ack, nextAuth.ChannelBinding)
	preserved, ok := machine.AttemptBinding()
	if machine.AttemptState() != AttemptTerminalCommitted || !ok ||
		preserved.ExpiresAtUnixMicro != fixture.binding.ExpiresAtUnixMicro || !sameAttemptBinding(preserved, fixture.binding) {
		t.Fatalf("state=%s or lease changed", machine.AttemptState())
	}
}

func TestConformanceMachineRejectsSequenceDirectionAndDigestBaitSwitch(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	offer := fixture.frame(DirectionPlatformToWorker, MessageLeaseOffer)
	offer.LeaseOffer = &LeaseOfferV1{Binding: fixture.binding, AttemptSequence: 1}
	wrongChannel := append([]byte(nil), fixture.auth.ChannelBinding...)
	wrongChannel[0] ^= 1
	requireCode(t, acceptAt(machine, DirectionPlatformToWorker, offer, wrongChannel, 1_800_000_000_000_000), ErrorUnauthorized)
	acceptOK(t, machine, DirectionPlatformToWorker, offer, fixture.auth.ChannelBinding)
	claim := fixture.frame(DirectionWorkerToPlatform, MessageLeaseClaim)
	changed := cloneBinding(fixture.binding)
	changed.PolicyDigest[0] ^= 1
	claim.LeaseClaim = &LeaseClaimV1{Binding: changed, AttemptSequence: 1}
	requireCode(t, acceptAt(machine, DirectionWorkerToPlatform, claim, fixture.auth.ChannelBinding, 1_800_000_000_000_000), ErrorConflict)

	fixture2 := newProtocolFixture(t)
	machine2 := fixture2.attachMachine(t)
	progress := fixture2.frame(DirectionWorkerToPlatform, MessageProgress)
	progress.Progress = &ProgressV1{Binding: fixture2.binding, AttemptSequence: 1, ProgressSequence: 1, Stage: ProgressStarted}
	requireCode(t, acceptAt(machine2, DirectionWorkerToPlatform, progress, fixture2.auth.ChannelBinding, 1_800_000_000_000_000), ErrorProtocolViolation)

	fixture3 := newProtocolFixture(t)
	machine3 := fixture3.attachMachine(t)
	heartbeat := fixture3.frame(DirectionWorkerToPlatform, MessageHeartbeat)
	heartbeat.Heartbeat = &HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}
	heartbeat.Sequence++
	heartbeat.MessageID = MessageIDV1(DirectionWorkerToPlatform, heartbeat.Sequence)
	requireCode(t, acceptAt(machine3, DirectionWorkerToPlatform, heartbeat, fixture3.auth.ChannelBinding, 1_800_000_000_000_000), ErrorProtocolViolation)
}

func TestCancelTerminalRaceIsDeterministic(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	offer := fixture.frame(DirectionPlatformToWorker, MessageLeaseOffer)
	offer.LeaseOffer = &LeaseOfferV1{Binding: fixture.binding, AttemptSequence: 1}
	acceptOK(t, machine, DirectionPlatformToWorker, offer, fixture.auth.ChannelBinding)
	claim := fixture.frame(DirectionWorkerToPlatform, MessageLeaseClaim)
	claim.LeaseClaim = &LeaseClaimV1{Binding: fixture.binding, AttemptSequence: 1}
	acceptOK(t, machine, DirectionWorkerToPlatform, claim, fixture.auth.ChannelBinding)
	accepted := fixture.frame(DirectionPlatformToWorker, MessageLeaseAccepted)
	accepted.LeaseAccepted = &LeaseAcceptedV1{Binding: fixture.binding, AttemptSequence: 2}
	acceptOK(t, machine, DirectionPlatformToWorker, accepted, fixture.auth.ChannelBinding)
	cancel := fixture.frame(DirectionPlatformToWorker, MessageCancel)
	cancel.Cancel = &CancelV1{Binding: fixture.binding, AttemptSequence: 3, CancelRevision: 1, Code: CancelRequested}
	acceptOK(t, machine, DirectionPlatformToWorker, cancel, fixture.auth.ChannelBinding)
	terminal := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	terminal.Terminal = &TerminalV1{Binding: fixture.binding, AttemptSequence: 2, TerminalSequence: 1, Status: TerminalCancelled, Result: TerminalResultCancelled, EvidenceDigest: digestByte(0x83)}
	acceptOK(t, machine, DirectionWorkerToPlatform, terminal, fixture.auth.ChannelBinding)
	lateAck := fixture.frame(DirectionWorkerToPlatform, MessageCancelAck)
	lateAck.CancelAck = &CancelAckV1{Binding: fixture.binding, AttemptSequence: 3, CancelRevision: 1}
	requireCode(t, acceptAt(machine, DirectionWorkerToPlatform, lateAck, fixture.auth.ChannelBinding, 1_800_000_000_000_000), ErrorProtocolViolation)
	if machine.AttemptState() != AttemptTerminalPending {
		t.Fatalf("state=%s", machine.AttemptState())
	}
}

func TestConformanceDrainRevokeAndOverflow(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	drain := fixture.frame(DirectionPlatformToWorker, MessageDrain)
	drain.Drain = &DrainV1{Revision: 1}
	acceptOK(t, machine, DirectionPlatformToWorker, drain, fixture.auth.ChannelBinding)
	drained := fixture.frame(DirectionWorkerToPlatform, MessageDrained)
	drained.Drained = &DrainedV1{Revision: 1}
	acceptOK(t, machine, DirectionWorkerToPlatform, drained, fixture.auth.ChannelBinding)
	revoke := fixture.frame(DirectionPlatformToWorker, MessageRevoke)
	revoke.Revoke = &RevokeV1{Revision: 2, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}
	acceptOK(t, machine, DirectionPlatformToWorker, revoke, fixture.auth.ChannelBinding)
	revoked := fixture.frame(DirectionWorkerToPlatform, MessageRevoked)
	revoked.Revoked = &RevokedV1{Revision: 2, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}
	acceptOK(t, machine, DirectionWorkerToPlatform, revoked, fixture.auth.ChannelBinding)
	if machine.ConnectionState() != ConnectionRevoked {
		t.Fatalf("state=%s", machine.ConnectionState())
	}
	if got := MessageIDV1(DirectionWorkerToPlatform, math.MaxUint64); len(got) > maxOpaqueBytes {
		t.Fatalf("message id unexpectedly unbounded: %q", got)
	}
	fixture2 := newProtocolFixture(t)
	machine2 := fixture2.attachMachine(t)
	machine2.worker.sequence = math.MaxUint64
	overflow := FrameV1{Version: ProtocolVersionV1, MessageID: MessageIDV1(DirectionWorkerToPlatform, 1), WorkerID: fixture2.auth.WorkerID,
		EnrollmentGeneration: fixture2.auth.EnrollmentGeneration, ConnectionGeneration: fixture2.auth.ConnectionGeneration,
		Sequence: 1, Ack: machine2.platform.sequence, Kind: MessageHeartbeat,
		Heartbeat: &HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}}
	requireCode(t, acceptAt(machine2, DirectionWorkerToPlatform, overflow, fixture2.auth.ChannelBinding, 1_800_000_000_000_000), ErrorProtocolViolation)
}

func TestBatchRoundTripEveryMessageKind(t *testing.T) {
	fixture := newProtocolFixture(t)
	binding := fixture.binding
	idleSummary := sealAttemptSummary(AttemptSummaryV1{State: AttemptIdle})
	watermarks := ConnectionWatermarksV1{}
	frames := []FrameV1{
		{Kind: MessageHello, Hello: &HelloV1{Offer: fixture.workerOffer, WorkerNonce: digestByte(1)}},
		{Kind: MessageChallenge, Challenge: &ChallengeV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1, WorkerNonce: digestByte(1), PlatformNonce: digestByte(2)}},
		{Kind: MessageAttach, Attach: &AttachV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1, WorkerNonce: digestByte(1), PlatformNonce: digestByte(2), CapabilityDigest: fixture.digest, Signature: bytes.Repeat([]byte{1}, ed25519.SignatureSize)}},
		{Kind: MessageAttachAccepted, AttachAccepted: &AttachAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1, WorkerNonce: digestByte(1), PlatformNonce: digestByte(2), CapabilityDigest: fixture.digest}},
		{Kind: MessageReconnect, Reconnect: &ReconnectV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1, PreviousConnectionGeneration: 8, WorkerNonce: digestByte(1), PlatformNonce: digestByte(2), CapabilityDigest: fixture.digest, PreviousWatermarks: watermarks, AttemptSummary: idleSummary, Signature: bytes.Repeat([]byte{1}, ed25519.SignatureSize)}},
		{Kind: MessageReconnectAccepted, ReconnectAccepted: &ReconnectAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1, WorkerNonce: digestByte(1), PlatformNonce: digestByte(2), CapabilityDigest: fixture.digest, AuthoritativeWatermarks: watermarks, AuthoritativeAttempt: idleSummary}},
		{Kind: MessageManifest, Manifest: &ManifestV1{Manifest: fixture.manifest, Digest: fixture.digest, Signature: bytes.Repeat([]byte{1}, ed25519.SignatureSize)}},
		{Kind: MessageHeartbeat, Heartbeat: &HeartbeatV1{ObservedAtUnixMicro: 1, Available: true}},
		{Kind: MessageLeaseOffer, LeaseOffer: &LeaseOfferV1{Binding: binding, AttemptSequence: 1}},
		{Kind: MessageLeaseClaim, LeaseClaim: &LeaseClaimV1{Binding: binding, AttemptSequence: 2}},
		{Kind: MessageLeaseAccepted, LeaseAccepted: &LeaseAcceptedV1{Binding: binding, AttemptSequence: 3}},
		{Kind: MessageProgress, Progress: &ProgressV1{Binding: binding, AttemptSequence: 4, ProgressSequence: 1, Stage: ProgressStarted}},
		{Kind: MessageCancel, Cancel: &CancelV1{Binding: binding, AttemptSequence: 5, CancelRevision: 1, Code: CancelRequested}},
		{Kind: MessageCancelAck, CancelAck: &CancelAckV1{Binding: binding, AttemptSequence: 6, CancelRevision: 1}},
		{Kind: MessageTerminal, Terminal: &TerminalV1{Binding: binding, AttemptSequence: 7, TerminalSequence: 1, Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x84)}},
		{Kind: MessageTerminalAck, TerminalAck: &TerminalAckV1{Binding: binding, AttemptSequence: 8, TerminalSequence: 1, Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x84)}},
		{Kind: MessageDrain, Drain: &DrainV1{Revision: 1}},
		{Kind: MessageDrained, Drained: &DrainedV1{Revision: 1}},
		{Kind: MessageRevoke, Revoke: &RevokeV1{Revision: 1, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}},
		{Kind: MessageRevoked, Revoked: &RevokedV1{Revision: 1, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}},
		{Kind: MessageError, Error: &ErrorV1{Code: ErrorConflict}},
	}
	for index := range frames {
		frames[index].Version = ProtocolVersionV1
		frames[index].MessageID = "fixture-" + string(rune('a'+index))
		frames[index].WorkerID = fixture.auth.WorkerID
		frames[index].EnrollmentGeneration = fixture.auth.EnrollmentGeneration
		frames[index].ConnectionGeneration = fixture.auth.ConnectionGeneration
		frames[index].Sequence = uint64(index + 1)
		if err := frames[index].Validate(); err != nil {
			t.Fatalf("frame %d kind %s: %v", index, frames[index].Kind, err)
		}
	}
	encoded, err := EncodeBatchV1(BatchV1{Version: 1, Frames: frames})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBatchV1(encoded)
	if err != nil || len(decoded.Frames) != len(frames) {
		t.Fatalf("decoded=%d err=%v", len(decoded.Frames), err)
	}
	remarshaled, _ := json.Marshal(decoded)
	if !bytes.Equal(encoded, remarshaled) {
		t.Fatal("round trip changed canonical encoding")
	}
}

func FuzzDecodeBatchV1(f *testing.F) {
	f.Add([]byte(`{"version":1,"frames":[]}`))
	f.Add([]byte(`{"version":1,"version":1,"frames":[]}`))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		batch, err := DecodeBatchV1(encoded)
		if err != nil {
			if len(err.Error()) > len(string(ErrorUnsupportedVersion)) {
				t.Fatalf("unsanitized error: %q", err)
			}
			return
		}
		roundTrip, err := EncodeBatchV1(batch)
		if err != nil || len(roundTrip) > MaxBatchBytes {
			t.Fatalf("round trip err=%v bytes=%d", err, len(roundTrip))
		}
	})
}
