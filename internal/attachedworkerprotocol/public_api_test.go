package attachedworkerprotocol_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"

	protocol "gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

func TestPublicReconnectSnapshotAndBuilders(t *testing.T) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x12}, ed25519.SeedSize))
	offer := protocol.VersionOfferV1{Window: protocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []protocol.ProtocolVersion{1}}
	auth := protocol.AuthContextV1{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1",
		IdentityPublicKey: private.Public().(ed25519.PublicKey), EnrollmentGeneration: 2,
		ConnectionGeneration: 3, Version: 1, ChannelBinding: publicDigest(0x31),
	}
	manifest := protocol.CapabilityManifestV1{
		WorkerID: "worker-1", EnrollmentGeneration: 2, Revision: 1, ProtocolOffer: offer,
		OperatingSystem: "linux", Architecture: "arm64", BuildID: "build-1",
		HarnessName: "sessionless", HarnessVersion: "1.0.0", HarnessSurface: protocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: publicDigest(0x41),
		IsolationEvidence:       []protocol.IsolationEvidenceV1{protocol.IsolationFilesystemBoundary, protocol.IsolationProcessBoundary},
		Features:                []protocol.ProtocolFeatureV1{protocol.FeatureCancellation, protocol.FeatureProgress, protocol.FeatureReconnect},
		MaxConcurrentAttempts:   1,
	}
	capabilityDigest, err := protocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := protocol.NewConformanceMachine(protocol.MachineConfig{
		Auth: auth, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []protocol.ProtocolVersion{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	workerNonce, platformNonce := publicDigest(0x51), publicDigest(0x61)
	hello := publicFrame(auth, protocol.DirectionWorkerToPlatform, 1, 0, protocol.MessageHello)
	hello.Hello = &protocol.HelloV1{Offer: offer, WorkerNonce: workerNonce}
	publicAccept(t, machine, auth, protocol.DirectionWorkerToPlatform, hello)
	challenge := publicFrame(auth, protocol.DirectionPlatformToWorker, 1, 1, protocol.MessageChallenge)
	challenge.Challenge = &protocol.ChallengeV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1,
		WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	publicAccept(t, machine, auth, protocol.DirectionPlatformToWorker, challenge)
	attach := publicFrame(auth, protocol.DirectionWorkerToPlatform, 2, 1, protocol.MessageAttach)
	attach.Attach = &protocol.AttachV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1,
		WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: capabilityDigest}
	if err := protocol.SignAttachV1(private, auth, &attach); err != nil {
		t.Fatal(err)
	}
	publicAccept(t, machine, auth, protocol.DirectionWorkerToPlatform, attach)
	accepted := publicFrame(auth, protocol.DirectionPlatformToWorker, 2, 2, protocol.MessageAttachAccepted)
	accepted.AttachAccepted = &protocol.AttachAcceptedV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1,
		WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: capabilityDigest}
	publicAccept(t, machine, auth, protocol.DirectionPlatformToWorker, accepted)
	manifestFrame := publicFrame(auth, protocol.DirectionWorkerToPlatform, 3, 2, protocol.MessageManifest)
	manifestFrame.Manifest = &protocol.ManifestV1{Manifest: manifest, Digest: capabilityDigest}
	if err := protocol.SignManifestV1(private, auth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	publicAccept(t, machine, auth, protocol.DirectionWorkerToPlatform, manifestFrame)
	durable, err := machine.Snapshot()
	if err != nil || durable.Validate() != nil {
		t.Fatalf("durable snapshot: err=%v validation=%v", err, durable.Validate())
	}
	if _, err := protocol.CanonicalMachineSnapshotBytesV1(durable); err != nil {
		t.Fatal(err)
	}
	restored, err := protocol.RestoreConformanceMachine(protocol.MachineConfig{
		Auth: auth, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []protocol.ProtocolVersion{1},
	}, durable.Clone())
	if err != nil {
		t.Fatalf("durable restore: %v", err)
	}
	if restored.ConnectionState() != protocol.ConnectionReady {
		t.Fatalf("durable restore: state=%s", restored.ConnectionState())
	}
	_ = protocol.MachineSnapshotV1{
		Version:  protocol.MachineSnapshotVersionV1,
		Platform: protocol.MachineEnvelopeSnapshotV1{}, Worker: protocol.MachineEnvelopeSnapshotV1{},
		Attempt: protocol.MachineAttemptSnapshotV1{
			Platform: protocol.MachineAttemptDirectionSnapshotV1{}, Worker: protocol.MachineAttemptDirectionSnapshotV1{},
		},
	}
	_ = protocol.CancelAuthorityV1{Revision: 1, Code: protocol.CancelRequested, NowUnixMicro: 1}
	_ = protocol.LeaseAcceptedAuthorityV1{NowUnixMicro: 1}
	_ = protocol.TerminalAckAuthorityV1{NowUnixMicro: 1}
	authority := protocol.LeaseOfferAuthorityV1{
		RunID: "run-1", AttemptID: "attempt-1", LeaseID: "lease-1", LeaseGeneration: 7,
		NowUnixMicro: 1_800_000_000_000_000, ExpiresAtUnixMicro: 1_900_000_000_000_000,
		ContextDigest: publicDigest(0x81), PolicyDigest: publicDigest(0x91),
	}
	config := protocol.MachineConfig{
		Auth: auth, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []protocol.ProtocolVersion{1},
	}
	attachedSnapshot, err := protocol.BuildInitialAttachSnapshotV1(config, attach, accepted)
	if err != nil || attachedSnapshot.Connection != protocol.ConnectionAttached {
		t.Fatalf("attach bootstrap: state=%s err=%v", attachedSnapshot.Connection, err)
	}
	readySnapshot, err := protocol.ApplyMachineFrameV1(config, attachedSnapshot,
		protocol.DirectionWorkerToPlatform, manifestFrame, 1)
	if err != nil || !bytes.Equal(readySnapshot.Digest, durable.Digest) {
		t.Fatalf("manifest continuity: equal=%v err=%v", bytes.Equal(readySnapshot.Digest, durable.Digest), err)
	}
	heartbeat := publicFrame(auth, protocol.DirectionWorkerToPlatform, 4, 2, protocol.MessageHeartbeat)
	heartbeat.Heartbeat = &protocol.HeartbeatV1{ObservedAtUnixMicro: 1_800_000_000_000_001, Available: true}
	checkpointed, err := protocol.ApplyMachineFrameV1(config, readySnapshot,
		protocol.DirectionWorkerToPlatform, heartbeat, 1_800_000_000_000_001)
	if err != nil || checkpointed.Worker.Sequence != 4 || len(checkpointed.Worker.Fingerprint) != sha256.Size {
		t.Fatalf("heartbeat continuity: worker=%+v err=%v", checkpointed.Worker, err)
	}
	encodedCheckpoint, err := protocol.EncodeMachineSnapshotV1(checkpointed)
	if err != nil {
		t.Fatal(err)
	}
	decodedCheckpoint, err := protocol.DecodeMachineSnapshotV1(encodedCheckpoint)
	if err != nil || !bytes.Equal(decodedCheckpoint.Digest, checkpointed.Digest) {
		t.Fatalf("snapshot codec: equal=%v err=%v", bytes.Equal(decodedCheckpoint.Digest, checkpointed.Digest), err)
	}
	offerFrame, offered, err := protocol.BuildLeaseOfferTransitionV1(config, durable, authority)
	if err != nil {
		t.Fatal(err)
	}
	if offerFrame.Kind != protocol.MessageLeaseOffer || offerFrame.Sequence != durable.Platform.Sequence+1 ||
		offerFrame.Ack != durable.Worker.Sequence || offerFrame.LeaseOffer.AttemptSequence != 1 ||
		offerFrame.LeaseOffer.Binding.LeaseGeneration != authority.LeaseGeneration ||
		len(offerFrame.LeaseOffer.Binding.FenceToken) != sha256.Size*2 ||
		!bytes.Equal(offerFrame.LeaseOffer.Binding.CapabilityDigest, capabilityDigest) ||
		offered.Attempt.Summary.State != protocol.AttemptOffered {
		t.Fatalf("non-canonical offer transition: frame=%+v state=%s", offerFrame, offered.Attempt.Summary.State)
	}
	replayFrame, replayed, err := protocol.BuildLeaseOfferTransitionV1(config, durable, authority)
	if err != nil || !bytes.Equal(offered.Digest, replayed.Digest) ||
		offerFrame.LeaseOffer.Binding.FenceToken != replayFrame.LeaseOffer.Binding.FenceToken {
		t.Fatalf("non-deterministic reducer: err=%v digest=%v fence=%v", err,
			bytes.Equal(offered.Digest, replayed.Digest), offerFrame.LeaseOffer.Binding.FenceToken == replayFrame.LeaseOffer.Binding.FenceToken)
	}
	authority.ContextDigest[0] ^= 1
	if offerFrame.LeaseOffer.Binding.ContextDigest[0] == authority.ContextDigest[0] {
		t.Fatal("offer retained caller-owned authority slices")
	}

	next := auth
	next.ConnectionGeneration++
	next.ChannelBinding = publicDigest(0x71)
	snapshot, err := machine.BeginReconnect(next)
	if err != nil || snapshot.Validate() != nil {
		t.Fatalf("snapshot err=%v validation=%v", err, snapshot.Validate())
	}
	snapshot.Attempt.Digest[0] ^= 1
	fresh, err := machine.SnapshotForReconnect()
	if err != nil || fresh.Validate() != nil || bytes.Equal(snapshot.Attempt.Digest, fresh.Attempt.Digest) {
		t.Fatalf("snapshot was not immutable: err=%v validation=%v", err, fresh.Validate())
	}
	negotiation := protocol.ReconnectNegotiationV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1,
		WorkerNonce: publicDigest(0x52), PlatformNonce: publicDigest(0x62), CapabilityDigest: capabilityDigest}
	reconnect, err := protocol.BuildReconnectV1(fresh, negotiation)
	if err != nil || len(reconnect.Signature) != 0 {
		t.Fatalf("reconnect builder: signature=%d err=%v", len(reconnect.Signature), err)
	}
	reconnectFrame := publicFrame(next, protocol.DirectionWorkerToPlatform, 1, 0, protocol.MessageReconnect)
	reconnectFrame.Reconnect = &reconnect
	if err := protocol.SignReconnectV1(private, next, &reconnectFrame); err != nil {
		t.Fatal(err)
	}
	if err := protocol.VerifyReconnectV1(next, reconnectFrame); err != nil {
		t.Fatal(err)
	}
	expectedNonce, expectedSummaryDigest := reconnect.WorkerNonce[0], reconnect.AttemptSummary.Digest[0]
	negotiation.WorkerNonce[0] ^= 1
	fresh.Attempt.Digest[0] ^= 1
	if reconnect.WorkerNonce[0] != expectedNonce || reconnect.AttemptSummary.Digest[0] != expectedSummaryDigest {
		t.Fatal("reconnect builder retained caller-owned slices")
	}
	negotiation.WorkerNonce[0] ^= 1
	fresh, err = machine.SnapshotForReconnect()
	if err != nil {
		t.Fatal(err)
	}
	acceptedReconnect, err := protocol.BuildReconnectAcceptedV1(fresh, fresh, negotiation)
	if err != nil || acceptedReconnect.ReplayPlan.TerminalDecision != protocol.ReconnectTerminalNone {
		t.Fatalf("accepted builder: plan=%+v err=%v", acceptedReconnect.ReplayPlan, err)
	}
}

func publicFrame(auth protocol.AuthContextV1, direction protocol.Direction, sequence, ack uint64, kind protocol.MessageKind) protocol.FrameV1 {
	return protocol.FrameV1{
		Version: auth.Version, MessageID: protocol.MessageIDV1(direction, sequence), WorkerID: auth.WorkerID,
		EnrollmentGeneration: auth.EnrollmentGeneration, ConnectionGeneration: auth.ConnectionGeneration,
		Sequence: sequence, Ack: ack, Kind: kind,
	}
}

func publicAccept(t *testing.T, machine *protocol.ConformanceMachine, auth protocol.AuthContextV1, direction protocol.Direction, frame protocol.FrameV1) {
	t.Helper()
	if err := machine.Accept(direction, frame, protocol.AcceptanceContextV1{
		ChannelBinding: auth.ChannelBinding, NowUnixMicro: 1_800_000_000_000_000,
	}); err != nil {
		t.Fatalf("accept %s: %v", kindLabel(frame.Kind), err)
	}
}

func publicDigest(value byte) []byte             { return bytes.Repeat([]byte{value}, sha256.Size) }
func kindLabel(kind protocol.MessageKind) string { return string(kind) }
