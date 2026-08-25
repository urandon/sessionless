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
