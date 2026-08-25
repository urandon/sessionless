package attachedworkerprotocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestMachineSnapshotRoundTripContinuesAttempt(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.claimMachine(t, true)
	progress := fixture.frame(DirectionWorkerToPlatform, MessageProgress)
	progress.Progress = &ProgressV1{Binding: fixture.binding, AttemptSequence: 2, ProgressSequence: 1, Stage: ProgressActive}
	acceptOK(t, machine, DirectionWorkerToPlatform, progress, fixture.auth.ChannelBinding)

	restored := roundTripMachine(t, machine)
	acceptOK(t, restored, DirectionWorkerToPlatform, progress, fixture.auth.ChannelBinding)
	next := fixture.frame(DirectionWorkerToPlatform, MessageProgress)
	next.Progress = &ProgressV1{Binding: fixture.binding, AttemptSequence: 3, ProgressSequence: 2, Stage: ProgressActive}
	acceptOK(t, machine, DirectionWorkerToPlatform, next, fixture.auth.ChannelBinding)
	acceptOK(t, restored, DirectionWorkerToPlatform, next, fixture.auth.ChannelBinding)
	assertSameMachineSnapshot(t, machine, restored)
}

func TestMachineSnapshotRoundTripsEveryHandshakeState(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine, err := NewConformanceMachine(MachineConfig{Auth: fixture.auth, WorkerOffer: fixture.workerOffer,
		PlatformOffer: fixture.platformOffer, ImplementedVersions: []ProtocolVersion{1}})
	if err != nil {
		t.Fatal(err)
	}
	machine = roundTripMachine(t, machine)
	workerNonce, platformNonce := digestByte(0x61), digestByte(0x71)
	hello := fixture.frame(DirectionWorkerToPlatform, MessageHello)
	hello.Hello = &HelloV1{Offer: fixture.workerOffer, WorkerNonce: workerNonce}
	acceptOK(t, machine, DirectionWorkerToPlatform, hello, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	challenge := fixture.frame(DirectionPlatformToWorker, MessageChallenge)
	challenge.Challenge = &ChallengeV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	acceptOK(t, machine, DirectionPlatformToWorker, challenge, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	attach := fixture.frame(DirectionWorkerToPlatform, MessageAttach)
	attach.Attach = &AttachV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest}
	if err := SignAttachV1(fixture.private, fixture.auth, &attach); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, attach, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	accepted := fixture.frame(DirectionPlatformToWorker, MessageAttachAccepted)
	accepted.AttachAccepted = &AttachAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest}
	acceptOK(t, machine, DirectionPlatformToWorker, accepted, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	manifest := fixture.frame(DirectionWorkerToPlatform, MessageManifest)
	manifest.Manifest = &ManifestV1{Manifest: fixture.manifest, Digest: fixture.digest}
	if err := SignManifestV1(fixture.private, fixture.auth, &manifest); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, manifest, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)

	next := fixture.auth
	next.ConnectionGeneration++
	next.ChannelBinding = digestByte(0x39)
	previous, err := machine.BeginReconnect(next)
	if err != nil {
		t.Fatal(err)
	}
	fixture.auth, fixture.workerSeq, fixture.platformSeq = next, 0, 0
	machine = roundTripMachine(t, machine)
	workerNonce, platformNonce = digestByte(0x62), digestByte(0x72)
	hello = fixture.frame(DirectionWorkerToPlatform, MessageHello)
	hello.Hello = &HelloV1{Offer: fixture.workerOffer, WorkerNonce: workerNonce}
	acceptOK(t, machine, DirectionWorkerToPlatform, hello, next.ChannelBinding)
	machine = roundTripMachine(t, machine)
	challenge = fixture.frame(DirectionPlatformToWorker, MessageChallenge)
	challenge.Challenge = &ChallengeV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	acceptOK(t, machine, DirectionPlatformToWorker, challenge, next.ChannelBinding)
	machine = roundTripMachine(t, machine)
	negotiation := ReconnectNegotiationV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
		SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest}
	reconnectPayload, err := BuildReconnectV1(previous, negotiation)
	if err != nil {
		t.Fatal(err)
	}
	reconnect := fixture.frame(DirectionWorkerToPlatform, MessageReconnect)
	reconnect.Reconnect = &reconnectPayload
	if err := SignReconnectV1(fixture.private, next, &reconnect); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, reconnect, next.ChannelBinding)
	machine = roundTripMachine(t, machine)
	reconnectAcceptedPayload, err := BuildReconnectAcceptedV1(previous, previous, negotiation)
	if err != nil {
		t.Fatal(err)
	}
	reconnectAccepted := fixture.frame(DirectionPlatformToWorker, MessageReconnectAccepted)
	reconnectAccepted.ReconnectAccepted = &reconnectAcceptedPayload
	acceptOK(t, machine, DirectionPlatformToWorker, reconnectAccepted, next.ChannelBinding)
	machine = roundTripMachine(t, machine)
	manifest = fixture.frame(DirectionWorkerToPlatform, MessageManifest)
	manifest.Manifest = &ManifestV1{Manifest: fixture.manifest, Digest: fixture.digest}
	if err := SignManifestV1(fixture.private, next, &manifest); err != nil {
		t.Fatal(err)
	}
	acceptOK(t, machine, DirectionWorkerToPlatform, manifest, next.ChannelBinding)
	if got := roundTripMachine(t, machine).ConnectionState(); got != ConnectionReady {
		t.Fatalf("state=%s", got)
	}
}

func TestMachineSnapshotRetainsTerminalReplayCommitment(t *testing.T) {
	authoritativeFixture := newProtocolFixture(t)
	authoritative := authoritativeFixture.claimMachine(t, true)
	workerFixture := newProtocolFixture(t)
	worker := workerFixture.claimMachine(t, true)
	terminal := workerFixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	terminal.Terminal = &TerminalV1{Binding: workerFixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x9a)}
	acceptOK(t, worker, DirectionWorkerToPlatform, terminal, workerFixture.auth.ChannelBinding)
	plan := reconnectPair(t, authoritative, worker, authoritativeFixture)
	if plan.TerminalDecision != ReconnectTerminalReplay {
		t.Fatalf("decision=%s", plan.TerminalDecision)
	}
	restored := roundTripMachine(t, authoritative)
	replay := authoritativeFixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	replay.Terminal = &TerminalV1{Binding: authoritativeFixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x9a)}
	divergent := replay
	divergent.Terminal = cloneTerminal(replay.Terminal)
	divergent.Terminal.EvidenceDigest = digestByte(0x9b)
	requireCode(t, acceptAt(restored, DirectionWorkerToPlatform, divergent, authoritativeFixture.auth.ChannelBinding,
		1_800_000_000_000_000), ErrorConflict)
	acceptOK(t, restored, DirectionWorkerToPlatform, replay, authoritativeFixture.auth.ChannelBinding)
	if restored.AttemptState() != AttemptTerminalPending || restored.attempt.pendingWorkerTerminal != nil {
		t.Fatalf("replay commitment did not survive restore: state=%s pending=%+v",
			restored.AttemptState(), restored.attempt.pendingWorkerTerminal)
	}
}

func TestMachineSnapshotRetainsDiscardTombstone(t *testing.T) {
	authoritativeFixture := newProtocolFixture(t)
	authoritative := authoritativeFixture.claimMachine(t, true)
	workerFixture := newProtocolFixture(t)
	worker := workerFixture.claimMachine(t, true)
	fence := authoritativeFixture.frame(DirectionPlatformToWorker, MessageCancel)
	fence.Cancel = &CancelV1{Binding: authoritativeFixture.binding, AttemptSequence: 3, CancelRevision: 1, Code: CancelFenced}
	acceptOK(t, authoritative, DirectionPlatformToWorker, fence, authoritativeFixture.auth.ChannelBinding)
	success := workerFixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	success.Terminal = &TerminalV1{Binding: workerFixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x9c)}
	acceptOK(t, worker, DirectionWorkerToPlatform, success, workerFixture.auth.ChannelBinding)
	plan := reconnectPair(t, authoritative, worker, authoritativeFixture)
	if plan.TerminalDecision != ReconnectTerminalDiscard {
		t.Fatalf("decision=%s", plan.TerminalDecision)
	}
	restored := roundTripMachine(t, authoritative)
	replay := authoritativeFixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	replay.Terminal = &TerminalV1{Binding: authoritativeFixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0x9c)}
	requireCode(t, acceptAt(restored, DirectionWorkerToPlatform, replay, authoritativeFixture.auth.ChannelBinding,
		1_800_000_000_000_000), ErrorProtocolViolation)
	cancelAck := replay
	cancelAck.Kind, cancelAck.Terminal = MessageCancelAck, nil
	cancelAck.CancelAck = &CancelAckV1{Binding: authoritativeFixture.binding, AttemptSequence: 3, CancelRevision: 1}
	acceptOK(t, restored, DirectionWorkerToPlatform, cancelAck, authoritativeFixture.auth.ChannelBinding)
	if restored.AttemptState() != AttemptFenced || restored.attempt.worker.sequence != 3 {
		t.Fatalf("discard tombstone blocked fenced cleanup: state=%s sequence=%d",
			restored.AttemptState(), restored.attempt.worker.sequence)
	}
}

func TestMachineSnapshotRetainsDrainAndRevoke(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	drain := fixture.frame(DirectionPlatformToWorker, MessageDrain)
	drain.Drain = &DrainV1{Revision: 7}
	acceptOK(t, machine, DirectionPlatformToWorker, drain, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	drained := fixture.frame(DirectionWorkerToPlatform, MessageDrained)
	drained.Drained = &DrainedV1{Revision: 7}
	acceptOK(t, machine, DirectionWorkerToPlatform, drained, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	revoke := fixture.frame(DirectionPlatformToWorker, MessageRevoke)
	revoke.Revoke = &RevokeV1{Revision: 8, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}
	acceptOK(t, machine, DirectionPlatformToWorker, revoke, fixture.auth.ChannelBinding)
	machine = roundTripMachine(t, machine)
	revoked := fixture.frame(DirectionWorkerToPlatform, MessageRevoked)
	revoked.Revoked = &RevokedV1{Revision: 8, NextEnrollmentGeneration: 5, NextConnectionGeneration: 10}
	acceptOK(t, machine, DirectionWorkerToPlatform, revoked, fixture.auth.ChannelBinding)
	if got := roundTripMachine(t, machine).ConnectionState(); got != ConnectionRevoked {
		t.Fatalf("state=%s", got)
	}
}

func TestMachineSnapshotRejectsTamperAndWrongAuthority(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.claimMachine(t, true)
	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	clone := snapshot.Clone()
	snapshot.Manifest.Features[0] = ProtocolFeatureV1("changed")
	snapshot.Attempt.Platform.Fingerprints[0].Fingerprint[0] ^= 1
	if clone.Validate() != nil || snapshot.Manifest.Features[0] == clone.Manifest.Features[0] {
		t.Fatal("snapshot clone retained caller-owned state")
	}

	for name, mutate := range map[string]func(*MachineSnapshotV1){
		"digest":   func(value *MachineSnapshotV1) { value.Digest[0] ^= 1 },
		"manifest": func(value *MachineSnapshotV1) { value.Manifest.BuildID = "changed" },
		"attempt fingerprint": func(value *MachineSnapshotV1) {
			value.Attempt.Platform.Fingerprints[0].Fingerprint[0] ^= 1
		},
		"configuration pin": func(value *MachineSnapshotV1) { value.ConfigurationDigest[0] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := clone.Clone()
			mutate(&candidate)
			if candidate.Validate() == nil {
				t.Fatal("tampered snapshot validated")
			}
		})
	}

	wrong := cloneMachineConfig(machine.config)
	wrong.Auth.OwnerUserID = "owner-2"
	_, err = RestoreConformanceMachine(wrong, clone)
	requireCode(t, err, ErrorUnauthorized)
}

func TestAttemptFrameFingerprintV1IgnoresConnectionEnvelope(t *testing.T) {
	fixture := newProtocolFixture(t)
	frame := FrameV1{Version: fixture.auth.Version, MessageID: MessageIDV1(DirectionWorkerToPlatform, 1),
		WorkerID: fixture.auth.WorkerID, EnrollmentGeneration: fixture.auth.EnrollmentGeneration,
		ConnectionGeneration: fixture.auth.ConnectionGeneration, Sequence: 1, Kind: MessageProgress,
		Progress: &ProgressV1{Binding: fixture.binding, AttemptSequence: 2, ProgressSequence: 1, Stage: ProgressActive}}
	first, err := AttemptFrameFingerprintV1(frame)
	if err != nil {
		t.Fatal(err)
	}
	frame.MessageID = MessageIDV1(DirectionWorkerToPlatform, 9)
	frame.ConnectionGeneration++
	frame.Sequence = 9
	frame.Ack = 4
	second, err := AttemptFrameFingerprintV1(frame)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("fingerprints differ or failed: equal=%v err=%v", bytes.Equal(first, second), err)
	}
	first[0] ^= 1
	if bytes.Equal(first, second) {
		t.Fatal("fingerprint result aliases internal state")
	}
}

func TestLeaseOfferTransitionRejectsNonAuthoritativeInputs(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.attachMachine(t)
	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	authority := LeaseOfferAuthorityV1{
		RunID: "run-1", AttemptID: "attempt-1", LeaseID: "lease-1", LeaseGeneration: 7,
		NowUnixMicro: 1_800_000_000_000_000, ExpiresAtUnixMicro: 1_900_000_000_000_000,
		ContextDigest: digestByte(0x41), PolicyDigest: digestByte(0x51),
	}
	frame, post, err := BuildLeaseOfferTransitionV1(machine.config, snapshot, authority)
	if err != nil || frame.Validate() != nil || post.Validate() != nil {
		t.Fatalf("build: frame=%v post=%v err=%v", frame.Validate(), post.Validate(), err)
	}
	if frame.LeaseOffer.Binding.FenceToken != opaqueLeaseFenceTokenV1(machine.config.Auth, authority) {
		t.Fatal("fence token was not canonically derived")
	}

	for name, mutate := range map[string]func(*LeaseOfferAuthorityV1){
		"zero fence": func(value *LeaseOfferAuthorityV1) { value.LeaseGeneration = 0 },
		"expired":    func(value *LeaseOfferAuthorityV1) { value.ExpiresAtUnixMicro = value.NowUnixMicro },
		"bad digest": func(value *LeaseOfferAuthorityV1) { value.PolicyDigest = []byte{1} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := authority
			mutate(&candidate)
			if _, _, err := BuildLeaseOfferTransitionV1(machine.config, snapshot, candidate); err == nil {
				t.Fatal("invalid authority was accepted")
			}
		})
	}
	if _, _, err := BuildLeaseOfferTransitionV1(machine.config, post, authority); err == nil {
		t.Fatal("non-idle machine accepted a second offer")
	}
	otherScope := cloneMachineConfig(machine.config)
	otherScope.Auth.OwnerUserID = "owner-2"
	if _, _, err := BuildLeaseOfferTransitionV1(otherScope, snapshot, authority); err == nil {
		t.Fatal("mismatched authority accepted snapshot")
	}
}

func TestCancelTransitionCoversOfferClaimAndExactReplay(t *testing.T) {
	fixture := newProtocolFixture(t)
	ready := fixture.attachMachine(t)
	readySnapshot, err := ready.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	offerAuthority := LeaseOfferAuthorityV1{
		RunID: fixture.binding.RunID, AttemptID: fixture.binding.AttemptID, LeaseID: fixture.binding.LeaseID,
		LeaseGeneration: fixture.binding.LeaseGeneration, NowUnixMicro: 1_800_000_000_000_000,
		ExpiresAtUnixMicro: fixture.binding.ExpiresAtUnixMicro,
		ContextDigest:      fixture.binding.ContextDigest, PolicyDigest: fixture.binding.PolicyDigest,
	}
	_, offered, err := BuildLeaseOfferTransitionV1(ready.config, readySnapshot, offerAuthority)
	if err != nil || offered.Attempt.Summary.State != AttemptOffered {
		t.Fatalf("offer: state=%s err=%v", offered.Attempt.Summary.State, err)
	}
	fenceAuthority := CancelAuthorityV1{Revision: 1, Code: CancelFenced, NowUnixMicro: fixture.binding.ExpiresAtUnixMicro}
	fence, fenced, err := BuildCancelTransitionV1(ready.config, offered, fenceAuthority)
	if err != nil || fenced.Attempt.Summary.State != AttemptFenced ||
		!sameAttemptBinding(fence.Cancel.Binding, offered.Attempt.Summary.Binding) ||
		fence.Cancel.AttemptSequence != 2 {
		t.Fatalf("offer expiry fence: frame=%+v state=%s err=%v", fence, fenced.Attempt.Summary.State, err)
	}
	replay, replayed, err := BuildCancelTransitionV1(ready.config, fenced, fenceAuthority)
	if err != nil || frameFingerprint(replay) != frameFingerprint(fence) || !bytes.Equal(replayed.Digest, fenced.Digest) {
		t.Fatalf("post-commit replay: exact=%v digest=%v err=%v", frameFingerprint(replay) == frameFingerprint(fence),
			bytes.Equal(replayed.Digest, fenced.Digest), err)
	}
	rebuilt, rebuiltPost, err := BuildCancelTransitionV1(ready.config, offered, fenceAuthority)
	if err != nil || frameFingerprint(rebuilt) != frameFingerprint(fence) || !bytes.Equal(rebuiltPost.Digest, fenced.Digest) {
		t.Fatalf("pre-commit rebuild: exact=%v digest=%v err=%v", frameFingerprint(rebuilt) == frameFingerprint(fence),
			bytes.Equal(rebuiltPost.Digest, fenced.Digest), err)
	}
	requestedAuthority := CancelAuthorityV1{Revision: 1, Code: CancelRequested, NowUnixMicro: fixture.binding.ExpiresAtUnixMicro}
	requested, requestedPost, err := BuildCancelTransitionV1(ready.config, offered, requestedAuthority)
	if err != nil || requestedPost.Attempt.Summary.State != AttemptCancelRequested || frameFingerprint(requested) == frameFingerprint(fence) {
		t.Fatalf("offer user cancel: state=%s divergent=%v err=%v", requestedPost.Attempt.Summary.State,
			frameFingerprint(requested) != frameFingerprint(fence), err)
	}
	if _, _, err := BuildCancelTransitionV1(ready.config, fenced, requestedAuthority); err == nil {
		t.Fatal("divergent committed cancel code replayed")
	}
	badRevision := fenceAuthority
	badRevision.Revision = 2
	if _, _, err := BuildCancelTransitionV1(ready.config, offered, badRevision); err == nil {
		t.Fatal("divergent cancellation revision accepted")
	}

	for name, deliverAccepted := range map[string]bool{"claim_pending": false, "claimed": true} {
		t.Run(name, func(t *testing.T) {
			claimFixture := newProtocolFixture(t)
			machine := claimFixture.claimMachine(t, deliverAccepted)
			snapshot, err := machine.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			_, post, err := BuildCancelTransitionV1(machine.config, snapshot, CancelAuthorityV1{
				Revision: 1, Code: CancelRequested, NowUnixMicro: 1_800_000_000_000_000,
			})
			if err != nil || post.Attempt.Summary.State != AttemptCancelRequested {
				t.Fatalf("state=%s err=%v", post.Attempt.Summary.State, err)
			}
		})
	}
	if _, _, err := BuildCancelTransitionV1(ready.config, readySnapshot, requestedAuthority); err == nil {
		t.Fatal("idle machine accepted cancellation")
	}
}

func TestLeaseAcceptedTransitionIsDerivedAndExpiryExclusive(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.claimMachine(t, false)
	snapshot, err := machine.Snapshot()
	if err != nil || snapshot.Attempt.Summary.State != AttemptClaimPending {
		t.Fatalf("snapshot state=%s err=%v", snapshot.Attempt.Summary.State, err)
	}
	authority := LeaseAcceptedAuthorityV1{NowUnixMicro: fixture.binding.ExpiresAtUnixMicro - 1}
	frame, post, err := BuildLeaseAcceptedTransitionV1(machine.config, snapshot, authority)
	if err != nil || post.Attempt.Summary.State != AttemptClaimed || frame.LeaseAccepted == nil ||
		!sameAttemptBinding(frame.LeaseAccepted.Binding, snapshot.Attempt.Summary.Binding) ||
		frame.LeaseAccepted.AttemptSequence != snapshot.Attempt.Platform.Sequence+1 {
		t.Fatalf("accepted: frame=%+v state=%s err=%v", frame, post.Attempt.Summary.State, err)
	}
	replay, replayed, err := BuildLeaseAcceptedTransitionV1(machine.config, post,
		LeaseAcceptedAuthorityV1{NowUnixMicro: fixture.binding.ExpiresAtUnixMicro})
	if err != nil || frameFingerprint(replay) != frameFingerprint(frame) || !bytes.Equal(replayed.Digest, post.Digest) {
		t.Fatalf("replay exact=%v digest=%v err=%v", frameFingerprint(replay) == frameFingerprint(frame),
			bytes.Equal(replayed.Digest, post.Digest), err)
	}
	if _, _, err := BuildLeaseAcceptedTransitionV1(machine.config, snapshot,
		LeaseAcceptedAuthorityV1{NowUnixMicro: fixture.binding.ExpiresAtUnixMicro}); err == nil {
		t.Fatal("lease accepted at exclusive expiry boundary")
	}
}

func TestTerminalAckTransitionDerivesCanonicalTerminal(t *testing.T) {
	fixture := newProtocolFixture(t)
	machine := fixture.claimMachine(t, true)
	terminal := fixture.frame(DirectionWorkerToPlatform, MessageTerminal)
	terminal.Terminal = &TerminalV1{Binding: fixture.binding, AttemptSequence: 2, TerminalSequence: 1,
		Status: TerminalSucceeded, Result: TerminalResultCompleted, EvidenceDigest: digestByte(0xab)}
	acceptOK(t, machine, DirectionWorkerToPlatform, terminal, fixture.auth.ChannelBinding)
	snapshot, err := machine.Snapshot()
	if err != nil || snapshot.Attempt.Summary.State != AttemptTerminalPending {
		t.Fatalf("snapshot state=%s err=%v", snapshot.Attempt.Summary.State, err)
	}
	authority := TerminalAckAuthorityV1{NowUnixMicro: fixture.binding.ExpiresAtUnixMicro - 1}
	frame, post, err := BuildTerminalAckTransitionV1(machine.config, snapshot, authority)
	if err != nil || post.Attempt.Summary.State != AttemptTerminalCommitted || frame.TerminalAck == nil ||
		frame.TerminalAck.Status != terminal.Terminal.Status || frame.TerminalAck.Result != terminal.Terminal.Result ||
		!bytes.Equal(frame.TerminalAck.EvidenceDigest, terminal.Terminal.EvidenceDigest) {
		t.Fatalf("terminal ack: frame=%+v state=%s err=%v", frame, post.Attempt.Summary.State, err)
	}
	replay, replayed, err := BuildTerminalAckTransitionV1(machine.config, post,
		TerminalAckAuthorityV1{NowUnixMicro: fixture.binding.ExpiresAtUnixMicro})
	if err != nil || frameFingerprint(replay) != frameFingerprint(frame) || !bytes.Equal(replayed.Digest, post.Digest) {
		t.Fatalf("replay exact=%v digest=%v err=%v", frameFingerprint(replay) == frameFingerprint(frame),
			bytes.Equal(replayed.Digest, post.Digest), err)
	}
	if _, _, err := BuildTerminalAckTransitionV1(machine.config, snapshot,
		TerminalAckAuthorityV1{NowUnixMicro: fixture.binding.ExpiresAtUnixMicro}); err == nil {
		t.Fatal("terminal ack accepted at exclusive lease expiry")
	}
}

func TestMachineSnapshotCodecIsStrictAndCanonical(t *testing.T) {
	fixture := newProtocolFixture(t)
	snapshot, err := fixture.attachMachine(t).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeMachineSnapshotV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := EncodeMachineSnapshotV1(decoded)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("codec is not canonical: equal=%v err=%v", bytes.Equal(encoded, reencoded), err)
	}

	invalid := map[string][]byte{
		"unknown":        append([]byte(`{"unknown":1,`), encoded[1:]...),
		"duplicate":      append([]byte(`{"version":1,`), encoded[1:]...),
		"case collision": append([]byte(`{"Version":1,`), encoded[1:]...),
		"null":           append([]byte(`{"hello":null,`), encoded[1:]...),
		"trailing":       append(append([]byte(nil), encoded...), []byte(` {}`)...),
	}
	for name, candidate := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeMachineSnapshotV1(candidate); err == nil {
				t.Fatal("invalid snapshot JSON decoded")
			}
		})
	}
	if _, err := DecodeMachineSnapshotV1(bytes.Repeat([]byte{'x'}, MaxBatchBytes+1)); err == nil {
		t.Fatal("oversized snapshot decoded")
	}

	// Snapshot persistence supports the full protocol replay bound rather than
	// inheriting the smaller HTTP batch-frame array limit.
	wide := snapshot.Clone()
	wide.Manifest.OperatingSystem = strings.Repeat("o", maxOpaqueBytes)
	wide.Manifest.Architecture = strings.Repeat("a", maxOpaqueBytes)
	wide.Manifest.BuildID = strings.Repeat("b", maxOpaqueBytes)
	wide.Manifest.HarnessName = strings.Repeat("h", maxOpaqueBytes)
	wide.Manifest.HarnessVersion = strings.Repeat("v", maxOpaqueBytes)
	wide.CapabilityDigest, err = ManifestDigestV1(*wide.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	wideBinding := cloneBinding(fixture.binding)
	wideBinding.CapabilityDigest = append([]byte(nil), wide.CapabilityDigest...)
	wide.Attempt.Summary = sealAttemptSummary(AttemptSummaryV1{
		State: AttemptClaimed, Binding: wideBinding, PlatformSequence: 2, WorkerSequence: MaxAttemptMessages,
	})
	wide.Attempt.Platform = MachineAttemptDirectionSnapshotV1{Sequence: 2, Fingerprints: []MachineAttemptFingerprintV1{
		{Sequence: 1, Fingerprint: digestByte(1)}, {Sequence: 2, Fingerprint: digestByte(2)},
	}}
	wide.Attempt.Worker = MachineAttemptDirectionSnapshotV1{Sequence: MaxAttemptMessages}
	for sequence := uint64(1); sequence <= MaxAttemptMessages; sequence++ {
		wide.Attempt.Worker.Fingerprints = append(wide.Attempt.Worker.Fingerprints,
			MachineAttemptFingerprintV1{Sequence: sequence, Fingerprint: digestByte(byte(sequence))})
	}
	wide.Digest, err = MachineSnapshotDigestV1(wide)
	if err != nil {
		t.Fatal(err)
	}
	encodedWide, err := EncodeMachineSnapshotV1(wide)
	if err != nil {
		t.Fatalf("full replay ledger encode: %v", err)
	}
	if _, err := DecodeMachineSnapshotV1(encodedWide); err != nil {
		t.Fatalf("full replay ledger decode: %v", err)
	}
}

func TestInitialAttachSnapshotRejectsTamperAndWrongConfig(t *testing.T) {
	fixture := newProtocolFixture(t)
	// Reuse the fixture helper's exact initial frames by building them here so
	// the bootstrap is tested independently from an already-mutated machine.
	workerNonce, platformNonce := digestByte(0x61), digestByte(0x71)
	attach := FrameV1{Version: fixture.auth.Version, MessageID: MessageIDV1(DirectionWorkerToPlatform, 2),
		WorkerID: fixture.auth.WorkerID, EnrollmentGeneration: fixture.auth.EnrollmentGeneration,
		ConnectionGeneration: fixture.auth.ConnectionGeneration, Sequence: 2, Ack: 1, Kind: MessageAttach,
		Attach: &AttachV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer, SelectedVersion: 1,
			WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest}}
	if err := SignAttachV1(fixture.private, fixture.auth, &attach); err != nil {
		t.Fatal(err)
	}
	accepted := FrameV1{Version: fixture.auth.Version, MessageID: MessageIDV1(DirectionPlatformToWorker, 2),
		WorkerID: fixture.auth.WorkerID, EnrollmentGeneration: fixture.auth.EnrollmentGeneration,
		ConnectionGeneration: fixture.auth.ConnectionGeneration, Sequence: 2, Ack: 2, Kind: MessageAttachAccepted,
		AttachAccepted: &AttachAcceptedV1{WorkerOffer: fixture.workerOffer, PlatformOffer: fixture.platformOffer,
			SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: fixture.digest}}
	config := MachineConfig{Auth: fixture.auth, WorkerOffer: fixture.workerOffer,
		PlatformOffer: fixture.platformOffer, ImplementedVersions: []ProtocolVersion{1}}
	if snapshot, err := BuildInitialAttachSnapshotV1(config, attach, accepted); err != nil || snapshot.Connection != ConnectionAttached {
		t.Fatalf("bootstrap: state=%s err=%v", snapshot.Connection, err)
	}
	tampered := accepted
	tamperedPayload := *accepted.AttachAccepted
	tamperedPayload.WorkerOffer = cloneVersionOffer(tamperedPayload.WorkerOffer)
	tamperedPayload.PlatformOffer = cloneVersionOffer(tamperedPayload.PlatformOffer)
	tamperedPayload.WorkerNonce = append([]byte(nil), tamperedPayload.WorkerNonce...)
	tamperedPayload.PlatformNonce = append([]byte(nil), tamperedPayload.PlatformNonce...)
	tamperedPayload.CapabilityDigest = append([]byte(nil), tamperedPayload.CapabilityDigest...)
	tampered.AttachAccepted = &tamperedPayload
	tampered.AttachAccepted.CapabilityDigest = digestByte(0xff)
	if _, err := BuildInitialAttachSnapshotV1(config, attach, tampered); err == nil {
		t.Fatal("tampered acceptance bootstrapped")
	}
	wrong := cloneMachineConfig(config)
	wrong.Auth.OwnerUserID = "owner-2"
	if _, err := BuildInitialAttachSnapshotV1(wrong, attach, accepted); err == nil {
		t.Fatal("wrong authority bootstrapped")
	}
}

func FuzzMachineSnapshotValidate(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"version":1,"connection":"ready"}`))
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 4096 {
			t.Skip()
		}
		var snapshot MachineSnapshotV1
		if json.Unmarshal(encoded, &snapshot) != nil {
			return
		}
		_ = snapshot.Validate()
		_ = snapshot.Clone()
	})
}

func roundTripMachine(t *testing.T, machine *ConformanceMachine) *ConformanceMachine {
	t.Helper()
	snapshot, err := machine.Snapshot()
	if err != nil || snapshot.Validate() != nil {
		t.Fatalf("snapshot: err=%v validation=%v state=%s", err, snapshot.Validate(), machine.ConnectionState())
	}
	restored, err := RestoreConformanceMachine(machine.config, snapshot)
	if err != nil {
		t.Fatalf("restore state=%s: %v", machine.ConnectionState(), err)
	}
	assertSameMachineSnapshot(t, machine, restored)
	return restored
}

func assertSameMachineSnapshot(t *testing.T, left, right *ConformanceMachine) {
	t.Helper()
	leftSnapshot, leftErr := left.Snapshot()
	rightSnapshot, rightErr := right.Snapshot()
	if leftErr != nil || rightErr != nil || !bytes.Equal(leftSnapshot.Digest, rightSnapshot.Digest) {
		t.Fatalf("snapshot mismatch: left=%x/%v right=%x/%v", leftSnapshot.Digest, leftErr, rightSnapshot.Digest, rightErr)
	}
}
