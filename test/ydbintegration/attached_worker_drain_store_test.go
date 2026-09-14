//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestAttachedWorkerDrainIsDurableReplayableAndOwnerScoped(t *testing.T) {
	store, _ := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	suffix := attachedWorkerDrainTestSuffix(t, "drain")
	tenantID := domain.TenantID(uniqueID("tenant-worker-" + suffix))
	ownerID := domain.UserID(uniqueID("owner-worker-" + suffix))
	otherOwnerID := domain.UserID(uniqueID("other-owner-worker-" + suffix))

	enrollment, createAudit := attachedWorkerEnrollmentFixture(suffix, tenantID, ownerID, now.Add(-time.Second))
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x61}, ed25519.SeedSize))
	claim := attachedWorkerClaimFixture(enrollment, 0x61)
	claim.IdentityPublicKey = append([]byte(nil), privateKey.Public().(ed25519.PublicKey)...)
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	worker := claimed.Worker
	challengeCreate := attachedWorkerChallengeCreateFixture(worker, suffix)
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, challengeCreate)
	if err != nil {
		t.Fatal(err)
	}
	channelBytes := bytes.Repeat([]byte{0x62}, 32)
	canonicalManifest, capabilityDigest, attachedSnapshot, readySnapshot, manifestSignature := attachedWorkerProtocolSnapshotFixture(t, worker, challenge, privateKey, channelBytes)
	secretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("drain-bearer"))
	activation := ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		Purpose: domain.AttachedWorkerAttachInitial, ExpectedChallengeRevision: challenge.Revision,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest:   challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channelBytes),
		ExpectedCapabilityDigest: capabilityDigest, ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
	}
	activated, err := store.ActivateAttachedWorkerConnection(ctx, activation)
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated {
		t.Fatalf("activate = %#v, %v", activated, err)
	}
	worker, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker after activation = %#v found=%t err=%v", worker, found, err)
	}
	manifest := ports.AttachedWorkerManifestAcceptance{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID: activated.Connection.ID, ConnectionGeneration: activated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: secretDigest,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: capabilityDigest, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey),
			CanonicalManifest: canonicalManifest, ManifestPayload: []byte(`{"version":1,"surface":"codex-exec"}`),
			Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: readySnapshot, PresenceTTL: 10 * time.Minute,
	}
	accepted, err := store.AcceptAttachedWorkerManifest(ctx, manifest)
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("manifest = %#v, %v", accepted, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("ready worker = %#v found=%t err=%v", worker, found, err)
	}

	request := ports.AttachedWorkerDrainRequest{TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ExpectedWorkerRevision: worker.Revision}
	if result, requestErr := store.RequestAttachedWorkerDrain(ctx, ports.AttachedWorkerDrainRequest{
		TenantID: tenantID, OwnerUserID: otherOwnerID, WorkerID: worker.ID, ExpectedWorkerRevision: worker.Revision,
	}); requestErr != nil || result.Status != ports.AttachedWorkerExecutionNotFound {
		t.Fatalf("cross-owner drain request = %#v, %v", result, requestErr)
	}
	drained, err := store.RequestAttachedWorkerDrain(ctx, request)
	if err != nil || drained.Status != ports.AttachedWorkerExecutionApplied || drained.Outbound == nil ||
		drained.Worker.DesiredState != domain.AttachedWorkerDesiredDrain || drained.Worker.ObservedState != domain.AttachedWorkerObservedDraining ||
		drained.Connection.State != domain.AttachedWorkerConnectionDraining {
		t.Fatalf("drain = %#v, %v", drained, err)
	}
	wantDrainRevision := request.ExpectedWorkerRevision + 1
	if drained.Worker.Revision != wantDrainRevision || drained.Outbound.DrainRevision != wantDrainRevision {
		t.Fatalf("immediate drain revisions: worker got %d want %d; semantic got %d want %d",
			drained.Worker.Revision, wantDrainRevision, drained.Outbound.DrainRevision, wantDrainRevision)
	}
	replayed, err := store.RequestAttachedWorkerDrain(ctx, request)
	if err != nil || replayed.Status != ports.AttachedWorkerExecutionReplayed || replayed.Outbound == nil ||
		!bytes.Equal(replayed.Outbound.Payload, drained.Outbound.Payload) {
		t.Fatalf("drain replay = %#v, %v", replayed, err)
	}
	for name, revision := range map[string]uint64{
		"stale":     request.ExpectedWorkerRevision - 1,
		"divergent": request.ExpectedWorkerRevision + 1,
	} {
		t.Run(name+" request", func(t *testing.T) {
			candidate := request
			candidate.ExpectedWorkerRevision = revision
			result, requestErr := store.RequestAttachedWorkerDrain(ctx, candidate)
			if requestErr != nil || result.Status != ports.AttachedWorkerExecutionConflict {
				t.Fatalf("drain request = %#v, %v", result, requestErr)
			}
		})
	}
	if result, err := store.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: tenantID, OwnerUserID: otherOwnerID, WorkerID: worker.ID,
		ConnectionID: drained.Connection.ID, PresentedSecretDigest: secretDigest,
	}); err != nil || result.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("cross-owner poll = %#v, %v", result, err)
	}
	poll, err := store.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID: drained.Connection.ID, PresentedSecretDigest: secretDigest,
	})
	if err != nil || poll.Status != ports.AttachedWorkerExecutionApplied || poll.Outbound == nil ||
		!bytes.Equal(poll.Outbound.Payload, drained.Outbound.Payload) {
		t.Fatalf("control poll = %#v, %v", poll, err)
	}
	batch, err := attachedworkerprotocol.DecodeBatchV1(poll.Outbound.Payload)
	if err != nil || len(batch.Frames) != 1 || batch.Frames[0].Drain == nil {
		t.Fatalf("drain frame = %#v, %v", batch, err)
	}
	drainFrame := batch.Frames[0]
	ack := attachedworkerprotocol.FrameV1{
		Version:   drainFrame.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, drained.Connection.WorkerSequence+1),
		WorkerID:  drainFrame.WorkerID, EnrollmentGeneration: drainFrame.EnrollmentGeneration,
		ConnectionGeneration: drainFrame.ConnectionGeneration, Sequence: drained.Connection.WorkerSequence + 1,
		Ack: drainFrame.Sequence, Kind: attachedworkerprotocol.MessageDrained,
		Drained: &attachedworkerprotocol.DrainedV1{Revision: drainFrame.Drain.Revision},
	}
	exchange := ports.AttachedWorkerControlExchange{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID: drained.Connection.ID, PresentedSecretDigest: secretDigest, InboundFrame: ack,
	}
	wrongRevision := exchange
	wrongRevision.InboundFrame.Drained = &attachedworkerprotocol.DrainedV1{Revision: drainFrame.Drain.Revision + 1}
	if result, exchangeErr := store.ExchangeAttachedWorkerControl(ctx, wrongRevision); exchangeErr != nil || result.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("divergent drained = %#v, %v", result, exchangeErr)
	}
	wrongOwner := exchange
	wrongOwner.OwnerUserID = otherOwnerID
	if result, exchangeErr := store.ExchangeAttachedWorkerControl(ctx, wrongOwner); exchangeErr != nil || result.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("cross-owner drained = %#v, %v", result, exchangeErr)
	}
	completed, err := store.ExchangeAttachedWorkerControl(ctx, exchange)
	if err != nil || completed.Status != ports.AttachedWorkerExecutionApplied {
		t.Fatalf("drained = %#v, %v", completed, err)
	}
	completedSnapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(completed.Connection.ProtocolSnapshot)
	if err != nil || completedSnapshot.Connection != attachedworkerprotocol.ConnectionDrained {
		t.Fatalf("completed snapshot = %#v, %v", completedSnapshot, err)
	}
	completedReplay, err := store.ExchangeAttachedWorkerControl(ctx, exchange)
	if err != nil || completedReplay.Status != ports.AttachedWorkerExecutionReplayed {
		t.Fatalf("drained replay = %#v, %v", completedReplay, err)
	}
	audits, err := store.ListAttachedWorkerAuditEvents(ctx, tenantID, ownerID, worker.ID, request.ExpectedWorkerRevision+1, 10)
	if err != nil || len(audits) != 2 || audits[0].Action != domain.AttachedWorkerAuditDrainRequested || audits[1].Action != domain.AttachedWorkerAuditDrained {
		t.Fatalf("drain audits = %#v, %v", audits, err)
	}
}

func TestAttachedWorkerDrainSerializesAgainstLeaseClaim(t *testing.T) {
	store, client, worker, connection, secretDigest, _, _, now := readyAttachedWorkerForDrain(t, "claim-race")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	offer := attachedWorkerOfferForDrain(t, store, client, worker, connection, now, "claim-race")
	batch, err := attachedworkerprotocol.DecodeBatchV1(offer.Outbound.Payload)
	if err != nil || len(batch.Frames) != 1 || batch.Frames[0].LeaseOffer == nil {
		t.Fatalf("offer frame = %#v, %v", batch, err)
	}
	offerFrame := batch.Frames[0]
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("offered connection = %#v found=%t err=%v", connection, found, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("offered worker = %#v found=%t err=%v", worker, found, err)
	}
	claimFrame := attachedworkerprotocol.FrameV1{
		Version:   offerFrame.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID:  string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: offerFrame.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offerFrame.LeaseOffer.Binding, AttemptSequence: 1},
	}
	claimRequest := ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, AttemptID: offer.Attempt.AttemptID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest, InboundFrame: claimFrame,
	}
	drainRequest := ports.AttachedWorkerDrainRequest{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedWorkerRevision: worker.Revision,
	}
	type claimOutcome struct {
		result ports.AttachedWorkerAttemptResult
		err    error
	}
	type drainOutcome struct {
		result ports.AttachedWorkerDrainResult
		err    error
	}
	start := make(chan struct{})
	claimDone := make(chan claimOutcome, 1)
	drainDone := make(chan drainOutcome, 1)
	go func() {
		<-start
		result, exchangeErr := store.ExchangeAttachedWorkerAttempt(ctx, claimRequest)
		claimDone <- claimOutcome{result: result, err: exchangeErr}
	}()
	go func() {
		<-start
		result, drainErr := store.RequestAttachedWorkerDrain(ctx, drainRequest)
		drainDone <- drainOutcome{result: result, err: drainErr}
	}()
	close(start)
	claim := <-claimDone
	drain := <-drainDone
	if drain.err != nil || drain.result.Status != ports.AttachedWorkerExecutionApplied || drain.result.Outbound != nil ||
		drain.result.Worker.DesiredState != domain.AttachedWorkerDesiredDrain ||
		drain.result.Worker.ObservedState != domain.AttachedWorkerObservedOnline {
		t.Fatalf("drain race result = %#v, %v", drain.result, drain.err)
	}
	if claim.err != nil || (claim.result.Status != ports.AttachedWorkerExecutionApplied && claim.result.Status != ports.AttachedWorkerExecutionDenied) {
		t.Fatalf("claim race result = %#v, %v", claim.result, claim.err)
	}
	retry, err := store.ExchangeAttachedWorkerAttempt(ctx, claimRequest)
	if err != nil {
		t.Fatal(err)
	}
	if claim.result.Status == ports.AttachedWorkerExecutionApplied {
		if retry.Status != ports.AttachedWorkerExecutionReplayed || retry.Outbound == nil {
			t.Fatalf("winning claim did not replay after drain = %#v", retry)
		}
	} else if retry.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("claim admitted after drain won = %#v", retry)
	}
	poll, err := store.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secretDigest,
	})
	if err != nil || poll.Status != ports.AttachedWorkerExecutionApplied || poll.Outbound != nil {
		t.Fatalf("drain overtook unacknowledged attempt frame = %#v, %v", poll, err)
	}
}

func TestAttachedWorkerDrainWaitsForActiveAttemptRetirement(t *testing.T) {
	store, client, worker, connection, secretDigest, _, _, now := readyAttachedWorkerForDrain(t, "active-attempt")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	offer := attachedWorkerOfferForDrain(t, store, client, worker, connection, now, "active-attempt")
	offerBatch, err := attachedworkerprotocol.DecodeBatchV1(offer.Outbound.Payload)
	if err != nil || len(offerBatch.Frames) != 1 || offerBatch.Frames[0].LeaseOffer == nil {
		t.Fatalf("offer frame = %#v, %v", offerBatch, err)
	}
	offerFrame := offerBatch.Frames[0]
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("offered connection = %#v found=%t err=%v", connection, found, err)
	}
	claimFrame := attachedworkerprotocol.FrameV1{
		Version: offerFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID: string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: offerFrame.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offerFrame.LeaseOffer.Binding, AttemptSequence: 1},
	}
	claim, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, AttemptID: offer.Attempt.AttemptID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest, InboundFrame: claimFrame,
	})
	if err != nil || claim.Status != ports.AttachedWorkerExecutionApplied || claim.Outbound == nil ||
		claim.Attempt.State != domain.AttachedWorkerAttemptClaimed {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	acceptedBatch, err := attachedworkerprotocol.DecodeBatchV1(claim.Outbound.Payload)
	if err != nil || len(acceptedBatch.Frames) != 1 || acceptedBatch.Frames[0].LeaseAccepted == nil {
		t.Fatalf("lease accepted frame = %#v, %v", acceptedBatch, err)
	}
	acceptedFrame := acceptedBatch.Frames[0]
	worker, found, err = store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("claimed worker = %#v found=%t err=%v", worker, found, err)
	}
	requestedRevision := worker.Revision
	drain, err := store.RequestAttachedWorkerDrain(ctx, ports.AttachedWorkerDrainRequest{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedWorkerRevision: worker.Revision,
	})
	if err != nil || drain.Status != ports.AttachedWorkerExecutionApplied || drain.Outbound != nil ||
		drain.Worker.DesiredState != domain.AttachedWorkerDesiredDrain || drain.Worker.ObservedState != domain.AttachedWorkerObservedOnline {
		t.Fatalf("drain with unacked lease accepted = %#v, %v", drain, err)
	}
	if drain.Worker.Revision != requestedRevision+1 {
		t.Fatalf("pending drain worker revision = %d, want %d", drain.Worker.Revision, requestedRevision+1)
	}
	progressFrame := attachedworkerprotocol.FrameV1{
		Version: acceptedFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, drain.Connection.WorkerSequence+1),
		WorkerID: string(worker.ID), EnrollmentGeneration: drain.Connection.EnrollmentGeneration,
		ConnectionGeneration: drain.Connection.ConnectionGeneration, Sequence: drain.Connection.WorkerSequence + 1,
		Ack: acceptedFrame.Sequence, Kind: attachedworkerprotocol.MessageProgress,
		Progress: &attachedworkerprotocol.ProgressV1{Binding: acceptedFrame.LeaseAccepted.Binding, AttemptSequence: 2, ProgressSequence: 1, Stage: attachedworkerprotocol.ProgressStarted},
	}
	progress, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: drain.Connection.ID, AttemptID: claim.Attempt.AttemptID, LeaseGeneration: claim.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest, InboundFrame: progressFrame,
	})
	if err != nil || progress.Status != ports.AttachedWorkerExecutionApplied || progress.Attempt.State != domain.AttachedWorkerAttemptClaimed {
		t.Fatalf("progress while draining = %#v, %v", progress, err)
	}
	poll, err := store.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: drain.Connection.ID, PresentedSecretDigest: secretDigest,
	})
	if err != nil || poll.Status != ports.AttachedWorkerExecutionApplied || poll.Outbound == nil ||
		poll.Worker.ObservedState != domain.AttachedWorkerObservedDraining {
		t.Fatalf("drain delivery after attempt ack = %#v, %v", poll, err)
	}
	wantDeliveryRevision := drain.Worker.Revision + 1
	if poll.Worker.Revision != wantDeliveryRevision || poll.Outbound.DrainRevision != drain.Worker.Revision {
		t.Fatalf("delayed drain revisions: worker got %d want %d; semantic got %d want %d",
			poll.Worker.Revision, wantDeliveryRevision, poll.Outbound.DrainRevision, drain.Worker.Revision)
	}
	drainBatch, err := attachedworkerprotocol.DecodeBatchV1(poll.Outbound.Payload)
	if err != nil || len(drainBatch.Frames) != 1 || drainBatch.Frames[0].Drain == nil {
		t.Fatalf("drain frame = %#v, %v", drainBatch, err)
	}
	drainFrame := drainBatch.Frames[0]
	drainedFrame := attachedworkerprotocol.FrameV1{
		Version: drainFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, poll.Connection.WorkerSequence+1),
		WorkerID: string(worker.ID), EnrollmentGeneration: poll.Connection.EnrollmentGeneration,
		ConnectionGeneration: poll.Connection.ConnectionGeneration, Sequence: poll.Connection.WorkerSequence + 1,
		Ack: drainFrame.Sequence, Kind: attachedworkerprotocol.MessageDrained,
		Drained: &attachedworkerprotocol.DrainedV1{Revision: drainFrame.Drain.Revision},
	}
	blocked, err := store.ExchangeAttachedWorkerControl(ctx, ports.AttachedWorkerControlExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: poll.Connection.ID, PresentedSecretDigest: secretDigest, InboundFrame: drainedFrame,
	})
	if err != nil || blocked.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("active attempt acknowledged as drained = %#v, %v", blocked, err)
	}

	materialization, evidence := attachedWorkerFailureForDrain(t, store, claim.Attempt, "active-attempt")
	terminalFrame := attachedworkerprotocol.FrameV1{
		Version: drainFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, poll.Connection.WorkerSequence+1),
		WorkerID: string(worker.ID), EnrollmentGeneration: poll.Connection.EnrollmentGeneration,
		ConnectionGeneration: poll.Connection.ConnectionGeneration, Sequence: poll.Connection.WorkerSequence + 1,
		Ack: drainFrame.Sequence, Kind: attachedworkerprotocol.MessageTerminal,
		Terminal: &attachedworkerprotocol.TerminalV1{
			Binding: acceptedFrame.LeaseAccepted.Binding, AttemptSequence: 3, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalFailed, Result: attachedworkerprotocol.TerminalResultFailed,
			EvidenceDigest: evidence,
		},
	}
	terminal, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: poll.Connection.ID, AttemptID: claim.Attempt.AttemptID, LeaseGeneration: claim.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest, InboundFrame: terminalFrame,
	})
	if err != nil || terminal.Status != ports.AttachedWorkerExecutionApplied ||
		terminal.Attempt.State != domain.AttachedWorkerAttemptTerminalPending {
		t.Fatalf("terminal while draining = %#v, %v", terminal, err)
	}
	committed, err := store.CommitAttachedWorkerTerminal(ctx, ports.AttachedWorkerTerminalCommit{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		AttemptID: terminal.Attempt.AttemptID, LeaseGeneration: terminal.Attempt.LeaseGeneration,
		Materialization: materialization,
	})
	if err != nil || committed.Status != ports.AttachedWorkerExecutionApplied || committed.Outbound == nil ||
		committed.Attempt.State != domain.AttachedWorkerAttemptTerminalCommitted {
		t.Fatalf("terminal commit while draining = %#v, %v", committed, err)
	}
	ackBatch, err := attachedworkerprotocol.DecodeBatchV1(committed.Outbound.Payload)
	if err != nil || len(ackBatch.Frames) != 1 || ackBatch.Frames[0].TerminalAck == nil {
		t.Fatalf("terminal ack frame = %#v, %v", ackBatch, err)
	}
	terminalAck := ackBatch.Frames[0]
	connection, found, err = store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("terminal committed connection = %#v found=%t err=%v", connection, found, err)
	}
	config, snapshot := attachedWorkerProtocolAuthorityForDrain(t, worker, connection)
	heartbeat := attachedworkerprotocol.FrameV1{
		Version:   attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion),
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID:  string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: terminalAck.Sequence, Kind: attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: time.Now().UTC().UnixMicro()},
	}
	acknowledged, err := attachedworkerprotocol.ApplyMachineFrameV1(
		config, snapshot, attachedworkerprotocol.DirectionWorkerToPlatform, heartbeat, time.Now().UTC().UnixMicro(),
	)
	if err != nil {
		t.Fatalf("terminal ack heartbeat: %v", err)
	}
	acknowledgedBytes, err := attachedworkerprotocol.EncodeMachineSnapshotV1(acknowledged)
	if err != nil {
		t.Fatal(err)
	}
	authorized, err := store.AuthorizeAttachedWorkerExchange(ctx, ports.AttachedWorkerExchangeAuthorization{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, ConnectionGeneration: connection.ConnectionGeneration,
		PresentedSecretDigest: secretDigest, ExpectedConnectionRevision: connection.Revision,
		PlatformSequence: acknowledged.Platform.Sequence, WorkerSequence: acknowledged.Worker.Sequence,
		PlatformAck: acknowledged.Platform.Ack, WorkerAck: acknowledged.Worker.Ack,
		ProtocolSnapshot: acknowledgedBytes, CheckpointInterval: time.Nanosecond, PresenceTTL: 10 * time.Minute,
	})
	if err != nil || authorized.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("terminal ack checkpoint = %#v, %v", authorized, err)
	}
	retired, err := store.PollAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptPoll{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secretDigest,
	})
	if err != nil || retired.Status != ports.AttachedWorkerExecutionApplied || retired.Outbound != nil ||
		retired.Attempt.State != domain.AttachedWorkerAttemptRetired {
		t.Fatalf("terminal retirement while draining = %#v, %v", retired, err)
	}
	connection, found, err = store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("retired connection = %#v found=%t err=%v", connection, found, err)
	}
	drainedFrame.Sequence = connection.WorkerSequence + 1
	drainedFrame.MessageID = attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, drainedFrame.Sequence)
	drainedFrame.Ack = connection.PlatformSequence
	completed, err := store.ExchangeAttachedWorkerControl(ctx, ports.AttachedWorkerControlExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secretDigest, InboundFrame: drainedFrame,
	})
	if err != nil || completed.Status != ports.AttachedWorkerExecutionApplied ||
		completed.Connection.State != domain.AttachedWorkerConnectionDraining {
		t.Fatalf("drained after terminal retirement = %#v, %v", completed, err)
	}
	completedSnapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(completed.Connection.ProtocolSnapshot)
	if err != nil || completedSnapshot.Connection != attachedworkerprotocol.ConnectionDrained {
		t.Fatalf("drained protocol snapshot = %#v, %v", completedSnapshot, err)
	}
}

func TestAttachedWorkerDrainRejectsFencedUnknownAttempt(t *testing.T) {
	store, client, worker, connection, secretDigest, _, _, now := readyAttachedWorkerForDrain(t, "fenced-unknown")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	offer := attachedWorkerOfferForDrain(t, store, client, worker, connection, now, "fenced-unknown")
	offerBatch, err := attachedworkerprotocol.DecodeBatchV1(offer.Outbound.Payload)
	if err != nil || len(offerBatch.Frames) != 1 || offerBatch.Frames[0].LeaseOffer == nil {
		t.Fatalf("offer frame = %#v, %v", offerBatch, err)
	}
	offerFrame := offerBatch.Frames[0]
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("offered connection = %#v found=%t err=%v", connection, found, err)
	}
	claim, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, AttemptID: offer.Attempt.AttemptID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest,
		InboundFrame: attachedworkerprotocol.FrameV1{
			Version: offerFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
			WorkerID: string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
			ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
			Ack: offerFrame.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
			LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offerFrame.LeaseOffer.Binding, AttemptSequence: 1},
		},
	})
	if err != nil || claim.Status != ports.AttachedWorkerExecutionApplied || claim.Outbound == nil ||
		claim.Attempt.State != domain.AttachedWorkerAttemptClaimed {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	acceptedBatch, err := attachedworkerprotocol.DecodeBatchV1(claim.Outbound.Payload)
	if err != nil || len(acceptedBatch.Frames) != 1 || acceptedBatch.Frames[0].LeaseAccepted == nil {
		t.Fatalf("lease accepted frame = %#v, %v", acceptedBatch, err)
	}
	acceptedFrame := acceptedBatch.Frames[0]
	worker, found, err = store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("claimed worker = %#v found=%t err=%v", worker, found, err)
	}
	drain, err := store.RequestAttachedWorkerDrain(ctx, ports.AttachedWorkerDrainRequest{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedWorkerRevision: worker.Revision,
	})
	if err != nil || drain.Status != ports.AttachedWorkerExecutionApplied || drain.Outbound != nil {
		t.Fatalf("pending drain = %#v, %v", drain, err)
	}
	progress, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: drain.Connection.ID, AttemptID: claim.Attempt.AttemptID, LeaseGeneration: claim.Attempt.LeaseGeneration,
		PresentedSecretDigest: secretDigest,
		InboundFrame: attachedworkerprotocol.FrameV1{
			Version: acceptedFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, drain.Connection.WorkerSequence+1),
			WorkerID: string(worker.ID), EnrollmentGeneration: drain.Connection.EnrollmentGeneration,
			ConnectionGeneration: drain.Connection.ConnectionGeneration, Sequence: drain.Connection.WorkerSequence + 1,
			Ack: acceptedFrame.Sequence, Kind: attachedworkerprotocol.MessageProgress,
			Progress: &attachedworkerprotocol.ProgressV1{
				Binding: acceptedFrame.LeaseAccepted.Binding, AttemptSequence: 2,
				ProgressSequence: 1, Stage: attachedworkerprotocol.ProgressStarted,
			},
		},
	})
	if err != nil || progress.Status != ports.AttachedWorkerExecutionApplied {
		t.Fatalf("progress before drain delivery = %#v, %v", progress, err)
	}
	poll, err := store.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: drain.Connection.ID, PresentedSecretDigest: secretDigest,
	})
	if err != nil || poll.Status != ports.AttachedWorkerExecutionApplied || poll.Outbound == nil {
		t.Fatalf("drain delivery = %#v, %v", poll, err)
	}
	drainBatch, err := attachedworkerprotocol.DecodeBatchV1(poll.Outbound.Payload)
	if err != nil || len(drainBatch.Frames) != 1 || drainBatch.Frames[0].Drain == nil {
		t.Fatalf("drain frame = %#v, %v", drainBatch, err)
	}
	drainFrame := drainBatch.Frames[0]
	cancellation, err := store.RequestAttachedWorkerCancellation(ctx, ports.AttachedWorkerCancellationRequest{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		AttemptID: claim.Attempt.AttemptID, LeaseGeneration: claim.Attempt.LeaseGeneration,
		AckTimeout: time.Nanosecond,
	})
	if err != nil || cancellation.Status != ports.AttachedWorkerExecutionApplied ||
		cancellation.Attempt.State != domain.AttachedWorkerAttemptCancelRequested {
		t.Fatalf("cancellation while draining = %#v, %v", cancellation, err)
	}
	var fenced ports.AttachedWorkerAttemptResult
	for attemptNumber := 1; attemptNumber <= 3; attemptNumber++ {
		fenced, err = store.FenceAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptFence{
			TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
			AttemptID: cancellation.Attempt.AttemptID, LeaseGeneration: cancellation.Attempt.LeaseGeneration,
			CandidateAttemptRevision: cancellation.Attempt.Revision,
			Reason:                   ports.AttachedWorkerFenceCancelAckUnknown, DeadlineAt: cancellation.Attempt.CancelDeadline,
		})
		if err != nil || fenced.Status != ports.AttachedWorkerExecutionDenied {
			break
		}
	}
	if err != nil || fenced.Status != ports.AttachedWorkerExecutionFenced ||
		fenced.Attempt.State != domain.AttachedWorkerAttemptFencedUnknown {
		t.Fatalf("fenced unknown = %#v, %v", fenced, err)
	}
	connection, found, err = store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("fenced connection = %#v found=%t err=%v", connection, found, err)
	}
	drainedFrame := attachedworkerprotocol.FrameV1{
		Version: drainFrame.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID: string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: drainFrame.Sequence, Kind: attachedworkerprotocol.MessageDrained,
		Drained: &attachedworkerprotocol.DrainedV1{Revision: drainFrame.Drain.Revision},
	}
	blocked, err := store.ExchangeAttachedWorkerControl(ctx, ports.AttachedWorkerControlExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secretDigest, InboundFrame: drainedFrame,
	})
	if err != nil || blocked.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("fenced unknown acknowledged as drained = %#v, %v", blocked, err)
	}
}

func TestAttachedWorkerDrainReenvelopesAfterReconnect(t *testing.T) {
	store, _, worker, _, _, privateKey, canonicalManifest, _ := readyAttachedWorkerForDrain(t, "drain-reconnect-v2")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	drain, err := store.RequestAttachedWorkerDrain(ctx, ports.AttachedWorkerDrainRequest{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedWorkerRevision: worker.Revision,
	})
	if err != nil || drain.Status != ports.AttachedWorkerExecutionApplied || drain.Outbound == nil {
		t.Fatalf("initial drain = %#v, %v", drain, err)
	}
	oldMessage := *drain.Outbound
	worker = drain.Worker
	reconnectCreate := attachedWorkerChallengeCreateFixture(worker, "reconnect")
	reconnectCreate.Purpose = domain.AttachedWorkerAttachReconnect
	reconnectCreate.ExpectedConnectionID = drain.Connection.ID
	reconnectCreate.ExpectedConnectionRevision = drain.Connection.Revision
	reconnectCreate.ExpectedCapabilityDigest = drain.Connection.CapabilityDigest
	reconnectCreate.ExpectedProtocolSnapshot = append([]byte(nil), drain.Connection.ProtocolSnapshot...)
	reconnectChallenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, reconnectCreate)
	if err != nil {
		t.Fatal(err)
	}
	reconnectChannelBytes := bytes.Repeat([]byte{0x73}, 32)
	reconnectAttachedSnapshot, reconnectReadySnapshot, reconnectManifestSignature := attachedWorkerReconnectProtocolSnapshotFixture(
		t, worker, drain.Connection, reconnectChallenge, privateKey, reconnectChannelBytes,
	)
	reconnectSecretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("drain-reconnect-bearer"))
	reconnectActivation := ports.AttachedWorkerConnectionActivation{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID, ChallengeID: reconnectChallenge.ID,
		Purpose: reconnectChallenge.Purpose, ExpectedChallengeRevision: reconnectChallenge.Revision,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration,
		ExpectedConnectionID:         drain.Connection.ID, ExpectedConnectionRevision: drain.Connection.Revision,
		ExpectedPreviousCapabilityDigest: drain.Connection.CapabilityDigest,
		ExpectedPreviousProtocolSnapshot: append([]byte(nil), drain.Connection.ProtocolSnapshot...),
		PresentedWorkerNonceDigest:       reconnectChallenge.WorkerNonceDigest, PresentedPlatformNonceDigest: reconnectChallenge.PlatformNonceDigest,
		ConnectionSecretDigest: reconnectSecretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(reconnectChannelBytes),
		ExpectedCapabilityDigest: drain.Connection.CapabilityDigest, ProtocolSnapshot: reconnectAttachedSnapshot, AuthTTL: time.Hour,
	}
	reconnectActivated, err := store.ActivateAttachedWorkerConnection(ctx, reconnectActivation)
	if err != nil || reconnectActivated.Status != ports.AttachedWorkerConnectionActivated {
		t.Fatalf("reconnect activation = %#v, %v", reconnectActivated, err)
	}
	worker, found, err := store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker after reconnect = %#v found=%t err=%v", worker, found, err)
	}
	reconnectAccepted, err := store.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: reconnectActivated.Connection.ID, ConnectionGeneration: reconnectActivated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: reconnectActivated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: reconnectSecretDigest,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: drain.Connection.CapabilityDigest, ProtocolVersion: reconnectChallenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey), CanonicalManifest: canonicalManifest,
			ManifestPayload: []byte(`{"version":1,"surface":"codex-exec"}`), Signature: reconnectManifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: reconnectReadySnapshot, PresenceTTL: 10 * time.Minute,
	})
	if err != nil || reconnectAccepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("reconnect manifest = %#v, %v", reconnectAccepted, err)
	}
	poll, err := store.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: reconnectAccepted.Connection.ID, PresentedSecretDigest: reconnectSecretDigest,
	})
	if err != nil || poll.Status != ports.AttachedWorkerExecutionApplied || poll.Outbound == nil {
		t.Fatalf("reconnected drain poll = %#v, %v", poll, err)
	}
	freshMessage := *poll.Outbound
	if freshMessage.DrainRevision != oldMessage.DrainRevision ||
		freshMessage.ConnectionGeneration != oldMessage.ConnectionGeneration+1 ||
		freshMessage.Fingerprint == oldMessage.Fingerprint || bytes.Equal(freshMessage.Payload, oldMessage.Payload) {
		t.Fatalf("drain was not re-enveloped: old=%#v fresh=%#v", oldMessage, freshMessage)
	}
	freshBatch, err := attachedworkerprotocol.DecodeBatchV1(freshMessage.Payload)
	if err != nil || len(freshBatch.Frames) != 1 || freshBatch.Frames[0].Drain == nil ||
		freshBatch.Frames[0].Drain.Revision != oldMessage.DrainRevision {
		t.Fatalf("fresh drain frame = %#v, %v", freshBatch, err)
	}
}

func TestAttachedWorkerDrainSerializesAgainstRevocation(t *testing.T) {
	store, _, worker, _, _, _, _, now := readyAttachedWorkerForDrain(t, "revoke-race")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	drainRequest := ports.AttachedWorkerDrainRequest{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedWorkerRevision: worker.Revision,
	}
	revoked := worker
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	revoked.EnrollmentGeneration++
	revoked.ConnectionGeneration++
	revoked.Revision++
	revoked.UpdatedAt = now.Add(time.Second)
	revoked.RevokedAt = revoked.UpdatedAt
	revokeRequest := ports.AttachedWorkerRevokeMutation{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedRevision: worker.Revision, Next: revoked,
		Audit: attachedWorkerMutationAudit(revoked, domain.AttachedWorkerAuditWorkerRevoked, revoked.UpdatedAt),
		At:    revoked.UpdatedAt,
	}
	type drainOutcome struct {
		result ports.AttachedWorkerDrainResult
		err    error
	}
	type revokeOutcome struct {
		revoked bool
		err     error
	}
	start := make(chan struct{})
	drainDone := make(chan drainOutcome, 1)
	revokeDone := make(chan revokeOutcome, 1)
	go func() {
		<-start
		result, requestErr := store.RequestAttachedWorkerDrain(ctx, drainRequest)
		drainDone <- drainOutcome{result: result, err: requestErr}
	}()
	go func() {
		<-start
		didRevoke, requestErr := store.RevokeAttachedWorker(ctx, revokeRequest)
		revokeDone <- revokeOutcome{revoked: didRevoke, err: requestErr}
	}()
	close(start)
	drain, revoke := <-drainDone, <-revokeDone
	if drain.err != nil || revoke.err != nil {
		t.Fatalf("drain=%#v err=%v revoke=%#v err=%v", drain.result, drain.err, revoke.revoked, revoke.err)
	}
	stored, found, err := store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker after drain/revoke race = %#v found=%t err=%v", stored, found, err)
	}
	if revoke.revoked {
		if drain.result.Status != ports.AttachedWorkerExecutionConflict || stored.DesiredState != domain.AttachedWorkerDesiredRevoked {
			t.Fatalf("revocation winner was reopened by drain: drain=%#v worker=%#v", drain.result, stored)
		}
	} else if drain.result.Status != ports.AttachedWorkerExecutionApplied || stored.DesiredState != domain.AttachedWorkerDesiredDrain {
		t.Fatalf("drain winner was bypassed: drain=%#v worker=%#v revoke=%t", drain.result, stored, revoke.revoked)
	}
}

func readyAttachedWorkerForDrain(t *testing.T, suffix string) (*ydbstore.Store, *ydbclient.Client, domain.AttachedWorker, domain.AttachedWorkerConnection, domain.AttachedWorkerConnectionSecretDigest, ed25519.PrivateKey, []byte, time.Time) {
	t.Helper()
	suffix = attachedWorkerDrainTestSuffix(t, suffix)
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-worker-" + suffix))
	ownerID := domain.UserID(uniqueID("owner-worker-" + suffix))
	enrollment, createAudit := attachedWorkerEnrollmentFixture(suffix, tenantID, ownerID, now.Add(-time.Second))
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, createAudit); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	claim := attachedWorkerClaimFixture(enrollment, 0x71)
	claim.IdentityPublicKey = append([]byte(nil), privateKey.Public().(ed25519.PublicKey)...)
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	worker := claimed.Worker
	challenge, err := store.CreateAttachedWorkerAttachChallenge(ctx, attachedWorkerChallengeCreateFixture(worker, suffix))
	if err != nil {
		t.Fatal(err)
	}
	channelBytes := bytes.Repeat([]byte{0x72}, 32)
	canonicalManifest, capabilityDigest, attachedSnapshot, readySnapshot, manifestSignature := attachedWorkerProtocolSnapshotFixture(t, worker, challenge, privateKey, channelBytes)
	secretDigest := domain.DigestAttachedWorkerConnectionSecret([]byte("drain-race-bearer-" + suffix))
	activated, err := store.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID, ChallengeID: challenge.ID,
		Purpose: domain.AttachedWorkerAttachInitial, ExpectedChallengeRevision: challenge.Revision,
		ExpectedWorkerRevision: worker.Revision, ExpectedEnrollmentGeneration: worker.EnrollmentGeneration,
		ExpectedConnectionGeneration: worker.ConnectionGeneration,
		PresentedWorkerNonceDigest:   challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: secretDigest, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channelBytes),
		ExpectedCapabilityDigest: capabilityDigest, ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
	})
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated {
		t.Fatalf("activate = %#v, %v", activated, err)
	}
	worker, found, err := store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("worker after activation = %#v found=%t err=%v", worker, found, err)
	}
	accepted, err := store.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: tenantID, OwnerUserID: ownerID, WorkerID: worker.ID,
		ConnectionID: activated.Connection.ID, ConnectionGeneration: activated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: worker.Revision,
		PresentedSecretDigest: secretDigest,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: capabilityDigest, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey),
			CanonicalManifest: canonicalManifest, ManifestPayload: []byte(`{"version":1,"surface":"codex-exec"}`),
			Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: readySnapshot, PresenceTTL: 10 * time.Minute,
	})
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("manifest = %#v, %v", accepted, err)
	}
	worker, found, err = store.LoadAttachedWorker(ctx, tenantID, ownerID, worker.ID)
	if err != nil || !found {
		t.Fatalf("ready worker = %#v found=%t err=%v", worker, found, err)
	}
	return store, client, worker, accepted.Connection, secretDigest, privateKey, canonicalManifest, now
}

func attachedWorkerDrainTestSuffix(t *testing.T, prefix string) string {
	t.Helper()
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		t.Fatal(err)
	}
	return prefix + "-" + hex.EncodeToString(entropy[:])
}

func attachedWorkerOfferForDrain(t *testing.T, store *ydbstore.Store, client *ydbclient.Client, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, now time.Time, suffix string) ports.AttachedWorkerAttemptResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runID := "run-drain-" + suffix
	ingress := ingressFixture(worker.TenantID, runID, 9801, now)
	policyDigest := domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy-" + suffix)))
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		CapabilityDigest: connection.CapabilityDigest, PolicyDigest: policyDigest,
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	binding := ingress.Dispatch.HarnessBinding
	binding.OwnerUserID = worker.OwnerUserID
	binding.Resource.OwnerUserID = worker.OwnerUserID
	binding.ExecutionPlacementDigest = string(placementDigest)
	ingress.Dispatch.ExecutionPlacementV2 = placement
	ingress.Dispatch.HarnessBinding = binding
	ingress.Dispatch.SubstrateBinding = nil
	ingress.Dispatch.AdmissionCostCeiling = nil
	ingress.Dispatch.CredentialOwnerUserID = worker.OwnerUserID
	if _, err := client.DB.ExecContext(ctx,
		`INSERT INTO subscription_connections
		 (tenant_id, subscription_connection_id, actor_id, provider, credential_ref,
		  entitlement_state, quota_state, observed_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		worker.TenantID, ingress.Run.SubscriptionConnectionID, worker.OwnerUserID, "codex", "",
		domain.EntitlementActive, domain.ProviderQuotaAvailable, now, now, now,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.IngestTelegram(ctx, ingress); err != nil {
		t.Fatal(err)
	}
	session, owner := canonicalSessionFixture(worker.TenantID, worker.OwnerUserID, ingress.Run.SessionID, now)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: 5 * time.Minute,
		MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20,
		MaxContextEvents: 64, MaxArtifacts: 8, MaxToolEvents: 16, MaxToolEventBytes: 1 << 18,
	}
	reservation := domain.QuotaReservation{
		ID: domain.QuotaReservationID("reservation-" + runID), TenantID: worker.TenantID, RunID: ingress.Run.ID,
		SubscriptionConnectionID: ingress.Run.SubscriptionConnectionID, Status: domain.ReservationHeld,
		CapacityUnits: 1, HeldAt: now, ExpiresAt: now.Add(30 * time.Minute), UpdatedAt: now,
	}
	job := domain.WorkerJob{
		TenantID: worker.TenantID, RunID: ingress.Run.ID, SessionID: ingress.Run.SessionID,
		TriggerEventID: ingress.Run.TriggerEventID, AttemptID: ingress.Attempt.ID, ReservationID: reservation.ID,
		InputManifestID: ingress.InputManifest.ID, ContextSnapshot: ingress.Dispatch.ContextSnapshot,
		CredentialOwnerUserID: worker.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: limits, DeliveryChat: ingress.Dispatch.DeliveryChat, ReplyToMessageID: ingress.Dispatch.ReplyToMessageID,
		CreatedAt: now,
	}
	err = store.Transact(ctx, worker.TenantID, func(tx ports.StateTx) error {
		run, found, getErr := tx.GetRun(ctx, ingress.Run.ID)
		if getErr != nil || !found {
			return getErr
		}
		run.Status, run.UpdatedAt = domain.RunQueued, now
		attempt, found, getErr := tx.GetAttempt(ctx, ingress.Attempt.ID)
		if getErr != nil || !found {
			return getErr
		}
		attempt.Status, attempt.UpdatedAt = domain.AttemptCreated, now
		if err := tx.PutRun(ctx, run); err != nil {
			return err
		}
		if err := tx.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if err := tx.PutQuotaReservation(ctx, reservation); err != nil {
			return err
		}
		return tx.PutWorkerJob(ctx, job)
	})
	if err != nil {
		t.Fatal(err)
	}
	leaseTTL, err := domain.AttachedWorkerLeaseTTLForLimitsV1(limits)
	if err != nil {
		t.Fatal(err)
	}
	leaseID, err := domain.NewAttachedWorkerLeaseIDV1(worker.TenantID, ingress.Run.ID, ingress.Attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	offer, err := store.OfferAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptOffer{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID, ReservationID: reservation.ID,
		LeaseID: leaseID, LeaseTTL: leaseTTL,
	})
	if err != nil || offer.Status != ports.AttachedWorkerExecutionApplied || offer.Outbound == nil {
		t.Fatalf("offer = %#v, %v", offer, err)
	}
	return offer
}

func attachedWorkerFailureForDrain(
	t *testing.T,
	store *ydbstore.Store,
	attempt domain.AttachedWorkerAttemptV1,
	suffix string,
) (ports.AttachedWorkerTerminalMaterialization, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var run domain.Run
	if err := store.Transact(ctx, attempt.TenantID, func(tx ports.StateTx) error {
		loaded, found, err := tx.GetRun(ctx, attempt.RunID)
		if err != nil || !found {
			return err
		}
		run = loaded
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	at := run.UpdatedAt.Add(time.Second).UTC().Truncate(time.Microsecond)
	eventID := domain.SessionEventID(uniqueID("event-drain-failure-" + suffix))
	event := domain.SessionEventDraft{
		ID: eventID, Kind: domain.SessionEventSystemNotice,
		IdempotencyKey: domain.IdempotencyKey(uniqueID("event-key-drain-failure-" + suffix)),
		Payload: domain.BlobRef{
			TenantID: attempt.TenantID,
			Key:      domain.SessionEventObjectPrefix(attempt.TenantID, run.SessionID, eventID) + "payload.json",
			Size:     2, SHA256: canonicalDigest,
		},
		DisplayText: "attached worker test failure", CreatedAt: at,
	}
	failure := &ports.WorkerFailure{
		TenantID: attempt.TenantID, RunID: attempt.RunID, AttemptID: attempt.AttemptID,
		ReservationID: attempt.ReservationID, LeaseID: attempt.LeaseID, Fence: attempt.LeaseGeneration,
		At: at, Code: "attached_worker_test_failure", Events: []domain.SessionEventDraft{event},
	}
	type finalizationEventIdentity struct {
		ID             domain.SessionEventID   `json:"id"`
		Kind           domain.SessionEventKind `json:"kind"`
		IdempotencyKey domain.IdempotencyKey   `json:"idempotency_key"`
		Payload        domain.BlobRef          `json:"payload"`
		DisplayText    string                  `json:"display_text,omitempty"`
	}
	payload, err := json.Marshal(struct {
		Status domain.RunStatus            `json:"status"`
		Events []finalizationEventIdentity `json:"events"`
	}{
		Status: domain.RunFailed,
		Events: []finalizationEventIdentity{{
			ID: event.ID, Kind: event.Kind, IdempotencyKey: event.IdempotencyKey,
			Payload: event.Payload, DisplayText: event.DisplayText,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	materialization := ports.AttachedWorkerTerminalMaterialization{
		EvidenceDigest: domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(digest[:])),
		Failure:        failure,
	}
	return materialization, append([]byte(nil), digest[:]...)
}

func attachedWorkerProtocolAuthorityForDrain(
	t *testing.T,
	worker domain.AttachedWorker,
	connection domain.AttachedWorkerConnection,
) (attachedworkerprotocol.MachineConfig, attachedworkerprotocol.MachineSnapshotV1) {
	t.Helper()
	snapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(connection.ProtocolSnapshot)
	if err != nil || snapshot.Hello == nil || snapshot.Challenge == nil {
		t.Fatalf("connection protocol snapshot = %#v, %v", snapshot, err)
	}
	channelBinding, err := hex.DecodeString(string(connection.ChannelBinding))
	if err != nil {
		t.Fatal(err)
	}
	config := attachedworkerprotocol.MachineConfig{
		Auth: attachedworkerprotocol.AuthContextV1{
			TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
			IdentityPublicKey:    append([]byte(nil), worker.IdentityPublicKey...),
			EnrollmentGeneration: connection.EnrollmentGeneration, ConnectionGeneration: connection.ConnectionGeneration,
			Version: attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion), ChannelBinding: channelBinding,
		},
		WorkerOffer: snapshot.Hello.Offer, PlatformOffer: snapshot.Challenge.PlatformOffer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion)},
	}
	if _, err := attachedworkerprotocol.RestoreConformanceMachine(config, snapshot); err != nil {
		t.Fatalf("connection protocol authority: %v", err)
	}
	return config, snapshot
}
