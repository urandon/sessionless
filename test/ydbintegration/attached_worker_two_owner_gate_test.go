//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This is a real YDB transaction test, not an owner-aware fake. Both workers
// are live at once, with claimed jobs, before cross-owner requests are tried.
func TestAW07TwoOwnerClaimAndRevocationKeepSealedInputIsolated(t *testing.T) {
	aStore, aClient, aWorker, aConnection, aSecret, _, _, aNow := readyAttachedWorkerForDrain(t, "aw07-a")
	bStore, bClient, bWorker, bConnection, bSecret, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t, "aw07-b", aWorker.TenantID, aWorker.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	a := aw07ClaimedInput(t, aStore, aClient, aWorker, aConnection, aSecret, aNow, attachedWorkerDrainTestSuffix(t, "aw07-a"))
	b := aw07ClaimedInput(t, bStore, bClient, bWorker, bConnection, bSecret, bNow, attachedWorkerDrainTestSuffix(t, "aw07-b"))
	if a.request.TenantID != b.request.TenantID || a.request.OwnerUserID == b.request.OwnerUserID ||
		a.request.WorkerID != b.request.WorkerID || a.request.ConnectionID == b.request.ConnectionID ||
		a.request.PresentedSecretDigest == b.request.PresentedSecretDigest ||
		!bytes.Equal(aWorker.IdentityPublicKey, bWorker.IdentityPublicKey) {
		t.Fatal("two-owner gate requires distinct owner/connection/secret authority with cloned worker locator and identity")
	}
	for _, pair := range []struct {
		name  string
		from  aw07ClaimedAuthorization
		to    aw07ClaimedAuthorization
		store *ydbstore.Store
	}{
		{"a_to_b", a, b, bStore}, {"b_to_a", b, a, aStore},
	} {
		t.Run(pair.name, func(t *testing.T) {
			stolen := pair.to.request
			stolen.PresentedSecretDigest = pair.from.request.PresentedSecretDigest
			aw07Denied(t, pair.store, ctx, stolen)
			guessed := pair.from.request
			guessed.TenantID, guessed.OwnerUserID, guessed.WorkerID =
				pair.to.request.TenantID, pair.to.request.OwnerUserID, pair.to.request.WorkerID
			aw07Denied(t, pair.store, ctx, guessed)
			aw07Authorized(t, pair.store, ctx, pair.to)
		})
	}

	// A cancel keyed by B's owner but A's attempt must not locate A's job,
	// even though both workers share the same tenant and worker locator.
	foreignCancel, err := bStore.RequestAttachedWorkerCancellation(ctx, ports.AttachedWorkerCancellationRequest{
		TenantID: b.request.TenantID, OwnerUserID: b.request.OwnerUserID, WorkerID: b.request.WorkerID,
		AttemptID: a.request.AttemptID, LeaseGeneration: a.request.LeaseGeneration, AckTimeout: time.Minute,
	})
	if err != nil || foreignCancel.Status != ports.AttachedWorkerExecutionNotFound {
		t.Fatalf("cross-owner cancel found owner A's attempt: result=%+v err=%v", foreignCancel, err)
	}
	aw07Authorized(t, aStore, ctx, a)
	aw07Authorized(t, bStore, ctx, b)
	cancellation, err := aStore.RequestAttachedWorkerCancellation(ctx, ports.AttachedWorkerCancellationRequest{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		AttemptID: a.request.AttemptID, LeaseGeneration: a.request.LeaseGeneration, AckTimeout: time.Minute,
	})
	if err != nil || cancellation.Status != ports.AttachedWorkerExecutionApplied ||
		cancellation.Attempt.State != domain.AttachedWorkerAttemptCancelRequested {
		t.Fatalf("cancel owner A's claimed attempt: result=%+v err=%v", cancellation, err)
	}
	aw07Denied(t, aStore, ctx, a.request)
	aw07Authorized(t, bStore, ctx, b)

	// A deny-first revocation fences A's old generation without touching B's
	// already claimed job, credential owner, or connection authority.
	aw07Revoke(t, aStore, ctx, aWorker)
	aw07Denied(t, aStore, ctx, a.request)
	aw07Authorized(t, bStore, ctx, b)

	// An old worker can still possess a previously valid terminal payload.
	// Neither its own revoked scope nor the other owner's scope may turn it
	// into canonical run finalization without a committed terminal head.
	baseline := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.attempt.RunID)
	materialization, _ := attachedWorkerFailureForDrain(t, aStore, a.attempt, "aw07-stale-terminal")
	for _, test := range []struct {
		name   string
		store  *ydbstore.Store
		owner  domain.UserID
		worker domain.AttachedWorkerID
		want   ports.AttachedWorkerExecutionStatus
	}{
		{name: "revoked_owner", store: aStore, owner: a.request.OwnerUserID,
			worker: a.request.WorkerID, want: ports.AttachedWorkerExecutionConflict},
		{name: "foreign_owner", store: bStore, owner: b.request.OwnerUserID,
			worker: b.request.WorkerID, want: ports.AttachedWorkerExecutionNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.store.CommitAttachedWorkerTerminal(ctx, ports.AttachedWorkerTerminalCommit{
				TenantID: a.request.TenantID, OwnerUserID: test.owner, WorkerID: test.worker,
				AttemptID: a.request.AttemptID, LeaseGeneration: a.request.LeaseGeneration,
				Materialization: materialization,
			})
			if err != nil || result.Status != test.want || result.Outbound != nil {
				t.Fatalf("stale terminal commit status=%s outbound=%v err=%v, want %s without ack",
					result.Status, result.Outbound, err, test.want)
			}
		})
	}
	if got := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.attempt.RunID); got != baseline {
		t.Fatalf("stale terminal changed owner A run status from %s to %s", baseline, got)
	}
	aw07Authorized(t, bStore, ctx, b)
}

// A terminal accepted before revocation is not itself canonical result
// authority. Revocation must fence the later server-side materialization,
// including when another owner still has a healthy claimed attempt.
func TestAW07RevocationFencesPendingTerminalBeforeRunFinalization(t *testing.T) {
	aStore, aClient, aWorker, aConnection, aSecret, _, _, aNow := readyAttachedWorkerForDrain(t, "aw07-terminal-a")
	bStore, bClient, bWorker, bConnection, bSecret, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t,
		"aw07-terminal-b", aWorker.TenantID, aWorker.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	a := aw07ClaimedInput(t, aStore, aClient, aWorker, aConnection, aSecret, aNow, attachedWorkerDrainTestSuffix(t, "aw07-terminal-a"))
	b := aw07ClaimedInput(t, bStore, bClient, bWorker, bConnection, bSecret, bNow, attachedWorkerDrainTestSuffix(t, "aw07-terminal-b"))
	materialization, evidence := attachedWorkerFailureForDrain(t, aStore, a.attempt, "aw07-revoked-terminal")
	terminalFrame := attachedworkerprotocol.FrameV1{
		Version:   a.accepted.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, a.nextWorkerSequence),
		WorkerID:  string(a.request.WorkerID), EnrollmentGeneration: a.request.EnrollmentGeneration,
		ConnectionGeneration: a.request.ConnectionGeneration, Sequence: a.nextWorkerSequence,
		Ack: a.accepted.Sequence, Kind: attachedworkerprotocol.MessageTerminal,
		Terminal: &attachedworkerprotocol.TerminalV1{
			Binding: a.binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalFailed, Result: attachedworkerprotocol.TerminalResultFailed,
			EvidenceDigest: evidence,
		},
	}
	terminal, err := aStore.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		ConnectionID: a.request.ConnectionID, AttemptID: a.request.AttemptID,
		LeaseGeneration: a.request.LeaseGeneration, PresentedSecretDigest: a.request.PresentedSecretDigest,
		InboundFrame: terminalFrame,
	})
	if err != nil || terminal.Status != ports.AttachedWorkerExecutionApplied ||
		terminal.Attempt.State != domain.AttachedWorkerAttemptTerminalPending {
		t.Fatalf("owner A terminal must be pending before revocation: result=%+v err=%v", terminal, err)
	}
	baseline := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.attempt.RunID)
	aw07Revoke(t, aStore, ctx, aWorker)
	result, err := aStore.CommitAttachedWorkerTerminal(ctx, ports.AttachedWorkerTerminalCommit{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		AttemptID: a.request.AttemptID, LeaseGeneration: a.request.LeaseGeneration,
		Materialization: materialization,
	})
	if err != nil || result.Status != ports.AttachedWorkerExecutionFenced || result.Outbound != nil {
		t.Fatalf("revoked terminal materialization status=%s outbound=%v err=%v, want fenced without ack",
			result.Status, result.Outbound, err)
	}
	if got := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.attempt.RunID); got != baseline {
		t.Fatalf("revoked terminal changed owner A run status from %s to %s", baseline, got)
	}
	aw07Authorized(t, bStore, ctx, b)
}

// An idle owner can recover with a rotated connection while another owner in
// the same tenant continues a claimed attempt. Neither the old bearer nor the
// old connection generation may regain sealed-input authority afterward.
func TestAW07TwoOwnerReconnectKeepsPeerClaimAndFencesOldBearer(t *testing.T) {
	aStore, aClient, aWorker, aConnection, aSecret, aKey, aManifest, aNow := readyAttachedWorkerForDrain(t, "aw07-reconnect-a")
	bStore, bClient, bWorker, bConnection, bSecret, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t,
		"aw07-reconnect-b", aWorker.TenantID, aWorker.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	b := aw07ClaimedInput(t, bStore, bClient, bWorker, bConnection, bSecret, bNow, attachedWorkerDrainTestSuffix(t, "aw07-peer-claim"))
	aw07Authorized(t, bStore, ctx, b)

	// The reconnect protocol fixture requires the challenge's reconnect nonce
	// pair, selected by this exact fixture mode.
	challengeCreate := attachedWorkerChallengeCreateFixture(aWorker, "reconnect")
	challengeCreate.Lifetime = 10 * time.Minute
	challengeCreate.Purpose = domain.AttachedWorkerAttachReconnect
	challengeCreate.ExpectedConnectionID = aConnection.ID
	challengeCreate.ExpectedConnectionRevision = aConnection.Revision
	challengeCreate.ExpectedCapabilityDigest = aConnection.CapabilityDigest
	challengeCreate.ExpectedProtocolSnapshot = append([]byte(nil), aConnection.ProtocolSnapshot...)
	challenge, err := aStore.CreateAttachedWorkerAttachChallenge(ctx, challengeCreate)
	if err != nil {
		t.Fatal(err)
	}
	channelBytes := bytes.Repeat([]byte{0x7a}, 32)
	attachedSnapshot, readySnapshot, manifestSignature := attachedWorkerReconnectProtocolSnapshotFixture(
		t, aWorker, aConnection, challenge, aKey, channelBytes,
	)
	newSecret := domain.DigestAttachedWorkerConnectionSecret([]byte("aw07-reconnected-owner-a"))
	activated, err := aStore.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: aWorker.TenantID, OwnerUserID: aWorker.OwnerUserID, WorkerID: aWorker.ID,
		ChallengeID: challenge.ID, Purpose: challenge.Purpose, ExpectedChallengeRevision: challenge.Revision,
		ExpectedWorkerRevision: aWorker.Revision, ExpectedEnrollmentGeneration: aWorker.EnrollmentGeneration,
		ExpectedConnectionGeneration: aWorker.ConnectionGeneration,
		ExpectedConnectionID:         aConnection.ID, ExpectedConnectionRevision: aConnection.Revision,
		ExpectedPreviousCapabilityDigest: aConnection.CapabilityDigest,
		ExpectedPreviousProtocolSnapshot: append([]byte(nil), aConnection.ProtocolSnapshot...),
		PresentedWorkerNonceDigest:       challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: newSecret, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channelBytes),
		ExpectedCapabilityDigest: aConnection.CapabilityDigest, ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
	})
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated ||
		activated.Connection.ID == aConnection.ID ||
		activated.Connection.ConnectionGeneration != aConnection.ConnectionGeneration+1 {
		t.Fatalf("owner A reconnect activation = %+v err=%v", activated, err)
	}
	aw07Authorized(t, bStore, ctx, b)
	aWorker, found, err := aStore.LoadAttachedWorker(ctx, aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID)
	if err != nil || !found {
		t.Fatalf("owner A worker after reconnect activation: found=%t err=%v", found, err)
	}
	accepted, err := aStore.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: aWorker.TenantID, OwnerUserID: aWorker.OwnerUserID, WorkerID: aWorker.ID,
		ConnectionID: activated.Connection.ID, ConnectionGeneration: activated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: aWorker.Revision,
		PresentedSecretDigest: newSecret,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: aConnection.CapabilityDigest, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(aWorker.IdentityPublicKey), CanonicalManifest: aManifest,
			ManifestPayload: attachedWorkerManifestPayloadFixture(t, readySnapshot), Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: readySnapshot, PresenceTTL: 10 * time.Minute,
	})
	if err != nil || accepted.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("owner A reconnect manifest = %+v err=%v", accepted, err)
	}
	aWorker, found, err = aStore.LoadAttachedWorker(ctx, aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID)
	if err != nil || !found {
		t.Fatalf("owner A worker after reconnect manifest: found=%t err=%v", found, err)
	}
	a := aw07ClaimedInput(t, aStore, aClient, aWorker, accepted.Connection, newSecret, aNow,
		attachedWorkerDrainTestSuffix(t, "aw07-recovered-claim"))
	aw07Authorized(t, aStore, ctx, a)
	aw07Authorized(t, bStore, ctx, b)
	for _, tc := range []struct {
		name   string
		change func(*ports.AttachedWorkerSealedInputAuthorization)
	}{
		{"old_bearer", func(request *ports.AttachedWorkerSealedInputAuthorization) {
			request.ConnectionID = aConnection.ID
			request.ConnectionGeneration = aConnection.ConnectionGeneration
			request.PresentedSecretDigest = aSecret
		}},
		{"old_generation", func(request *ports.AttachedWorkerSealedInputAuthorization) {
			request.ConnectionGeneration = aConnection.ConnectionGeneration
		}},
		{"old_secret", func(request *ports.AttachedWorkerSealedInputAuthorization) {
			request.PresentedSecretDigest = aSecret
		}},
		{"old_locator", func(request *ports.AttachedWorkerSealedInputAuthorization) {
			request.ConnectionID = aConnection.ID
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stale := a.request
			tc.change(&stale)
			aw07Denied(t, aStore, ctx, stale)
			aw07Authorized(t, bStore, ctx, b)
		})
	}
	aw07Authorized(t, aStore, ctx, a)
	aw07Authorized(t, bStore, ctx, b)
	aw07Revoke(t, aStore, ctx, aWorker)
	aw07Denied(t, aStore, ctx, a.request)
	aw07Authorized(t, bStore, ctx, b)
}

type aw07ClaimedAuthorization struct {
	request            ports.AttachedWorkerSealedInputAuthorization
	revision           uint64
	attempt            domain.AttachedWorkerAttemptV1
	binding            attachedworkerprotocol.AttemptBindingV1
	accepted           attachedworkerprotocol.FrameV1
	nextWorkerSequence uint64
}

func aw07ClaimedInput(t *testing.T, store *ydbstore.Store, client *ydbclient.Client,
	worker domain.AttachedWorker, connection domain.AttachedWorkerConnection,
	secret domain.AttachedWorkerConnectionSecretDigest, now time.Time, suffix string,
) aw07ClaimedAuthorization {
	t.Helper()
	return aw07ClaimedInputWithPayload(t, store, client, worker, connection, secret, now, suffix, nil, nil)
}

func aw07ClaimedInputWithPayload(t *testing.T, store *ydbstore.Store, client *ydbclient.Client,
	worker domain.AttachedWorker, connection domain.AttachedWorkerConnection,
	secret domain.AttachedWorkerConnectionSecretDigest, now time.Time, suffix string,
	contextBody, artifactBody []byte,
) aw07ClaimedAuthorization {
	return aw07ClaimedInputWithPayloadAndBinding(t, store, client, worker, connection, secret, now, suffix, contextBody, artifactBody, nil)
}

func aw07ClaimedInputWithPayloadAndBinding(t *testing.T, store *ydbstore.Store, client *ydbclient.Client,
	worker domain.AttachedWorker, connection domain.AttachedWorkerConnection,
	secret domain.AttachedWorkerConnectionSecretDigest, now time.Time, suffix string,
	contextBody, artifactBody []byte, amendBinding func(*domain.HarnessBindingV1),
) aw07ClaimedAuthorization {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	offer := attachedWorkerOfferForDrainWithPayloadAndBinding(t, store, client, worker, connection, now, suffix, contextBody, artifactBody, amendBinding)
	batch, err := attachedworkerprotocol.DecodeBatchV1(offer.Outbound.Payload)
	if err != nil || len(batch.Frames) != 1 || batch.Frames[0].LeaseOffer == nil {
		t.Fatalf("decode owner %s offer: frames=%d err=%v", suffix, len(batch.Frames), err)
	}
	connection, found, err := store.LoadAttachedWorkerConnection(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("load owner %s connection: found=%t err=%v", suffix, found, err)
	}
	offerFrame := batch.Frames[0]
	claimFrame := attachedworkerprotocol.FrameV1{
		Version:   offerFrame.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, connection.WorkerSequence+1),
		WorkerID:  string(worker.ID), EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration, Sequence: connection.WorkerSequence + 1,
		Ack: offerFrame.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offerFrame.LeaseOffer.Binding, AttemptSequence: 1},
	}
	claim, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, AttemptID: offer.Attempt.AttemptID,
		LeaseGeneration: offer.Attempt.LeaseGeneration, PresentedSecretDigest: secret,
		InboundFrame: claimFrame,
	})
	if err != nil || claim.Status != ports.AttachedWorkerExecutionApplied ||
		claim.Attempt.State != domain.AttachedWorkerAttemptClaimed || claim.Outbound == nil {
		t.Fatalf("claim owner %s: result=%+v err=%v", suffix, claim, err)
	}
	acceptedBatch, err := attachedworkerprotocol.DecodeBatchV1(claim.Outbound.Payload)
	if err != nil || len(acceptedBatch.Frames) != 1 || acceptedBatch.Frames[0].LeaseAccepted == nil {
		t.Fatalf("decode owner %s accepted claim: frames=%d err=%v", suffix, len(acceptedBatch.Frames), err)
	}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ConnectionID: connection.ID, PresentedSecretDigest: secret,
		EnrollmentGeneration: connection.EnrollmentGeneration,
		ConnectionGeneration: connection.ConnectionGeneration,
		RunID:                offer.Attempt.RunID, AttemptID: offer.Attempt.AttemptID, AttemptSequence: 1,
		LeaseID: offer.Attempt.LeaseID, LeaseGeneration: offer.Attempt.LeaseGeneration,
		FenceToken:              offer.Attempt.FenceToken,
		LeaseExpiresAtUnixMicro: offer.Attempt.LeaseExpiresAt.UnixMicro(),
		ContextDigest:           offer.Attempt.ContextDigest,
		CapabilityDigest:        offer.Attempt.CapabilityDigest,
		PolicyDigest:            offer.Attempt.PolicyDigest,
	}
	authorized, err := store.AuthorizeAttachedWorkerSealedInput(ctx, request)
	if err != nil || authorized.Status != ports.AttachedWorkerExecutionApplied ||
		authorized.AttemptRevision != claim.Attempt.Revision {
		t.Fatalf("authorize owner %s claimed head: result=%+v err=%v", suffix, authorized, err)
	}
	return aw07ClaimedAuthorization{
		request: request, revision: authorized.AttemptRevision, attempt: claim.Attempt,
		binding: offerFrame.LeaseOffer.Binding, accepted: acceptedBatch.Frames[0],
		nextWorkerSequence: claimFrame.Sequence + 1,
	}
}

func aw07Revoke(t *testing.T, store *ydbstore.Store, ctx context.Context, worker domain.AttachedWorker) {
	t.Helper()
	current, found, err := store.LoadAttachedWorker(ctx, worker.TenantID, worker.OwnerUserID, worker.ID)
	if err != nil || !found {
		t.Fatalf("load owner %s before revoke: found=%t err=%v", worker.OwnerUserID, found, err)
	}
	revoked := current
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	revoked.EnrollmentGeneration++
	revoked.ConnectionGeneration++
	revoked.Revision++
	revoked.UpdatedAt = current.UpdatedAt.Add(time.Microsecond).UTC().Truncate(time.Microsecond)
	revoked.RevokedAt = revoked.UpdatedAt
	changed, err := store.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedRevision: current.Revision, Next: revoked,
		Audit: attachedWorkerMutationAudit(revoked, domain.AttachedWorkerAuditWorkerRevoked, revoked.UpdatedAt),
		At:    revoked.UpdatedAt,
	})
	if err != nil || !changed {
		t.Fatalf("revoke owner %s: changed=%t err=%v", worker.OwnerUserID, changed, err)
	}
}

func aw07RunStatus(t *testing.T, store *ydbstore.Store, ctx context.Context,
	tenant domain.TenantID, runID domain.RunID,
) domain.RunStatus {
	t.Helper()
	var status domain.RunStatus
	if err := store.Transact(ctx, tenant, func(tx ports.StateTx) error {
		run, found, err := tx.GetRun(ctx, runID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("run %s not found", runID)
		}
		status = run.Status
		return nil
	}); err != nil {
		t.Fatalf("load run %s status: %v", runID, err)
	}
	return status
}

func aw07Denied(t *testing.T, store *ydbstore.Store, ctx context.Context, request ports.AttachedWorkerSealedInputAuthorization) {
	t.Helper()
	result, err := store.AuthorizeAttachedWorkerSealedInput(ctx, request)
	if err != nil || result.Status != ports.AttachedWorkerExecutionDenied || result.AttemptRevision != 0 {
		t.Fatalf("foreign or revoked authority escaped: result=%+v err=%v", result, err)
	}
}

func aw07Authorized(t *testing.T, store *ydbstore.Store, ctx context.Context, owner aw07ClaimedAuthorization) {
	t.Helper()
	result, err := store.AuthorizeAttachedWorkerSealedInput(ctx, owner.request)
	if err != nil || result.Status != ports.AttachedWorkerExecutionApplied || result.AttemptRevision != owner.revision {
		t.Fatalf("owner's exact claimed authority changed: result=%+v err=%v", result, err)
	}
}
