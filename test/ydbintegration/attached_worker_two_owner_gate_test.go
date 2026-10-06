//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
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
	worker, found, err := aStore.LoadAttachedWorker(ctx, aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID)
	if err != nil || !found {
		t.Fatalf("load owner A before revoke: found=%t err=%v", found, err)
	}
	revoked := worker
	revoked.DesiredState = domain.AttachedWorkerDesiredRevoked
	revoked.EnrollmentGeneration++
	revoked.ConnectionGeneration++
	revoked.Revision++
	revoked.UpdatedAt = worker.UpdatedAt.Add(time.Microsecond).UTC().Truncate(time.Microsecond)
	revoked.RevokedAt = revoked.UpdatedAt
	changed, err := aStore.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		ExpectedRevision: worker.Revision, Next: revoked,
		Audit: attachedWorkerMutationAudit(revoked, domain.AttachedWorkerAuditWorkerRevoked, revoked.UpdatedAt),
		At:    revoked.UpdatedAt,
	})
	if err != nil || !changed {
		t.Fatalf("revoke owner A: changed=%t err=%v", changed, err)
	}
	aw07Denied(t, aStore, ctx, a.request)
	aw07Authorized(t, bStore, ctx, b)
}

type aw07ClaimedAuthorization struct {
	request  ports.AttachedWorkerSealedInputAuthorization
	revision uint64
}

func aw07ClaimedInput(t *testing.T, store *ydbstore.Store, client *ydbclient.Client,
	worker domain.AttachedWorker, connection domain.AttachedWorkerConnection,
	secret domain.AttachedWorkerConnectionSecretDigest, now time.Time, suffix string,
) aw07ClaimedAuthorization {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	offer := attachedWorkerOfferForDrain(t, store, client, worker, connection, now, suffix)
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
		claim.Attempt.State != domain.AttachedWorkerAttemptClaimed {
		t.Fatalf("claim owner %s: result=%+v err=%v", suffix, claim, err)
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
	return aw07ClaimedAuthorization{request: request, revision: authorized.AttemptRevision}
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
