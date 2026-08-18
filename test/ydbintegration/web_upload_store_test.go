//go:build ydbintegration

package ydbintegration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestWebUploadStoreAuthorizationIdempotencyAndClaims(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID(fmt.Sprintf("tenant-web-upload-%d", now.UnixNano())))
	userID := domain.UserID(uniqueID("user-web-upload"))
	otherUserID := domain.UserID(uniqueID("user-web-upload-other"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	seedCanonicalMembership(t, client.DB, tenantID, otherUserID, now)
	session, owner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-web-upload")), now)
	if _, _, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: session, Owner: owner, IdempotencyKey: "create-web-upload-session",
	}); err != nil {
		t.Fatal(err)
	}
	otherSession, otherOwner := canonicalSessionFixture(
		tenantID, userID, domain.SessionID(uniqueID("session-web-upload-other")), now,
	)
	if _, _, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: otherSession, Owner: otherOwner, IdempotencyKey: "create-web-upload-other-session",
	}); err != nil {
		t.Fatal(err)
	}

	intent := webUploadIntentFixture(tenantID, userID, session.ID, now, 10*time.Minute)
	create := ports.WebUploadCreateRequest{Intent: intent, IdempotencyKey: "create-upload-1"}
	created, fresh, err := store.CreateWebUploadIntent(ctx, create)
	if err != nil || !fresh || created.ID != intent.ID {
		t.Fatalf("create intent = %+v fresh=%t err=%v", created, fresh, err)
	}
	retryCreate := create
	retryCreate.Intent.CreatedAt = retryCreate.Intent.CreatedAt.Add(30 * time.Second)
	retryCreate.Intent.ExpiresAt = retryCreate.Intent.ExpiresAt.Add(30 * time.Second)
	retried, fresh, err := store.CreateWebUploadIntent(ctx, retryCreate)
	if err != nil || fresh || retried.ID != intent.ID {
		t.Fatalf("retry create = %+v fresh=%t err=%v", retried, fresh, err)
	}
	conflict := create
	conflict.Intent = webUploadIntentFixture(tenantID, userID, session.ID, now, 10*time.Minute)
	if _, _, err := store.CreateWebUploadIntent(ctx, conflict); !errors.Is(err, domain.ErrUploadIntentConflict) {
		t.Fatalf("conflicting create error = %v", err)
	}
	unauthorized := ports.WebUploadCreateRequest{
		Intent:         webUploadIntentFixture(tenantID, otherUserID, session.ID, now, 10*time.Minute),
		IdempotencyKey: "create-upload-other-user",
	}
	if _, _, err := store.CreateWebUploadIntent(ctx, unauthorized); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("cross-user create error = %v", err)
	}
	foreignTenant := domain.TenantID(uniqueID("tenant-web-upload-foreign"))
	foreignIntent := webUploadIntentFixture(foreignTenant, userID, session.ID, now, 10*time.Minute)
	if _, _, err := store.CreateWebUploadIntent(ctx, ports.WebUploadCreateRequest{
		Intent: foreignIntent, IdempotencyKey: "create-upload-foreign-tenant",
	}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("cross-tenant create error = %v", err)
	}

	metadata := ports.ObjectMetadata{
		Blob: domain.BlobRef{
			TenantID: tenantID, Key: intent.ObjectKey, Size: intent.ExpectedSize, SHA256: intent.ExpectedSHA256,
		},
		MediaType: intent.MediaType,
		ETag:      "etag-upload-1",
	}
	commit := ports.WebUploadCommitRequest{
		TenantID: tenantID, UserID: userID,
		UploadID: intent.ID, Observed: metadata, At: now.Add(time.Minute),
	}
	committed, err := store.CommitWebUploadIntent(ctx, commit)
	if err != nil || committed.Status != domain.UploadIntentCommitted || committed.ObservedETag != metadata.ETag {
		t.Fatalf("commit intent = %+v err=%v", committed, err)
	}
	commit.At = commit.At.Add(time.Minute)
	retriedCommit, err := store.CommitWebUploadIntent(ctx, commit)
	if err != nil || retriedCommit.CommittedAt == nil || !retriedCommit.CommittedAt.Equal(*committed.CommittedAt) {
		t.Fatalf("retry commit = %+v err=%v", retriedCommit, err)
	}
	changedCommit := commit
	changedCommit.Observed.ETag = "overwritten-etag"
	if _, err := store.CommitWebUploadIntent(ctx, changedCommit); !errors.Is(err, domain.ErrUploadIntentConflict) {
		t.Fatalf("overwritten commit error = %v", err)
	}
	forgedCommit := commit
	forgedCommit.UploadID = "forged-upload"
	if _, err := store.CommitWebUploadIntent(ctx, forgedCommit); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("forged upload commit error = %v", err)
	}
	otherUserCommit := commit
	otherUserCommit.UserID = otherUserID
	if _, err := store.CommitWebUploadIntent(ctx, otherUserCommit); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("cross-user commit error = %v", err)
	}

	claim := ports.WebUploadClaimRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID,
		UploadIDs: []domain.UploadIntentID{intent.ID}, MessageIdempotencyKey: "message-upload-1",
		At: now.Add(3 * time.Minute),
	}
	claimed, err := store.ClaimWebUploadIntents(ctx, claim)
	if err != nil || len(claimed) != 1 || claimed[0].ClaimedBy == nil || *claimed[0].ClaimedBy != claim.MessageIdempotencyKey {
		t.Fatalf("claim = %+v err=%v", claimed, err)
	}
	if _, err := store.ClaimWebUploadIntents(ctx, claim); err != nil {
		t.Fatalf("exact claim retry failed: %v", err)
	}
	claim.MessageIdempotencyKey = "message-upload-2"
	if _, err := store.ClaimWebUploadIntents(ctx, claim); !errors.Is(err, domain.ErrUploadIntentClaimed) {
		t.Fatalf("cross-message reuse error = %v", err)
	}
	claim.MessageIdempotencyKey, claim.SessionID = "message-upload-3", otherSession.ID
	if _, err := store.ClaimWebUploadIntents(ctx, claim); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("cross-session claim error = %v", err)
	}

	expired := webUploadIntentFixture(tenantID, userID, session.ID, now, time.Minute)
	if _, _, err := store.CreateWebUploadIntent(ctx, ports.WebUploadCreateRequest{
		Intent: expired, IdempotencyKey: "create-expired-upload",
	}); err != nil {
		t.Fatal(err)
	}
	expiredMetadata := metadata
	expiredMetadata.Blob.Key = expired.ObjectKey
	if _, err := store.CommitWebUploadIntent(ctx, ports.WebUploadCommitRequest{
		TenantID: tenantID, UserID: userID, UploadID: expired.ID,
		Observed: expiredMetadata, At: expired.ExpiresAt,
	}); !errors.Is(err, domain.ErrUploadIntentExpired) {
		t.Fatalf("expired commit error = %v", err)
	}
}

func TestWebUploadClaimsAreAtomicAndWebResourceReadsAreBounded(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID(fmt.Sprintf("tenant-web-resource-%d", now.UnixNano())))
	userID := domain.UserID(uniqueID("user-web-resource"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	session, owner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-web-resource")), now)
	if _, _, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: session, Owner: owner, IdempotencyKey: "create-web-resource-session",
	}); err != nil {
		t.Fatal(err)
	}

	committedIntent := webUploadIntentFixture(tenantID, userID, session.ID, now, 10*time.Minute)
	pendingIntent := webUploadIntentFixture(tenantID, userID, session.ID, now, 10*time.Minute)
	for index, intent := range []domain.UploadIntent{committedIntent, pendingIntent} {
		if _, _, err := store.CreateWebUploadIntent(ctx, ports.WebUploadCreateRequest{
			Intent: intent, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("atomic-create-%d", index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.CommitWebUploadIntent(ctx, ports.WebUploadCommitRequest{
		TenantID: tenantID, UserID: userID, UploadID: committedIntent.ID,
		Observed: ports.ObjectMetadata{
			Blob:      domain.BlobRef{TenantID: tenantID, Key: committedIntent.ObjectKey, Size: committedIntent.ExpectedSize, SHA256: committedIntent.ExpectedSHA256},
			MediaType: committedIntent.MediaType, ETag: "atomic-etag",
		},
		At: now.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimWebUploadIntents(ctx, ports.WebUploadClaimRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID,
		UploadIDs:             []domain.UploadIntentID{committedIntent.ID, pendingIntent.ID},
		MessageIdempotencyKey: "atomic-message", At: now.Add(2 * time.Minute),
	}); !errors.Is(err, domain.ErrUploadIntentNotCommitted) {
		t.Fatalf("mixed claim error = %v", err)
	}
	if _, err := store.ClaimWebUploadIntents(ctx, ports.WebUploadClaimRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID,
		UploadIDs:             []domain.UploadIntentID{committedIntent.ID},
		MessageIdempotencyKey: "after-rollback-message", At: now.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("first intent was partially claimed: %v", err)
	}

	event := canonicalEventFixture(tenantID, session.ID, userID, domain.SessionEventID(uniqueID("event-web-resource")), now.Add(time.Second))
	if _, err := store.AppendSessionEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	connectionID := domain.SubscriptionConnectionID(uniqueID("connection-web-resource"))
	if _, err := client.DB.ExecContext(ctx,
		`INSERT INTO subscription_connections
		 (tenant_id, subscription_connection_id, actor_id, provider, credential_ref,
		  entitlement_state, quota_state, observed_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		tenantID, connectionID, "actor-web-resource", "codex", "lockbox-secret-must-not-leak",
		domain.EntitlementActive, domain.ProviderQuotaAvailable, now, now, now,
	); err != nil {
		t.Fatal(err)
	}
	run := domain.Run{
		ID: domain.RunID(uniqueID("run-web-resource")), TenantID: tenantID, SessionID: session.ID,
		TriggerEventID: event.ID, SubscriptionConnectionID: connectionID,
		Status: domain.RunCreated, IdempotencyKey: "run-web-resource-key", CreatedAt: now.Add(2 * time.Second), UpdatedAt: now.Add(2 * time.Second),
	}
	if err := store.Transact(ctx, tenantID, func(tx ports.StateTx) error { return tx.PutRun(ctx, run) }); err != nil {
		t.Fatal(err)
	}
	record, found, err := store.GetRunForUser(ctx, tenantID, userID, run.ID)
	if err != nil || !found || record.Run.ID != run.ID || record.Provider != "codex" {
		t.Fatalf("point run = %+v found=%t err=%v", record, found, err)
	}
	if _, found, err := store.GetRunForUser(ctx, tenantID, userID, "forged-run"); err != nil || found {
		t.Fatalf("forged run found=%t err=%v", found, err)
	}
	if _, found, err := store.GetRunForUser(ctx, tenantID, "forged-user", run.ID); !errors.Is(err, domain.ErrMembershipDenied) || found {
		t.Fatalf("forged user run found=%t err=%v", found, err)
	}
	connections, err := store.ResolveComputeConnectionsForUser(ctx, ports.ComputeConnectionResolveRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID,
	})
	if err != nil || len(connections) != 1 || connections[0].ID != connectionID ||
		connections[0].Entitlement != domain.EntitlementActive || connections[0].Quota != domain.ProviderQuotaAvailable {
		t.Fatalf("compute connections = %+v err=%v", connections, err)
	}
	if _, err := store.ResolveComputeConnectionsForUser(ctx, ports.ComputeConnectionResolveRequest{
		TenantID: tenantID, UserID: "forged-user", SessionID: session.ID,
	}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("forged compute resolver error = %v", err)
	}
}

func webUploadIntentFixture(
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	at time.Time,
	ttl time.Duration,
) domain.UploadIntent {
	uploadID := domain.UploadIntentID(uniqueID("web-upload"))
	return domain.UploadIntent{
		ID: uploadID, TenantID: tenantID, UserID: userID, SessionID: sessionID,
		ObjectKey: domain.UploadIntentObjectPrefix(tenantID, uploadID) + "file.txt",
		Name:      "file.txt", MediaType: "text/plain", ExpectedSize: 42,
		ExpectedSHA256: canonicalDigest, Status: domain.UploadIntentPending,
		CreatedAt: at, ExpiresAt: at.Add(ttl),
	}
}
