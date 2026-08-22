//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
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
	md5Conflict := create
	md5Conflict.Intent.ExpectedMD5 = "AQEBAQEBAQEBAQEBAQEBAQ=="
	if _, _, err := store.CreateWebUploadIntent(ctx, md5Conflict); !errors.Is(err, domain.ErrUploadIntentConflict) {
		t.Fatalf("changed Content-MD5 create error = %v", err)
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
	insertComputeActor(t, client.DB, tenantID, "actor-web-resource", userID, now)
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
	manifest := domain.ArtifactManifest{
		ID:       domain.ArtifactManifestID(uniqueID("manifest-web-resource-output")),
		TenantID: tenantID, RunID: run.ID, CreatedAt: now.Add(3 * time.Second),
		Artifacts: []domain.Artifact{{
			Name: "worker-output.txt", MediaType: "text/plain",
			Blob: domain.BlobRef{
				TenantID: tenantID,
				Key: domain.SessionRunObjectPrefix(tenantID, session.ID, run.ID) +
					"artifacts/sha256/" + canonicalDigest,
				Size: 42, SHA256: canonicalDigest,
			},
		}},
	}
	if err := store.Transact(ctx, tenantID, func(tx ports.StateTx) error {
		return tx.PutArtifactManifest(ctx, manifest)
	}); err != nil {
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
	artifactRequest := ports.WebRunArtifactRequest{
		TenantID: tenantID, UserID: userID, SessionID: session.ID,
		RunID: run.ID, ManifestID: manifest.ID, Index: 0,
	}
	artifact, found, err := store.GetRunArtifactForUser(ctx, artifactRequest)
	if err != nil || !found || artifact.Name != "worker-output.txt" ||
		artifact.MediaType != "text/plain" || artifact.Blob != manifest.Artifacts[0].Blob {
		t.Fatalf("point worker artifact = %+v found=%t err=%v", artifact, found, err)
	}
	for _, mutate := range []func(*ports.WebRunArtifactRequest){
		func(request *ports.WebRunArtifactRequest) {
			request.TenantID = domain.TenantID(uniqueID("forged-tenant"))
		},
		func(request *ports.WebRunArtifactRequest) { request.UserID = domain.UserID(uniqueID("forged-user")) },
		func(request *ports.WebRunArtifactRequest) {
			request.SessionID = domain.SessionID(uniqueID("forged-session"))
		},
		func(request *ports.WebRunArtifactRequest) { request.RunID = domain.RunID(uniqueID("forged-run")) },
		func(request *ports.WebRunArtifactRequest) {
			request.ManifestID = domain.ArtifactManifestID(uniqueID("forged-manifest"))
		},
		func(request *ports.WebRunArtifactRequest) { request.Index = 1 },
	} {
		forged := artifactRequest
		mutate(&forged)
		_, found, err := store.GetRunArtifactForUser(ctx, forged)
		if forged.UserID != userID {
			if !errors.Is(err, domain.ErrMembershipDenied) || found {
				t.Fatalf("forged participant artifact found=%t err=%v", found, err)
			}
			continue
		}
		if err != nil || found {
			t.Fatalf("forged artifact selector = %+v found=%t err=%v", forged, found, err)
		}
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

func TestResolveComputeConnectionsForUserFiltersByActorOwner(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID(fmt.Sprintf("tenant-compute-owner-%d", now.UnixNano())))
	ownerID := domain.UserID(uniqueID("user-compute-owner"))
	otherUserID := domain.UserID(uniqueID("user-compute-other"))
	seedCanonicalMembership(t, client.DB, tenantID, ownerID, now)
	seedCanonicalMembership(t, client.DB, tenantID, otherUserID, now)
	session, owner := canonicalSessionFixture(
		tenantID, ownerID, domain.SessionID(uniqueID("session-compute-owner")), now,
	)
	if _, _, err := store.CreateSessionForUser(ctx, ports.SessionCreateRequest{
		Session: session, Owner: owner, IdempotencyKey: "create-compute-owner-session",
	}); err != nil {
		t.Fatal(err)
	}
	insertSessionParticipant(t, client.DB, domain.SessionParticipant{
		TenantID: tenantID, SessionID: session.ID, UserID: otherUserID,
		Role: domain.SessionParticipantMember, Status: domain.SessionParticipantActive,
		CreatedAt: now, UpdatedAt: now,
	})

	ownerActorID := domain.ActorID(uniqueID("actor-compute-owner"))
	otherActorID := domain.ActorID(uniqueID("actor-compute-other"))
	insertComputeActor(t, client.DB, tenantID, ownerActorID, ownerID, now)
	insertComputeActor(t, client.DB, tenantID, otherActorID, otherUserID, now)
	ownerConnectionID := domain.SubscriptionConnectionID(uniqueID("connection-compute-owner"))
	otherConnectionID := domain.SubscriptionConnectionID(uniqueID("connection-compute-other"))
	insertComputeConnection(t, client.DB, tenantID, ownerConnectionID, ownerActorID, now)
	insertComputeConnection(t, client.DB, tenantID, otherConnectionID, otherActorID, now)

	ownerConnections, err := store.ResolveComputeConnectionsForUser(ctx, ports.ComputeConnectionResolveRequest{
		TenantID: tenantID, UserID: ownerID, SessionID: session.ID,
	})
	if err != nil || len(ownerConnections) != 1 || ownerConnections[0].ID != ownerConnectionID {
		t.Fatalf("owner connections = %+v err=%v", ownerConnections, err)
	}
	otherConnections, err := store.ResolveComputeConnectionsForUser(ctx, ports.ComputeConnectionResolveRequest{
		TenantID: tenantID, UserID: otherUserID, SessionID: session.ID,
	})
	if err != nil || len(otherConnections) != 1 || otherConnections[0].ID != otherConnectionID {
		t.Fatalf("other user connections = %+v err=%v", otherConnections, err)
	}

	if _, err := client.DB.ExecContext(ctx,
		`DELETE FROM subscription_connections WHERE tenant_id = $1`, tenantID,
	); err != nil {
		t.Fatal(err)
	}
	insertComputeConnection(
		t, client.DB, tenantID,
		domain.SubscriptionConnectionID(uniqueID("connection-compute-missing-actor")),
		domain.ActorID(uniqueID("actor-compute-missing")), now,
	)
	missing, err := store.ResolveComputeConnectionsForUser(ctx, ports.ComputeConnectionResolveRequest{
		TenantID: tenantID, UserID: ownerID, SessionID: session.ID,
	})
	if err != nil || len(missing) != 0 {
		t.Fatalf("missing actor mapping connections = %+v err=%v", missing, err)
	}

	if _, err := client.DB.ExecContext(ctx,
		`DELETE FROM subscription_connections WHERE tenant_id = $1`, tenantID,
	); err != nil {
		t.Fatal(err)
	}
	insertComputeConnection(
		t, client.DB, tenantID,
		domain.SubscriptionConnectionID(uniqueID("connection-compute-mismatched-actor")),
		otherActorID, now,
	)
	mismatched, err := store.ResolveComputeConnectionsForUser(ctx, ports.ComputeConnectionResolveRequest{
		TenantID: tenantID, UserID: ownerID, SessionID: session.ID,
	})
	if err != nil || len(mismatched) != 0 {
		t.Fatalf("mismatched actor mapping connections = %+v err=%v", mismatched, err)
	}
}

func insertSessionParticipant(t *testing.T, db *sql.DB, participant domain.SessionParticipant) {
	t.Helper()
	record, err := json.Marshal(participant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_participants
		 (tenant_id, session_id, user_id, role, status, created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, CAST($8 AS JsonDocument))`,
		participant.TenantID, participant.SessionID, participant.UserID,
		participant.Role, participant.Status, participant.CreatedAt, participant.UpdatedAt, string(record),
	); err != nil {
		t.Fatal(err)
	}
}

func insertComputeActor(
	t *testing.T,
	db *sql.DB,
	tenantID domain.TenantID,
	actorID domain.ActorID,
	userID domain.UserID,
	at time.Time,
) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO actors
		 (tenant_id, actor_id, user_id, frontend, external_id, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tenantID, actorID, userID, domain.FrontendWeb, string(actorID), at, at,
	); err != nil {
		t.Fatal(err)
	}
}

func insertComputeConnection(
	t *testing.T,
	db *sql.DB,
	tenantID domain.TenantID,
	connectionID domain.SubscriptionConnectionID,
	actorID domain.ActorID,
	at time.Time,
) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO subscription_connections
		 (tenant_id, subscription_connection_id, actor_id, provider, credential_ref,
		  entitlement_state, quota_state, observed_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		tenantID, connectionID, actorID, "codex", "credential-must-not-leak",
		domain.EntitlementActive, domain.ProviderQuotaAvailable, at, at, at,
	); err != nil {
		t.Fatal(err)
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
		ExpectedSHA256: canonicalDigest, ExpectedMD5: "AAAAAAAAAAAAAAAAAAAAAA==",
		Status:    domain.UploadIntentPending,
		CreatedAt: at, ExpiresAt: at.Add(ttl),
	}
}
