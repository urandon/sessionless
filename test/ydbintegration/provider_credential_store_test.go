//go:build ydbintegration

package ydbintegration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestProviderCredentialAuthorityReplayRotationCleanupAndRevoke(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	locator := ports.ProviderCredentialLocatorV1{
		TenantID:     domain.TenantID(uniqueID("provider-credential-tenant")),
		OwnerUserID:  domain.UserID(uniqueID("provider-credential-owner")),
		ResourceKind: domain.ProviderResourceRouterAccountV1,
		ResourceID:   uniqueID("openrouter-account"),
	}
	first := providerCredentialBindingFixture(t, locator, 1, 1, "lockbox/provider/candidate-1", "openrouter-key-1", now)
	applied, err := store.CompareAndSwapProviderCredential(ctx, 0, first)
	if err != nil || !applied.Applied || applied.Binding != first || applied.AuditReceiptID == "" {
		t.Fatalf("initial swap=%+v err=%v", applied, err)
	}
	replayed, err := store.CompareAndSwapProviderCredential(ctx, 0, first)
	if err != nil || !replayed.Applied || replayed.Binding != first || replayed.AuditReceiptID != applied.AuditReceiptID {
		t.Fatalf("exact replay=%+v err=%v", replayed, err)
	}
	foreign := locator
	foreign.OwnerUserID = domain.UserID(uniqueID("foreign-owner"))
	if _, found, err := store.LoadProviderCredential(ctx, foreign); err != nil || found {
		t.Fatalf("foreign lookup found=%v err=%v", found, err)
	}
	second := providerCredentialBindingFixture(t, locator, 2, 2, "lockbox/provider/candidate-2", "openrouter-key-2", now.Add(time.Second))
	rotated, err := store.CompareAndSwapProviderCredential(ctx, 1, second)
	if err != nil || !rotated.Applied || rotated.Binding != second || rotated.AuditReceiptID == "" || rotated.AuditReceiptID == applied.AuditReceiptID {
		t.Fatalf("rotation=%+v err=%v", rotated, err)
	}
	cleanups, err := store.ListProviderCredentialCleanups(ctx, locator, 8)
	if err != nil || len(cleanups) != 1 || cleanups[0].CredentialGeneration != 1 || cleanups[0].Reference != first.SecretRef {
		t.Fatalf("rotation cleanups=%+v err=%v", cleanups, err)
	}
	bucket, err := ydbpartition.BucketV1(strings.Join([]string{string(locator.TenantID), string(locator.OwnerUserID), string(locator.ResourceKind), locator.ResourceID}, "\x00"))
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.ListDueProviderCredentialCleanups(ctx, bucket, now.Add(2*time.Second), ports.ProviderCredentialCleanupCursorV1{}, 8)
	if err != nil || len(page.Items) != 1 || page.Items[0].Cleanup != cleanups[0] || !page.NextCursor.Present {
		t.Fatalf("global cleanup page=%+v err=%v", page, err)
	}
	if err := store.AcknowledgeProviderCredentialCleanup(ctx, cleanups[0]); err != nil {
		t.Fatal(err)
	}
	if cleanups, err = store.ListProviderCredentialCleanups(ctx, locator, 8); err != nil || len(cleanups) != 0 {
		t.Fatalf("acknowledged cleanups=%+v err=%v", cleanups, err)
	}
	revoked, err := store.RevokeProviderCredential(ctx, locator, now.Add(2*time.Second))
	if err != nil || !revoked.Applied || revoked.AuditReceiptID == "" || revoked.Binding.State != domain.ProviderCredentialRevokedV1 ||
		revoked.Binding.ResourceRevision != 3 || revoked.Binding.CredentialGeneration != 3 ||
		!revoked.Binding.SecretRef.IsZero() || revoked.Binding.SecretFingerprint != "" {
		t.Fatalf("revoked=%+v err=%v", revoked, err)
	}
	revokedReplay, err := store.RevokeProviderCredential(ctx, locator, now.Add(3*time.Second))
	if err != nil || revokedReplay.Applied || revokedReplay.Binding != revoked.Binding || revokedReplay.AuditReceiptID != revoked.AuditReceiptID {
		t.Fatalf("revoke replay=%+v err=%v", revokedReplay, err)
	}
}

func TestProviderCredentialPhysicalProjectionMismatchFailsClosed(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	locator := ports.ProviderCredentialLocatorV1{
		TenantID:     domain.TenantID(uniqueID("provider-credential-corrupt-tenant")),
		OwnerUserID:  domain.UserID(uniqueID("provider-credential-corrupt-owner")),
		ResourceKind: domain.ProviderResourceAPIAccountV1,
		ResourceID:   uniqueID("api-account"),
	}
	binding := providerCredentialBindingFixture(t, locator, 1, 1, "lockbox/provider/corrupt", "provider-key", now)
	if result, err := store.CompareAndSwapProviderCredential(ctx, 0, binding); err != nil || !result.Applied {
		t.Fatalf("initial swap=%+v err=%v", result, err)
	}
	if _, err := client.DB.ExecContext(ctx,
		`UPDATE provider_credential_bindings SET credential_generation=$1
		 WHERE tenant_id=$2 AND owner_user_id=$3 AND resource_kind=$4 AND resource_id=$5`,
		uint64(99), locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID,
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.LoadProviderCredential(ctx, locator); !errors.Is(err, ydbstore.ErrProviderCredentialConflict) {
		t.Fatalf("projection mismatch error=%v", err)
	}
}

func TestProviderCredentialCandidateFenceSerializesCleanupAgainstBindingCAS(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	newCandidate := func(label string) (ports.ProviderCredentialSecretCandidateV1, domain.ProviderCredentialBindingV1) {
		locator := ports.ProviderCredentialLocatorV1{
			TenantID: domain.TenantID(uniqueID("provider-candidate-tenant-" + label)), OwnerUserID: domain.UserID(uniqueID("provider-candidate-owner-" + label)),
			ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: uniqueID("provider-candidate-resource-" + label),
		}
		binding := providerCredentialBindingFixture(t, locator, 1, 1, "lockbox/provider/candidate-fence-"+label, "candidate-fence-key-"+label, now)
		candidate := ports.ProviderCredentialSecretCandidateV1{
			Scope:     ports.ProviderCredentialCandidateScopeV1{Locator: locator, ResourceRevision: 1, CredentialGeneration: 1, MutationID: binding.CandidateMutationID},
			Reference: binding.SecretRef, Fingerprint: binding.SecretFingerprint, CreatedAt: now.Add(-time.Minute),
		}
		return candidate, binding
	}

	t.Run("cleanup fence wins", func(t *testing.T) {
		candidate, binding := newCandidate("fence-first")
		fenced, err := store.FenceProviderCredentialCandidate(ctx, candidate, now)
		if err != nil || fenced.Authoritative {
			t.Fatalf("fence result=%+v err=%v", fenced, err)
		}
		replayedFence, err := store.FenceProviderCredentialCandidate(ctx, candidate, now.Add(time.Second))
		if err != nil || replayedFence.Authoritative {
			t.Fatalf("fence replay=%+v err=%v", replayedFence, err)
		}
		swap, err := store.CompareAndSwapProviderCredential(ctx, 0, binding)
		if err != nil || swap.Applied || swap.Found {
			t.Fatalf("late CAS crossed cleanup fence: swap=%+v err=%v", swap, err)
		}
	})

	t.Run("binding CAS wins", func(t *testing.T) {
		candidate, binding := newCandidate("cas-first")
		swap, err := store.CompareAndSwapProviderCredential(ctx, 0, binding)
		if err != nil || !swap.Applied || swap.Binding != binding {
			t.Fatalf("CAS=%+v err=%v", swap, err)
		}
		fenced, err := store.FenceProviderCredentialCandidate(ctx, candidate, now.Add(time.Second))
		if err != nil || !fenced.Authoritative || fenced.Binding != binding {
			t.Fatalf("fence did not recover authority: result=%+v err=%v", fenced, err)
		}
	})
}

func TestProviderCredentialRawRowsNeverContainPlaintextMarker(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	marker := uniqueID("raw-provider-secret-marker")
	locator := ports.ProviderCredentialLocatorV1{
		TenantID: domain.TenantID(uniqueID("provider-marker-tenant")), OwnerUserID: domain.UserID(uniqueID("provider-marker-owner")),
		ResourceKind: domain.ProviderResourceRouterAccountV1, ResourceID: uniqueID("provider-marker-resource"),
	}
	first := providerCredentialBindingFixture(t, locator, 1, 1, "lockbox/provider/opaque-one", marker, now)
	if result, err := store.CompareAndSwapProviderCredential(ctx, 0, first); err != nil || !result.Applied {
		t.Fatalf("first swap=%+v err=%v", result, err)
	}
	second := providerCredentialBindingFixture(t, locator, 2, 2, "lockbox/provider/opaque-two", marker+"-rotated", now.Add(time.Second))
	if result, err := store.CompareAndSwapProviderCredential(ctx, 1, second); err != nil || !result.Applied {
		t.Fatalf("second swap=%+v err=%v", result, err)
	}
	candidate := ports.ProviderCredentialSecretCandidateV1{
		Scope:     ports.ProviderCredentialCandidateScopeV1{Locator: locator, ResourceRevision: 3, CredentialGeneration: 3, MutationID: uniqueID("provider-marker-fence")},
		Reference: first.SecretRef, Fingerprint: domain.FingerprintCredential([]byte(marker + "-candidate")), CreatedAt: now.Add(-time.Minute),
	}
	if result, err := store.FenceProviderCredentialCandidate(ctx, candidate, now.Add(2*time.Second)); err != nil || result.Authoritative {
		t.Fatalf("candidate fence=%+v err=%v", result, err)
	}

	var bindingRecord, bindingRef, auditRecord, cleanupRef, fenceRef, fenceFingerprint string
	if err := client.DB.QueryRowContext(ctx,
		`SELECT record,secret_ref FROM provider_credential_bindings
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID,
	).Scan(&bindingRecord, &bindingRef); err != nil {
		t.Fatal(err)
	}
	if err := client.DB.QueryRowContext(ctx,
		`SELECT record FROM provider_credential_audit_events
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4 AND resource_revision=$5`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID, uint64(2),
	).Scan(&auditRecord); err != nil {
		t.Fatal(err)
	}
	if err := client.DB.QueryRowContext(ctx,
		`SELECT secret_ref FROM provider_credential_cleanups
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4 AND credential_generation=$5`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID, uint64(1),
	).Scan(&cleanupRef); err != nil {
		t.Fatal(err)
	}
	if err := client.DB.QueryRowContext(ctx,
		`SELECT secret_ref,secret_fingerprint FROM provider_credential_candidate_fences
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4 AND mutation_id=$5`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID, candidate.Scope.MutationID,
	).Scan(&fenceRef, &fenceFingerprint); err != nil {
		t.Fatal(err)
	}
	for field, value := range map[string]string{
		"binding record": bindingRecord, "binding reference": bindingRef, "audit record": auditRecord,
		"cleanup reference": cleanupRef, "fence reference": fenceRef, "fence fingerprint": fenceFingerprint,
	} {
		if strings.Contains(value, marker) {
			t.Fatalf("%s contains plaintext marker", field)
		}
	}
}

func providerCredentialBindingFixture(
	t *testing.T,
	locator ports.ProviderCredentialLocatorV1,
	revision, generation uint64,
	reference, secret string,
	updatedAt time.Time,
) domain.ProviderCredentialBindingV1 {
	t.Helper()
	ref, err := domain.NewCredentialSecretRef(reference)
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.ProviderCredentialBindingV1{
		Version:  domain.ProviderCredentialBindingVersionV1,
		TenantID: locator.TenantID, OwnerUserID: locator.OwnerUserID,
		ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID,
		ResourceRevision: revision, CredentialGeneration: generation,
		CandidateMutationID: uniqueID("provider-credential-mutation"),
		State:               domain.ProviderCredentialActiveV1, SecretRef: ref,
		SecretFingerprint: domain.FingerprintCredential([]byte(secret)), UpdatedAt: updatedAt,
	}
	if err := binding.Validate(); err != nil {
		t.Fatal(err)
	}
	return binding
}
