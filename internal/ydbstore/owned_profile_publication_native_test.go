package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Reuse only the explicit disposable loopback gate. The verifier is scripted,
// but these are the actual Store writer and native serializable transactions.
// This proves storage CAS/audit, not authentic installed-source approval.
func nativeProfilePublicationFixture(t *testing.T) (*ownedReaderFixture, context.Context, *fixtureProfileVerifier, ports.OwnedProfilePublicationGrantV1) {
	t.Helper()
	f, ctx := nativeOwnedCurrentFixture(t)
	lDigest, err := f.profile.Locator.Digest()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := f.store.db.ExecContext(ctx, `DELETE FROM owned_profile_publication_audit_v1 WHERE tenant_id=$1 AND locator_digest=$2`, f.scope.tenant, lDigest); err != nil {
			t.Error(err)
		}
	})
	v := &fixtureProfileVerifier{key: []byte("native-test-owned-fixture-authenticator"), allowed: make(map[string]bool)}
	f.store.ownedProfilePublicationVerifier = v
	g := ports.OwnedProfilePublicationGrantV1{Profile: f.profile.Clone(), IssuedAt: f.profile.Window.ObservedAt, ExpiresAt: f.profile.Window.ObservedAt.Add(10 * time.Second)}
	g = v.install(t, g)
	return f, ctx, v, g
}

func TestOwnedCurrentYDBProfilePublicationHistoricalReplay(t *testing.T) {
	f, ctx, v, g := nativeProfilePublicationFixture(t)
	first, err := f.store.PublishOwnedProfile(ctx, g)
	if err != nil || first.Replayed {
		t.Fatalf("native create err=%v replay=%v", err, first.Replayed)
	}
	r := g.Clone()
	r.Profile = first.Profile.Clone()
	r.Profile.State = domain.OwnedAgentCurrentRevokedV1
	r.Profile.RevocationGeneration = 2
	r.Profile.MutationID = "native-revoke"
	r.ExpectedGeneration = 1
	r.ExpectedDigest = first.Digest
	r = v.install(t, r)
	second, err := f.store.PublishOwnedProfile(ctx, r)
	if err != nil || second.Profile.State != domain.OwnedAgentCurrentRevokedV1 {
		t.Fatalf("native revoke err=%v", err)
	}
	replayed, err := f.store.PublishOwnedProfile(ctx, g)
	if err != nil || !replayed.Replayed || !reflect.DeepEqual(first.Profile, replayed.Profile) {
		t.Fatalf("native historical replay err=%v", err)
	}
	var tombstone domain.OwnedAgentProfileCurrentV1
	err = f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var found bool
		var e error
		tombstone, found, e = loadOwnedProfileProjection(ctx, q, g.Profile.Locator)
		if e != nil || !found {
			return domain.ErrMembershipDenied
		}
		return nil
	})
	if err != nil || tombstone.State != domain.OwnedAgentCurrentRevokedV1 || tombstone.RevocationGeneration != 2 {
		t.Fatalf("native replay resurrected head err=%v gen=%d state=%s", err, tombstone.RevocationGeneration, tombstone.State)
	}
}

func TestOwnedCurrentYDBProfilePublicationConcurrentCAS(t *testing.T) {
	for _, phase := range []string{"create_create", "replace_revoke"} {
		t.Run(phase, func(t *testing.T) {
			f, ctx, v, original := nativeProfilePublicationFixture(t)
			one, two := original.Clone(), original.Clone()
			if phase == "replace_revoke" {
				first, err := f.store.PublishOwnedProfile(ctx, original)
				if err != nil {
					t.Fatal(err)
				}
				one.Profile = first.Profile.Clone()
				one.ExpectedGeneration = 1
				one.ExpectedDigest = first.Digest
				one.Profile.RevocationGeneration = 2
				two = one.Clone()
				two.Profile.State = domain.OwnedAgentCurrentRevokedV1
				one.Profile.ProfileRevision++
				one.Profile.InstallationRevision++
				one.Profile.ReviewReceipt.Digest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
			}
			one.Profile.MutationID = "native-cas-one"
			two.Profile.MutationID = "native-cas-two"
			one = v.install(t, one)
			two = v.install(t, two)
			start := make(chan struct{})
			results := make(chan error, 2)
			for _, grant := range []ports.OwnedProfilePublicationGrantV1{one, two} {
				go func(g ports.OwnedProfilePublicationGrantV1) {
					<-start
					_, err := f.store.PublishOwnedProfile(ctx, g)
					results <- err
				}(grant)
			}
			close(start)
			success, denied := 0, 0
			for range 2 {
				select {
				case err := <-results:
					if err == nil {
						success++
					} else if errors.Is(err, domain.ErrMembershipDenied) {
						denied++
					} else {
						t.Errorf("unexpected native CAS error: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("native CAS workers did not terminate")
				}
			}
			var count uint64
			lDigest, _ := original.Profile.Locator.Digest()
			if err := f.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM owned_profile_publication_audit_v1 WHERE tenant_id=$1 AND locator_digest=$2`, f.scope.tenant, lDigest).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if phase == "replace_revoke" {
				want = 2
			}
			if success != 1 || denied != 1 || count != want {
				t.Fatalf("phase=%s winners=%d denied=%d immutable_receipts=%d want=%d", phase, success, denied, count, want)
			}
		})
	}
}
