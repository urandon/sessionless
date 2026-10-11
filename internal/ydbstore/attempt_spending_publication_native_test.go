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

// Native storage composition with explicitly synthetic profile/source verifiers.
// It is NOT installed multi-operation approval or canonical admission proof.
func nativeSpendingPublicationFixture(t *testing.T) (*ownedReaderFixture, context.Context, *fixtureSpendingVerifier, [4]ports.AttemptSpendingPublicationGrantV1, *fixtureProfileVerifier, ports.OwnedProfilePublicationGrantV1) {
	t.Helper()
	f, ctx, pv, pg := nativeProfilePublicationFixture(t)
	first, err := f.store.PublishOwnedProfile(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	f.profile, f.ref.Digest = first.Profile.Clone(), first.Digest
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := f.store.db.ExecContext(cleanup, `DELETE FROM attempt_spending_publication_audit_v1 WHERE tenant_id=$1 AND attempt_id=$2`, f.scope.tenant, f.scope.attempt); err != nil {
			t.Error(err)
		}
	})
	v := &fixtureSpendingVerifier{key: []byte("native-synthetic-source-owner"), allowed: make(map[string]bool)}
	f.store.attemptSpendingPublicationVerifier = v
	var grants [4]ports.AttemptSpendingPublicationGrantV1
	for i := range f.spending {
		f.spending[i].SourceProfile = f.ref
		f.spending[i].Window.PublishedAt = first.Profile.Window.PublishedAt
		f.spending[i].UpdatedAt = first.Profile.Window.PublishedAt
		bindReaderSpendingSource(t, &f.spending[i])
		grants[i] = v.install(t, ports.AttemptSpendingPublicationGrantV1{Authorization: f.spending[i].Clone(), IssuedAt: pg.IssuedAt, ExpiresAt: pg.ExpiresAt})
	}
	return f, ctx, v, grants, pv, pg
}

func TestOwnedCurrentYDBSpendingPublicationAllComponentsAndSourceRevoke(t *testing.T) {
	f, ctx, v, grants, pv, pg := nativeSpendingPublicationFixture(t)
	var originals [4]ports.AttemptSpendingPublicationReceiptV1
	for i, g := range grants {
		r, err := f.store.PublishAttemptSpending(ctx, g)
		if err != nil {
			t.Fatalf("component=%s create err=%v", g.Authorization.Locator.ComponentKind, err)
		}
		originals[i] = r
		f.refs[i] = ownedSpendingReference{locator: r.Authorization.Locator, generation: 1, digest: r.Digest}
	}
	if _, err := f.read(); err != nil {
		t.Fatalf("all four actual published current joins: %v", err)
	}
	pg.Profile = f.profile.Clone()
	pg.Profile.State = domain.OwnedAgentCurrentRevokedV1
	pg.Profile.RevocationGeneration = 2
	pg.Profile.MutationID = "native-source-revoke"
	pg.ExpectedGeneration = 1
	pg.ExpectedDigest = f.ref.Digest
	pg = pv.install(t, pg)
	if _, err := f.store.PublishOwnedProfile(ctx, pg); err != nil {
		t.Fatal(err)
	}
	if _, err := f.read(); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("revoked source authorized current snapshot: %v", err)
	}
	for i, g := range grants {
		before := originals[i]
		replayed, err := f.store.PublishAttemptSpending(ctx, g)
		if err != nil || !replayed.Replayed || !reflect.DeepEqual(before.Authorization, replayed.Authorization) {
			t.Fatalf("historical component=%s err=%v", g.Authorization.Locator.ComponentKind, err)
		}
		r := spendingRevoke(t, v, g, before, g.IssuedAt)
		if _, err := f.store.PublishAttemptSpending(ctx, r); err != nil {
			t.Fatalf("cleanup component=%s err=%v", g.Authorization.Locator.ComponentKind, err)
		}
		late := g.Clone()
		late.Authorization.MutationID = "no-recreation"
		late = v.install(t, late)
		got, err := f.store.PublishAttemptSpending(ctx, late)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) {
			t.Fatalf("source/tombstone recreation component=%s err=%v", g.Authorization.Locator.ComponentKind, err)
		}
	}
}

func TestOwnedCurrentYDBSpendingPublicationConcurrentCAS(t *testing.T) {
	for _, phase := range []string{"create_create", "revoke_revoke"} {
		t.Run(phase, func(t *testing.T) {
			f, ctx, v, grants, _, _ := nativeSpendingPublicationFixture(t)
			one := grants[0].Clone()
			if phase == "revoke_revoke" {
				r, err := f.store.PublishAttemptSpending(ctx, one)
				if err != nil {
					t.Fatal(err)
				}
				one = spendingRevoke(t, v, one, r, one.IssuedAt)
			}
			two := one.Clone()
			one.Authorization.MutationID = "native-one"
			two.Authorization.MutationID = "native-two"
			one = v.install(t, one)
			two = v.install(t, two)
			start := make(chan struct{})
			results := make(chan error, 2)
			for _, g := range []ports.AttemptSpendingPublicationGrantV1{one, two} {
				go func(g ports.AttemptSpendingPublicationGrantV1) {
					<-start
					_, err := f.store.PublishAttemptSpending(ctx, g)
					results <- err
				}(g)
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
						t.Errorf("CAS error: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("spending CAS workers did not terminate")
				}
			}
			var count uint64
			if err := f.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_spending_publication_audit_v1 WHERE tenant_id=$1 AND attempt_id=$2`, f.scope.tenant, f.scope.attempt).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if phase == "revoke_revoke" {
				want = 2
			}
			if success != 1 || denied != 1 || count != want {
				t.Fatalf("phase=%s winners=%d denied=%d receipts=%d want=%d", phase, success, denied, count, want)
			}
		})
	}
}

func TestOwnedCurrentYDBSpendingCreateVersusSourceRevocation(t *testing.T) {
	f, ctx, _, grants, pv, pg := nativeSpendingPublicationFixture(t)
	pg.Profile = f.profile.Clone()
	pg.Profile.State = domain.OwnedAgentCurrentRevokedV1
	pg.Profile.RevocationGeneration = 2
	pg.Profile.MutationID = "native-concurrent-source-revoke"
	pg.ExpectedGeneration, pg.ExpectedDigest = 1, f.ref.Digest
	pg = pv.install(t, pg)
	start := make(chan struct{})
	type outcome struct {
		revoke bool
		err    error
	}
	results := make(chan outcome, 2)
	go func() {
		<-start
		_, err := f.store.PublishAttemptSpending(ctx, grants[0])
		results <- outcome{err: err}
	}()
	go func() {
		<-start
		_, err := f.store.PublishOwnedProfile(ctx, pg)
		results <- outcome{revoke: true, err: err}
	}()
	close(start)
	created := false
	for range 2 {
		select {
		case got := <-results:
			if got.revoke && got.err != nil {
				t.Fatalf("source revocation failed: %v", got.err)
			}
			if !got.revoke {
				if got.err == nil {
					created = true
				} else if !errors.Is(got.err, domain.ErrMembershipDenied) {
					t.Errorf("create error: %v", got.err)
				}
			}
		case <-ctx.Done():
			t.Fatal("source/create race workers did not terminate")
		}
	}
	var count uint64
	if err := f.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_spending_publication_audit_v1 WHERE tenant_id=$1 AND attempt_id=$2`, f.scope.tenant, f.scope.attempt).Scan(&count); err != nil {
		t.Fatal(err)
	}
	want := uint64(0)
	if created {
		want = 1
	}
	if count != want {
		t.Fatalf("race created=%v audit_count=%d want=%d", created, count, want)
	}
	// Either serialization order is legitimate; after confirmed revocation,
	// a fresh create must deny and a current source read must never authorize.
	err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, err := readOwnedProfileCurrent(ctx, q, f.ref)
		return err
	})
	if !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("current revoked source authorized: %v", err)
	}
	got, err := f.store.PublishAttemptSpending(ctx, grants[1])
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.AttemptSpendingPublicationReceiptV1{}) {
		t.Fatalf("post-revoke new component accepted: %v", err)
	}
}
