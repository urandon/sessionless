package ydbstore

import (
	"context"
	"crypto/rand"
	"errors"
	"math"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

func nativeOwnedCurrentFixture(t *testing.T) (*ownedReaderFixture, context.Context) {
	t.Helper()
	if os.Getenv("SESSIONLESS_OWNED_CURRENT_YDB_GATE") != "1" {
		t.Skip("explicit disposable YDB current-reader gate not requested")
	}
	dsn := os.Getenv("YDB_CONNECTION_STRING")
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") || os.Getenv("SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE") != "1" {
		t.Fatal("requires explicit disposable migrated loopback database; shared/cloud mutation forbidden")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	client, err := ydbclient.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := client.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	var at time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	f := newOwnedReaderFixtureAt(t, at.UTC(), rand.Text())
	f.store = &Store{db: client.DB}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		l := f.ref.Locator
		if _, err := client.DB.ExecContext(cleanup, `DELETE FROM attempt_spending_authorization_current_v1 WHERE tenant_id=$1 AND attempt_id=$2`, f.scope.tenant, f.scope.attempt); err != nil {
			t.Error(err)
		}
		if _, err := client.DB.ExecContext(cleanup, `DELETE FROM owned_agent_profile_current_v1 WHERE tenant_id=$1 AND owner_user_id=$2 AND profile=$3 AND backend_profile_digest=$4 AND resource_kind=$5 AND resource_id=$6 AND model_vendor_id=$7 AND model_id=$8 AND input_data_class=$9`, l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass); err != nil {
			t.Error(err)
		}
	})
	return f, ctx
}

func putNativeOwnedProfile(t *testing.T, ctx context.Context, f *ownedReaderFixture) {
	t.Helper()
	p := f.profile
	l := p.Locator
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.db.ExecContext(ctx, `UPSERT INTO owned_agent_profile_current_v1 (tenant_id,owner_user_id,profile,backend_profile_digest,resource_kind,resource_id,model_vendor_id,model_id,input_data_class,record,version,profile_revision,revocation_generation,state,digest,installation_id,installation_revision,published_at,expires_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`, l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass, string(raw), uint64(p.Version), p.ProfileRevision, p.RevocationGeneration, p.State, digest, p.InstallationID, p.InstallationRevision, p.Window.PublishedAt, p.Window.ExpiresAt, p.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
}

// Test-only direct raw installation bypasses the unimplemented trusted writer.
// This proves bounded storage correlation, NEVER approval/payer permission.
func putNativeOwnedRows(t *testing.T, ctx context.Context, f *ownedReaderFixture) {
	t.Helper()
	putNativeOwnedProfile(t, ctx, f)
	for _, v := range f.spending {
		raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := v.Digest()
		if err != nil {
			t.Fatal(err)
		}
		l := v.Locator
		_, err = f.store.db.ExecContext(ctx, `INSERT INTO attempt_spending_authorization_current_v1 (tenant_id,attempt_id,component_kind,billing_owner_user_id,record,version,revocation_generation,state,digest,run_id,session_id,owner_user_id,published_at,expires_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID, string(raw), uint64(v.Version), v.RevocationGeneration, v.State, digest, v.RunID, v.SessionID, v.OwnerUserID, v.Window.PublishedAt, v.Window.ExpiresAt, v.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOwnedCurrentYDBLosslessScalarsAndBoundedRead(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	// Prove native String + Uint64 preserve values beyond IEEE-754 precision
	// and signed int64. No float or JsonDocument round-trip is involved.
	f.profile.ProfileRevision = math.MaxUint64
	f.ref.Digest, _ = f.profile.Digest()
	for i := range f.spending {
		f.spending[i].SourceProfile = f.ref
		if f.spending[i].Locator.ComponentKind == domain.AttemptSpendingProviderV1 {
			f.spending[i].ProfileRevision = math.MaxUint64
		}
		bindReaderSpendingSource(t, &f.spending[i])
		f.refs[i].digest, _ = f.spending[i].Digest()
	}
	putNativeOwnedRows(t, ctx, f)
	got, err := f.read()
	if err != nil || got.profile.ProfileRevision != math.MaxUint64 || !reflect.DeepEqual(got.spending, f.spending) {
		t.Fatalf("native exact integer/current join err=%v revision=%d", err, got.profile.ProfileRevision)
	}
	l := f.ref.Locator
	if _, err := f.store.db.ExecContext(ctx, `UPDATE owned_agent_profile_current_v1 SET record=$10 WHERE tenant_id=$1 AND owner_user_id=$2 AND profile=$3 AND backend_profile_digest=$4 AND resource_kind=$5 AND resource_id=$6 AND model_vendor_id=$7 AND model_id=$8 AND input_data_class=$9`, l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass, strings.Repeat("x", ownedAuthorityHeadBytes+100)); err != nil {
		t.Fatal(err)
	}
	var pending ownedCurrentSnapshot
	err = f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		current, readErr := readOwnedCurrentSnapshot(ctx, q, f.scope, f.ref, f.refs)
		if q.budget.bytes > ownedAuthorityHeadBytes+1+17+65+161+56+8 {
			t.Fatalf("native SQL prefix transferred too much metadata: %d", q.budget.bytes)
		}
		if readErr == nil {
			pending = current
		}
		return readErr
	})
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(pending, ownedCurrentSnapshot{}) {
		t.Fatalf("native oversize err=%v partial=%t", err, !reflect.DeepEqual(pending, ownedCurrentSnapshot{}))
	}
}

func TestOwnedCurrentYDBRevocationConflictsWithWritingSnapshot(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	putNativeOwnedRows(t, ctx, f)
	callbacks := 0
	var pending ownedCurrentSnapshot
	err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		pending = ownedCurrentSnapshot{}
		callbacks++
		if _, err := readOwnedProfileCurrent(ctx, q, f.ref); err != nil {
			return err
		}
		if callbacks == 1 {
			// A real distinct transaction commits revoke AFTER this transaction's
			// original head read. The stale read must not commit as usable success.
			f.profile.State = domain.OwnedAgentCurrentRevokedV1
			f.profile.RevocationGeneration++
			putNativeOwnedProfile(t, ctx, f)
		}
		current, err := readOwnedCurrentSnapshot(ctx, q, f.scope, f.ref, f.refs)
		if err != nil {
			return err
		}
		// A pure read-only transaction can legitimately serialize BEFORE the
		// concurrent revoke. Demand actual read-write conflict with a harmless
		// same-value write to our exact owned fixture row. This is NOT an
		// operation reservation/debit implementation or source-publication proof.
		ref := f.refs[0]
		if _, err := q.ExecContext(ctx, `UPDATE attempt_spending_authorization_current_v1 SET digest=$5 WHERE tenant_id=$1 AND attempt_id=$2 AND component_kind=$3 AND billing_owner_user_id=$4`, ref.locator.TenantID, ref.locator.AttemptID, ref.locator.ComponentKind, ref.locator.BillingOwnerUserID, ref.digest); err != nil {
			return err
		}
		pending = current
		return nil
	})
	if err != nil {
		pending = ownedCurrentSnapshot{}
	}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(pending, ownedCurrentSnapshot{}) || callbacks != 2 {
		t.Fatalf("native stale snapshot err=%v callbacks=%d partial=%t", err, callbacks, !reflect.DeepEqual(pending, ownedCurrentSnapshot{}))
	}
}
