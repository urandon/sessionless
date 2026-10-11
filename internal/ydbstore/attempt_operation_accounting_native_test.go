package ydbstore

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func nativeOperationSetup(t *testing.T, g nativeOwnedParentGraph) {
	t.Helper()
	issuer, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time {
		var at time.Time
		if g.f.store.db.QueryRowContext(g.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at) != nil {
			return time.Time{}
		}
		return at.UTC()
	}, strings.NewReader(strings.Repeat("k", 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	g.f.store.attemptOperationIssuer, g.f.store.ownedPreparationGrantVerifier = issuer, g.issuer
}

func nativeAccountingBytes(t *testing.T, g nativeOwnedParentGraph) (ownedAttemptAccounting, string, time.Time) {
	t.Helper()
	var raw, digest string
	var burn time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT operation_accounting_record,operation_accounting_digest,preparation_started_at
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&raw, &digest, &burn); err != nil {
		t.Fatal(err)
	}
	parent, _ := g.persistedParent(t)
	var value ownedAttemptAccounting
	if json.Unmarshal([]byte(raw), &value) != nil || value.validate(parent, g.final.Source, nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)) != nil {
		t.Fatal("invalid actual committed setup accounting")
	}
	canonical, actual, err := accountingBytes(value)
	if err != nil || string(canonical) != raw || actual != digest || !value.SetupReservedAt.Equal(burn) {
		t.Fatal("native setup debit and preparation burn must be SAME canonical transaction observation")
	}
	return value, raw, burn
}

func TestOwnedOperationYDBSetupConcurrentBurnAndFreshStoreNeverRedebits(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	nativeOperationSetup(t, g)
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("authentic outer prerequisite: %v", err)
	}
	_, parentBefore := g.persistedParent(t)
	storeA, storeB := *g.f.store, *g.f.store
	type outcome struct {
		fresh bool
		err   error
	}
	results, start := make(chan outcome, 2), make(chan struct{})
	ctx, cancel := context.WithTimeout(g.ctx, 25*time.Second)
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	for _, store := range []*Store{&storeA, &storeB} {
		wg.Go(func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- outcome{err: ctx.Err()}
				return
			}
			fresh, err := store.beginBoundedOwnedAttemptPreparation(ctx, *created.Grant)
			results <- outcome{fresh, err}
		})
	}
	close(start)
	var got [2]outcome
	for i := range got {
		select {
		case got[i] = <-results:
		case <-ctx.Done():
			t.Fatalf("bounded concurrent setup: %v", ctx.Err())
		}
	}
	wg.Wait()
	if got[0].err != nil || got[1].err != nil || got[0].fresh == got[1].fresh {
		t.Fatalf("exactly one actual fresh setup required: %+v", got)
	}
	value, raw, burn := nativeAccountingBytes(t, g)
	if value.Amounts[1] != *g.final.Source.Spending[1].MaxAmountMicrounits || value.Aggregate.Operations != 0 {
		t.Fatal("must reserve exact original substrate, not an operation or a guessed free amount")
	}
	resumed, err := New(g.f.store.db, Options{AttemptEffectGrantIssuer: g.issuer, OwnedAttemptEffectSourceVerifier: g.f.store.ownedAttemptEffectSourceVerifier, OwnedPreparationGrantVerifier: g.issuer, AttemptOperationIssuer: g.f.store.attemptOperationIssuer})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := resumed.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant)
	if err != nil || fresh {
		t.Fatalf("fresh Store historical setup: fresh=%t err=%v", fresh, err)
	}
	valueAfter, rawAfter, burnAfter := nativeAccountingBytes(t, g)
	_, parentAfter := g.persistedParent(t)
	if !reflect.DeepEqual(value, valueAfter) || raw != rawAfter || !burn.Equal(burnAfter) || parentBefore != parentAfter {
		t.Fatal("resume changed/refunded/replaced original setup or parent")
	}
	if got, err := resumed.reserveBoundedOwnedAttemptEffect(g.ctx, g.request); err != nil || got.Status != ports.AttemptEffectReconcileOnlyV1 || got.Grant != nil {
		t.Fatalf("genuinely reconstructed Store must not mint another outer claimant: status=%s err=%v", got.Status, err)
	}
}

func TestOwnedOperationYDBCurrentRevocationLeavesSetupAndBurnAbsent(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	nativeOperationSetup(t, g)
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("authentic outer prerequisite: %v", err)
	}
	g.revokeParentProfile(t)
	if fresh, err := g.f.store.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant); err == nil || fresh {
		t.Fatalf("current revoked setup must deny: fresh=%t err=%v", fresh, err)
	}
	var fields uint64
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CASE WHEN operation_accounting_record IS NULL AND operation_accounting_digest IS NULL AND preparation_started_at IS NULL AND preparation_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&fields); err != nil {
		t.Fatal(err)
	}
	if fields != 0 {
		t.Fatal("denied current authority created a setup debit or physical preparation burn")
	}
}
