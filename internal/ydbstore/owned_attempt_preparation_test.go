package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func beginOwnedPreparation(t *testing.T, f *ownedAttemptEffectParentFixture, grant ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return f.store.beginBoundedOwnedAttemptPreparation(ctx, grant)
}

func TestBoundedOwnedCurrentParentMemoIsExactAndWriteInvalidated(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("create actual parent before current-join memo control")
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(f.sql.parentRow[13].(string)))
	if err != nil {
		t.Fatal(err)
	}
	err = f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if err := f.store.validateCurrentOwnedAttemptParent(ctx, q, f.request, parent); err != nil {
			return err
		}
		before := len(f.graph.statements)
		if err := f.store.validateCurrentOwnedAttemptParent(ctx, q, f.request, parent); err != nil || len(f.graph.statements) != before {
			t.Error("same exact successful join was not reused within its callback")
			return q.budget.deny()
		}
		key := q.currentParentKey
		otherStore := *f.store
		if err := otherStore.validateCurrentOwnedAttemptParent(ctx, q, f.request, parent); err != nil || q.currentParentStore != &otherStore {
			t.Error("join was incorrectly shared across Store compositions")
			return q.budget.deny()
		}
		if q.currentParentKey != key {
			t.Error("exact binding changed across equivalent current joins")
			return q.budget.deny()
		}
		digest, _ := parent.Digest()
		if _, err := q.ExecContext(ctx, ownedAttemptPreparationBurnUpdateSQL, q.at, digest, parent.Reservation.TenantID, parent.Reservation.AttemptID, parent.Reservation.Kind); err != nil {
			return err
		}
		if q.currentParentKey != "" || q.currentParentStore != nil {
			t.Error("successful authority join survived an Exec")
			return q.budget.deny()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A changed selector must not reuse an earlier successful current join.
	err = f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		if q.currentParentKey != "" || q.currentParentStore != nil {
			t.Error("current join leaked into a new callback")
		}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if err := f.store.validateCurrentOwnedAttemptParent(ctx, q, f.request, parent); err != nil {
			return err
		}
		changed := f.request
		changed.PhysicalInvocationClaimID += "-changed"
		if f.store.validateCurrentOwnedAttemptParent(ctx, q, changed, parent) == nil {
			t.Error("changed physical claim reused a successful join")
		}
		return q.budget.deny()
	})
	if err == nil {
		t.Fatal("changed binding must deny the transaction")
	}
}

func TestBoundedOwnedCurrentParentMemoCannotEscapeFinishedCallback(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("create actual parent before finished callback control")
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(f.sql.parentRow[13].(string)))
	if err != nil {
		t.Fatal(err)
	}
	var retained *ownedAuthorityQuery
	err = f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if err := f.store.validateCurrentOwnedAttemptParent(ctx, q, f.request, parent); err != nil {
			return err
		}
		retained = q // deliberate test misuse: production never exports this query
		return nil
	})
	if err != nil || retained == nil || retained.currentParentKey == "" {
		t.Fatalf("authentic successful original callback: %v", err)
	}
	before := len(f.graph.statements)
	if f.store.validateCurrentOwnedAttemptParent(t.Context(), retained, f.request, parent) == nil || len(f.graph.statements) != before {
		t.Fatal("finished callback memo escaped its transaction or issued SQL")
	}
}

func TestBoundedOwnedCurrentParentMemoInvalidatesBeforeRejectedExec(t *testing.T) {
	for _, name := range []string{"cancelled", "over_budget"} {
		t.Run(name, func(t *testing.T) {
			b := &ownedAuthorityBudget{}
			callback, err := b.begin()
			if err != nil {
				t.Fatal(err)
			}
			defer b.finish(callback)
			q := &ownedAuthorityQuery{budget: b, callback: callback, currentParentStore: &Store{}, currentParentKey: "test successful memo"}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if name == "cancelled" {
				cancel()
			} else {
				b.callbackStatements = ownedAuthorityMaxStatements
			}
			// No sql.Tx exists: reaching physical Exec would panic this control.
			if _, err := q.ExecContext(ctx, "UPDATE never_executed SET value=$1", "fixture"); err == nil || q.currentParentStore != nil || q.currentParentKey != "" {
				t.Fatal("rejected Exec retained a successful current-authority memo")
			}
		})
	}
}

func TestBoundedOwnedAttemptPreparationBurnsOriginalParentOnce(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Status != ports.AttemptEffectOwnedV1 || created.Grant == nil || f.sql.parentRow == nil {
		t.Fatalf("create exact parent before first preparation burn: %+v", created)
	}
	parentRow := append([]driver.Value(nil), f.sql.parentRow...)
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(parentRow[13].(string)))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := parent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if f.sql.burnRow[0] != uint64(0) || f.sql.burnRow[1] != uint64(0) || f.sql.burnRow[3] != "" {
		t.Fatalf("new parent must have SQL-null burn fields: %v", f.sql.burnRow)
	}
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || !fresh {
		t.Fatalf("first confirmed preparation burn: fresh=%t err=%v", fresh, err)
	}
	if f.sql.burnUpdates != 1 || f.sql.burnRow[0] != uint64(1) || f.sql.burnRow[1] != uint64(1) || f.sql.burnRow[3] != digest || !f.sql.burnRow[2].(time.Time).Equal(f.at) {
		t.Fatalf("first burn row/update: updates=%d row=%v want at=%s digest=%s", f.sql.burnUpdates, f.sql.burnRow, f.at, digest)
	}
	if !reflect.DeepEqual(f.sql.parentRow, parentRow) {
		t.Fatal("burn changed the immutable original owned parent bytes")
	}
	firstBurn := append([]driver.Value(nil), f.sql.burnRow...)
	fresh, err = beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || fresh {
		t.Fatalf("repeated preparation burn must be historical false: fresh=%t err=%v", fresh, err)
	}
	if f.sql.burnUpdates != 1 || !reflect.DeepEqual(f.sql.burnRow, firstBurn) || !reflect.DeepEqual(f.sql.parentRow, parentRow) {
		t.Fatalf("repeat changed the burn or original parent: updates=%d burnPreserved=%t parentPreserved=%t", f.sql.burnUpdates, reflect.DeepEqual(f.sql.burnRow, firstBurn), reflect.DeepEqual(f.sql.parentRow, parentRow))
	}
}

func TestBoundedOwnedAttemptPreparationRejectsMissingVerifierAndForgedGrantBeforeSQL(t *testing.T) {
	for _, name := range []string{"nil_verifier", "forged_grant"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			created := f.reserve(t, f.request)
			if created.Grant == nil {
				t.Fatal("create authentic parent grant for denial case")
			}
			grant := created.Grant.Clone()
			if name == "nil_verifier" {
				f.store.ownedPreparationGrantVerifier = nil
			} else {
				grant.Authenticator[0] ^= 0xff
			}
			statementCount, parentReads, burnReads, updates := len(f.graph.statements), f.sql.parentReads, f.sql.burnReads, f.sql.burnUpdates
			fresh, err := beginOwnedPreparation(t, f, grant)
			if err == nil || fresh {
				t.Fatalf("invalid preparation authority was not denied: fresh=%t err=%v", fresh, err)
			}
			if len(f.graph.statements) != statementCount || f.sql.parentReads != parentReads || f.sql.burnReads != burnReads || f.sql.burnUpdates != updates {
				t.Fatalf("pre-SQL denial issued SQL: statements=%d/%d parentReads=%d/%d burnReads=%d/%d updates=%d/%d", len(f.graph.statements), statementCount, f.sql.parentReads, parentReads, f.sql.burnReads, burnReads, f.sql.burnUpdates, updates)
			}
		})
	}
}

func TestBoundedOwnedAttemptPreparationDeniesExpiredGrantBeforeSQL(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("create authentic parent grant before expiry case")
	}
	f.graph.at = created.Grant.GrantExpiresAt
	statementCount, parentReads, burnReads, updates := len(f.graph.statements), f.sql.parentReads, f.sql.burnReads, f.sql.burnUpdates
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err == nil || fresh {
		t.Fatalf("expired ownership grant must deny: fresh=%t err=%v", fresh, err)
	}
	if len(f.graph.statements) != statementCount || f.sql.parentReads != parentReads || f.sql.burnReads != burnReads || f.sql.burnUpdates != updates {
		t.Fatalf("expired-grant denial issued SQL: statements=%d/%d parentReads=%d/%d burnReads=%d/%d updates=%d/%d", len(f.graph.statements), statementCount, f.sql.parentReads, parentReads, f.sql.burnReads, burnReads, f.sql.burnUpdates, updates)
	}
}

func TestBoundedOwnedAttemptPreparationDeniesFutureOriginalReservation(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	// All canonical authorities were written at f.at. Create the genuine
	// reservation one microsecond later, then move ONLY the DB snapshot back.
	f.graph.at = f.at.Add(time.Microsecond)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("create authentic original parent before DB clock rewind")
	}
	grant := created.Grant.Clone()
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return grant.Reservation.ReservedAt }, strings.NewReader(strings.Repeat("o", 32)))
	if err != nil || issuer.VerifyGrant(grant) != nil {
		t.Fatalf("independent issuer clock must still authenticate original grant: %v", err)
	}
	f.store.ownedPreparationGrantVerifier = issuer
	f.graph.at = f.at
	parentRow, burnRow := append([]driver.Value(nil), f.sql.parentRow...), append([]driver.Value(nil), f.sql.burnRow...)
	fresh, err := beginOwnedPreparation(t, f, grant)
	if err == nil || fresh {
		t.Fatalf("DB snapshot before original reservation must deny: fresh=%t err=%v", fresh, err)
	}
	if f.sql.burnUpdates != 0 || !reflect.DeepEqual(f.sql.burnRow, burnRow) || !reflect.DeepEqual(f.sql.parentRow, parentRow) || f.sql.activeTransactions != 0 {
		t.Fatalf("future original reservation changed state: updates=%d active=%d", f.sql.burnUpdates, f.sql.activeTransactions)
	}
}

func TestBoundedOwnedAttemptPreparationDeniesChangedOriginalParent(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("create original grant before parent replacement case")
	}
	final, err := domain.DecodeAttemptExecutionAdmissionV1([]byte(f.base.final[0].(string)))
	if err != nil {
		t.Fatal(err)
	}
	otherClaim := f.request.PhysicalInvocationClaimID + "-different"
	other, err := domain.BuildOwnedAttemptEffectParentV1(final, f.parent.Authority, otherClaim, f.request.UpstreamIdempotencyKeyDigest, f.at)
	if err != nil {
		t.Fatal(err)
	}
	f.sql.parentRow = parentRowFromDomain(t, other)
	parentRow := append([]driver.Value(nil), f.sql.parentRow...)
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err == nil || fresh {
		t.Fatalf("grant for the replaced parent must deny: fresh=%t err=%v", fresh, err)
	}
	if f.sql.burnUpdates != 0 || !reflect.DeepEqual(f.sql.parentRow, parentRow) {
		t.Fatal("mismatched original parent was burned or changed")
	}
}

func TestBoundedOwnedAttemptPreparationDeniesInvalidCurrentAuthority(t *testing.T) {
	for _, name := range []string{"changed_worker", "removed_principal", "profile_revoked", "spending_revoked", "expired_quota_hold", "partial_burn"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			created := f.reserve(t, f.request)
			if created.Grant == nil {
				t.Fatal("create authentic parent grant for current-authority denial")
			}
			switch name {
			case "changed_worker":
				attempt := ratedFixtureRecord[domain.Attempt](t, f.graph.ratedSQLFixture, "attempts")
				attempt.WorkerID = "worker-other"
				ratedFixturePut(t, f.graph.ratedSQLFixture, "attempts", attempt)
			case "removed_principal":
				var participant domain.SessionParticipant
				if err := json.Unmarshal([]byte(f.base.participant[0].(string)), &participant); err != nil {
					t.Fatal(err)
				}
				participant.Status, participant.UpdatedAt = domain.SessionParticipantRemoved, f.at
				raw, err := json.Marshal(participant)
				if err != nil {
					t.Fatal(err)
				}
				f.base.participant[0], f.base.participant[2] = string(raw), string(participant.Status)
			case "profile_revoked":
				f.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
				f.base.reader.profile.RevocationGeneration++
				f.base.reader.sync(t)
			case "spending_revoked":
				f.base.reader.spending[0].State = domain.OwnedAgentCurrentRevokedV1
				f.base.reader.spending[0].RevocationGeneration++
				f.base.reader.sync(t)
			case "expired_quota_hold":
				reservation := ratedFixtureRecord[domain.QuotaReservation](t, f.graph.ratedSQLFixture, "quota_reservations")
				reservation.ExpiresAt = f.at.Add(10 * time.Microsecond)
				ratedFixturePut(t, f.graph.ratedSQLFixture, "quota_reservations", reservation)
				f.graph.at = f.at.Add(20 * time.Microsecond)
			case "partial_burn":
				parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(f.sql.parentRow[13].(string)))
				if err != nil {
					t.Fatal(err)
				}
				digest, err := parent.Digest()
				if err != nil {
					t.Fatal(err)
				}
				f.sql.burnRow = []driver.Value{uint64(1), uint64(0), f.at, digest}
			}
			parentRow, burnRow := append([]driver.Value(nil), f.sql.parentRow...), append([]driver.Value(nil), f.sql.burnRow...)
			fresh, err := beginOwnedPreparation(t, f, *created.Grant)
			if err == nil || fresh {
				t.Fatalf("invalid current authority %q must deny: fresh=%t err=%v", name, fresh, err)
			}
			if f.sql.burnUpdates != 0 || !reflect.DeepEqual(f.sql.burnRow, burnRow) || !reflect.DeepEqual(f.sql.parentRow, parentRow) || f.sql.activeTransactions != 0 {
				t.Fatalf("denial changed preparation state case=%s updates=%d burnPreserved=%t parentPreserved=%t active=%d", name, f.sql.burnUpdates, reflect.DeepEqual(f.sql.burnRow, burnRow), reflect.DeepEqual(f.sql.parentRow, parentRow), f.sql.activeTransactions)
			}
		})
	}
}

func TestBoundedOwnedAttemptPreparationKnownRollbackAndLostAckNeverDoubleBurn(t *testing.T) {
	for _, test := range []struct {
		name       string
		modes      []string
		wantFresh  bool
		wantUpdate int
	}{
		{name: "known_rollback_then_commit", modes: []string{"rollback", ""}, wantFresh: true, wantUpdate: 2},
		{name: "committed_lost_ack", modes: []string{"ambiguous", ""}, wantUpdate: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			created := f.reserve(t, f.request)
			if created.Grant == nil {
				t.Fatal("create authentic parent grant before retry case")
			}
			parentRow := append([]driver.Value(nil), f.sql.parentRow...)
			f.graph.commitModes = append([]string(nil), test.modes...)
			fresh, err := beginOwnedPreparation(t, f, *created.Grant)
			if err != nil || fresh != test.wantFresh {
				t.Fatalf("retry burn status: fresh=%t want=%t err=%v", fresh, test.wantFresh, err)
			}
			parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(parentRow[13].(string)))
			if err != nil {
				t.Fatal(err)
			}
			parentDigest, err := parent.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if f.sql.burnUpdates != test.wantUpdate || f.sql.burnRow[0] != uint64(1) || f.sql.burnRow[1] != uint64(1) || f.sql.burnRow[3] != parentDigest || !f.sql.burnRow[2].(time.Time).Equal(f.at) || !reflect.DeepEqual(f.sql.parentRow, parentRow) {
				t.Fatalf("retry burn durability/update count=%d/%d row=%v parentPreserved=%t", f.sql.burnUpdates, test.wantUpdate, f.sql.burnRow, reflect.DeepEqual(f.sql.parentRow, parentRow))
			}
		})
	}
}

func TestBoundedOwnedAttemptPreparationUnresolvedAndPostCommitFailureNeverResetBurn(t *testing.T) {
	t.Run("three_ambiguous_commits_then_resume", func(t *testing.T) {
		f := newOwnedAttemptEffectParentFixture(t)
		created := f.reserve(t, f.request)
		if created.Grant == nil {
			t.Fatal("create authentic parent grant before ambiguous commit case")
		}
		f.graph.commitModes = []string{"ambiguous", "ambiguous", "ambiguous"}
		fresh, err := beginOwnedPreparation(t, f, *created.Grant)
		if err == nil || fresh || f.sql.burnRow[0] != uint64(1) || f.sql.burnUpdates != 1 {
			t.Fatalf("unresolved commit must fail with durable burn preserved: fresh=%t err=%v updates=%d burn=%v", fresh, err, f.sql.burnUpdates, f.sql.burnRow)
		}
		f.store = clonePreparationStore(f.store)
		fresh, err = beginOwnedPreparation(t, f, *created.Grant)
		if err != nil || fresh || f.sql.burnUpdates != 1 {
			t.Fatalf("fresh store resume must observe existing burn only: fresh=%t err=%v updates=%d", fresh, err, f.sql.burnUpdates)
		}
	})

	t.Run("postcommit_verification_failure_keeps_burn", func(t *testing.T) {
		f := newOwnedAttemptEffectParentFixture(t)
		created := f.reserve(t, f.request)
		if created.Grant == nil {
			t.Fatal("create authentic parent grant before postcommit verifier case")
		}
		parentRow := append([]driver.Value(nil), f.sql.parentRow...)
		f.issuer.rejectVerifyCall = f.issuer.verifyCalls + 2
		fresh, err := beginOwnedPreparation(t, f, *created.Grant)
		if err == nil || fresh || f.sql.burnRow[0] != uint64(1) || f.sql.burnUpdates != 1 || !reflect.DeepEqual(f.sql.parentRow, parentRow) {
			t.Fatalf("postcommit grant verification failure must not reset committed burn: fresh=%t err=%v updates=%d burn=%v parentPreserved=%t", fresh, err, f.sql.burnUpdates, f.sql.burnRow, reflect.DeepEqual(f.sql.parentRow, parentRow))
		}
		f.issuer.rejectVerifyCall = 0
		f.store = clonePreparationStore(f.store)
		fresh, err = beginOwnedPreparation(t, f, *created.Grant)
		if err != nil || fresh || f.sql.burnUpdates != 1 {
			t.Fatalf("resume after postcommit verification failure must not freshen: fresh=%t err=%v updates=%d", fresh, err, f.sql.burnUpdates)
		}
	})
}

func clonePreparationStore(store *Store) *Store {
	clone := *store
	return &clone
}

func TestBoundedOwnedAttemptPreparationBurnUpdateBindsPersistedParent(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("create authentic parent grant before UPDATE binding assertion")
	}
	parentDigest := f.sql.parentRow[14].(string)
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || !fresh {
		t.Fatalf("burn result: fresh=%t err=%v", fresh, err)
	}
	if parentDigest != f.sql.burnRow[3].(string) {
		t.Fatalf("burn digest does not bind exact persisted parent: got=%q want=%q", f.sql.burnRow[3], parentDigest)
	}
}

func TestBoundedOwnedAttemptPreparationMalformedMarkerNeverRestoresPermission(t *testing.T) {
	for _, scenario := range []string{"missing_time", "missing_digest", "foreign_digest", "future_time", "before_parent", "empty_digest", "oversized_digest", "invalid_presence"} {
		t.Run(scenario, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			created := f.reserve(t, f.request)
			if created.Grant == nil {
				t.Fatal("missing authentic original control grant")
			}
			marker := []driver.Value{uint64(1), uint64(1), f.at, f.sql.parentRow[14].(string)}
			switch scenario {
			case "missing_time":
				marker[0] = uint64(0)
			case "missing_digest":
				marker[1], marker[3] = uint64(0), ""
			case "foreign_digest":
				marker[3] = strings.Repeat("a", 64)
			case "future_time":
				marker[2] = f.at.Add(time.Microsecond)
			case "before_parent":
				marker[2] = f.at.Add(-time.Microsecond)
			case "empty_digest":
				marker[3] = ""
			case "oversized_digest":
				marker[3] = strings.Repeat("a", 65)
			case "invalid_presence":
				marker[0] = uint64(2)
			}
			f.sql.burnRow = append([]driver.Value(nil), marker...)
			fresh, err := beginOwnedPreparation(t, f, *created.Grant)
			if err == nil || fresh {
				t.Fatalf("malformed %s restored permission: fresh=%t err=%v", scenario, fresh, err)
			}
			for _, claim := range []string{f.request.PhysicalInvocationClaimID, "other-original-parent-reader"} {
				request := f.request
				request.PhysicalInvocationClaimID = claim
				f.deny(t, request)
			}
			if f.sql.burnUpdates != 0 || !reflect.DeepEqual(f.sql.burnRow, marker) {
				t.Fatalf("malformed %s was repaired or cleared: updates=%d marker=%v", scenario, f.sql.burnUpdates, f.sql.burnRow)
			}
		})
	}
}
