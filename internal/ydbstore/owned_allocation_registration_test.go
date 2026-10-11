package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

type allocationBurnAdapter struct{ store *Store }

func (b allocationBurnAdapter) BeginOwnedAttemptPreparation(ctx context.Context, grant ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	return b.store.beginBoundedOwnedAttemptPreparation(ctx, grant)
}

// This extends the existing scripted sole-parent graph with test-owned child
// rows. It tests callback/protocol behavior, not native serializable isolation.
type allocationSQLFixture struct {
	parent       *ownedAttemptEffectParentSQLFixture
	row, pending []driver.Value
	writes       int
}
type allocationConnector struct{ f *allocationSQLFixture }
type allocationDriver struct{ f *allocationSQLFixture }
type allocationConn struct{ f *allocationSQLFixture }
type allocationTx struct {
	f     *allocationSQLFixture
	inner driver.Tx
}

func (c allocationConnector) Connect(context.Context) (driver.Conn, error) {
	return allocationConn{c.f}, nil
}
func (c allocationConnector) Driver() driver.Driver         { return allocationDriver{c.f} }
func (d allocationDriver) Open(string) (driver.Conn, error) { return allocationConn{d.f}, nil }
func (c allocationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("allocation fixture forbids Prepare")
}
func (c allocationConn) Close() error { return nil }
func (c allocationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("allocation fixture requires BeginTx")
}
func (c allocationConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	inner, err := (ownedAttemptEffectParentConn{c.f.parent}).BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	c.f.pending = nil
	return allocationTx{c.f, inner}, nil
}
func (c allocationConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement == ownedAttemptPreparationBurnSQL {
		c.f.parent.burnReads++
		c.f.parent.base.recordQuery(statement, args)
		row := c.f.row
		if c.f.pending != nil {
			row = c.f.pending
		}
		var anyChild uint64
		if len(row) >= 6 {
			for _, flag := range row[:6] {
				if flag != uint64(0) {
					anyChild = 1
					break
				}
			}
		}
		values := append(append([]driver.Value(nil), c.f.parent.burnRow...), anyChild)
		return &explanationFixtureRows{values: values}, nil
	}
	if statement != ownedAllocationRegistrationSQL {
		return (ownedAttemptEffectParentConn{c.f.parent}).QueryContext(ctx, statement, args)
	}
	c.f.parent.base.recordQuery(statement, args)
	row := c.f.row
	if c.f.pending != nil {
		row = c.f.pending
	}
	if row == nil {
		row = []driver.Value{uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), "", "", "", "", "", ""}
	}
	return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
}
func (c allocationConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement != ownedAllocationRegistrationCreateSQL && statement != ownedAllocationRegistrationTransitionSQL {
		return (ownedAttemptEffectParentConn{c.f.parent}).ExecContext(ctx, statement, args)
	}
	if len(args) != 7 {
		return nil, errors.New("allocation write requires seven bounded parameters")
	}
	result, err := c.f.parent.base.ExecContext(ctx, statement, args)
	if err != nil {
		return nil, err
	}
	c.f.writes++
	if statement == ownedAllocationRegistrationCreateSQL {
		if c.f.row != nil {
			return nil, errors.New("allocation fixture rejects replacement")
		}
		c.f.pending = []driver.Value{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(0), args[0].Value, args[1].Value, args[0].Value, args[1].Value, args[2].Value, ""}
	} else {
		if c.f.row == nil || c.f.row[9] != args[2].Value {
			return nil, errors.New("allocation fixture requires exact current CAS")
		}
		c.f.pending = append([]driver.Value(nil), c.f.row...)
		c.f.pending[5], c.f.pending[8], c.f.pending[9], c.f.pending[11] = uint64(1), args[0].Value, args[1].Value, args[2].Value
	}
	return result, nil
}
func (tx allocationTx) Commit() error {
	mode := ""
	if len(tx.f.parent.graph.commitModes) > 0 {
		mode = tx.f.parent.graph.commitModes[0]
	}
	err := tx.inner.Commit()
	if mode != "rollback" && tx.f.pending != nil {
		tx.f.row = append([]driver.Value(nil), tx.f.pending...)
	}
	tx.f.pending = nil
	return err
}
func (tx allocationTx) Rollback() error { tx.f.pending = nil; return tx.inner.Rollback() }

type ownedAllocationFixture struct {
	parent      *ownedAttemptEffectParentFixture
	sql         *allocationSQLFixture
	publication ports.OwnedAllocationPublicationV1
	bridge      *serverlessharness.BoundedOwnedAllocationPreparationV1
}

func newOwnedAllocationFixture(t *testing.T) ownedAllocationFixture {
	t.Helper()
	f := newOwnedAttemptEffectParentFixture(t)
	s := &allocationSQLFixture{parent: f.sql}
	db := sql.OpenDB(allocationConnector{s})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store.db = db
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("allocation test requires authentic sole-parent grant")
	}
	driver, err := serverlessharness.NewInProcessExecutionSubstrateV1(func() time.Time { return f.graph.at.UTC() }, f.issuer.issuer, f.parent.Authority.HarnessBinding.Backend)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := serverlessharness.NewBoundedOwnedAllocationPreparationV1(f.issuer.issuer, f.parent.Authority.HarnessBinding.Backend, f.parent.Authority.SubstrateBinding, driver, allocationBurnAdapter{f.store})
	if err != nil {
		t.Fatal(err)
	}
	f.store.ownedAllocationPublicationVerifier = bridge
	p, err := bridge.PrepareAllocationPublication(t.Context(), *created.Grant)
	if err != nil {
		t.Fatalf("actual trusted preflight receipt prerequisite: %v", err)
	}
	return ownedAllocationFixture{parent: f, sql: s, publication: p, bridge: bridge}
}

func TestOwnedAllocationRegistrationCreateReadReplayAndRetire(t *testing.T) {
	f := newOwnedAllocationFixture(t)
	first, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
	if err != nil || first.Replayed || first.Digest == "" || first.Original.ValidateInitial() != nil {
		t.Fatalf("first actual observation registration err=%v receipt=%+v", err, first)
	}
	originalRow := append([]driver.Value(nil), f.sql.row...)
	parentRow := append([]driver.Value(nil), f.parent.sql.parentRow...)
	f.parent.graph.at = f.parent.graph.at.Add(time.Microsecond)
	second, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication.Clone())
	if err != nil || !second.Replayed || second.Digest != first.Digest || !reflect.DeepEqual(first.Original, second.Original) || f.sql.writes != 1 || !reflect.DeepEqual(f.sql.row, originalRow) {
		t.Fatalf("exact replay changed original observation: err=%v replayed=%t writes=%d", err, second.Replayed, f.sql.writes)
	}
	observed, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true)
	if err != nil || observed.Digest != first.Digest {
		t.Fatalf("current complete canonical readback err=%v", err)
	}
	f.parent.graph.at = f.publication.PreparedInvocationExpiresAt
	if got, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, true); err == nil || got.Digest != "" {
		t.Fatalf("expired active allocation yielded current observation: err=%v digest=%s", err, got.Digest)
	}
	history, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
	if err != nil || history.Digest != first.Digest {
		t.Fatalf("expired historical exact snapshot should remain readable: %v", err)
	}
	retired, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, first.Digest, domain.RegisteredPreparationRetiredV1)
	if err != nil || retired.Replayed || retired.Current.State != domain.RegisteredPreparationRetiredV1 || !reflect.DeepEqual(first.Original, retired.Original) {
		t.Fatalf("monotonic historical retirement err=%v receipt=%+v", err, retired)
	}
	recovered, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, first.Digest, domain.RegisteredPreparationRetiredV1)
	if err != nil || !recovered.Replayed || recovered.Digest != retired.Digest || f.sql.writes != 2 {
		t.Fatalf("exact retirement acknowledgement recovery err=%v writes=%d", err, f.sql.writes)
	}
	revoked, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, retired.Digest, domain.RegisteredPreparationRevokedV1)
	if err != nil || revoked.Current.RevocationGeneration != 3 {
		t.Fatalf("retired→revoked exact CAS err=%v receipt=%+v", err, revoked)
	}
	if _, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, first.Digest, domain.RegisteredPreparationRetiredV1); err == nil {
		t.Fatal("superseded retirement replay resurrected or rewrote revoked head")
	}
	recovered, err = f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, retired.Digest, domain.RegisteredPreparationRevokedV1)
	if err != nil || !recovered.Replayed || recovered.Digest != revoked.Digest || f.sql.writes != 3 {
		t.Fatalf("latest revoke acknowledgement recovery err=%v writes=%d", err, f.sql.writes)
	}
	if !reflect.DeepEqual(f.parent.sql.parentRow, parentRow) {
		t.Fatal("registration lifecycle changed immutable sole-parent bytes")
	}
}

func TestOwnedAllocationRegistrationLostAckAndKnownRollback(t *testing.T) {
	for _, test := range []struct {
		name     string
		modes    []string
		writes   int
		replayed bool
	}{{"lost_ack", []string{"ambiguous", ""}, 1, true}, {"known_rollback", []string{"rollback", ""}, 2, false}} {
		t.Run(test.name, func(t *testing.T) {
			f := newOwnedAllocationFixture(t)
			burns := f.parent.sql.burnUpdates
			f.parent.graph.commitModes = append([]string(nil), test.modes...)
			got, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
			if err != nil || got.Replayed != test.replayed || f.sql.writes != test.writes || f.parent.sql.burnUpdates != burns || got.Digest == "" {
				t.Fatalf("registration retry must recover same prepared observation: err=%v replayed=%t writes=%d want=%d burns=%d/%d", err, got.Replayed, f.sql.writes, test.writes, f.parent.sql.burnUpdates, burns)
			}
		})
	}
}

func TestOwnedAllocationRegistrationTerminalUnknownCommitReturnsZeroThenHistory(t *testing.T) {
	f := newOwnedAllocationFixture(t)
	f.parent.graph.commitModes = []string{"ambiguous", "ambiguous", "ambiguous"}
	got, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
	if err == nil || !reflect.DeepEqual(got, ports.OwnedAllocationRegistrationReceiptV1{}) || f.sql.writes != 1 {
		t.Fatalf("terminal ambiguous result must be zero: err=%v writes=%d receipt=%+v", err, f.sql.writes, got)
	}
	f.parent.graph.commitModes = nil
	got, err = f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
	if err != nil || got.Digest == "" || f.sql.writes != 1 {
		t.Fatalf("retained committed observation should reconcile without preparation: err=%v writes=%d", err, f.sql.writes)
	}
}

func TestOwnedAllocationRegistrationLifecycleLostAckRecoveryPreservesOriginal(t *testing.T) {
	for _, state := range []domain.RegisteredPreparationStateV1{domain.RegisteredPreparationRetiredV1, domain.RegisteredPreparationRevokedV1} {
		t.Run(string(state), func(t *testing.T) {
			f := newOwnedAllocationFixture(t)
			first, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
			if err != nil {
				t.Fatal(err)
			}
			f.parent.graph.at = f.parent.graph.at.Add(time.Microsecond)
			f.parent.graph.commitModes = []string{"ambiguous", ""}
			result, err := f.parent.store.retireBoundedOwnedAllocation(t.Context(), f.parent.request, first.Digest, state)
			if err != nil || !result.Replayed || result.Current.State != state || f.sql.writes != 2 || !reflect.DeepEqual(result.Original, first.Original) {
				t.Fatalf("lost lifecycle ACK must recover exact monotonic transition: err=%v state=%s replay=%t writes=%d", err, result.Current.State, result.Replayed, f.sql.writes)
			}
		})
	}
}

func TestOwnedAllocationRegistrationHistoricalOriginalCannotPrecedeBurn(t *testing.T) {
	f := newOwnedAllocationFixture(t)
	if _, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication); err != nil {
		t.Fatal(err)
	}
	// This remains a structurally valid later burn in the original setup window,
	// but cannot authenticate a preparation already observed a microsecond earlier.
	f.parent.graph.at = f.parent.graph.at.Add(time.Microsecond)
	f.parent.sql.burnRow[2] = f.parent.graph.at
	got, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
	if err == nil || got.Digest != "" || f.sql.writes != 1 {
		t.Fatalf("foreign later burn cannot authenticate earlier retained preparation: err=%v writes=%d", err, f.sql.writes)
	}
}

func TestOwnedAllocationRegistrationOrphanChildDeniesFreshPreparation(t *testing.T) {
	f := newOwnedAllocationFixture(t)
	// A partial child under an unburned parent is an unknown/corrupt prior
	// preparation, never permission to burn and prepare another physical instance.
	f.sql.row = []driver.Value{uint64(0), uint64(1), uint64(0), uint64(0), uint64(0), uint64(0), "", f.publication.PreparedInvocationDigest, "", "", "", ""}
	f.parent.sql.burnRow = []driver.Value{uint64(0), uint64(0), time.Unix(0, 0).UTC(), ""}
	grant, err := f.parent.issuer.issuer.MintAttemptEffectOwnershipGrant(f.publication.Authority, f.publication.Reservation, f.publication.PreparedInvocationExpiresAt)
	if err != nil {
		t.Fatalf("authentic remint prerequisite: %v", err)
	}
	before := append([]driver.Value(nil), f.sql.row...)
	burns := f.parent.sql.burnUpdates
	fresh, err := f.parent.store.beginBoundedOwnedAttemptPreparation(t.Context(), grant)
	if err == nil || fresh || f.parent.sql.burnUpdates != burns || f.sql.writes != 0 || !reflect.DeepEqual(before, f.sql.row) {
		t.Fatalf("orphan child cannot authorize or repair a fresh burn: err=%v fresh=%t burns=%d/%d", err, fresh, f.parent.sql.burnUpdates, burns)
	}
	reservation, err := f.parent.reserveResult(t, f.parent.request)
	if err == nil || reservation.Grant != nil {
		t.Fatalf("orphan child cannot mint another original owner grant: err=%v status=%s", err, reservation.Status)
	}
}

func TestOwnedAllocationRegistrationProofAndCurrentAuthorityDenials(t *testing.T) {
	for _, name := range []string{"nil_verifier", "forged_proof", "changed_claim", "changed_id", "expired_first", "missing_burn", "partial_burn", "revoked_profile", "removed_principal"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAllocationFixture(t)
			p := f.publication.Clone()
			before := len(f.parent.graph.statements)
			switch name {
			case "nil_verifier":
				f.parent.store.ownedAllocationPublicationVerifier = nil
			case "forged_proof":
				p.Authenticator[0] ^= 0xff
			case "changed_claim":
				p.Reservation.PhysicalInvocationClaimID += "-other"
			case "changed_id":
				p.AllocationID += "-other"
			case "expired_first":
				f.parent.graph.at = p.PreparedInvocationExpiresAt
			case "missing_burn":
				f.parent.sql.burnRow = []driver.Value{uint64(0), uint64(0), time.Unix(0, 0).UTC(), ""}
			case "partial_burn":
				f.parent.sql.burnRow[1] = uint64(0)
			case "revoked_profile":
				f.parent.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
				f.parent.base.reader.profile.RevocationGeneration++
				f.parent.base.reader.profile.UpdatedAt = f.parent.graph.at
				f.parent.base.reader.sync(t)
			case "removed_principal":
				var participant domain.SessionParticipant
				if err := json.Unmarshal([]byte(f.parent.base.participant[0].(string)), &participant); err != nil {
					t.Fatal(err)
				}
				participant.Status, participant.UpdatedAt = domain.SessionParticipantRemoved, f.parent.graph.at
				raw, err := json.Marshal(participant)
				if err != nil {
					t.Fatal(err)
				}
				f.parent.base.participant[0], f.parent.base.participant[2] = string(raw), string(participant.Status)
			}
			got, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), p)
			if err == nil || got.Digest != "" || f.sql.writes != 0 || f.sql.row != nil {
				t.Fatalf("%s must deny without registration: err=%v writes=%d receipt=%+v", name, err, f.sql.writes, got)
			}
			if (name == "nil_verifier" || name == "forged_proof" || name == "changed_id") && len(f.parent.graph.statements) != before {
				t.Fatalf("%s must deny before SQL", name)
			}
		})
	}
}

func TestOwnedAllocationRegistrationRejectsMalformedAndForeignRows(t *testing.T) {
	for _, name := range []string{"partial", "oversized", "digest", "intent", "foreign_valid_intent", "original_replacement", "resurrected", "orphan_transition"} {
		t.Run(name, func(t *testing.T) {
			f := newOwnedAllocationFixture(t)
			first, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "partial":
				f.sql.row[1] = uint64(0)
			case "oversized":
				f.sql.row[6] = string(make([]byte, ownedAuthorityHeadBytes+1))
			case "digest":
				f.sql.row[7] = "bad"
			case "intent":
				f.sql.row[10] = "bad"
			case "foreign_valid_intent":
				f.sql.row[10] = strings.Repeat("a", 64)
			case "original_replacement":
				v := first.Original.Clone()
				v.Scope.OwnerUserID = "foreign-owner"
				v.Profile.Locator.OwnerUserID = "foreign-owner"
				raw, e := domain.EncodeRegisteredAllocationV1(v)
				if e != nil {
					t.Fatalf("foreign owner fixture must remain structurally valid, not merely malformed: %v", e)
				}
				f.sql.row[6] = string(raw)
				d, _ := v.Digest()
				f.sql.row[7] = string(d)
			case "resurrected":
				v := first.Current.Clone()
				v.RevocationGeneration = 2
				raw, e := domain.EncodeRegisteredAllocationV1(v)
				if e == nil {
					f.sql.row[8] = string(raw)
					d, _ := v.Digest()
					f.sql.row[9] = string(d)
				} else {
					f.sql.row[8] = "resurrected"
				}
			case "orphan_transition":
				f.sql.row[5] = uint64(1)
				f.sql.row[11] = first.Digest
			}
			before := append([]driver.Value(nil), f.sql.row...)
			got, err := f.parent.store.observeBoundedOwnedAllocation(t.Context(), f.parent.request, false)
			if err == nil || got.Digest != "" || !reflect.DeepEqual(before, f.sql.row) || f.sql.writes != 1 {
				t.Fatalf("malformed/foreign %s was accepted or repaired: err=%v writes=%d", name, err, f.sql.writes)
			}
		})
	}
}
