package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// In-process scripted SQL only: no service, credential or effect authority.
func legacyReaderLocalCapacity(t *testing.T, tenant domain.TenantID) (domain.LocalCapacityAssociationV1, capacityStorageV1) {
	t.Helper()
	local := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: tenant, OwnerUserID: "owner-local-readers", ResourceID: "resource-local-readers"}
	stored, err := encodeCapacityStorageV1(tenant, "", &local)
	if err != nil {
		t.Fatal(err)
	}
	return local, stored
}

func TestLocalCapacityAttachedChoiceRejectsNonSubscriptionSlots(t *testing.T) {
	for _, fault := range []string{"local-collision", "association-only", "digest-only", "noncanonical", "foreign-key", "oversize-association"} {
		t.Run(fault, func(t *testing.T) {
			f := newComputeChoiceAttachedFixture(t)
			_, local := legacyReaderLocalCapacity(t, f.pin.tenantID)
			row := f.source.driver.rows[computeChoiceAttachedSlotSQL]
			switch fault {
			case "local-collision":
				row[6], row[7] = local.association.String, local.digest.String
			case "association-only":
				row[6] = local.association.String
			case "digest-only":
				row[7] = local.digest.String
			case "noncanonical":
				row[5], row[6], row[7] = local.key, " "+local.association.String, local.digest.String
			case "foreign-key":
				row[5] = "subscription-foreign"
			case "oversize-association":
				row[6], row[7] = strings.Repeat("x", domain.OwnedAgentCurrentMaxRecordBytesV1+1), local.digest.String
			}
			if fault == "oversize-association" {
				budget := computechoice.NewResourceBudget()
				got, err := f.read(budget)
				if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || !reflect.DeepEqual(got, computeChoiceAttachedSnapshot{}) || f.source.driver.writes != 0 {
					t.Fatalf("oversize capacity published facts: got=%+v err=%v", got, err)
				}
			} else {
				assertComputeChoiceAttachedCorrupt(t, f)
			}
		})
	}
}

type legacyCapacitySQLFixture struct {
	row     []driver.Value
	queries []string
	writes  int
	args    []driver.NamedValue
}

func (f *legacyCapacitySQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*legacyCapacitySQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*legacyCapacitySQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (*legacyCapacitySQLFixture) Close() error { return nil }
func (*legacyCapacitySQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("BeginTx required")
}
func (*legacyCapacitySQLFixture) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return legacyCapacitySQLTx{}, nil
}
func (f *legacyCapacitySQLFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.queries = append(f.queries, statement)
	if !strings.Contains(statement, "local_capacity_association") || !strings.Contains(statement, "local_capacity_digest") || !strings.Contains(statement, "WHERE tenant_id = $1 AND subscription_connection_id = $2") || len(args) != 2 || args[0].Value != "tenant-local-readers" || args[1].Value != "subscription-original" {
		return nil, errors.New("unscoped legacy slot read")
	}
	return &explanationFixtureRows{values: f.row}, nil
}
func (f *legacyCapacitySQLFixture) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !strings.Contains(statement, "UPDATE subscription_scheduler_slots") || len(args) != 5 || args[3].Value != "tenant-local-readers" || args[4].Value != "subscription-original" {
		return nil, errors.New("unexpected legacy slot mutation")
	}
	f.writes++
	f.args = append([]driver.NamedValue(nil), args...)
	return driver.RowsAffected(1), nil
}

type legacyCapacitySQLTx struct{}

func (legacyCapacitySQLTx) Commit() error   { return nil }
func (legacyCapacitySQLTx) Rollback() error { return nil }

func legacyCapacityStateTx(t *testing.T, f *legacyCapacitySQLFixture) *stateTx {
	t.Helper()
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return &stateTx{sqlTx: tx, tenantID: "tenant-local-readers"}
}

func TestLocalCapacityLegacyOperationsKeepExactSubscriptionArm(t *testing.T) {
	at := time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)
	f := &legacyCapacitySQLFixture{row: []driver.Value{"subscription-original", string(domain.SchedulerReady), nil, nil}}
	tx := legacyCapacityStateTx(t, f)
	state, err := readLegacySchedulerSlotStateTx(t.Context(), tx, "subscription-original")
	if err != nil || state != string(domain.SchedulerReady) || f.writes != 0 {
		t.Fatalf("legacy state=%q writes=%d err=%v", state, f.writes, err)
	}
	if err := setSchedulerSlotState(t.Context(), tx, "subscription-original", domain.SchedulerReauthRequired, nil, at); err != nil {
		t.Fatal(err)
	}
	if len(f.queries) != 2 || f.writes != 1 || f.args[0].Value != string(domain.SchedulerReauthRequired) || f.args[1].Value != time.Unix(0, 0).UTC() || f.args[2].Value != at {
		t.Fatal("legacy update changed identity, state or timestamps")
	}
}

func TestLocalCapacityLegacyOperationsKeepMissingSlotNoOp(t *testing.T) {
	f := &legacyCapacitySQLFixture{}
	tx := legacyCapacityStateTx(t, f)
	if err := setSchedulerSlotState(t.Context(), tx, "subscription-original", domain.SchedulerReauthRequired, nil, time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)); err != nil || f.writes != 0 || len(f.queries) != 1 {
		t.Fatalf("missing legacy slot no-op: reads=%d writes=%d err=%v", len(f.queries), f.writes, err)
	}
}

func TestLocalCapacityLegacyOperationsDenyCollisionBeforeWrite(t *testing.T) {
	_, local := legacyReaderLocalCapacity(t, "tenant-local-readers")
	for _, fault := range []string{"local-collision", "association-only", "digest-only", "foreign-key", "both-null-local-key", "noncanonical", "foreign-owner", "oversize"} {
		t.Run(fault, func(t *testing.T) {
			stored := local
			switch fault {
			case "local-collision":
				stored.key = "subscription-original"
			case "association-only":
				stored.key, stored.digest = "subscription-original", sql.NullString{}
			case "digest-only":
				stored.key, stored.association = "subscription-original", sql.NullString{}
			case "foreign-key":
				stored = capacityStorageV1{key: "subscription-foreign"}
			case "both-null-local-key":
				stored.association, stored.digest = sql.NullString{}, sql.NullString{}
			case "noncanonical":
				stored.association.String = " " + stored.association.String
			case "foreign-owner":
				stored.association.String = strings.ReplaceAll(stored.association.String, "owner-local-readers", "owner-foreign")
			case "oversize":
				stored.association.String = strings.Repeat("x", domain.OwnedAgentCurrentMaxRecordBytesV1+1)
			}
			f := &legacyCapacitySQLFixture{row: []driver.Value{stored.key, string(domain.SchedulerReady), nil, nil}}
			if stored.association.Valid {
				f.row[2] = stored.association.String
			}
			if stored.digest.Valid {
				f.row[3] = stored.digest.String
			}
			tx := legacyCapacityStateTx(t, f)
			if state, err := readLegacySchedulerSlotStateTx(t.Context(), tx, "subscription-original"); !errors.Is(err, ErrSubscriptionConnectionConflict) || state != "" {
				t.Fatalf("collision read: fault=%s state=%q err=%v", fault, state, err)
			}
			if err := setSchedulerSlotState(t.Context(), tx, "subscription-original", domain.SchedulerReauthRequired, nil, time.Date(2026, 10, 29, 18, 0, 0, 0, time.UTC)); !errors.Is(err, ErrSubscriptionConnectionConflict) || f.writes != 0 {
				t.Fatalf("collision update: fault=%s writes=%d err=%v", fault, f.writes, err)
			}
		})
	}
}

func TestLocalCapacityAttachedChoiceKeepsLegacyPositive(t *testing.T) {
	f := newComputeChoiceAttachedFixture(t)
	got, err := f.read(computechoice.NewResourceBudget())
	if err != nil || got.slot == nil || got.slot.SubscriptionConnectionID != f.pin.subscriptionID || got.slot.LocalCapacityAssociation != nil {
		t.Fatalf("legacy attached source changed: got=%+v err=%v", got.slot, err)
	}
}

func TestLocalCapacityRepairPreservesExactProjection(t *testing.T) {
	for _, arm := range []string{"subscription", "local"} {
		for _, existing := range []bool{false, true} {
			t.Run(arm+map[bool]string{false: "/missing", true: "/existing"}[existing], func(t *testing.T) {
				store, f, sources, ctx := newRepairFixture(t, "quota", "apply")
				if arm == "local" {
					local, _ := legacyReaderLocalCapacity(t, sources.run.TenantID)
					sources.run.SubscriptionConnectionID, sources.quota.SubscriptionConnectionID = "", ""
					sources.run.LocalCapacityAssociation, sources.quota.LocalCapacityAssociation = &local, &local
				}
				f.seed(t, sources)
				stored, err := encodeCapacityStorageV1(sources.run.TenantID, sources.run.SubscriptionConnectionID, sources.run.LocalCapacityAssociation)
				if err != nil {
					t.Fatal(err)
				}
				var association, digest driver.Value
				if stored.association.Valid {
					association, digest = stored.association.String, stored.digest.String
				}
				if existing {
					f.rows["quota_expiry_v2"] = []driver.Value{string(sources.run.ID), stored.key, association, digest}
				}
				out, err := store.RepairReadyExpiryV2(ctx, f.request)
				if err != nil || len(out) == 0 || len(f.arguments) != 1 || len(f.arguments[0]) != 8 {
					t.Fatalf("exact repair: arm=%s existing=%t writes=%d err=%v", arm, existing, len(f.arguments), err)
				}
				args := f.arguments[0]
				if args[5].Value != stored.key || args[6].Value != association || args[7].Value != digest || args[1].Value != sources.quota.ExpiresAt {
					t.Fatal("repair erased/reinterpreted capacity ownership or renewed expiry")
				}
				if arm == "local" && (f.rows["runs"][4] != "" || f.rows["quota_reservations"][4] != "") {
					t.Fatal("local physical address entered a domain subscription scalar")
				}
				if len(f.queries) != 4 {
					t.Fatalf("repair graph: reads=%d want=4", len(f.queries))
				}
			})
		}
	}
}

func TestLocalCapacityRepairDeniesDivergentOwnership(t *testing.T) {
	for _, fault := range []string{"run-scalar", "quota-scalar", "quota-owner", "missing-local", "expiry-run", "expiry-legacy-collision", "legacy-parent-local-expiry", "expiry-partial-null", "expiry-digest", "expiry-owner", "expiry-noncanonical"} {
		t.Run(fault, func(t *testing.T) {
			store, f, sources, ctx := newRepairFixture(t, "quota", "apply")
			local, stored := legacyReaderLocalCapacity(t, sources.run.TenantID)
			if fault != "legacy-parent-local-expiry" {
				sources.run.SubscriptionConnectionID, sources.quota.SubscriptionConnectionID = "", ""
				sources.run.LocalCapacityAssociation, sources.quota.LocalCapacityAssociation = &local, &local
			}
			if fault == "quota-owner" {
				other := local
				other.OwnerUserID = "owner-foreign"
				sources.quota.LocalCapacityAssociation = &other
			}
			if fault == "missing-local" {
				sources.run.LocalCapacityAssociation, sources.quota.LocalCapacityAssociation = nil, nil
			}
			f.seed(t, sources)
			f.rows["quota_expiry_v2"] = []driver.Value{string(sources.run.ID), stored.key, stored.association.String, stored.digest.String}
			switch fault {
			case "run-scalar":
				f.rows["runs"][4] = "subscription-foreign"
			case "quota-scalar":
				f.rows["quota_reservations"][4] = "subscription-foreign"
			case "expiry-run":
				f.rows["quota_expiry_v2"][0] = "run-foreign"
			case "expiry-legacy-collision":
				f.rows["quota_expiry_v2"][1], f.rows["quota_expiry_v2"][2], f.rows["quota_expiry_v2"][3] = "subscription-original", nil, nil
			case "expiry-partial-null":
				f.rows["quota_expiry_v2"][3] = nil
			case "expiry-digest":
				f.rows["quota_expiry_v2"][3] = strings.Repeat("0", 64)
			case "expiry-owner":
				other := local
				other.OwnerUserID = "owner-foreign"
				otherStored, err := encodeCapacityStorageV1(other.TenantID, "", &other)
				if err != nil {
					t.Fatal(err)
				}
				f.rows["quota_expiry_v2"][1], f.rows["quota_expiry_v2"][2], f.rows["quota_expiry_v2"][3] = otherStored.key, otherStored.association.String, otherStored.digest.String
			case "expiry-noncanonical":
				f.rows["quota_expiry_v2"][2] = " " + stored.association.String
			}
			requireRepairDenied(t, store, f, ctx)
		})
	}
}

func TestLocalCapacityRepairKeepsOriginalSmallSourceBudget(t *testing.T) {
	store, f, sources, ctx := newRepairFixture(t, "quota", "apply")
	local, stored := legacyReaderLocalCapacity(t, sources.run.TenantID)
	sources.run.SubscriptionConnectionID, sources.quota.SubscriptionConnectionID = "", ""
	sources.run.LocalCapacityAssociation, sources.quota.LocalCapacityAssociation = &local, &local
	f.seed(t, sources)
	f.rows["quota_expiry_v2"] = []driver.Value{string(sources.run.ID), stored.key, stored.association.String, stored.digest.String}
	f.request.MaxSourceBytes = max(len(f.rows["runs"][0].(string)), len(f.rows["quota_reservations"][0].(string)))
	if out, err := store.RepairReadyExpiryV2(ctx, f.request); err != nil || len(out) == 0 || len(f.writes) != 1 || len(f.queries) != 4 {
		t.Fatalf("small original source cap: cap=%d reads=%d writes=%d err=%v", f.request.MaxSourceBytes, len(f.queries), len(f.writes), err)
	}
}
