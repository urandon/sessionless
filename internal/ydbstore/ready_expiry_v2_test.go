package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// Drives the real stateTx writers using one explicit source snapshot. This is
// SQL/argument/branch evidence, not native YDB isolation or rollout evidence.
type readyExpirySQLFixture struct {
	tenant             domain.TenantID
	rows               map[string][]driver.Value
	writes             []string
	args               [][]driver.NamedValue
	failTable          string
	commits, rollbacks int
}

func (f *readyExpirySQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*readyExpirySQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*readyExpirySQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (*readyExpirySQLFixture) Close() error { return nil }
func (*readyExpirySQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit transaction required")
}
func (f *readyExpirySQLFixture) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("serializable required")
	}
	return readyExpiryFixtureTx{f}, nil
}

type readyExpiryFixtureTx struct{ f *readyExpirySQLFixture }

func (tx readyExpiryFixtureTx) Commit() error   { tx.f.commits++; return nil }
func (tx readyExpiryFixtureTx) Rollback() error { tx.f.rollbacks++; return nil }
func (f *readyExpirySQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(args) < 2 || args[0].Value != string(f.tenant) {
		return nil, errors.New("fixture requires exact tenant point read")
	}
	fields := strings.Fields(query)
	for i, field := range fields {
		if field != "FROM" || i+1 >= len(fields) {
			continue
		}
		table := fields[i+1]
		switch table {
		case "runs", "attempts", "leases", "quota_reservations", "telegram_delivery_outbox", "session_deletions", "run_explanation_heads_v1":
			return &explanationFixtureRows{values: f.rows[table]}, nil
		}
	}
	return nil, fmt.Errorf("unexpected fixture read: %s", query)
}
func (f *readyExpirySQLFixture) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.writes = append(f.writes, strings.Join(strings.Fields(query), " "))
	f.args = append(f.args, append([]driver.NamedValue(nil), args...))
	fields := strings.Fields(query)
	if len(fields) < 3 {
		return nil, errors.New("invalid fixture write")
	}
	if fields[2] == f.failTable {
		return nil, errors.New("fixture projection write failed")
	}
	return driver.RowsAffected(1), nil
}
func readyExpiryFixture(t *testing.T) (*Store, *readyExpirySQLFixture, ports.WorkerJobState, domain.DispatchOutbox, context.Context) {
	t.Helper()
	_, graph, request := managedGraphFixture(t)
	var run domain.Run
	var attempt domain.Attempt
	var outbox domain.DispatchOutbox
	for _, value := range []struct {
		table  string
		target any
	}{{"runs", &run}, {"attempts", &attempt}, {"dispatch_outbox", &outbox}} {
		if err := json.Unmarshal([]byte(graph.rows[value.table][0].(string)), value.target); err != nil {
			t.Fatal(err)
		}
	}
	f := &readyExpirySQLFixture{tenant: request.TenantID, rows: map[string][]driver.Value{}}
	readyExpiryPut(t, f, "runs", run)
	readyExpiryPut(t, f, "attempts", attempt)
	head, _, err := runexplanation.Select(nil, run, attempt, outbox.ExecutionPlacementV2, graph.at)
	if err != nil {
		t.Fatal(err)
	}
	readyExpiryPut(t, f, "run_explanation_heads_v1", head)
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return &Store{db: db, operationalRetention: time.Hour}, f, ports.WorkerJobState{Run: run, Attempt: attempt}, outbox, ctx
}
func readyExpiryPut(t *testing.T, f *readyExpirySQLFixture, table string, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	f.rows[table] = []driver.Value{string(b)}
}
func readyExpiryWriteTables(f *readyExpirySQLFixture) []string {
	result := make([]string, len(f.writes))
	for i, query := range f.writes {
		fields := strings.Fields(query)
		result[i] = fields[0] + " " + fields[2]
	}
	return result
}
func readyExpiryAssert(t *testing.T, f *readyExpirySQLFixture, want []string) {
	t.Helper()
	if got := readyExpiryWriteTables(f); !reflect.DeepEqual(got, want) {
		t.Fatalf("writes=%v, want %v", got, want)
	}
	readyExpiryNoLegacy(t, f.writes)
}

func readyExpiryNoLegacy(t *testing.T, queries []string) {
	t.Helper()
	for _, query := range queries {
		for _, field := range strings.Fields(query) {
			switch field {
			case "lease_expiry", "dispatch_ready", "quota_expiry", "telegram_delivery_ready":
				t.Errorf("legacy write survived: %s", query)
			}
		}
	}
}

// Pure count-policy control for BOTH native positive loops; this does not
// enable their opt-in gate or open a database. Keep expected tables explicit
// so adding/removing a table cannot silently inherit a positive count.
func TestReadyExpiryCommittedNativeEffectCounts(t *testing.T) {
	want := map[string]uint64{
		"worker_jobs": 1, "quota_reservations": 1,
		"quota_expiry": 0, "quota_expiry_v2": 1,
		"subscription_scheduler_slots": 1, "tenant_scheduler_counters": 1,
		"run_explanation_heads_v1": 1, "audit_events": 1,
	}
	if len(nativeManagedAdmissionEffectTables) != len(want) {
		t.Fatalf("effect table count=%d, want %d", len(nativeManagedAdmissionEffectTables), len(want))
	}
	seen := make(map[string]bool, len(want))
	for _, table := range nativeManagedAdmissionEffectTables {
		expected, known := want[table]
		if !known || seen[table] {
			t.Fatalf("unknown or duplicate effect table %q", table)
		}
		seen[table] = true
		if got := nativeManagedAdmissionCommittedCount(table); got != expected {
			t.Errorf("committed/replay table=%s count=%d, want %d", table, got, expected)
		}
	}
}

func TestReadyExpiryV2OnlyLeaseNewReplacementAndInvalid(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement=%t", replacement), func(t *testing.T) {
			store, f, s, _, ctx := readyExpiryFixture(t)
			lease := domain.Lease{ID: "lease-v2", TenantID: s.Run.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, WorkerID: "worker-v2", FenceToken: 2, AcquiredAt: s.Run.CreatedAt, ExpiresAt: s.Run.CreatedAt.Add(time.Hour)}
			if replacement {
				previous := lease
				previous.ExpiresAt = previous.ExpiresAt.Add(-time.Minute)
				readyExpiryPut(t, f, "leases", previous)
			}
			err := store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error { return state.PutLease(ctx, lease) })
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"UPSERT leases", "UPSERT lease_expiry_v2"}
			if replacement {
				want = append([]string{"DELETE lease_expiry_v2"}, want...)
			}
			readyExpiryAssert(t, f, want)
			last := f.args[len(f.args)-1]
			bucket, _ := ydbpartition.BucketV1(string(lease.RunID))
			if last[0].Value != int64(bucket) || last[1].Value != lease.ExpiresAt || last[2].Value != string(lease.TenantID) || last[3].Value != string(lease.RunID) || last[4].Value != string(lease.ID) || last[5].Value != int64(lease.FenceToken) {
				t.Fatalf("wrong retained lease tuple: %v", last)
			}
			f.writes = nil
			f.args = nil
			lease.TenantID = "foreign"
			if err := store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error { return state.PutLease(ctx, lease) }); err == nil || len(f.writes) != 0 {
				t.Fatalf("foreign lease err=%v writes=%v", err, f.writes)
			}
		})
	}
}
func TestReadyExpiryV2OnlyQuotaBranchesAndWriteFailure(t *testing.T) {
	for _, status := range []domain.ReservationStatus{domain.ReservationHeld, domain.ReservationCommitted, domain.ReservationReleased, domain.ReservationExpired} {
		for _, replacement := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replacement=%t", status, replacement), func(t *testing.T) {
				store, f, s, _, ctx := readyExpiryFixture(t)
				r := domain.QuotaReservation{ID: "reservation-v2", TenantID: s.Run.TenantID, RunID: s.Run.ID, SubscriptionConnectionID: s.Run.SubscriptionConnectionID, Status: status, CapacityUnits: 1, HeldAt: s.Run.CreatedAt, ExpiresAt: s.Run.CreatedAt.Add(time.Hour), UpdatedAt: s.Run.CreatedAt}
				if replacement {
					previous := r
					previous.Status = domain.ReservationHeld
					previous.ExpiresAt = previous.ExpiresAt.Add(-time.Minute)
					readyExpiryPut(t, f, "quota_reservations", previous)
				}
				if err := store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error { return state.PutQuotaReservation(ctx, r) }); err != nil {
					t.Fatal(err)
				}
				want := []string{"UPSERT quota_reservations"}
				if replacement {
					want = append([]string{"DELETE quota_expiry_v2"}, want...)
				}
				if status == domain.ReservationHeld {
					want = append(want, "UPSERT quota_expiry_v2")
				}
				readyExpiryAssert(t, f, want)
			})
		}
	}
	store, f, s, _, ctx := readyExpiryFixture(t)
	r := domain.QuotaReservation{ID: "reservation-v2", TenantID: s.Run.TenantID, RunID: s.Run.ID, SubscriptionConnectionID: s.Run.SubscriptionConnectionID, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: s.Run.CreatedAt, ExpiresAt: s.Run.CreatedAt.Add(time.Hour), UpdatedAt: s.Run.CreatedAt}
	f.failTable = "quota_expiry_v2"
	if err := store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error { return state.PutQuotaReservation(ctx, r) }); err == nil || f.commits != 0 || f.rollbacks != 1 {
		t.Fatalf("write failure err=%v commit=%d rollback=%d", err, f.commits, f.rollbacks)
	}
}
func TestReadyExpiryV2OnlyDispatchPendingAndTerminal(t *testing.T) {
	for _, status := range []domain.DispatchStatus{domain.DispatchPending, domain.DispatchPublished, domain.DispatchCancelled} {
		t.Run(string(status), func(t *testing.T) {
			store, f, s, outbox, ctx := readyExpiryFixture(t)
			outbox.Status = status
			if err := store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error { return state.PutDispatchOutbox(ctx, outbox) }); err != nil {
				t.Fatal(err)
			}
			want := []string{"DELETE dispatch_ready_v2", "UPSERT dispatch_outbox"}
			if status == domain.DispatchPending {
				want = append(want, "UPSERT dispatch_ready_v2")
			}
			readyExpiryAssert(t, f, want)
		})
	}
}
func TestReadyExpiryV2OnlyDeliveryBranchesKeepReverseInventory(t *testing.T) {
	for _, status := range []domain.DeliveryStatus{domain.DeliveryPending, domain.DeliveryRetryWait, domain.DeliverySending, domain.DeliverySent, domain.DeliveryFailed, domain.DeliveryCancelled} {
		for _, replacement := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replacement=%t", status, replacement), func(t *testing.T) {
				store, f, s, _, ctx := readyExpiryFixture(t)
				at := s.Run.CreatedAt
				d := domain.TelegramDeliveryOutbox{ID: "delivery-v2", TenantID: s.Run.TenantID, RunID: s.Run.ID, Chat: domain.TelegramChatRef{TenantID: s.Run.TenantID, ChatID: 123}, ReplyToMessageID: 1, Text: "retained useful answer", Status: status, IdempotencyKey: "delivery-key", CreatedAt: at, UpdatedAt: at}
				if status == domain.DeliveryRetryWait {
					due := at.Add(time.Minute)
					d.NextAttemptAt = &due
				}
				if replacement {
					previous := d
					previous.Status = domain.DeliveryPending
					previous.NextAttemptAt = nil
					previous.UpdatedAt = at.Add(-time.Second)
					previous.CreatedAt = previous.UpdatedAt
					readyExpiryPut(t, f, "telegram_delivery_outbox", previous)
				}
				if err := store.Transact(ctx, s.Run.TenantID, func(state ports.StateTx) error { return state.PutTelegramDeliveryOutbox(ctx, d) }); err != nil {
					t.Fatal(err)
				}
				want := []string{"UPSERT telegram_delivery_outbox", "UPSERT telegram_deliveries_by_run"}
				if replacement {
					want = append([]string{"DELETE telegram_delivery_ready_v2"}, want...)
				}
				if status == domain.DeliveryPending || status == domain.DeliveryRetryWait || status == domain.DeliverySending {
					want = append(want, "UPSERT telegram_delivery_ready_v2")
				}
				readyExpiryAssert(t, f, want)
				if want[len(want)-1] == "UPSERT telegram_delivery_ready_v2" && f.args[len(f.args)-1][1].Value != telegramDeliveryAvailableAt(d) {
					t.Fatal("delivery due time changed")
				}
			})
		}
	}
}

// Complements actual writer tests and retained integration controls: catch a
// legacy SQL dependency in less directly injectable cleanup/claim paths too.
func TestReadyExpiryLegacySQLAbsentFromSelectedSource(t *testing.T) {
	for _, path := range []string{"store.go", "operations.go", "worker.go", "attached_worker_execution_helpers.go", "session_lifecycle.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if fn, ok := node.(*ast.FuncDecl); ok && fn.Name.Name == "ListExpiredLeases" {
				t.Errorf("%s retains obsolete tenant reader", path)
			}
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range strings.Fields(value) {
				switch field {
				case "lease_expiry", "quota_expiry", "dispatch_ready", "telegram_delivery_ready":
					t.Errorf("%s retains legacy SQL %q", path, value)
				}
			}
			return true
		})
	}
}
