package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Test-owned connector exercises the actual Store.Transact and all readers/DML.
// It is not native YDB/IAM/isolation or rollout evidence.
type repairSQLFixture struct {
	rows                       map[string][]driver.Value
	request                    ReadyExpiryRepairRequest
	queries, writes            []string
	arguments                  [][]driver.NamedValue
	pending, committed         [][]driver.NamedValue
	indices                    map[string][]driver.NamedValue
	modes                      []string
	begins, commits, rollbacks int
	afterCommit                func()
	onRead                     func(string)
	failRead, failWrite        bool
	deadline                   time.Time
}

func (f *repairSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*repairSQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*repairSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (*repairSQLFixture) CheckNamedValue(v *driver.NamedValue) error {
	if _, ok := v.Value.(uint64); ok {
		return nil
	}
	return driver.ErrSkip
}
func (*repairSQLFixture) Close() error              { return nil }
func (*repairSQLFixture) Begin() (driver.Tx, error) { return nil, errors.New("real BeginTx required") }
func (f *repairSQLFixture) check(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	d, ok := ctx.Deadline()
	if !ok || !d.Equal(f.deadline) {
		return errors.New("original deadline changed")
	}
	return nil
}
func (f *repairSQLFixture) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if e := f.check(ctx); e != nil {
		return nil, e
	}
	if o.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("serializable required")
	}
	f.begins++
	f.pending = nil
	return repairFixtureTx{f}, nil
}

type repairFixtureTx struct{ f *repairSQLFixture }

func (t repairFixtureTx) Commit() error {
	f := t.f
	f.commits++
	mode := ""
	if len(f.modes) > 0 {
		mode = f.modes[0]
		f.modes = f.modes[1:]
	}
	if mode != "rollback" {
		f.committed = append(f.committed, f.pending...)
		for _, a := range f.pending {
			key := fmt.Sprint(a[0].Value, a[1].Value, a[2].Value, a[3].Value)
			f.indices[key] = a
		}
	}
	f.pending = nil
	if f.afterCommit != nil {
		h := f.afterCommit
		f.afterCommit = nil
		h()
	}
	if mode == "rollback" || mode == "ambiguous" {
		return retry.RetryableError(errors.New("SECRET uncertain commit"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if mode == "unknown" {
		return errors.New("SECRET lost acknowledgement")
	}
	return nil
}
func (t repairFixtureTx) Rollback() error { t.f.rollbacks++; t.f.pending = nil; return nil }

var repairCaps = regexp.MustCompile(`SUBSTRING\((?:CAST\([^)]* AS String\)|[a-z_]+),0,([0-9]+)\)`)

func (f *repairSQLFixture) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if e := f.check(ctx); e != nil {
		return nil, e
	}
	f.queries = append(f.queries, q)
	if f.failRead {
		return nil, errors.New("SECRET read failure")
	}
	parts := strings.Fields(q)
	table := ""
	for i, x := range parts {
		if x == "FROM" {
			table = parts[i+1]
			break
		}
	}
	if table == "quota_expiry_v2" {
		if !strings.Contains(q, "WHERE shard_bucket=$1 AND expires_at=$2 AND tenant_id=$3 AND quota_reservation_id=$4") || len(args) != 4 || args[2].Value != string(f.request.TenantID) || args[3].Value != f.request.ID {
			return nil, errors.New("unscoped quota expiry read")
		}
		bucket, _ := ydbpartition.BucketV1(f.request.ID)
		if args[0].Value != int64(bucket) || args[1].Value != f.rows["quota_reservations"][7] {
			return nil, errors.New("wrong exact quota expiry key")
		}
	} else if !strings.Contains(q, " WHERE tenant_id=$1 AND ") || len(args) != 2 || args[0].Value != string(f.request.TenantID) {
		return nil, errors.New("unscoped read")
	}
	switch table {
	case "runs", "attempts", "leases", "lease_heads", "dispatch_outbox", "quota_reservations", "quota_expiry_v2", "telegram_delivery_outbox", "session_deletions":
	default:
		return nil, errors.New("unexpected table")
	}
	if table == "runs" || table == "lease_heads" {
		if args[1].Value != string(f.request.RunID) {
			return nil, errors.New("wrong exact run")
		}
	}
	if table == "session_deletions" {
		if len(repairCaps.FindAllStringSubmatch(q, -1)) != 0 || !strings.HasPrefix(q, "SELECT 1 ") {
			return nil, errors.New("unbounded deletion read")
		}
	} else if len(repairCaps.FindAllStringSubmatch(q, -1)) == 0 {
		return nil, errors.New("unbounded payload/scalars")
	}
	values := append([]driver.Value(nil), f.rows[table]...)
	caps := repairCaps.FindAllStringSubmatch(q, -1)
	ci := 0
	for i, v := range values {
		if s, ok := v.(string); ok && ci < len(caps) {
			cap, _ := strconv.Atoi(caps[ci][1])
			ci++
			if len(s) > cap {
				values[i] = s[:cap]
			}
		}
	}
	if f.onRead != nil {
		f.onRead(table)
	}
	return &explanationFixtureRows{values: values}, nil
}
func (f *repairSQLFixture) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if e := f.check(ctx); e != nil {
		return nil, e
	}
	f.writes = append(f.writes, q)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	expected := map[string]string{"lease": "lease_expiry_v2", "dispatch": "dispatch_ready_v2", "quota": "quota_expiry_v2", "telegram": "telegram_delivery_ready_v2"}[f.request.Kind]
	if !strings.HasPrefix(q, "UPSERT INTO "+expected+" ") || len(args) < 5 || args[2].Value != string(f.request.TenantID) {
		return nil, errors.New("parent/unrelated mutation")
	}
	if f.failWrite {
		return nil, errors.New("SECRET write failure")
	}
	f.pending = append(f.pending, append([]driver.NamedValue(nil), args...))
	return driver.RowsAffected(1), nil
}

type repairSources struct {
	run      domain.Run
	attempt  domain.Attempt
	lease    domain.Lease
	dispatch domain.DispatchOutbox
	quota    domain.QuotaReservation
	delivery domain.TelegramDeliveryOutbox
}

func repairRaw(t *testing.T, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}
func (f *repairSQLFixture) seed(t *testing.T, s repairSources) {
	t.Helper()
	f.rows["runs"] = []driver.Value{repairRaw(t, s.run), string(s.run.ID), string(s.run.TenantID), string(s.run.SessionID), string(s.run.SubscriptionConnectionID), string(s.run.Status), s.run.CreatedAt, s.run.UpdatedAt, string(s.run.TriggerEventID)}
	f.rows["attempts"] = []driver.Value{repairRaw(t, s.attempt), string(s.attempt.ID), string(s.attempt.TenantID), string(s.attempt.RunID), s.attempt.WorkerID, string(s.attempt.Status), int64(s.attempt.Number), s.attempt.CreatedAt, s.attempt.UpdatedAt}
	l := s.lease
	f.rows["lease_heads"] = []driver.Value{string(l.ID), string(l.TenantID), string(l.RunID), string(l.AttemptID), l.WorkerID, l.FenceToken, l.ExpiresAt}
	f.rows["leases"] = []driver.Value{repairRaw(t, l), string(l.ID), string(l.TenantID), string(l.RunID), string(l.AttemptID), l.WorkerID, l.FenceToken, l.AcquiredAt, l.ExpiresAt}
	d := s.dispatch
	f.rows["dispatch_outbox"] = []driver.Value{repairRaw(t, d), string(d.ID), string(d.TenantID), string(d.RunID), string(d.AttemptID), string(d.Status), d.CreatedAt, d.UpdatedAt}
	q := s.quota
	f.rows["quota_reservations"] = []driver.Value{repairRaw(t, q), string(q.ID), string(q.TenantID), string(q.RunID), string(q.SubscriptionConnectionID), string(q.Status), q.HeldAt, q.ExpiresAt, int64(q.CapacityUnits)}
	v := s.delivery
	next := time.Unix(0, 0).UTC()
	if v.NextAttemptAt != nil {
		next = *v.NextAttemptAt
	}
	f.rows["telegram_delivery_outbox"] = []driver.Value{repairRaw(t, v), string(v.ID), string(v.TenantID), string(v.RunID), string(v.Status), v.CreatedAt, v.UpdatedAt, next, v.Chat.ChatID}
}
func newRepairFixture(t *testing.T, kind, mode string) (*Store, *repairSQLFixture, repairSources, context.Context) {
	t.Helper()
	_, graph, _ := managedGraphFixture(t)
	s := repairSources{}
	for _, v := range []struct {
		table  string
		target any
	}{{"runs", &s.run}, {"attempts", &s.attempt}, {"dispatch_outbox", &s.dispatch}} {
		if e := json.Unmarshal([]byte(graph.rows[v.table][0].(string)), v.target); e != nil {
			t.Fatal(e)
		}
	}
	at := s.run.CreatedAt
	s.lease = domain.Lease{ID: "repair-lease", TenantID: s.run.TenantID, RunID: s.run.ID, AttemptID: s.attempt.ID, WorkerID: "repair-worker", FenceToken: 7, AcquiredAt: at, ExpiresAt: at.Add(time.Minute)}
	s.quota = domain.QuotaReservation{ID: "repair-quota", TenantID: s.run.TenantID, RunID: s.run.ID, SubscriptionConnectionID: s.run.SubscriptionConnectionID, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Minute), UpdatedAt: at}
	s.delivery = domain.TelegramDeliveryOutbox{ID: "repair-delivery", TenantID: s.run.TenantID, RunID: s.run.ID, Chat: domain.TelegramChatRef{TenantID: s.run.TenantID, ChatID: 123}, ReplyToMessageID: 1, Text: "PRIVATE Привет 🌍", Status: domain.DeliveryPending, IdempotencyKey: "repair-delivery-key", CreatedAt: at, UpdatedAt: at}
	id := map[string]string{"lease": string(s.lease.ID), "dispatch": string(s.dispatch.ID), "quota": string(s.quota.ID), "telegram": string(s.delivery.ID)}[kind]
	req := ReadyExpiryRepairRequest{Mode: mode, Kind: kind, TenantID: s.run.TenantID, RunID: s.run.ID, ID: id, MaxSourceBytes: ReadyExpiryRepairMaxSourceBytes, MaxOutputBytes: ReadyExpiryRepairMaxOutputBytes}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	deadline, _ := ctx.Deadline()
	f := &repairSQLFixture{request: req, rows: map[string][]driver.Value{}, indices: map[string][]driver.NamedValue{}, deadline: deadline}
	f.seed(t, s)
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	return &Store{db: db}, f, s, ctx
}
func requireRepairDenied(t *testing.T, store *Store, f *repairSQLFixture, ctx context.Context) {
	t.Helper()
	out, e := store.RepairReadyExpiryV2(ctx, f.request)
	if e == nil || len(out) != 0 || len(f.writes) != 0 || len(f.committed) != 0 {
		t.Fatalf("denial err=%v out=%q attempts=%d commits=%d", e, out, len(f.writes), len(f.committed))
	}
	if strings.Contains(e.Error(), "SECRET") || strings.Contains(e.Error(), "PRIVATE") {
		t.Fatal("raw diagnostic leaked")
	}
}
func TestMetadataRepairMaxFencePastDueAndPlanNotPermit(t *testing.T) {
	store, f, s, ctx := newRepairFixture(t, "lease", "apply")
	s.lease.FenceToken = ^uint64(0)
	f.seed(t, s)
	out, e := store.RepairReadyExpiryV2(ctx, f.request)
	if e != nil || !strings.Contains(string(out), "18446744073709551615") {
		t.Fatalf("full uint64 fence err=%v", e)
	}
	if f.arguments[0][5].Value != s.lease.FenceToken || f.arguments[0][1].Value != s.lease.ExpiresAt {
		t.Fatal("fence narrowed or past due time renewed")
	}
	store, f, s, ctx = newRepairFixture(t, "quota", "plan")
	if _, e = store.RepairReadyExpiryV2(ctx, f.request); e != nil {
		t.Fatal(e)
	}
	s.quota.Status = domain.ReservationReleased
	f.seed(t, s)
	f.request.Mode = "apply"
	requireRepairDenied(t, store, f, ctx)
}
func TestMetadataRepairLeaseCallbackAndReadCeiling(t *testing.T) {
	store, f, _, ctx := newRepairFixture(t, "lease", "apply")
	f.modes = []string{"rollback", "rollback", "rollback", "rollback"}
	out, e := store.RepairReadyExpiryV2(ctx, f.request)
	if !errors.Is(e, ErrReadyExpiryRepairPossiblyApplied) || len(out) == 0 || len(f.queries) != repairMaxReads || len(f.writes) != repairMaxCallbacks || f.begins != repairMaxCallbacks+1 {
		t.Fatalf("err=%v read=%d attempts=%d callbacks=%d", e, len(f.queries), len(f.writes), f.begins)
	}
	b := repairBudget{limitStrings: 10}
	if b.read(10) != nil || b.read(1) == nil || b.reservedStrings != 10 {
		t.Fatal("cumulative read reservation reset")
	}
}
func TestMetadataRepairRequestCapsBeforeTransaction(t *testing.T) {
	for _, name := range []string{"zero-source", "over-source", "zero-output", "over-output", "bad-kind", "bad-mode", "foreign-locator", "no-deadline"} {
		t.Run(name, func(t *testing.T) {
			store, f, _, ctx := newRepairFixture(t, "quota", "apply")
			switch name {
			case "zero-source":
				f.request.MaxSourceBytes = 0
			case "over-source":
				f.request.MaxSourceBytes = ReadyExpiryRepairMaxSourceBytes + 1
			case "zero-output":
				f.request.MaxOutputBytes = 0
			case "over-output":
				f.request.MaxOutputBytes = ReadyExpiryRepairMaxOutputBytes + 1
			case "bad-kind":
				f.request.Kind = "quota_expiry"
			case "bad-mode":
				f.request.Mode = "execute"
			case "foreign-locator":
				f.request.ID = "a/b"
			case "no-deadline":
				ctx = t.Context()
			}
			out, e := store.RepairReadyExpiryV2(ctx, f.request)
			if !errors.Is(e, ErrReadyExpiryRepairInvalid) || len(out) != 0 || f.begins != 0 || len(f.queries) != 0 || len(f.writes) != 0 {
				t.Fatalf("validation did work: %v", e)
			}
		})
	}
}

func TestMetadataRepairAllFamiliesPlanApplyAndRepeat(t *testing.T) {
	for _, kind := range []string{"lease", "dispatch", "quota", "telegram"} {
		for _, mode := range []string{"plan", "apply"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				store, f, s, ctx := newRepairFixture(t, kind, mode)
				before := map[string][]driver.Value{}
				for k, v := range f.rows {
					before[k] = append([]driver.Value(nil), v...)
				}
				out, e := store.RepairReadyExpiryV2(ctx, f.request)
				if e != nil {
					t.Fatal(e)
				}
				var o readyExpiryRepairOutput
				if json.Unmarshal(out, &o) != nil || strings.Contains(string(out), "PRIVATE") {
					t.Fatal("invalid/private output")
				}
				due := map[string]time.Time{"lease": s.lease.ExpiresAt, "quota": s.quota.ExpiresAt, "dispatch": s.dispatch.CreatedAt, "telegram": telegramDeliveryAvailableAt(s.delivery)}[kind]
				if !o.DueAt.Equal(due) || o.ID != f.request.ID || o.RunID != s.run.ID || o.TenantID != s.run.TenantID {
					t.Fatal("original tuple/time lost")
				}
				bucketID := f.request.ID
				if kind == "lease" {
					bucketID = string(s.run.ID)
				}
				b, _ := ydbpartitionBucket(bucketID)
				if o.Bucket != b {
					t.Fatal("wrong bucket")
				}
				want := 0
				if mode == "apply" {
					want = 1
				}
				if len(f.writes) != want || len(f.committed) != want || !reflect.DeepEqual(before, f.rows) {
					t.Fatal("wrong write set/parent mutation")
				}
				second, e := store.RepairReadyExpiryV2(ctx, f.request)
				if e != nil || string(second) != string(out) {
					t.Fatalf("repeat err=%v", e)
				}
				if mode == "apply" && (len(f.indices) != 1 || !reflect.DeepEqual(f.arguments[0], f.arguments[1])) {
					t.Fatal("repeat renewed/changed metadata")
				}
			})
		}
	}
}
func ydbpartitionBucket(id string) (uint32, error) { return ydbpartition.BucketV1(id) }
func TestMetadataRepairCompletedRunTelegramAndAvailability(t *testing.T) {
	for _, status := range []domain.DeliveryStatus{domain.DeliveryPending, domain.DeliveryRetryWait, domain.DeliverySending} {
		t.Run(string(status), func(t *testing.T) {
			store, f, s, ctx := newRepairFixture(t, "telegram", "apply")
			finished := s.run.CreatedAt.Add(time.Second)
			s.run.Status = domain.RunSucceeded
			s.run.FinishedAt = &finished
			s.run.UpdatedAt = finished
			s.delivery.Status = status
			if status == domain.DeliveryRetryWait {
				next := finished.Add(time.Minute)
				s.delivery.NextAttemptAt = &next
			}
			f.seed(t, s)
			out, e := store.RepairReadyExpiryV2(ctx, f.request)
			if e != nil {
				t.Fatal(e)
			}
			var o readyExpiryRepairOutput
			_ = json.Unmarshal(out, &o)
			if !o.DueAt.Equal(telegramDeliveryAvailableAt(s.delivery)) {
				t.Fatal("availability changed")
			}
		})
	}
}
func TestMetadataRepairDeletedAndPhysicalMismatchAllFamilies(t *testing.T) {
	for _, kind := range []string{"lease", "dispatch", "quota", "telegram"} {
		for _, bad := range []string{"deletion", "run-key", "trigger-key", "source-key", "missing", "null", "oversize-scalar"} {
			t.Run(kind+"/"+bad, func(t *testing.T) {
				store, f, _, ctx := newRepairFixture(t, kind, "apply")
				table := map[string]string{"lease": "leases", "dispatch": "dispatch_outbox", "quota": "quota_reservations", "telegram": "telegram_delivery_outbox"}[kind]
				switch bad {
				case "deletion":
					f.rows["session_deletions"] = []driver.Value{int64(1)}
				case "run-key":
					f.rows["runs"][1] = "foreign-run"
				case "trigger-key":
					f.rows["runs"][8] = "foreign-trigger"
				case "source-key":
					f.rows[table][1] = "foreign-id"
				case "missing":
					f.rows[table] = nil
				case "null":
					f.rows[table][0] = nil
				case "oversize-scalar":
					f.rows[table][1] = strings.Repeat("x", 161)
				}
				requireRepairDenied(t, store, f, ctx)
			})
		}
	}
}
func TestMetadataRepairStaleLeaseAndFamilyTerminal(t *testing.T) {
	for _, bad := range []string{"head-id", "head-fence", "head-expiry", "acquired", "attempt-worker", "foreign-body"} {
		t.Run(bad, func(t *testing.T) {
			store, f, s, ctx := newRepairFixture(t, "lease", "apply")
			switch bad {
			case "head-id":
				f.rows["lease_heads"][0] = "old-lease"
			case "head-fence":
				f.rows["lease_heads"][5] = int64(9)
			case "head-expiry":
				f.rows["lease_heads"][6] = s.lease.ExpiresAt.Add(time.Second)
			case "acquired":
				f.rows["leases"][7] = s.lease.AcquiredAt.Add(time.Second)
			case "attempt-worker":
				s.attempt.WorkerID = "foreign-worker"
				f.seed(t, s)
			case "foreign-body":
				s.lease.TenantID = "foreign-tenant"
				f.rows["leases"][0] = repairRaw(t, s.lease)
			}
			requireRepairDenied(t, store, f, ctx)
		})
	}
	for _, kind := range []string{"dispatch", "quota", "telegram"} {
		t.Run(kind, func(t *testing.T) {
			store, f, s, ctx := newRepairFixture(t, kind, "apply")
			switch kind {
			case "dispatch":
				s.dispatch.Status = domain.DispatchPublished
			case "quota":
				s.quota.Status = domain.ReservationExpired
			case "telegram":
				s.delivery.Status = domain.DeliverySent
			}
			f.seed(t, s)
			requireRepairDenied(t, store, f, ctx)
		})
	}
}
func TestMetadataRepairByteOutputUTF8AndDepthBounds(t *testing.T) {
	for _, name := range []string{"exact", "first-over", "invalid-utf8", "null", "malformed", "depth", "output-tiny", "output-confirmed-only", "output-exact"} {
		t.Run(name, func(t *testing.T) {
			store, f, _, ctx := newRepairFixture(t, "telegram", "apply")
			raw := f.rows["telegram_delivery_outbox"][0].(string)
			switch name {
			case "exact":
				f.request.MaxSourceBytes = len(raw)
				if len(f.rows["runs"][0].(string)) > len(raw) {
					t.Fatal("fixture cap too small")
				}
				f.rows["telegram_delivery_outbox"][0] = raw
			case "first-over":
				f.request.MaxSourceBytes = len(raw)
				f.rows["telegram_delivery_outbox"][0] = raw + " "
			case "invalid-utf8":
				f.rows["telegram_delivery_outbox"][0] = string([]byte{0xff})
			case "null":
				f.rows["telegram_delivery_outbox"][0] = "null"
			case "malformed":
				f.rows["telegram_delivery_outbox"][0] = "{"
			case "depth":
				f.rows["telegram_delivery_outbox"][0] = strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65)
			case "output-tiny":
				f.request.MaxOutputBytes = 1
			case "output-confirmed-only":
				f.request.Mode = "plan"
				out, e := store.RepairReadyExpiryV2(ctx, f.request)
				if e != nil {
					t.Fatal(e)
				}
				f.request.MaxOutputBytes = len(out)
				f.request.Mode = "apply"
			case "output-exact":
				f.request.Mode = "plan"
				out, e := store.RepairReadyExpiryV2(ctx, f.request)
				if e != nil {
					t.Fatal(e)
				}
				f.request.MaxOutputBytes = len(out) + len("possibly_applied_metadata") - len("planned")
				f.request.Mode = "apply"
			}
			if name == "exact" || name == "output-exact" {
				if _, e := store.RepairReadyExpiryV2(ctx, f.request); e != nil {
					t.Fatal(e)
				}
			} else {
				requireRepairDenied(t, store, f, ctx)
			}
		})
	}
}
func TestMetadataRepairCancellationFailuresAndRetryDisposition(t *testing.T) {
	for _, name := range []string{"cancelled", "read", "write", "unknown", "changed-source", "changed-deletion", "cap-callbacks", "retry-valid"} {
		t.Run(name, func(t *testing.T) {
			store, f, s, ctx := newRepairFixture(t, "quota", "apply")
			switch name {
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "read":
				f.failRead = true
			case "write":
				f.failWrite = true
			case "unknown":
				f.modes = []string{"unknown"}
			case "changed-source":
				f.modes = []string{"ambiguous"}
				f.afterCommit = func() { s.quota.Status = domain.ReservationReleased; f.seed(t, s) }
			case "changed-deletion":
				f.modes = []string{"ambiguous"}
				f.afterCommit = func() { f.rows["session_deletions"] = []driver.Value{int64(1)} }
			case "cap-callbacks":
				f.modes = []string{"rollback", "rollback", "rollback", "rollback"}
			case "retry-valid":
				f.modes = []string{"rollback"}
			}
			out, e := store.RepairReadyExpiryV2(ctx, f.request)
			if name == "retry-valid" {
				if e != nil || len(out) == 0 || f.begins != 2 || len(f.committed) != 1 {
					t.Fatalf("retry e=%v callbacks=%d", e, f.begins)
				}
				return
			}
			if e == nil || strings.Contains(e.Error(), "SECRET") || strings.Contains(string(out), "SECRET") {
				t.Fatalf("unsafe failure %v", e)
			}
			if name == "unknown" || name == "changed-source" || name == "changed-deletion" || name == "write" || name == "cap-callbacks" {
				if !errors.Is(e, ErrReadyExpiryRepairPossiblyApplied) || !strings.Contains(string(out), "possibly_applied_metadata") {
					t.Fatalf("forgot prior attempted write: %v", e)
				}
			} else if len(out) != 0 {
				t.Fatal("unexpected output before any write")
			}
			if f.begins > 4 || len(f.queries) > repairMaxReads || len(f.writes) > repairMaxCallbacks {
				t.Fatal("retry cap reset")
			}
			if (name == "changed-source" || name == "changed-deletion" || name == "unknown") && len(f.committed) != 1 {
				t.Fatal("fixture lost possibly-applied first commit")
			}
		})
	}
}
