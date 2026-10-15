package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"time"
	"unicode/utf8"
)

// Operator refusal caps, not historical-fit/native-memory/RU certificates.
// ONE locator, at most three original SDK callbacks and five reads per callback.
const (
	ReadyExpiryRepairMaxSourceBytes = ownedCanonicalEnvelopeBytes
	ReadyExpiryRepairMaxOutputBytes = 4096
	ReadyExpiryRepairMaxDuration    = 10 * time.Minute
	repairMaxCallbacks              = 3
	repairMaxReads                  = 15
)

var (
	ErrReadyExpiryRepairInvalid         = errors.New("invalid ready/expiry repair request")
	ErrReadyExpiryRepairUnavailable     = errors.New("ready/expiry repair source unavailable")
	ErrReadyExpiryRepairFailed          = errors.New("ready/expiry repair failed")
	ErrReadyExpiryRepairPossiblyApplied = errors.New("ready/expiry repair apply may already be committed")
)

type ReadyExpiryRepairRequest struct {
	Mode, Kind                     string
	TenantID                       domain.TenantID
	RunID                          domain.RunID
	ID                             string
	MaxSourceBytes, MaxOutputBytes int
}

func (r ReadyExpiryRepairRequest) Validate() error {
	if (r.Mode != "plan" && r.Mode != "apply") || (r.Kind != "lease" && r.Kind != "dispatch" && r.Kind != "quota" && r.Kind != "telegram") ||
		r.TenantID.Validate() != nil || r.RunID.Validate() != nil || domain.ValidateOpaqueID("repair.id", r.ID) != nil ||
		r.MaxSourceBytes <= 0 || r.MaxSourceBytes > ReadyExpiryRepairMaxSourceBytes || r.MaxOutputBytes <= 0 || r.MaxOutputBytes > ReadyExpiryRepairMaxOutputBytes {
		return ErrReadyExpiryRepairInvalid
	}
	return nil
}

type readyExpiryRepairOutput struct {
	Kind     string          `json:"kind"`
	TenantID domain.TenantID `json:"tenant_id"`
	RunID    domain.RunID    `json:"run_id"`
	ID       string          `json:"id"`
	Bucket   uint32          `json:"bucket"`
	DueAt    time.Time       `json:"due_at"`
	Fence    uint64          `json:"fence,omitempty"`
	Outcome  string          `json:"outcome"`
}
type repairBudget struct {
	callbacks, reads              int
	reservedStrings, limitStrings int
	writeAttempted                bool
	possibleOutput                []byte
}

func (b *repairBudget) start() error {
	if b.callbacks >= repairMaxCallbacks {
		return ErrReadyExpiryRepairUnavailable
	}
	b.callbacks++
	return nil
}
func (b *repairBudget) read(strings int) error {
	if b.reads >= repairMaxReads || strings < 0 || strings > b.limitStrings-b.reservedStrings {
		return ErrReadyExpiryRepairUnavailable
	}
	b.reads++
	b.reservedStrings += strings
	return nil
}

type repairReader struct {
	tx *stateTx
	r  ReadyExpiryRepairRequest
	b  *repairBudget
}

func (q repairReader) query(ctx context.Context, strings int, sql string, args ...any) (*sql.Row, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := q.b.read(strings); err != nil {
		return nil, err
	}
	return q.tx.sqlTx.QueryRowContext(ctx, sql, args...), nil
}
func repairScalar(value string, empty bool) bool {
	if empty && value == "" {
		return true
	}
	return len(value) <= 160 && utf8.ValidString(value) && domain.ValidateOpaqueID("repair.scalar", value) == nil
}
func repairStatus(value string) bool {
	return len(value) > 0 && len(value) <= 32 && utf8.ValidString(value)
}
func repairTime(a, b time.Time) bool {
	return canonicalAttachedWorkerTime(a).Equal(canonicalAttachedWorkerTime(b))
}
func repairDecode(raw string, limit int, target any) error {
	// Sentinel precedes UTF-8 scan, JSON allocation and all typed validation.
	if len(raw) == 0 || len(raw) > limit || !utf8.ValidString(raw) {
		return ErrReadyExpiryRepairUnavailable
	}
	// Fixed metadata depth refusal prevents disproportionate recursive decoding;
	// string content, including quoted JSON, is not mistaken for structure.
	depth := 0
	quoted, escaped := false, false
	for _, c := range []byte(raw) {
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > 64 {
				return ErrReadyExpiryRepairUnavailable
			}
		case '}', ']':
			depth--
		}
	}
	if json.Unmarshal([]byte(raw), target) != nil {
		return ErrReadyExpiryRepairUnavailable
	}
	return nil
}
func (q repairReader) run(ctx context.Context) (domain.Run, error) {
	var raw, id, tenant, session, connection, status, trigger string
	var created, updated time.Time
	s := fmt.Sprintf("SELECT SUBSTRING(CAST(payload AS String),0,%d),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(session_id AS String),0,161),SUBSTRING(CAST(subscription_connection_id AS String),0,161),SUBSTRING(CAST(status AS String),0,33),created_at,updated_at,SUBSTRING(CAST(trigger_event_id AS String),0,161) FROM runs WHERE tenant_id=$1 AND run_id=$2", q.r.MaxSourceBytes+1)
	row, err := q.query(ctx, q.r.MaxSourceBytes+1+5*161+33, s, q.r.TenantID, q.r.RunID)
	if err != nil {
		return domain.Run{}, err
	}
	if err = row.Scan(&raw, &id, &tenant, &session, &connection, &status, &created, &updated, &trigger); err != nil {
		return domain.Run{}, err
	}
	var run domain.Run
	if !repairScalar(id, false) || !repairScalar(tenant, false) || !repairScalar(session, false) || !repairScalar(connection, false) || !repairScalar(trigger, false) || !repairStatus(status) ||
		repairDecode(raw, q.r.MaxSourceBytes, &run) != nil || run.Validate() != nil || run.ID != q.r.RunID || run.TenantID != q.r.TenantID ||
		string(run.ID) != id || string(run.TenantID) != tenant || string(run.SessionID) != session || string(run.SubscriptionConnectionID) != connection ||
		string(run.TriggerEventID) != trigger || string(run.Status) != status || !repairTime(run.CreatedAt, created) || !repairTime(run.UpdatedAt, updated) {
		return domain.Run{}, ErrReadyExpiryRepairUnavailable
	}
	return run, nil
}
func (q repairReader) attempt(ctx context.Context, run domain.Run, id domain.AttemptID) (domain.Attempt, error) {
	if id.Validate() != nil {
		return domain.Attempt{}, ErrReadyExpiryRepairUnavailable
	}
	var raw, physicalID, tenant, runID, worker, status string
	var number uint32
	var created, updated time.Time
	s := fmt.Sprintf("SELECT SUBSTRING(CAST(payload AS String),0,%d),SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(worker_id AS String),0,161),SUBSTRING(CAST(status AS String),0,33),attempt_number,created_at,updated_at FROM attempts WHERE tenant_id=$1 AND attempt_id=$2", q.r.MaxSourceBytes+1)
	row, err := q.query(ctx, q.r.MaxSourceBytes+1+4*161+33, s, q.r.TenantID, id)
	if err != nil {
		return domain.Attempt{}, err
	}
	if err = row.Scan(&raw, &physicalID, &tenant, &runID, &worker, &status, &number, &created, &updated); err != nil {
		return domain.Attempt{}, err
	}
	var attempt domain.Attempt
	if !repairScalar(physicalID, false) || !repairScalar(tenant, false) || !repairScalar(runID, false) || !repairScalar(worker, true) || !repairStatus(status) ||
		repairDecode(raw, q.r.MaxSourceBytes, &attempt) != nil || attempt.ValidateForRun(run) != nil || attempt.ID != id ||
		string(attempt.ID) != physicalID || string(attempt.TenantID) != tenant || string(attempt.RunID) != runID || attempt.WorkerID != worker ||
		string(attempt.Status) != status || attempt.Number != number || !repairTime(attempt.CreatedAt, created) || !repairTime(attempt.UpdatedAt, updated) {
		return domain.Attempt{}, ErrReadyExpiryRepairUnavailable
	}
	return attempt, nil
}
func (q repairReader) lease(ctx context.Context, run domain.Run) (domain.Lease, error) {
	var id, tenant, runID, attemptID, worker string
	var fence uint64
	var expiry time.Time
	row, err := q.query(ctx, 5*161, "SELECT SUBSTRING(CAST(lease_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(worker_id AS String),0,161),fence_token,expires_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2", q.r.TenantID, q.r.RunID)
	if err != nil {
		return domain.Lease{}, err
	}
	if err = row.Scan(&id, &tenant, &runID, &attemptID, &worker, &fence, &expiry); err != nil {
		return domain.Lease{}, err
	}
	if !repairScalar(id, false) || !repairScalar(tenant, false) || !repairScalar(runID, false) || !repairScalar(attemptID, false) || !repairScalar(worker, false) || id != q.r.ID || tenant != string(q.r.TenantID) || runID != string(q.r.RunID) || fence == 0 || expiry.IsZero() {
		return domain.Lease{}, ErrReadyExpiryRepairUnavailable
	}
	var raw, lid, lt, lr, la, lw string
	var lf uint64
	var acquired, expires time.Time
	s := fmt.Sprintf("SELECT SUBSTRING(CAST(payload AS String),0,%d),SUBSTRING(CAST(lease_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(worker_id AS String),0,161),fence_token,acquired_at,expires_at FROM leases WHERE tenant_id=$1 AND lease_id=$2", q.r.MaxSourceBytes+1)
	row, err = q.query(ctx, q.r.MaxSourceBytes+1+5*161, s, q.r.TenantID, q.r.ID)
	if err != nil {
		return domain.Lease{}, err
	}
	if err = row.Scan(&raw, &lid, &lt, &lr, &la, &lw, &lf, &acquired, &expires); err != nil {
		return domain.Lease{}, err
	}
	var lease domain.Lease
	if !repairScalar(lid, false) || !repairScalar(lt, false) || !repairScalar(lr, false) || !repairScalar(la, false) || !repairScalar(lw, false) ||
		repairDecode(raw, q.r.MaxSourceBytes, &lease) != nil || lid != id || lt != tenant || lr != runID || la != attemptID || lw != worker || lf != fence ||
		lease.ID != domain.LeaseID(id) || lease.TenantID != run.TenantID || lease.RunID != run.ID || lease.AttemptID != domain.AttemptID(attemptID) ||
		lease.WorkerID != worker || lease.FenceToken != fence || !repairTime(lease.AcquiredAt, acquired) || !repairTime(lease.ExpiresAt, expires) || !repairTime(lease.ExpiresAt, expiry) {
		return domain.Lease{}, ErrReadyExpiryRepairUnavailable
	}
	attempt, err := q.attempt(ctx, run, lease.AttemptID)
	if err != nil {
		return domain.Lease{}, err
	}
	if lease.ValidateForAttempt(run, attempt) != nil || (attempt.WorkerID != "" && attempt.WorkerID != worker) {
		return domain.Lease{}, ErrReadyExpiryRepairUnavailable
	}
	return lease, nil
}
func (q repairReader) dispatch(ctx context.Context, run domain.Run) (domain.DispatchOutbox, error) {
	var raw, id, tenant, runID, attempt, status string
	var created, updated time.Time
	s := fmt.Sprintf("SELECT SUBSTRING(CAST(payload AS String),0,%d),SUBSTRING(CAST(dispatch_outbox_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(status AS String),0,33),created_at,updated_at FROM dispatch_outbox WHERE tenant_id=$1 AND dispatch_outbox_id=$2", q.r.MaxSourceBytes+1)
	row, err := q.query(ctx, q.r.MaxSourceBytes+1+4*161+33, s, q.r.TenantID, q.r.ID)
	if err != nil {
		return domain.DispatchOutbox{}, err
	}
	if err = row.Scan(&raw, &id, &tenant, &runID, &attempt, &status, &created, &updated); err != nil {
		return domain.DispatchOutbox{}, err
	}
	var outbox domain.DispatchOutbox
	if !repairScalar(id, false) || !repairScalar(tenant, false) || !repairScalar(runID, false) || !repairScalar(attempt, false) || !repairStatus(status) || repairDecode(raw, q.r.MaxSourceBytes, &outbox) != nil ||
		id != q.r.ID || tenant != string(q.r.TenantID) || runID != string(run.ID) || outbox.ID != domain.DispatchOutboxID(id) || outbox.TenantID != run.TenantID || outbox.RunID != run.ID || outbox.AttemptID != domain.AttemptID(attempt) ||
		string(outbox.Status) != status || outbox.Status != domain.DispatchPending || !repairTime(outbox.CreatedAt, created) || !repairTime(outbox.UpdatedAt, updated) {
		return domain.DispatchOutbox{}, ErrReadyExpiryRepairUnavailable
	}
	a, err := q.attempt(ctx, run, outbox.AttemptID)
	if err != nil {
		return domain.DispatchOutbox{}, err
	}
	if outbox.ValidateForAttempt(run, a) != nil {
		return domain.DispatchOutbox{}, ErrReadyExpiryRepairUnavailable
	}
	return outbox, nil
}
func (q repairReader) quota(ctx context.Context, run domain.Run) (domain.QuotaReservation, error) {
	var raw, id, tenant, runID, connection, status string
	var held, expires time.Time
	var capacity uint32
	s := fmt.Sprintf("SELECT SUBSTRING(CAST(payload AS String),0,%d),SUBSTRING(CAST(quota_reservation_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(subscription_connection_id AS String),0,161),SUBSTRING(CAST(status AS String),0,33),held_at,expires_at,capacity_units FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", q.r.MaxSourceBytes+1)
	row, err := q.query(ctx, q.r.MaxSourceBytes+1+4*161+33, s, q.r.TenantID, q.r.ID)
	if err != nil {
		return domain.QuotaReservation{}, err
	}
	if err = row.Scan(&raw, &id, &tenant, &runID, &connection, &status, &held, &expires, &capacity); err != nil {
		return domain.QuotaReservation{}, err
	}
	var r domain.QuotaReservation
	if !repairScalar(id, false) || !repairScalar(tenant, false) || !repairScalar(runID, false) || !repairScalar(connection, false) || !repairStatus(status) || repairDecode(raw, q.r.MaxSourceBytes, &r) != nil ||
		id != q.r.ID || tenant != string(q.r.TenantID) || runID != string(run.ID) || connection != string(run.SubscriptionConnectionID) ||
		r.ID != domain.QuotaReservationID(id) || string(r.TenantID) != tenant || string(r.RunID) != runID || string(r.SubscriptionConnectionID) != connection || string(r.Status) != status || r.Status != domain.ReservationHeld ||
		r.CapacityUnits != capacity || !repairTime(r.HeldAt, held) || !repairTime(r.ExpiresAt, expires) || r.ValidateForRun(run) != nil {
		return domain.QuotaReservation{}, ErrReadyExpiryRepairUnavailable
	}
	return r, nil
}
func (q repairReader) telegram(ctx context.Context, run domain.Run) (domain.TelegramDeliveryOutbox, error) {
	var raw, id, tenant, runID, status string
	var created, updated, next time.Time
	var chat int64
	s := fmt.Sprintf("SELECT SUBSTRING(CAST(payload AS String),0,%d),SUBSTRING(CAST(telegram_delivery_id AS String),0,161),SUBSTRING(CAST(tenant_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161),SUBSTRING(CAST(status AS String),0,33),created_at,updated_at,next_attempt_at,chat_id FROM telegram_delivery_outbox WHERE tenant_id=$1 AND telegram_delivery_id=$2", q.r.MaxSourceBytes+1)
	row, err := q.query(ctx, q.r.MaxSourceBytes+1+3*161+33, s, q.r.TenantID, q.r.ID)
	if err != nil {
		return domain.TelegramDeliveryOutbox{}, err
	}
	if err = row.Scan(&raw, &id, &tenant, &runID, &status, &created, &updated, &next, &chat); err != nil {
		return domain.TelegramDeliveryOutbox{}, err
	}
	var d domain.TelegramDeliveryOutbox
	if !repairScalar(id, false) || !repairScalar(tenant, false) || !repairScalar(runID, false) || !repairStatus(status) || repairDecode(raw, q.r.MaxSourceBytes, &d) != nil ||
		id != q.r.ID || tenant != string(q.r.TenantID) || runID != string(run.ID) || d.ID != domain.TelegramDeliveryID(id) || string(d.TenantID) != tenant || string(d.RunID) != runID || string(d.Status) != status ||
		d.Chat.ChatID != chat || !repairTime(d.CreatedAt, created) || !repairTime(d.UpdatedAt, updated) || d.ValidateForRun(run) != nil ||
		(d.Status != domain.DeliveryPending && d.Status != domain.DeliveryRetryWait && d.Status != domain.DeliverySending) {
		return domain.TelegramDeliveryOutbox{}, ErrReadyExpiryRepairUnavailable
	}
	expectedNext := time.Unix(0, 0).UTC()
	if d.NextAttemptAt != nil {
		expectedNext = *d.NextAttemptAt
	}
	if !repairTime(expectedNext, next) {
		return domain.TelegramDeliveryOutbox{}, ErrReadyExpiryRepairUnavailable
	}
	return d, nil
}

// Operator-only metadata method; not a serving port, tenant-authenticator or
// permission issuer. Cumulative counters never reset across SDK retries.
func (store *Store) RepairReadyExpiryV2(ctx context.Context, r ReadyExpiryRepairRequest) ([]byte, error) {
	if r.Validate() != nil || ctx == nil || store == nil || store.db == nil {
		return nil, ErrReadyExpiryRepairInvalid
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > ReadyExpiryRepairMaxDuration {
		return nil, ErrReadyExpiryRepairInvalid
	}
	// Max lease callback: THREE JSON cells, 19 ID scalars, TWO status scalars.
	// Reserve UTF-8/string returned-byte maxima BEFORE every query, including
	// missing/error rows. Fixed-width columns have bounded counts. Explicit
	// raw-to-byte copies and decoding are bounded by these admitted cell sizes;
	// native buffers/object overhead/server I/O/RU are not certified here.
	b := &repairBudget{limitStrings: repairMaxCallbacks * (3*(r.MaxSourceBytes+1) + 19*161 + 2*33)}
	var result []byte
	err := store.Transact(ctx, r.TenantID, func(state ports.StateTx) error {
		result = nil
		if err := b.start(); err != nil {
			return err
		}
		q := repairReader{tx: state.(*stateTx), r: r, b: b}
		run, err := q.run(ctx)
		if err != nil {
			return err
		}
		row, err := q.query(ctx, 0, "SELECT 1 FROM session_deletions WHERE tenant_id=$1 AND session_id=$2", r.TenantID, run.SessionID)
		if err != nil {
			return err
		}
		var present int
		if err = row.Scan(&present); err == nil {
			return ErrReadyExpiryRepairUnavailable
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		o := readyExpiryRepairOutput{Kind: r.Kind, TenantID: r.TenantID, RunID: r.RunID, ID: r.ID, Outcome: "planned"}
		if r.Mode == "apply" {
			o.Outcome = "applied"
		}
		var statement string
		var args []any
		switch r.Kind {
		case "lease":
			v, e := q.lease(ctx, run)
			if e != nil {
				return e
			}
			o.Bucket, _ = ydbpartition.BucketV1(string(run.ID))
			o.DueAt, o.Fence = v.ExpiresAt, v.FenceToken
			statement = "UPSERT INTO lease_expiry_v2 (shard_bucket,expires_at,tenant_id,run_id,lease_id,fence_token) VALUES ($1,$2,$3,$4,$5,$6)"
			args = []any{o.Bucket, v.ExpiresAt, r.TenantID, r.RunID, v.ID, v.FenceToken}
		case "dispatch":
			v, e := q.dispatch(ctx, run)
			if e != nil {
				return e
			}
			o.Bucket, _ = ydbpartition.BucketV1(r.ID)
			o.DueAt = v.CreatedAt
			statement = "UPSERT INTO dispatch_ready_v2 (shard_bucket,available_at,tenant_id,dispatch_outbox_id,run_id,attempt_id) VALUES ($1,$2,$3,$4,$5,$6)"
			args = []any{o.Bucket, o.DueAt, r.TenantID, v.ID, r.RunID, v.AttemptID}
		case "quota":
			v, e := q.quota(ctx, run)
			if e != nil {
				return e
			}
			o.Bucket, _ = ydbpartition.BucketV1(r.ID)
			o.DueAt = v.ExpiresAt
			statement = "UPSERT INTO quota_expiry_v2 (shard_bucket,expires_at,tenant_id,quota_reservation_id,run_id,subscription_connection_id) VALUES ($1,$2,$3,$4,$5,$6)"
			args = []any{o.Bucket, o.DueAt, r.TenantID, v.ID, r.RunID, v.SubscriptionConnectionID}
		case "telegram":
			v, e := q.telegram(ctx, run)
			if e != nil {
				return e
			}
			o.Bucket, _ = ydbpartition.BucketV1(r.ID)
			o.DueAt = telegramDeliveryAvailableAt(v)
			statement = "UPSERT INTO telegram_delivery_ready_v2 (shard_bucket,available_at,tenant_id,telegram_delivery_id,run_id) VALUES ($1,$2,$3,$4,$5)"
			args = []any{o.Bucket, o.DueAt, r.TenantID, v.ID, r.RunID}
		}
		encoded, e := json.Marshal(o)
		if e != nil || len(encoded)+1 > r.MaxOutputBytes {
			return ErrReadyExpiryRepairUnavailable
		}
		encoded = append(encoded, '\n')
		var possible []byte
		if r.Mode == "apply" {
			o.Outcome = "possibly_applied_metadata"
			possible, e = json.Marshal(o)
			if e != nil || len(possible)+1 > r.MaxOutputBytes {
				return ErrReadyExpiryRepairUnavailable
			}
			possible = append(possible, '\n')
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.Mode == "apply" {
			// This closed tuple describes the latest attempted snapshot, not current
			// permission or proof that all prior attempts rolled back. Reserve BOTH
			// confirmed and longer ambiguous output variants before any DML.
			b.writeAttempted = true
			b.possibleOutput = possible
			if _, e = q.tx.sqlTx.ExecContext(ctx, statement, args...); e != nil {
				return e
			}
		}
		result = encoded
		return nil
	})
	if err != nil {
		if b.writeAttempted {
			return b.possibleOutput, ErrReadyExpiryRepairPossiblyApplied
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, ErrReadyExpiryRepairUnavailable) || errors.Is(err, sql.ErrNoRows) {
			return nil, ErrReadyExpiryRepairUnavailable
		}
		return nil, ErrReadyExpiryRepairFailed
	}
	return result, nil
}
