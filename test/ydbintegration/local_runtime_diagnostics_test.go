//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type localRuntimeHookCounts struct{ attached, running, tail, started int }

// Observe registration detachment, NOT callback joining. Each original f is
// context cancellation propagation (including Go's child-context propagation),
// not application I/O. A running callback and our outer observer tail can outlive
// that propagation's own completion signal. The actual keeper/bridge/gate/writer
// and durable release checks remain the ACK prerequisite in the native test.
type localRuntimeLifetimeCaller struct {
	context.Context
	mu    sync.Mutex
	hooks localRuntimeHookCounts
	// Used only by the pure control to hold the observer AFTER f has returned.
	afterReturn func()
}

func (*localRuntimeLifetimeCaller) Value(any) any { return nil }
func (c *localRuntimeLifetimeCaller) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	c.hooks.attached++
	c.hooks.started++
	c.mu.Unlock()
	var once sync.Once
	detach := func() { once.Do(func() { c.mu.Lock(); c.hooks.attached--; c.mu.Unlock() }) }
	stop := context.AfterFunc(c.Context, func() {
		detach() // This registration cannot fire again; f is NOT joined here.
		c.mu.Lock()
		c.hooks.running++
		c.mu.Unlock()
		f()
		c.mu.Lock()
		c.hooks.running--
		c.hooks.tail++
		c.mu.Unlock()
		if c.afterReturn != nil {
			c.afterReturn()
		}
		c.mu.Lock()
		c.hooks.tail--
		c.mu.Unlock()
	})
	return func() bool {
		stopped := stop()
		if stopped {
			detach()
		}
		return stopped
	}
}
func (c *localRuntimeLifetimeCaller) counts() localRuntimeHookCounts {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hooks
}

func TestLocalRuntimeYDBCallerRegistrationObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		caller := &localRuntimeLifetimeCaller{Context: ctx}
		stop := caller.AfterFunc(func() { t.Error("stopped callback ran") })
		if got := caller.counts(); got != (localRuntimeHookCounts{attached: 1, started: 1}) {
			t.Fatal(got)
		}
		if !stop() {
			t.Fatal("live registration did not stop")
		}
		stop()
		if got := caller.counts(); got != (localRuntimeHookCounts{started: 1}) {
			t.Fatal(got)
		}
		// The native caller is consumed through Go's public cancellation
		// propagation path as well, not only by direct calls to this observer.
		child, closeChild := context.WithCancel(caller)
		closeChild()
		if child.Err() != context.Canceled {
			t.Fatal("original child cancellation not forwarded")
		}
		if got := caller.counts(); got != (localRuntimeHookCounts{started: 2}) {
			t.Fatal("Go child retained its live-parent registration", got)
		}
	})
}

func TestLocalRuntimeYDBCallerObserverTailIsNotOriginalWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		releaseWork, releaseTail := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		finishWork := func() { releaseOnce.Do(func() { close(releaseWork) }) }
		defer finishWork()
		defer close(releaseTail)
		completed := make(chan struct{})
		caller := &localRuntimeLifetimeCaller{Context: ctx, afterReturn: func() { <-releaseTail }}
		stop := caller.AfterFunc(func() { <-releaseWork; close(completed) })
		cancel()
		synctest.Wait()
		if got := caller.counts(); got != (localRuntimeHookCounts{running: 1, started: 1}) {
			t.Fatal("blocked original work", got)
		}
		if stop() {
			t.Fatal("started callback falsely stopped")
		}
		select {
		case <-completed:
			t.Fatal("original work falsely completed")
		default:
		}
		finishWork()
		synctest.Wait()
		select {
		case <-completed:
		default:
			t.Fatal("original work did not complete")
		}
		if got := caller.counts(); got != (localRuntimeHookCounts{tail: 1, started: 1}) {
			t.Fatal("completed f with held outer tail", got)
		}
		// The old active count (running+tail) is still 1, despite f returning.
		// Detachment was already true while f was blocked: it is not a join.
		if stop() {
			t.Fatal("completed callback falsely stopped")
		}
	})
}

const localRuntimeDiagnosticSignalLimit = 16

type localRuntimeDiagnosticSignals struct {
	mu      sync.Mutex
	records []opssignal.Record
	dropped uint64
}

func (s *localRuntimeDiagnosticSignals) Observe(ctx context.Context, r opssignal.Record) error {
	if ctx == nil || ctx.Err() != nil || r.Validate() != nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.records) == localRuntimeDiagnosticSignalLimit {
		s.dropped++
		return nil
	}
	s.records = append(s.records, r)
	return nil
}
func (s *localRuntimeDiagnosticSignals) snapshot() ([]opssignal.Record, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]opssignal.Record(nil), s.records...), s.dropped
}

// Every probe has a fixed label/result vocabulary. Never emit driver errors,
// record bodies, identifiers, paths, SQL arguments or provider material.
func localRuntimeProbeResult(err error) string {
	switch {
	case err == nil:
		return "observed"
	case errors.Is(err, sql.ErrNoRows):
		return "absent"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "unavailable"
	}
}
func localRuntimeRunStatus(raw string) string {
	if domain.RunStatus(raw).Valid() {
		return raw
	}
	return "invalid"
}
func localRuntimeQuotaStatus(raw string) string {
	if domain.ReservationStatus(raw).Valid() {
		return raw
	}
	return "invalid"
}
func localRuntimeAllocationState(raw string, proxy bool) string {
	// The SQL projection returns at most limit+1 bytes; over-limit DATA is never
	// parsed or misreported as an authentic current allocation.
	if len(raw) > 32<<10 {
		return "over_limit"
	}
	if raw == "" {
		return "absent"
	}
	var state domain.RegisteredPreparationStateV1
	if proxy {
		v, err := domain.DecodeRegisteredProxyAttestationV2([]byte(raw))
		if err != nil {
			return "invalid"
		}
		state = v.State
	} else {
		v, err := domain.DecodeRegisteredAllocationV2([]byte(raw))
		if err != nil {
			return "invalid"
		}
		state = v.State
	}
	switch state {
	case domain.RegisteredPreparationActiveV1, domain.RegisteredPreparationRetiredV1, domain.RegisteredPreparationRevokedV1:
		return string(state)
	default:
		return "invalid"
	}
}

// Failure-only observations, NOT an atomic snapshot, validated frontier or
// authority. The native test already guarded this newly owned tenant/database.
// An independent 3s total context allows observation after the caller expired;
// it grants no retry, write, renewal, repair or additional execution lifetime.
func localRuntimeLogFailure(t *testing.T, db *sql.DB, tenant domain.TenantID, job ports.WorkerJobState, signals *localRuntimeDiagnosticSignals, caller *localRuntimeLifetimeCaller) {
	t.Helper()
	records, dropped := signals.snapshot()
	raw, _ := json.Marshal(records) // All fields passed the closed Record validator.
	t.Logf("runtime failure signals=%s dropped=%d caller_registrations=%+v (running/tail are not join evidence)", raw, dropped, caller.counts())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var at time.Time
	err := db.QueryRowContext(ctx, "SELECT CurrentUtcTimestamp()").Scan(&at)
	clockKnown := err == nil && !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 && at.Nanosecond()%1000 == 0
	clock := "unknown"
	if clockKnown {
		clock = at.UTC().Format(time.RFC3339Nano)
	}
	t.Logf("runtime failure database_clock result=%s at=%s valid_microseconds=%t (sample precedes separate non-atomic probes)", localRuntimeProbeResult(err), clock, clockKnown)
	var status string
	err = db.QueryRowContext(ctx, "SELECT SUBSTRING(CAST(status AS String),0,33) FROM runs WHERE tenant_id=$1 AND run_id=$2", tenant, job.Run.ID).Scan(&status)
	t.Logf("runtime failure run result=%s status=%s", localRuntimeProbeResult(err), localRuntimeRunStatus(status))
	status = ""
	var expires time.Time
	err = db.QueryRowContext(ctx, "SELECT SUBSTRING(CAST(status AS String),0,33),expires_at FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", tenant, job.Reservation.ID).Scan(&status, &expires)
	t.Logf("runtime failure quota result=%s status=%s expiry_comparable=%t expired_at_db_sample=%t", localRuntimeProbeResult(err), localRuntimeQuotaStatus(status), err == nil && clockKnown, err == nil && clockKnown && !expires.After(at))
	status = ""
	err = db.QueryRowContext(ctx, "SELECT SUBSTRING(CAST(terminal_status AS String),0,33) FROM run_finalizations WHERE tenant_id=$1 AND run_id=$2", tenant, job.Run.ID).Scan(&status)
	t.Logf("runtime failure finalization result=%s status=%s", localRuntimeProbeResult(err), localRuntimeRunStatus(status))
	var allocation, proxy string
	err = db.QueryRowContext(ctx, "SELECT SUBSTRING(allocation_current_record,0,32769),SUBSTRING(proxy_current_record,0,32769) FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3", tenant, job.Attempt.ID, domain.ProviderEffectTurnV1).Scan(&allocation, &proxy)
	t.Logf("runtime failure allocation result=%s allocation=%s proxy=%s", localRuntimeProbeResult(err), localRuntimeAllocationState(allocation, false), localRuntimeAllocationState(proxy, true))
	var operations, observed uint64
	err = db.QueryRowContext(ctx, "SELECT COUNT(*),COUNT(observation_digest) FROM attempt_operations_v1 WHERE tenant_id=$1 AND attempt_id=$2", tenant, job.Attempt.ID).Scan(&operations, &observed)
	t.Logf("runtime failure operation_frontier result=%s rows=%d nonnull_observation_digests=%d (not validated completion)", localRuntimeProbeResult(err), operations, observed)
}

func TestLocalRuntimeYDBDiagnosticClosedFieldsAndBounds(t *testing.T) {
	secret := "private-tenant-key-path-provider-body"
	for _, check := range []struct {
		err  error
		want string
	}{
		{nil, "observed"}, {errors.New(secret), "unavailable"},
		{errors.Join(sql.ErrNoRows, errors.New(secret)), "absent"},
		{context.Canceled, "canceled"}, {context.DeadlineExceeded, "deadline"},
	} {
		if got := localRuntimeProbeResult(check.err); got != check.want {
			t.Fatalf("error class=%s want=%s", got, check.want)
		}
	}
	if localRuntimeRunStatus(secret) != "invalid" || localRuntimeQuotaStatus(secret) != "invalid" || localRuntimeAllocationState(secret, false) != "invalid" || localRuntimeAllocationState(strings.Repeat("x", (32<<10)+1), true) != "over_limit" {
		t.Fatal("unbounded or open diagnostic vocabulary")
	}
	s := &localRuntimeDiagnosticSignals{}
	ctx := t.Context()
	for range localRuntimeDiagnosticSignalLimit + 2 {
		opssignal.Emit(ctx, s, opssignal.ManagedCanonicalUnknown, opssignal.CanonicalReadback, opssignal.None)
	}
	_ = s.Observe(ctx, opssignal.Record{SignalID: opssignal.SignalID(secret)})
	records, dropped := s.snapshot()
	if len(records) != localRuntimeDiagnosticSignalLimit || dropped != 2 {
		t.Fatal("signal bound", len(records), dropped)
	}
	records[0].SignalID = opssignal.SignalID(secret)
	copyRecords, _ := s.snapshot()
	raw, err := json.Marshal(copyRecords)
	if err != nil || len(raw) > localRuntimeDiagnosticSignalLimit*opssignal.MaxRecordBytes || strings.Contains(string(raw), secret) {
		t.Fatal("signal redaction/copy/byte bound", err)
	}
	for _, r := range copyRecords {
		if r.Validate() != nil {
			t.Fatal("unvalidated record")
		}
	}
	var group sync.WaitGroup
	for range 32 {
		group.Go(func() { opssignal.Emit(ctx, s, opssignal.ManagedCleanupSuccess, opssignal.Cleanup, opssignal.None) })
	}
	group.Wait()
	if records, dropped := s.snapshot(); len(records) != localRuntimeDiagnosticSignalLimit || dropped != 34 {
		t.Fatal("concurrent sink exceeded bound or lost accounting", len(records), dropped)
	}
}
