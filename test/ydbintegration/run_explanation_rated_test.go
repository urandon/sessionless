//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type ratedExplanationYDBFixture struct {
	explanationYDBFixture
	second   *ydbstore.Store
	identity string
	keys     [runexplanationrate.ProbeCount]uint32
}

func newRatedExplanationYDBFixture(t *testing.T) ratedExplanationYDBFixture {
	t.Helper()
	f := newExplanationYDBFixture(t)
	// A separate SDK client and sql.DB are essential: two Store wrappers on
	// one in-process limiter or one connection are not a replica-sharing proof.
	client, err := ydbclient.Open(f.ctx, requireConnectionString(t))
	if err != nil {
		t.Fatalf("open independent rated reader client: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Close(ctx); err != nil {
			t.Errorf("close independent rated reader client: %v", err)
		}
	})
	second, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	identity, keys, err := runexplanationrate.IdentityKeys(f.auth.TenantID, f.auth.UserID)
	if err != nil {
		t.Fatal(err)
	}
	rated := ratedExplanationYDBFixture{explanationYDBFixture: f, second: second, identity: identity, keys: keys}
	// Finite slots are shared with unrelated tenants. Inspect only the four
	// exact candidate keys, and delete only this fixture's identity, atomically
	// with the ownership check. Even corrupt typed rows retain this identity.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := f.client.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			t.Errorf("begin owned rate cleanup: %v", err)
			return
		}
		defer tx.Rollback()
		for _, key := range keys {
			var record string
			err := tx.QueryRowContext(ctx, `SELECT CAST(record AS String) FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, key).Scan(&record)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				t.Errorf("read owned cleanup key=%d: %v", key, err)
				return
			}
			var owner struct {
				Identity string `json:"identity_digest"`
			}
			if json.Unmarshal([]byte(record), &owner) != nil || owner.Identity != identity {
				continue // Never erase an unrelated live or expired occupant.
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, key); err != nil {
				t.Errorf("delete owned cleanup key=%d: %v", key, err)
				return
			}
		}
		if err := tx.Commit(); err != nil {
			t.Errorf("commit owned rate cleanup: %v", err)
		}
	})
	return rated
}

func readRatedExplanationYDB(t *testing.T, f ratedExplanationYDBFixture, store *ydbstore.Store, requestID string, runID domain.RunID, want ports.RunExplanationReadOutcomeKindV1) ports.RunExplanationReadOutcomeV1 {
	t.Helper()
	result, err := store.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, requestID, runID)
	if err != nil || result.Kind != want {
		t.Fatalf("rated read request=%q selector=%q outcome=%+v want=%s err=%v", requestID, runID, result, want, err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("rated read request=%q invalid outcome: %v", requestID, err)
	}
	return result
}

func ownedRatedExplanationSlot(t *testing.T, f ratedExplanationYDBFixture) (uint32, runexplanationrate.Slot) {
	t.Helper()
	var found bool
	var selected uint32
	var slot runexplanationrate.Slot
	for _, key := range f.keys {
		var encoded string
		var expiry time.Time
		err := f.client.DB.QueryRowContext(f.ctx, `SELECT CAST(record AS String), expire_at FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, key).Scan(&encoded, &expiry)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			t.Fatalf("read rate key=%d: %v", key, err)
		}
		decoded, err := runexplanationrate.DecodeSlot([]byte(encoded))
		if err != nil {
			t.Fatalf("decode rate key=%d: %v", key, err)
		}
		if decoded.IdentityDigest != f.identity {
			continue
		}
		if found || !expiry.Equal(decoded.ExpiresAt) || key >= runexplanationrate.MaxSlots {
			t.Fatalf("owned rate invariant key=%d duplicate=%t expiry=%v row_expiry=%v", key, found, expiry, decoded.ExpiresAt)
		}
		found, selected, slot = true, key, decoded
	}
	if !found {
		t.Fatal("no owned rate row at the four exact identity keys")
	}
	return selected, slot
}

func replaceOwnedRatedExplanationSlot(t *testing.T, f ratedExplanationYDBFixture, key uint32, encoded string, expiry time.Time) {
	t.Helper()
	tx, err := f.client.DB.BeginTx(f.ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var previous string
	if err := tx.QueryRowContext(f.ctx, `SELECT CAST(record AS String) FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, key).Scan(&previous); err != nil {
		t.Fatalf("read fixture-owned mutation key=%d: %v", key, err)
	}
	var owner struct {
		Identity string `json:"identity_digest"`
	}
	if json.Unmarshal([]byte(previous), &owner) != nil || owner.Identity != f.identity {
		t.Fatalf("refuse mutation of non-owned rate key=%d", key)
	}
	if _, err := tx.ExecContext(f.ctx, `UPDATE run_explanation_rate_slots_v1 SET record=CAST($1 AS JsonDocument), expire_at=$2 WHERE slot_id=$3`, encoded, expiry, key); err != nil {
		t.Fatalf("mutate owned rate key=%d: %v", key, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit owned rate mutation key=%d: %v", key, err)
	}
}

func assertRatedExplanationWebSessionUnchanged(t *testing.T, f ratedExplanationYDBFixture) {
	t.Helper()
	bucket, err := ydbpartition.BucketV1(string(f.web.SessionDigest))
	if err != nil {
		t.Fatal(err)
	}
	var encoded string
	if err := f.client.DB.QueryRowContext(f.ctx, `SELECT CAST(record AS String) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, f.web.SessionDigest).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var actual domain.WebSession
	if err := json.Unmarshal([]byte(encoded), &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, f.web) {
		t.Errorf("rated opening changed Web session: got=%+v want=%+v", actual, f.web)
	}
}

// ratedExplanationAdmissionOracle uses only the committed YDB debit timestamps,
// not elapsed wall time. Under slow infrastructure a third request may legally
// arrive after refill; it may never be charged earlier than the shared TAT.
func ratedExplanationAdmissionOracle(slot runexplanationrate.Slot, outcomes map[string]ports.RunExplanationReadOutcomeV1, uncertain map[string]bool) error {
	if len(slot.Receipts) == 0 {
		return errors.New("no committed debit receipts")
	}
	seen := make(map[string]bool, len(slot.Receipts))
	var tat time.Time
	for _, receipt := range slot.Receipts {
		outcome, found := outcomes[receipt.RequestID]
		if !found && !uncertain[receipt.RequestID] || seen[receipt.RequestID] || outcome.Kind == ports.RunExplanationReadRateLimitedV1 {
			return fmt.Errorf("receipt=%q has no unique allowed outcome", receipt.RequestID)
		}
		if found && outcome.Kind != ports.RunExplanationReadSuccessV1 && outcome.Kind != ports.RunExplanationReadNotFoundV1 {
			return fmt.Errorf("receipt=%q unexpected outcome=%s", receipt.RequestID, outcome.Kind)
		}
		if outcome.Explanation != nil && !outcome.Explanation.ReadAt.Equal(receipt.DebitedAt) {
			return fmt.Errorf("receipt=%q debit=%v differs from transaction read_at=%v", receipt.RequestID, receipt.DebitedAt, outcome.Explanation.ReadAt)
		}
		if tat.After(receipt.DebitedAt.Add(runexplanationrate.Interval)) {
			return fmt.Errorf("receipt=%q debit=%v precedes shared eligibility=%v", receipt.RequestID, receipt.DebitedAt, tat.Add(-runexplanationrate.Interval))
		}
		if tat.Before(receipt.DebitedAt) {
			tat = receipt.DebitedAt
		}
		tat = tat.Add(runexplanationrate.Interval)
		seen[receipt.RequestID] = true
	}
	for requestID, outcome := range outcomes {
		allowed := outcome.Kind == ports.RunExplanationReadSuccessV1 || outcome.Kind == ports.RunExplanationReadNotFoundV1
		if seen[requestID] != allowed {
			return fmt.Errorf("request=%q kind=%s committed_receipt=%t", requestID, outcome.Kind, seen[requestID])
		}
	}
	lastDebit := slot.Receipts[len(slot.Receipts)-1].DebitedAt
	if !slot.TheoreticalArrivalAt.Equal(tat) || !slot.LastDebitAt.Equal(lastDebit) || !slot.ExpiresAt.Equal(lastDebit.Add(runexplanationrate.IdleTTL)) {
		return fmt.Errorf("committed shared TAT/last-debit/expiry differs from admitted-receipt recurrence")
	}
	return nil
}

func ratedExplanationYDBClock(t *testing.T, f ratedExplanationYDBFixture) time.Time {
	t.Helper()
	var at time.Time
	if err := f.client.DB.QueryRowContext(f.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		t.Fatalf("sample replay authority clock: %v", err)
	}
	return at.UTC()
}

// Successful explanation reads expose the exact transaction clock. Opaque404
// deliberately does not; its transaction clock is bracketed by native YDB
// samples. A straddled boundary proves only interval consistency, not which
// side of the boundary was sampled. Fixed-clock unit/SQL fixtures prove that
// exact boundary separately, without broadening production receipt retention.
func ratedExplanationReplayOracle(before, after runexplanationrate.Slot, requestID string, lower, upper, exact time.Time) error {
	if lower.IsZero() || upper.Before(lower) || !exact.IsZero() && (exact.Before(lower) || exact.After(upper)) {
		return errors.New("invalid native transaction clock interval")
	}
	if !exact.IsZero() {
		lower, upper = exact, exact
	}
	var previous, next *runexplanationrate.Receipt
	for i := range before.Receipts {
		if before.Receipts[i].RequestID == requestID {
			previous = &before.Receipts[i]
		}
	}
	for i := range after.Receipts {
		if after.Receipts[i].RequestID == requestID {
			next = &after.Receipts[i]
		}
	}
	if previous == nil || next == nil || previous.SelectorDigest != next.SelectorDigest {
		return errors.New("replay lacks exact matching selector receipt")
	}
	expires := previous.DebitedAt.Add(runexplanationrate.ReceiptTTL)
	if next.DebitedAt.Equal(previous.DebitedAt) {
		if !lower.Before(expires) || !reflect.DeepEqual(before, after) {
			return errors.New("retained replay expired or mutated rate authority")
		}
		return nil
	}
	if next.DebitedAt.Before(expires) || next.DebitedAt.Before(lower) || next.DebitedAt.After(upper) {
		return errors.New("replay charged before receipt expiration or outside native clock interval")
	}
	// Independently derive the new debit from the prior authority. Pruning is
	// part of a confirmed new debit; a replay/denial never refreshes its TTL.
	want := before
	want.Receipts = nil
	tat := before.TheoreticalArrivalAt
	if !next.DebitedAt.Before(before.ExpiresAt) {
		tat = next.DebitedAt
	} else {
		for _, receipt := range before.Receipts {
			if next.DebitedAt.Sub(receipt.DebitedAt) < runexplanationrate.ReceiptTTL {
				want.Receipts = append(want.Receipts, receipt)
			}
		}
	}
	if tat.After(next.DebitedAt.Add(runexplanationrate.Interval)) {
		return errors.New("expired replay debited before shared eligibility")
	}
	if tat.Before(next.DebitedAt) {
		tat = next.DebitedAt
	}
	want.Receipts = append(want.Receipts, *next)
	want.TheoreticalArrivalAt = tat.Add(runexplanationrate.Interval)
	want.LastDebitAt = next.DebitedAt
	want.ExpiresAt = next.DebitedAt.Add(runexplanationrate.IdleTTL)
	if !reflect.DeepEqual(want, after) {
		return errors.New("expired replay debit differs from expected committed authority")
	}
	return nil
}

func TestRunExplanationYDBRatedReplayOracleChecksRetentionBoundary(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	before := runexplanationrate.Slot{Version: 1, IdentityDigest: "identity", TheoreticalArrivalAt: at.Add(runexplanationrate.Interval), LastDebitAt: at, ExpiresAt: at.Add(runexplanationrate.IdleTTL), Receipts: []runexplanationrate.Receipt{{RequestID: "replay", SelectorDigest: "selector", DebitedAt: at}}}
	expires := at.Add(runexplanationrate.ReceiptTTL)
	renew := func(debit time.Time) runexplanationrate.Slot {
		after := before
		after.Receipts = []runexplanationrate.Receipt{{RequestID: "replay", SelectorDigest: "selector", DebitedAt: debit}}
		after.TheoreticalArrivalAt = debit.Add(runexplanationrate.Interval)
		after.LastDebitAt = debit
		after.ExpiresAt = debit.Add(runexplanationrate.IdleTTL)
		return after
	}
	for _, test := range []struct {
		name                string
		after               runexplanationrate.Slot
		lower, upper, exact time.Time
		wantError           bool
	}{
		{name: "live_receipt_unchanged", after: before, lower: at.Add(time.Second), upper: at.Add(2 * time.Second)},
		{name: "expired_receipt_renewed", after: renew(expires), lower: expires, upper: expires.Add(time.Second)},
		{name: "early_double_debit", after: renew(expires.Add(-time.Second)), lower: expires.Add(-2 * time.Second), upper: expires, wantError: true},
		{name: "expired_receipt_retained", after: before, lower: expires, upper: expires.Add(time.Second), wantError: true},
		{name: "opaque_boundary_interval_retained", after: before, lower: expires.Add(-time.Second), upper: expires.Add(time.Second)},
		{name: "opaque_boundary_interval_renewed", after: renew(expires), lower: expires.Add(-time.Second), upper: expires.Add(time.Second)},
		{name: "exact_clock_disambiguates_retained", after: before, lower: expires.Add(-time.Second), upper: expires.Add(time.Second), exact: expires, wantError: true},
		{name: "debit_outside_native_interval", after: renew(expires), lower: expires.Add(time.Second), upper: expires.Add(2 * time.Second), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ratedExplanationReplayOracle(before, test.after, "replay", test.lower, test.upper, test.exact); (err != nil) != test.wantError {
				t.Errorf("replay clock oracle err=%v want_error=%t", err, test.wantError)
			}
		})
	}
}

func TestRunExplanationYDBRatedAdmissionOracleRejectsSameWindowOversubscription(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		thirdOffset time.Duration
		wantError   bool
	}{
		{name: "same_window_oversubscription", thirdOffset: time.Second, wantError: true},
		{name: "genuine_refill", thirdOffset: runexplanationrate.Interval},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcomes := make(map[string]ports.RunExplanationReadOutcomeV1)
			slot := runexplanationrate.Slot{}
			for i, offset := range []time.Duration{0, 0, test.thirdOffset} {
				requestID := fmt.Sprintf("oracle-%d", i)
				debit := at.Add(offset)
				slot.Receipts = append(slot.Receipts, runexplanationrate.Receipt{RequestID: requestID, DebitedAt: debit})
				outcomes[requestID] = ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadNotFoundV1}
			}
			slot.TheoreticalArrivalAt = at.Add(3 * runexplanationrate.Interval)
			slot.LastDebitAt = at.Add(test.thirdOffset)
			slot.ExpiresAt = slot.LastDebitAt.Add(runexplanationrate.IdleTTL)
			if err := ratedExplanationAdmissionOracle(slot, outcomes, nil); (err != nil) != test.wantError {
				t.Errorf("admission oracle third_offset=%v err=%v want_error=%t", test.thirdOffset, err, test.wantError)
			}
			// A lost commit acknowledgement is not a confirmed success, but
			// cannot exempt its committed debit from the safety recurrence.
			delete(outcomes, "oracle-2")
			if err := ratedExplanationAdmissionOracle(slot, outcomes, map[string]bool{"oracle-2": true}); (err != nil) != test.wantError {
				t.Errorf("uncertain-commit oracle third_offset=%v err=%v want_error=%t", test.thirdOffset, err, test.wantError)
			}
		})
	}
}

func TestRunExplanationYDBRatedTwoClientsBurstReplayAndNoActivity(t *testing.T) {
	f := newRatedExplanationYDBFixture(t)
	runID := f.ingress.Run.ID
	first := readRatedExplanationYDB(t, f, f.store, "rated-first", runID, ports.RunExplanationReadSuccessV1)
	second := readRatedExplanationYDB(t, f, f.second, "rated-second", runID, ports.RunExplanationReadSuccessV1)
	// Keep the actual burst adjacent: no replay or fixture probes between
	// its two allowed debits and third request.
	third, err := f.store.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, "rated-third", runID)
	if err != nil || third.Validate() != nil || third.Kind != ports.RunExplanationReadRateLimitedV1 && third.Kind != ports.RunExplanationReadSuccessV1 {
		t.Fatalf("third cross-client burst outcome=%+v err=%v want confirmed success/refill or rate denial", third, err)
	}
	_, before := ownedRatedExplanationSlot(t, f)
	if err := ratedExplanationAdmissionOracle(before, map[string]ports.RunExplanationReadOutcomeV1{"rated-first": first, "rated-second": second, "rated-third": third}, nil); err != nil {
		t.Fatalf("cross-client burst violated committed timestamp oracle: %v", err)
	}
	if third.Kind == ports.RunExplanationReadSuccessV1 {
		t.Log("third request legally charged after shared eligibility, proven by committed debit timestamp")
	}
	lower := ratedExplanationYDBClock(t, f)
	replay := readRatedExplanationYDB(t, f, f.second, "rated-first", runID, ports.RunExplanationReadSuccessV1)
	upper := ratedExplanationYDBClock(t, f)
	assertExplanationSnapshot(t, *replay.Explanation, *first.Explanation)
	_, afterReplay := ownedRatedExplanationSlot(t, f)
	if err := ratedExplanationReplayOracle(before, afterReplay, "rated-first", lower, upper, replay.Explanation.ReadAt); err != nil {
		t.Errorf("committed same-ID replay violated clock/receipt oracle: %v", err)
	}
	// Exact-selector mismatch and its 30s boundary are tested with a fixed
	// clock in the limiter and the actual SQL adapter's retry fixtures. A later
	// native mismatched-selector request cannot assume the receipt is live.
	assertRatedExplanationWebSessionUnchanged(t, f)
}

func TestRunExplanationYDBRatedCommittedNotFoundConsumesDebit(t *testing.T) {
	for _, target := range []string{"missing", "nonparticipant"} {
		t.Run(target, func(t *testing.T) {
			f := newRatedExplanationYDBFixture(t)
			runID := f.ingress.Run.ID
			if target == "missing" {
				runID = "missing-run"
			} else if _, err := f.client.DB.ExecContext(f.ctx, `DELETE FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, f.auth.TenantID, f.ingress.Run.SessionID, f.auth.UserID); err != nil {
				t.Fatal(err)
			}
			first := readRatedExplanationYDB(t, f, f.store, "not-found-first", runID, ports.RunExplanationReadNotFoundV1)
			second := readRatedExplanationYDB(t, f, f.second, "not-found-second", runID, ports.RunExplanationReadNotFoundV1)
			third, err := f.store.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, "not-found-third", runID)
			if err != nil || third.Validate() != nil || third.Kind != ports.RunExplanationReadRateLimitedV1 && third.Kind != ports.RunExplanationReadNotFoundV1 {
				t.Fatalf("third opaque404 burst outcome=%+v err=%v", third, err)
			}
			_, charged := ownedRatedExplanationSlot(t, f)
			if err := ratedExplanationAdmissionOracle(charged, map[string]ports.RunExplanationReadOutcomeV1{"not-found-first": first, "not-found-second": second, "not-found-third": third}, nil); err != nil {
				t.Fatalf("committed opaque404 violated timestamp oracle: %v", err)
			}
			lower := ratedExplanationYDBClock(t, f)
			readRatedExplanationYDB(t, f, f.second, "not-found-first", runID, ports.RunExplanationReadNotFoundV1)
			upper := ratedExplanationYDBClock(t, f)
			_, replayed := ownedRatedExplanationSlot(t, f)
			if err := ratedExplanationReplayOracle(charged, replayed, "not-found-first", lower, upper, time.Time{}); err != nil {
				t.Errorf("opaque404 replay violated clock/receipt oracle: %v", err)
			}
			for _, receipt := range charged.Receipts {
				boundary := receipt.DebitedAt.Add(runexplanationrate.ReceiptTTL)
				if receipt.RequestID == "not-found-first" && lower.Before(boundary) && !upper.Before(boundary) {
					t.Log("opaque404 clock interval straddled receipt expiry: interval consistency only; exact boundary covered by deterministic fixtures")
				}
			}
			assertRatedExplanationWebSessionUnchanged(t, f)
		})
	}
}

func TestRunExplanationYDBRatedCurrentAuthBeforeDenialAndReplay(t *testing.T) {
	for _, mutation := range []struct {
		name string
		want error
	}{
		{name: "revoked", want: domain.ErrWebSessionRevoked},
		{name: "version_changed", want: domain.ErrMembershipVersionChanged},
		{name: "membership_removed", want: domain.ErrMembershipDenied},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			f := newRatedExplanationYDBFixture(t)
			readRatedExplanationYDB(t, f, f.store, "auth-first", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
			readRatedExplanationYDB(t, f, f.second, "auth-second", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
			_, before := ownedRatedExplanationSlot(t, f)
			if mutation.name == "revoked" {
				if err := f.store.RevokeWebSession(f.ctx, f.web.SessionDigest, f.now); err != nil {
					t.Fatal(err)
				}
			} else {
				bucket, err := ydbpartition.BucketV1(string(f.auth.UserID))
				if err != nil {
					t.Fatal(err)
				}
				if mutation.name == "membership_removed" {
					if _, err := f.client.DB.ExecContext(f.ctx, `DELETE FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, bucket, f.auth.UserID, f.auth.TenantID); err != nil {
						t.Fatal(err)
					}
				} else {
					membership := domain.TenantMembership{TenantID: f.auth.TenantID, UserID: f.auth.UserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 2, CreatedAt: f.web.IssuedAt, UpdatedAt: f.now}
					encoded, err := json.Marshal(membership)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE tenant_memberships SET record=CAST($1 AS JsonDocument) WHERE user_bucket=$2 AND user_id=$3 AND tenant_id=$4`, string(encoded), bucket, f.auth.UserID, f.auth.TenantID); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, requestID := range []string{"auth-third", "auth-first"} {
				result, err := f.second.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, requestID, f.ingress.Run.ID)
				if !errors.Is(err, mutation.want) || result.Kind != "" || result.Explanation != nil {
					t.Errorf("current authority before denial/replay request=%q got=%+v err=%v want=%v", requestID, result, err, mutation.want)
				}
			}
			_, after := ownedRatedExplanationSlot(t, f)
			if !reflect.DeepEqual(before, after) {
				t.Errorf("authorization failure changed shared debit: before=%+v after=%+v", before, after)
			}
			if mutation.name != "revoked" {
				assertRatedExplanationWebSessionUnchanged(t, f)
			}
		})
	}
}

func TestRunExplanationYDBRatedResourceValidationFailureRollsBackDebit(t *testing.T) {
	f := newRatedExplanationYDBFixture(t)
	readRatedExplanationYDB(t, f, f.store, "rollback-first", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
	_, before := ownedRatedExplanationSlot(t, f)
	var original string
	if err := f.client.DB.QueryRowContext(f.ctx, `SELECT CAST(payload AS String) FROM runs WHERE tenant_id=$1 AND run_id=$2`, f.auth.TenantID, f.ingress.Run.ID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	var invalid domain.Run
	if err := json.Unmarshal([]byte(original), &invalid); err != nil {
		t.Fatalf("decode fixture-owned canonical Run: %v", err)
	}
	// Preserve the exact tenant, Run and Session selectors. Removing them is
	// an intentionally opaque not-found, which correctly commits its debit;
	// an unsupported status instead reaches canonical-source validation.
	invalid.Status = domain.RunStatus("invalid_fixture_status")
	invalidRecord, err := json.Marshal(invalid)
	if err != nil {
		t.Fatalf("encode fixture-owned invalid-status Run: %v", err)
	}
	// This committed, fixture-owned invalid canonical row is discovered after
	// the tentative second debit. Its validation error must abort both, unlike
	// the explicitly committed opaque not-found outcome.
	if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE runs SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND run_id=$3`, string(invalidRecord), f.auth.TenantID, f.ingress.Run.ID); err != nil {
		t.Fatal(err)
	}
	result, err := f.second.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, "rollback-aborted", f.ingress.Run.ID)
	if !errors.Is(err, ports.ErrRunExplanationUnavailable) || result.Kind != "" {
		t.Errorf("invalid canonical resource outcome=%+v err=%v want unavailable", result, err)
	}
	_, after := ownedRatedExplanationSlot(t, f)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("known-aborted resource validation committed a debit: before=%+v after=%+v", before, after)
	}
	if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE runs SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND run_id=$3`, original, f.auth.TenantID, f.ingress.Run.ID); err != nil {
		t.Fatal(err)
	}
	readRatedExplanationYDB(t, f, f.second, "rollback-second", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
	_, committed := ownedRatedExplanationSlot(t, f)
	if len(committed.Receipts) != 2 || committed.Receipts[1].RequestID != "rollback-second" {
		t.Errorf("post-rollback debit receipts=%+v want only first and confirmed second", committed.Receipts)
	}
	assertRatedExplanationWebSessionUnchanged(t, f)
}

func TestRunExplanationYDBRatedTwoClientsCompeteForThirdDebit(t *testing.T) {
	f := newRatedExplanationYDBFixture(t)
	initial := readRatedExplanationYDB(t, f, f.store, "race-initial", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
	type result struct {
		request string
		outcome ports.RunExplanationReadOutcomeV1
		err     error
	}
	start := make(chan struct{})
	completed := make(chan result, 2)
	for i, store := range []*ydbstore.Store{f.store, f.second} {
		go func() {
			select {
			case <-start:
			case <-f.ctx.Done():
				completed <- result{err: f.ctx.Err()}
				return
			}
			requestID := fmt.Sprintf("race-contender-%d", i)
			outcome, err := store.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, requestID, f.ingress.Run.ID)
			completed <- result{request: requestID, outcome: outcome, err: err}
		}()
	}
	close(start)
	var success, denied, unavailable int
	outcomes := map[string]ports.RunExplanationReadOutcomeV1{"race-initial": initial}
	uncertain := make(map[string]bool)
	for range 2 {
		select {
		case result := <-completed:
			if result.err != nil {
				// A retry/budget exhaustion is a permitted 503, not a 429.
				// Its commit can be unknown: the committed receipt (if any)
				// must still satisfy the same timestamp safety oracle.
				if !errors.Is(result.err, ports.ErrRunExplanationUnavailable) || !reflect.DeepEqual(result.outcome, ports.RunExplanationReadOutcomeV1{}) {
					t.Errorf("third-debit contender=%q unexpected failure outcome=%+v err=%v", result.request, result.outcome, result.err)
				}
				uncertain[result.request] = true
				unavailable++
				t.Logf("contender=%q failed closed unavailable; not counted as a rate denial or retried", result.request)
				continue
			}
			if err := result.outcome.Validate(); err != nil {
				t.Errorf("third-debit contender=%q invalid outcome: %v", result.request, err)
			}
			outcomes[result.request] = result.outcome
			switch result.outcome.Kind {
			case ports.RunExplanationReadSuccessV1:
				success++
			case ports.RunExplanationReadRateLimitedV1:
				denied++
			default:
				t.Errorf("third-debit contender=%q unexpected outcome=%+v", result.request, result.outcome)
			}
		case <-f.ctx.Done():
			t.Fatal("third-debit contenders did not finish within fixture deadline")
		}
	}
	if success < 1 || success+denied+unavailable != 2 {
		t.Errorf("independent-client third-debit race successes=%d denials=%d unavailable=%d want at least one confirmed success and two accounted outcomes", success, denied, unavailable)
	}
	_, slot := ownedRatedExplanationSlot(t, f)
	if err := ratedExplanationAdmissionOracle(slot, outcomes, uncertain); err != nil {
		t.Errorf("independent-client race oversubscribed shared authority: %v", err)
	}
	if success == 2 {
		t.Log("both contenders legally charged only after clock-based refill, proven by ordered committed debit timestamps and shared TAT")
	}
	assertRatedExplanationWebSessionUnchanged(t, f)
}

func TestRunExplanationYDBRatedCorruptExpiredRowFailsClosedAndValidExpiryReuses(t *testing.T) {
	for _, corruption := range []string{"unsupported_version", "oversize", "expiry_column_mismatch", "valid_expiry"} {
		t.Run(corruption, func(t *testing.T) {
			f := newRatedExplanationYDBFixture(t)
			readRatedExplanationYDB(t, f, f.store, "expiry-original", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
			key, slot := ownedRatedExplanationSlot(t, f)
			// All temporal fields derive from the sampled YDB clock. Expiry
			// is moved into the past; no wall-clock sleep authorizes reuse.
			at := f.now.Add(-runexplanationrate.IdleTTL - time.Minute)
			slot.LastDebitAt = at
			slot.TheoreticalArrivalAt = at.Add(runexplanationrate.Interval)
			slot.ExpiresAt = at.Add(runexplanationrate.IdleTTL)
			slot.Receipts[0].DebitedAt = at
			encoded, err := runexplanationrate.EncodeSlot(slot)
			if err != nil {
				t.Fatal(err)
			}
			expiry := slot.ExpiresAt
			switch corruption {
			case "unsupported_version":
				encoded = []byte(strings.Replace(string(encoded), `"version":1`, `"version":99`, 1))
			case "oversize":
				encoded = []byte(fmt.Sprintf(`{"identity_digest":%q,"oversize":%q}`, f.identity, strings.Repeat("x", runexplanationrate.MaxRowBytes+1)))
			case "expiry_column_mismatch":
				expiry = expiry.Add(time.Second)
			}
			replaceOwnedRatedExplanationSlot(t, f, key, string(encoded), expiry)
			if corruption == "valid_expiry" {
				readRatedExplanationYDB(t, f, f.second, "expiry-new", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
				newKey, replacement := ownedRatedExplanationSlot(t, f)
				if newKey != key || len(replacement.Receipts) != 1 || replacement.Receipts[0].RequestID != "expiry-new" || !replacement.LastDebitAt.After(slot.ExpiresAt) {
					t.Errorf("logical-expiry reuse key=%d want=%d row=%+v", newKey, key, replacement)
				}
			} else if result, err := f.second.ReadRatedRunExplanationV1(f.ctx, f.web.SessionDigest, "expiry-corrupt", f.ingress.Run.ID); !errors.Is(err, ports.ErrRunExplanationUnavailable) || result.Kind != "" {
				t.Errorf("expired corrupt row kind=%s outcome=%+v err=%v want unavailable", corruption, result, err)
			}
			assertRatedExplanationWebSessionUnchanged(t, f)
		})
	}
}

func TestRunExplanationYDBRatedRateSlotReadPlanIsExactPoint(t *testing.T) {
	requireExplanationTrueTransactions(t)
	_, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var ast, plan string
	if err := client.DB.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ExplainQueryMode), `SELECT SUBSTRING(CAST(record AS String),0,8193), expire_at FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, uint32(0)).Scan(&ast, &plan); err != nil {
		t.Fatalf("explain bounded exact rate key: %v", err)
	}
	if err := validateBoundedQueryPlan(plan, queryPlanContract{operator: "TablePointLookup", table: "run_explanation_rate_slots_v1"}); err != nil {
		t.Fatalf("rate row/expiry exact-key plan: %v", err)
	}
}
