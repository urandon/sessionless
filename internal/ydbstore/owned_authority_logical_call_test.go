package ydbstore

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func TestOwnedAuthorityOriginalBodySharesMetadataBudget(t *testing.T) {
	t.Run("body_then_SQL_exact_aggregate", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		if err := chargeOwnedAuthorityBody(ctx, ownedAuthorityMaxBytes-8); err != nil {
			t.Fatal("bounded original body", err)
		}
		if err := f.store.ownedAuthorityTx(ctx, func(context.Context, *ownedAuthorityQuery) error { return nil }); err != nil {
			t.Fatal("exact remaining SQL clock bytes", err)
		}
		b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if b.bytes != ownedAuthorityMaxBytes || f.driver.commits != 1 {
			t.Fatalf("body and SQL do not share budget: bytes=%d commits=%d", b.bytes, f.driver.commits)
		}
		if err := chargeOwnedAuthorityBody(ctx, 1); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("one excess metadata byte admitted: %v", err)
		}
		if err := chargeOwnedAuthorityBody(ctx, 0); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("sticky exhaustion cleared on zero bytes: %v", err)
		}
	})
	t.Run("overflow_never_wraps_or_restarts", func(t *testing.T) {
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		if err := chargeOwnedAuthorityBody(ctx, math.MaxUint64); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("MaxUint64 accepted: %v", err)
		}
		b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if b.bytes != 0 || !b.failed || chargeOwnedAuthorityBody(ctx, 1) == nil {
			t.Fatalf("overflow changed/renewed budget: bytes=%d failed=%t", b.bytes, b.failed)
		}
	})
	t.Run("no_body_IO_inside_callback", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, _ *ownedAuthorityQuery) error {
			return chargeOwnedAuthorityBody(ctx, 1)
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || f.driver.commits != 0 || f.driver.execCalls != 0 {
			t.Fatalf("body callback failed to abort: err=%v commits=%d writes=%d", err, f.driver.commits, f.driver.execCalls)
		}
	})
	t.Run("absent_or_cancelled_context_denied", func(t *testing.T) {
		if err := chargeOwnedAuthorityBody(t.Context(), 0); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("missing logical context silently admitted: %v", err)
		}
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		cancel()
		if err := chargeOwnedAuthorityBody(ctx, 1); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatalf("cancelled original body admitted: %v", err)
		}
		b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if b.bytes != 0 {
			t.Fatalf("cancelled read mutated metadata bytes=%d", b.bytes)
		}
	})
}

func TestOwnedAuthorityLogicalCallSharesAllHelperTransactions(t *testing.T) {
	t.Run("three_callbacks_and_96_statements_then_no_write", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		var original *ownedAuthorityBudget
		for phase := 0; phase < ownedAuthorityMaxCallbacks; phase++ {
			if err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
				if original == nil {
					original = q.budget
				} else if q.budget != original {
					t.Fatal("helper transaction reset the logical-call budget")
				}
				for statement := 1; statement < ownedAuthorityMaxStatements; statement++ {
					var at time.Time
					if err := q.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
						return err
					}
					if err := q.budget.materialized(q.callback, 8); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("valid bounded phase%d: %v", phase, err)
			}
		}
		called := false
		err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			called = true
			_, err := q.ExecContext(ctx, "forbidden_payload_write")
			return err
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || called || original.callbacks != 3 || original.statements != 96 || original.bytes != 96*8 || f.driver.execCalls != 0 || f.driver.commits != 3 || len(f.driver.statements) != 96 {
			t.Fatalf("logical aggregate err=%v callback=%t callbacks=%d statements=%d bytes=%d exec=%d commits=%d driverStatements=%d", err, called, original.callbacks, original.statements, original.bytes, f.driver.execCalls, f.driver.commits, len(f.driver.statements))
		}
	})
	t.Run("metadata_aggregate_not_reset_between_phases", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		for phase := 0; phase < 2; phase++ {
			if err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
				return q.budget.materialized(q.callback, ownedAuthorityMaxBytes/2-8)
			}); err != nil {
				t.Fatalf("valid half-budget phase%d: %v", phase, err)
			}
		}
		called := false
		err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			called = true
			_, err := q.ExecContext(ctx, "forbidden_payload_write")
			return err
		})
		b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if !errors.Is(err, domain.ErrMembershipDenied) || called || b.bytes != ownedAuthorityMaxBytes || f.driver.execCalls != 0 || f.driver.commits != 2 {
			t.Fatalf("metadata aggregate err=%v callback=%t bytes=%d exec=%d commits=%d", err, called, b.bytes, f.driver.execCalls, f.driver.commits)
		}
	})
	t.Run("actual_sdk_retry_consumes_later_phase_budget", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		f.driver.commitModes = []string{"rollback", ""}
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		for phase := 0; phase < 2; phase++ {
			if err := f.store.ownedAuthorityTx(ctx, func(context.Context, *ownedAuthorityQuery) error { return nil }); err != nil {
				t.Fatalf("phase%d including SDK retry: %v", phase, err)
			}
		}
		called := false
		err := f.store.ownedAuthorityTx(ctx, func(context.Context, *ownedAuthorityQuery) error { called = true; return nil })
		b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if !errors.Is(err, domain.ErrMembershipDenied) || called || b.callbacks != 3 || b.retries != 1 || f.driver.commits != 3 || f.driver.execCalls != 0 {
			t.Fatalf("retry aggregate err=%v callback=%t callbacks=%d retries=%d commits=%d exec=%d", err, called, b.callbacks, b.retries, f.driver.commits, f.driver.execCalls)
		}
	})
	t.Run("helper_deadline_preserves_original_absolute_window", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newOwnedReaderFixture(t)
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			original, _ := ctx.Deadline()
			// Logical fake time, not real-time synchronization around external I/O.
			time.Sleep(4 * time.Second)
			if err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
				actual, ok := ctx.Deadline()
				if !ok || !actual.Equal(original) {
					t.Fatalf("helper renewed deadline: got=%v want=%v", actual, original)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(ownedAuthorityDeadline)
			synctest.Wait()
			called := false
			err := f.store.ownedAuthorityTx(ctx, func(context.Context, *ownedAuthorityQuery) error { called = true; return nil })
			if !errors.Is(err, domain.ErrMembershipDenied) || called || f.driver.commits != 1 || len(f.driver.statements) != 1 {
				t.Fatalf("expired logical call touched driver/history: err=%v callback=%t commits=%d statements=%d", err, called, f.driver.commits, len(f.driver.statements))
			}
		})
	})
}

type ownedLogicalDeadlineBodyKeeper struct {
	entered  bool
	deadline time.Time
}

func (k *ownedLogicalDeadlineBodyKeeper) ReadAttemptOperationBody(ctx context.Context, _ domain.AttemptOperationScopeV1, _ string, _ uint64) ([]byte, error) {
	k.entered = true
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, domain.ErrMembershipDenied
	}
	k.deadline = deadline
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestOwnedAuthorityLogicalBeforeSendBoundsOutsideSQLBodyRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOwnedAttemptEffectParentFixture(t)
		r := ownedResidualInventoryFixture(t).Operations[0].Reservation
		issuer, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return r.ReservedAt }, strings.NewReader(strings.Repeat("logical-deadline-key", 4)), 16)
		if err != nil {
			t.Fatal(err)
		}
		capability, err := issuer.MintAttemptOperationCapability(r, r.ReservedAt, r.Operation.Deadline)
		if err != nil {
			t.Fatal("authentic capability prerequisite", err)
		}
		keeper := &ownedLogicalDeadlineBodyKeeper{}
		f.store.attemptOperationIssuer, f.store.attemptOperationBodies = issuer, keeper
		beforeStatements, start := len(f.graph.statements), time.Now()
		err = f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), capability)
		if !errors.Is(err, domain.ErrMembershipDenied) || !keeper.entered || !keeper.deadline.Equal(start.Add(ownedAuthorityDeadline)) || time.Since(start) != ownedAuthorityDeadline || len(f.graph.statements) != beforeStatements {
			t.Fatalf("outside SQL deadline err=%v entered=%t deadline=%v expected=%v elapsed=%v statements=%d/%d", err, keeper.entered, keeper.deadline, start.Add(ownedAuthorityDeadline), time.Since(start), len(f.graph.statements), beforeStatements)
		}
		// The expired body read must not reach SQL or consume send authority.
		if err := issuer.ConsumeAttemptOperationCapability(capability); err != nil {
			t.Fatal("failed before-send consumed the authentic one-use capability", err)
		}
	})
}
