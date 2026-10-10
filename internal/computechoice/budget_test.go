package computechoice

import (
	"math"
	"sync"
	"testing"
	"time"
)

func mustBudgetAttempt(t *testing.T, budget *ResourceBudget) *AttemptBudget {
	t.Helper()
	attempt, err := budget.NewAttempt()
	if err != nil {
		t.Fatalf("NewAttempt: %v snapshot%+v", err, budget.Snapshot())
	}
	return attempt
}

func assertBudgetDenied(t *testing.T, err error) {
	t.Helper()
	if err != ErrResourceBudgetExceeded || err.Error() != "compute choice resource budget exceeded" {
		t.Fatalf("got %v; want content-free ErrResourceBudgetExceeded", err)
	}
}

func assertBudgetSticky(t *testing.T, budget *ResourceBudget, attempt *AttemptBudget) {
	t.Helper()
	before := budget.Snapshot()
	assertBudgetDenied(t, attempt.BeforeStatement())
	assertBudgetDenied(t, attempt.RecordMaterialized(SourceOrdinary, 1))
	assertBudgetDenied(t, attempt.Finish())
	if next, err := budget.NewAttempt(); next != nil || err != ErrResourceBudgetExceeded {
		t.Fatalf("failure revived retry: %v %v", next, err)
	}
	after := budget.Snapshot()
	if !after.Failed || after.TotalStatements != before.TotalStatements || after.TotalBytes != before.TotalBytes {
		t.Fatalf("failed operation gained work: before%+v after%+v", before, after)
	}
}

func TestResourceBudgetStatementBoundsAndRetryTotals(t *testing.T) {
	t.Run("attempt", func(t *testing.T) {
		budget := NewResourceBudget()
		attempt := mustBudgetAttempt(t, budget)
		for i := 0; i < MaxStatementsPerAttempt; i++ {
			if err := attempt.BeforeStatement(); err != nil {
				t.Fatalf("statement%d denied before ceiling: %v", i+1, err)
			}
		}
		assertBudgetDenied(t, attempt.BeforeStatement())
		if snapshot := budget.Snapshot(); snapshot.TotalStatements != 64 || snapshot.AttemptStatements != 64 {
			t.Fatalf("denied statement was charged/executed: %+v", snapshot)
		}
		assertBudgetSticky(t, budget, attempt)
	})
	t.Run("aggregate", func(t *testing.T) {
		budget := NewResourceBudget()
		for round, count := range []int{64, 32} {
			attempt := mustBudgetAttempt(t, budget)
			for i := 0; i < count; i++ {
				if err := attempt.BeforeStatement(); err != nil {
					t.Fatalf("round%d statement%d: %v", round+1, i+1, err)
				}
			}
			if round == 1 {
				assertBudgetDenied(t, attempt.BeforeStatement())
				if snapshot := budget.Snapshot(); snapshot.TotalStatements != 96 || snapshot.AttemptStatements != 32 || snapshot.AttemptNumber != 2 {
					t.Fatalf("retry aggregate escaped: %+v", snapshot)
				}
				assertBudgetSticky(t, budget, attempt)
			} else if err := attempt.Finish(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestResourceBudgetRecordCeilingsAndPreparseCharging(t *testing.T) {
	for _, test := range []struct {
		name string
		kind SourceRecordKind
		cap  int64
	}{
		{"ordinary", SourceOrdinary, MaxOrdinaryRecordBytes},
		{"connection", SourceConnection, MaxConnectionRecordBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, size := range []int64{0, test.cap - 1, test.cap, test.cap + 1} {
				budget := NewResourceBudget()
				attempt := mustBudgetAttempt(t, budget)
				parsed := false
				err := attempt.RecordMaterialized(test.kind, size)
				if err == nil {
					parsed = true
				}
				if snapshot := budget.Snapshot(); snapshot.TotalBytes != uint64(size) || snapshot.AttemptBytes != uint64(size) {
					t.Fatalf("prefix size%d not charged before parsing: %+v", size, snapshot)
				}
				if size > test.cap {
					assertBudgetDenied(t, err)
					if parsed {
						t.Fatal("oversize prefix reached parser")
					}
					assertBudgetSticky(t, budget, attempt)
				} else if err != nil {
					t.Fatalf("size%d denied within record ceiling%d: %v", size, test.cap, err)
				}
			}
		})
	}
}

func TestResourceBudgetMaterializedAggregateBounds(t *testing.T) {
	t.Run("attempt", func(t *testing.T) {
		budget := NewResourceBudget()
		attempt := mustBudgetAttempt(t, budget)
		for i := 0; i < 6; i++ {
			if err := attempt.RecordMaterialized(SourceConnection, MaxConnectionRecordBytes); err != nil {
				t.Fatalf("record%d: %v", i+1, err)
			}
		}
		if got := budget.Snapshot().AttemptBytes; got != MaxMaterializedBytesPerAttempt {
			t.Fatalf("exact attempt boundary bytes%d", got)
		}
		assertBudgetDenied(t, attempt.RecordMaterialized(SourceOrdinary, 1))
		if got := budget.Snapshot().TotalBytes; got != MaxMaterializedBytesPerAttempt+1 {
			t.Fatalf("oversize aggregate prefix lost: %d", got)
		}
		assertBudgetSticky(t, budget, attempt)
	})
	t.Run("retry_total", func(t *testing.T) {
		budget := NewResourceBudget()
		for round := 0; round < 2; round++ {
			attempt := mustBudgetAttempt(t, budget)
			if snapshot := budget.Snapshot(); snapshot.AttemptBytes != 0 || snapshot.AttemptStatements != 0 {
				t.Fatalf("new callback counters not reset: %+v", snapshot)
			}
			for i := 0; i < 4; i++ {
				if err := attempt.RecordMaterialized(SourceConnection, MaxConnectionRecordBytes); err != nil {
					t.Fatalf("round%d record%d: %v", round+1, i+1, err)
				}
			}
			if round == 1 {
				if snapshot := budget.Snapshot(); snapshot.AttemptBytes != 512*1024 || snapshot.TotalBytes != MaxMaterializedBytesTotal {
					t.Fatalf("retry totals reset or boundary rejected: %+v", snapshot)
				}
				assertBudgetDenied(t, attempt.RecordMaterialized(SourceOrdinary, 1))
				if got := budget.Snapshot().TotalBytes; got != MaxMaterializedBytesTotal+1 {
					t.Fatalf("retry oversize transferred prefix lost: %d", got)
				}
				assertBudgetSticky(t, budget, attempt)
			} else if err := attempt.Finish(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestResourceBudgetCannotResetOrUseStaleCopies(t *testing.T) {
	t.Run("active_double_start", func(t *testing.T) {
		budget := NewResourceBudget()
		attempt := mustBudgetAttempt(t, budget)
		if err := attempt.BeforeStatement(); err != nil {
			t.Fatal(err)
		}
		copyBudget := *budget
		if next, err := copyBudget.NewAttempt(); next != nil || err != ErrResourceBudgetExceeded {
			t.Fatalf("active reset escaped via copy: %v %v", next, err)
		}
		assertBudgetSticky(t, budget, attempt)
	})
	t.Run("finished_copy", func(t *testing.T) {
		budget := NewResourceBudget()
		first := mustBudgetAttempt(t, budget)
		copyAttempt := *first
		if err := first.BeforeStatement(); err != nil {
			t.Fatal(err)
		}
		if err := first.Finish(); err != nil {
			t.Fatal(err)
		}
		second := mustBudgetAttempt(t, budget)
		assertBudgetDenied(t, copyAttempt.RecordMaterialized(SourceOrdinary, 1))
		if got := budget.Snapshot().TotalStatements; got != 1 {
			t.Fatalf("retry erased shared total: %d", got)
		}
		assertBudgetSticky(t, budget, second)
	})
	t.Run("double_finish", func(t *testing.T) {
		budget := NewResourceBudget()
		first := mustBudgetAttempt(t, budget)
		if err := first.Finish(); err != nil {
			t.Fatal(err)
		}
		second := mustBudgetAttempt(t, budget)
		assertBudgetDenied(t, first.Finish())
		assertBudgetSticky(t, budget, second)
	})
	t.Run("snapshot_is_copy", func(t *testing.T) {
		budget := NewResourceBudget()
		attempt := mustBudgetAttempt(t, budget)
		if err := attempt.BeforeStatement(); err != nil {
			t.Fatal(err)
		}
		snapshot := budget.Snapshot()
		snapshot.TotalStatements = 0
		snapshot.Active = false
		if got := budget.Snapshot(); got.TotalStatements != 1 || !got.Active {
			t.Fatalf("snapshot mutation altered budget: %+v", got)
		}
	})
}

func TestResourceBudgetInvalidCountsAndZeroValuesFailClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		kind SourceRecordKind
		size int64
		want uint64
	}{
		{"negative", SourceOrdinary, -1, 0},
		{"min_int", SourceOrdinary, math.MinInt64, 0},
		{"huge_positive", SourceOrdinary, math.MaxInt64, math.MaxInt64},
		{"kind_zero", 0, 1, 1}, {"kind_unknown", 255, 8, 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			budget := NewResourceBudget()
			attempt := mustBudgetAttempt(t, budget)
			assertBudgetDenied(t, attempt.RecordMaterialized(test.kind, test.size))
			if got := budget.Snapshot().TotalBytes; got != test.want {
				t.Fatalf("count wrapped/disappeared: got%d want%d", got, test.want)
			}
			assertBudgetSticky(t, budget, attempt)
		})
	}
	for _, budget := range []*ResourceBudget{nil, {}} {
		if attempt, err := budget.NewAttempt(); attempt != nil || err != ErrResourceBudgetExceeded || !budget.Snapshot().Failed {
			t.Fatalf("unconstructed budget accepted: %v %v", attempt, err)
		}
	}
	for _, attempt := range []*AttemptBudget{nil, {}} {
		assertBudgetDenied(t, attempt.BeforeStatement())
		assertBudgetDenied(t, attempt.RecordMaterialized(SourceOrdinary, 0))
		assertBudgetDenied(t, attempt.Finish())
	}
	if ResourceDeadline != 3*time.Second {
		t.Fatalf("resource deadline changed: %v", ResourceDeadline)
	}
}

func TestResourceBudgetOverflowGuards(t *testing.T) {
	// Public caps fail long before uint64 overflow. Synthetic near-overflow
	// counters exercise the additional checked-addition guard directly, proving
	// it cannot wrap a corrupted counter into renewed headroom.
	for _, axis := range []string{"attempt", "total"} {
		t.Run(axis, func(t *testing.T) {
			budget := NewResourceBudget()
			attempt := mustBudgetAttempt(t, budget)
			if axis == "attempt" {
				budget.state.snapshot.AttemptBytes = math.MaxUint64
			} else {
				budget.state.snapshot.TotalBytes = math.MaxUint64
			}
			before := budget.Snapshot()
			assertBudgetDenied(t, attempt.RecordMaterialized(SourceOrdinary, 1))
			if got := budget.Snapshot(); got.AttemptBytes != before.AttemptBytes || got.TotalBytes != before.TotalBytes || !got.Failed {
				t.Fatalf("overflow wrapped or partially charged: before%+v after%+v", before, got)
			}
			assertBudgetSticky(t, budget, attempt)
		})
	}
	t.Run("attempt_number", func(t *testing.T) {
		budget := NewResourceBudget()
		budget.state.snapshot.AttemptNumber = math.MaxUint64
		if attempt, err := budget.NewAttempt(); attempt != nil || err != ErrResourceBudgetExceeded {
			t.Fatalf("attempt number wrapped: %v %v", attempt, err)
		}
		if snapshot := budget.Snapshot(); snapshot.AttemptNumber != math.MaxUint64 || !snapshot.Failed {
			t.Fatalf("attempt overflow escaped: %+v", snapshot)
		}
	})
}

func TestResourceBudgetExactByteCeilingDoesNotForbidZeroByteWork(t *testing.T) {
	budget := NewResourceBudget()
	for round := 0; round < 2; round++ {
		attempt := mustBudgetAttempt(t, budget)
		for range 4 {
			if err := attempt.RecordMaterialized(SourceConnection, MaxConnectionRecordBytes); err != nil {
				t.Fatal(err)
			}
		}
		if err := attempt.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	// Starting a callback adds no statement or byte. Exact ceilings are valid;
	// only an additional positive materialization exceeds the total limit.
	attempt := mustBudgetAttempt(t, budget)
	if err := attempt.BeforeStatement(); err != nil {
		t.Fatal(err)
	}
	if err := attempt.RecordMaterialized(SourceOrdinary, 0); err != nil {
		t.Fatal(err)
	}
	assertBudgetDenied(t, attempt.RecordMaterialized(SourceOrdinary, 1))
	assertBudgetSticky(t, budget, attempt)
}

func TestResourceBudgetConcurrentCopiesCannotOverrun(t *testing.T) {
	budget := NewResourceBudget()
	attempt := mustBudgetAttempt(t, budget)
	var workers sync.WaitGroup
	errors := make(chan error, 64)
	for range 32 {
		workers.Go(func() {
			copyAttempt := *attempt
			for range 2 {
				errors <- copyAttempt.BeforeStatement()
			}
		})
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("valid concurrent statement denied: %v", err)
		}
	}
	if snapshot := budget.Snapshot(); snapshot.TotalStatements != 64 || snapshot.AttemptStatements != 64 {
		t.Fatalf("shared copies lost counters: %+v", snapshot)
	}
	assertBudgetDenied(t, attempt.BeforeStatement())
	assertBudgetSticky(t, budget, attempt)
}
