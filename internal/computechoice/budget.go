package computechoice

import (
	"errors"
	"math"
	"sync"
	"time"
)

const (
	MaxStatementsPerAttempt        = 64
	MaxStatementsTotal             = 96
	MaxOrdinaryRecordBytes         = 8 * 1024
	MaxConnectionRecordBytes       = 128 * 1024
	MaxMaterializedBytesPerAttempt = 768 * 1024
	MaxMaterializedBytesTotal      = 1024 * 1024
	ResourceDeadline               = 3 * time.Second
)

var ErrResourceBudgetExceeded = errors.New("compute choice resource budget exceeded")

type SourceRecordKind uint8

const (
	SourceOrdinary SourceRecordKind = iota + 1
	SourceConnection
)

// ResourceBudget owns no clock or authorization. One instance is shared by all
// Serializable retry callbacks; callers enforce ResourceDeadline independently.
// Copies share private state and cannot reset totals or manufacture a new budget.
// Construct once per operation, never once per retry.
type ResourceBudget struct{ state *resourceBudgetState }

type resourceBudgetState struct {
	mu       sync.Mutex
	snapshot BudgetSnapshot
}

// BudgetSnapshot is diagnostic counter evidence, not a public resource DTO.
type BudgetSnapshot struct {
	AttemptNumber     uint64
	AttemptStatements uint32
	TotalStatements   uint32
	AttemptBytes      uint64
	TotalBytes        uint64
	Active            bool
	Failed            bool
}

type AttemptBudget struct {
	state  *resourceBudgetState
	number uint64
}

func NewResourceBudget() *ResourceBudget {
	return &ResourceBudget{state: &resourceBudgetState{}}
}

// NewAttempt starts one callback only after its predecessor has finished.
// Defer Finish immediately on entry to a retry callback. Starting twice, using
// stale handles or exhaustion fail sticky; no reset can revive the operation.
func (budget *ResourceBudget) NewAttempt() (*AttemptBudget, error) {
	if budget == nil || budget.state == nil {
		return nil, ErrResourceBudgetExceeded
	}
	state := budget.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.snapshot.Failed || state.snapshot.Active || state.snapshot.AttemptNumber == math.MaxUint64 {
		state.snapshot.Failed = true
		return nil, ErrResourceBudgetExceeded
	}
	state.snapshot.AttemptNumber++
	state.snapshot.AttemptStatements = 0
	state.snapshot.AttemptBytes = 0
	state.snapshot.Active = true
	return &AttemptBudget{state: state, number: state.snapshot.AttemptNumber}, nil
}

// BeforeStatement charges before issuing SQL, including clock, authorization,
// limiter work and every source read. A denied statement must not be issued.
func (attempt *AttemptBudget) BeforeStatement() error {
	if attempt == nil || attempt.state == nil {
		return ErrResourceBudgetExceeded
	}
	state := attempt.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !attempt.validLocked() || state.snapshot.AttemptStatements >= MaxStatementsPerAttempt || state.snapshot.TotalStatements >= MaxStatementsTotal {
		state.snapshot.Failed = true
		return ErrResourceBudgetExceeded
	}
	state.snapshot.AttemptStatements++
	state.snapshot.TotalStatements++
	return nil
}

// RecordMaterialized charges the transferred database prefix before parsing,
// including ceiling+1 oversize detection bytes. Call once for each materialized
// record, not its decoded content size. A denial forbids any partial decoding.
func (attempt *AttemptBudget) RecordMaterialized(kind SourceRecordKind, transferredBytes int64) error {
	if attempt == nil || attempt.state == nil {
		return ErrResourceBudgetExceeded
	}
	state := attempt.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !attempt.validLocked() || transferredBytes < 0 {
		state.snapshot.Failed = true
		return ErrResourceBudgetExceeded
	}
	size := uint64(transferredBytes)
	if size > math.MaxUint64-state.snapshot.AttemptBytes || size > math.MaxUint64-state.snapshot.TotalBytes {
		state.snapshot.Failed = true
		return ErrResourceBudgetExceeded
	}
	state.snapshot.AttemptBytes += size
	state.snapshot.TotalBytes += size
	var ceiling uint64
	switch kind {
	case SourceOrdinary:
		ceiling = MaxOrdinaryRecordBytes
	case SourceConnection:
		ceiling = MaxConnectionRecordBytes
	default:
		state.snapshot.Failed = true
		return ErrResourceBudgetExceeded
	}
	if size > ceiling || state.snapshot.AttemptBytes > MaxMaterializedBytesPerAttempt || state.snapshot.TotalBytes > MaxMaterializedBytesTotal {
		state.snapshot.Failed = true
		return ErrResourceBudgetExceeded
	}
	return nil
}

// Finish closes the callback handle without clearing totals or a failure.
// An exhausted callback may still finish for deferred cleanup, but reports the
// same failure. Finished/stale copies cannot close or reset another attempt.
func (attempt *AttemptBudget) Finish() error {
	if attempt == nil || attempt.state == nil {
		return ErrResourceBudgetExceeded
	}
	state := attempt.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.snapshot.Active || state.snapshot.AttemptNumber != attempt.number {
		state.snapshot.Failed = true
		return ErrResourceBudgetExceeded
	}
	state.snapshot.Active = false
	if state.snapshot.Failed {
		return ErrResourceBudgetExceeded
	}
	return nil
}

func (attempt *AttemptBudget) validLocked() bool {
	return attempt.state.snapshot.Active && !attempt.state.snapshot.Failed && attempt.state.snapshot.AttemptNumber == attempt.number
}

func (budget *ResourceBudget) Snapshot() BudgetSnapshot {
	if budget == nil || budget.state == nil {
		return BudgetSnapshot{Failed: true}
	}
	budget.state.mu.Lock()
	defer budget.state.mu.Unlock()
	return budget.state.snapshot
}
