package ydbstore

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerOfferDiagnosticPreservesConflictAndUsesClosedStages(t *testing.T) {
	cases := []struct {
		status ports.AttachedWorkerExecutionStatus
		stage  attachedWorkerOfferConflictStage
	}{
		{ports.AttachedWorkerExecutionDenied, attachedWorkerOfferStageStatusDenied},
		{ports.AttachedWorkerExecutionNotFound, attachedWorkerOfferStageStatusNotFound},
		{ports.AttachedWorkerExecutionConflict, attachedWorkerOfferStageStatusConflict},
		{ports.AttachedWorkerExecutionExpired, attachedWorkerOfferStageStatusExpired},
		{ports.AttachedWorkerExecutionFenced, attachedWorkerOfferStageStatusFenced},
		{"unexpected-private-status", attachedWorkerOfferStageStatusOther},
	}
	for _, test := range cases {
		t.Run(string(test.status), func(t *testing.T) {
			err := attachedWorkerOfferConflictAt(attachedWorkerOfferStatusStage(test.status))
			if !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Fatalf("diagnostic lost conflict classification: %v", err)
			}
			var diagnostic attachedWorkerOfferConflictDiagnostic
			if !errors.As(err, &diagnostic) || diagnostic.stage != test.stage {
				t.Fatalf("diagnostic stage=%v want=%v", diagnostic.stage, test.stage)
			}
			if got, want := err.Error(), ErrAttachedWorkerAttemptConflict.Error()+": stage="+string(test.stage); got != want || strings.Contains(got, string(test.status)) && test.stage == attachedWorkerOfferStageStatusOther {
				t.Fatalf("diagnostic error=%q want fixed closed-stage message %q", got, want)
			}
		})
	}
}

func TestAttachedWorkerOfferStatusDiagnosticUsesActualSchedulerAndOfferPaths(t *testing.T) {
	for _, scheduler := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct_offer", true: "scheduler_admission"}[scheduler], func(t *testing.T) {
			f := newGenerationAllocationFixture(t, scheduler)
			workerKey := generationSQLKey(`SELECT record FROM attached_workers WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`)
			var worker domain.AttachedWorker
			if err := json.Unmarshal([]byte(f.db.generationSQLFixture.rows[workerKey][0].(string)), &worker); err != nil {
				t.Fatalf("decode worker fixture: %v", err)
			}
			worker.ObservedState = domain.AttachedWorkerObservedOffline
			f.db.generationSQLFixture.rows[workerKey] = []driver.Value{generationJSON(t, worker)}

			before := cloneGenerationAllocationRows(f.db.rows)
			if scheduler {
				_, err := f.admit(t)
				if !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
					t.Fatalf("scheduler error=%v want attached-worker conflict", err)
				}
				var diagnostic attachedWorkerOfferConflictDiagnostic
				if !errors.As(err, &diagnostic) || diagnostic.stage != attachedWorkerOfferStageStatusDenied {
					t.Fatalf("scheduler diagnostic=%v want denied offer status", err)
				}
				if f.db.commits != 0 || f.db.rollbacks != 1 || f.db.committed != 0 || !reflect.DeepEqual(before, f.db.rows) {
					t.Fatalf("denied scheduler offer committed state: commits=%d rollbacks=%d committed=%d", f.db.commits, f.db.rollbacks, f.db.committed)
				}
			} else {
				result, err := f.offerAttempt(t)
				if err != nil || result.Status != ports.AttachedWorkerExecutionDenied {
					t.Fatalf("direct offer result=%+v err=%v want denied status", result, err)
				}
				if f.db.commits != 1 || f.db.committed != 0 || f.db.writes != 0 || !reflect.DeepEqual(before, f.db.rows) {
					t.Fatalf("denied direct offer changed state: commits=%d writes=%d committed=%d", f.db.commits, f.db.writes, f.db.committed)
				}
			}
		})
	}
}
