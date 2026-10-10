package ydbpartition

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"
	"time"
)

func admissionCostCutoverFixture(t *testing.T) (*harnessV2CutoverSQL, *sql.DB) {
	t.Helper()
	fixture := &harnessV2CutoverSQL{
		markerID: "admission-cost-v2-empty-cutover", markerTable: "managed_execution_authority_v2_cutover_state",
		clock: time.Date(2026, 10, 10, 17, 0, 0, 0, time.UTC),
		markers: map[string]time.Time{
			"harness-binding-v2-empty-cutover":             time.Unix(10, 0).UTC(),
			"managed-execution-authority-v2-empty-cutover": time.Unix(10, 0).UTC(),
		},
	}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close cutover fixture database: %v", err)
		}
	})
	return fixture, db
}

func TestAdmissionCostV2CutoverRequiresItsOwnEmptyBacklog(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"dispatch_outbox", "worker_jobs", "attempt_effect_reservations"} {
		t.Run(table, func(t *testing.T) {
			fixture, db := admissionCostCutoverFixture(t)
			fixture.filled = table
			_, err := verifyAdmissionCostV2Cutover(context.Background(), db, false)
			if err == nil || !strings.Contains(err.Error(), table) {
				t.Fatalf("filled %s error=%v, want exact backlog denial despite all prior markers", table, err)
			}
			if fixture.beginOptions.Isolation != driver.IsolationLevel(sql.LevelSerializable) ||
				fixture.writes != 0 || fixture.commits != 0 || fixture.rollbacks != 1 {
				t.Fatalf("denied transaction: isolation=%v writes=%d commits=%d rollbacks=%d", fixture.beginOptions.Isolation, fixture.writes, fixture.commits, fixture.rollbacks)
			}
		})
	}
}

func TestAdmissionCostV2CutoverUsesDatabaseClockAndCommitsDistinctMarker(t *testing.T) {
	t.Parallel()
	for _, dryRun := range []bool{true, false} {
		name := "commit"
		if dryRun {
			name = "dry_run"
		}
		t.Run(name, func(t *testing.T) {
			fixture, db := admissionCostCutoverFixture(t)
			result, err := verifyAdmissionCostV2Cutover(context.Background(), db, dryRun)
			if err != nil || result.Table != "admission_cost_cutover_v2" {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			wantQueries, wantCommits := 5, 1
			if dryRun {
				wantQueries, wantCommits = 4, 0
			}
			if len(fixture.queries) != wantQueries || fixture.writes != wantCommits || fixture.commits != wantCommits {
				t.Fatalf("queries=%v writes=%d commits=%d, want %d/%d/%d", fixture.queries, fixture.writes, fixture.commits, wantQueries, wantCommits, wantCommits)
			}
		})
	}
}

func TestAdmissionCostV2CutoverExistingReceiptAndFaultsFailClosed(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"committed", "zero_marker", "query_failure", "zero_clock", "write_failure", "commit_failure"} {
		t.Run(name, func(t *testing.T) {
			fixture, db := admissionCostCutoverFixture(t)
			switch name {
			case "committed":
				fixture.markers[fixture.markerID] = fixture.clock
				fixture.filled = "worker_jobs"
			case "zero_marker":
				fixture.markers[fixture.markerID] = time.Time{}
			case "query_failure":
				fixture.failQuery = true
			case "zero_clock":
				fixture.clock = time.Time{}
			case "write_failure":
				fixture.failExec = true
			case "commit_failure":
				fixture.failCommit = true
			}
			_, err := verifyAdmissionCostV2Cutover(context.Background(), db, false)
			if (err == nil) != (name == "committed") {
				t.Fatalf("scenario=%s error=%v, want success only for committed receipt", name, err)
			}
			wantWrites, wantCommits := 0, 0
			if name == "commit_failure" {
				wantWrites, wantCommits = 1, 1
			}
			if fixture.writes != wantWrites || fixture.commits != wantCommits {
				t.Fatalf("%s writes=%d commits=%d, want %d/%d", name, fixture.writes, fixture.commits, wantWrites, wantCommits)
			}
			if (name == "committed" || name == "zero_marker") && len(fixture.queries) != 1 {
				t.Fatalf("existing receipt rechecked or mutated live backlog: %v", fixture.queries)
			}
		})
	}
}
