package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func failureRunPayloadData(t *testing.T, run domain.Run) domain.Run {
	t.Helper()
	run.Status = domain.RunFailed
	at := run.UpdatedAt.Add(time.Second)
	run.UpdatedAt = at
	run.FinishedAt = &at
	run.TerminalCustody = runPayloadTerminalMetadata(t, run, false)
	c := run.TerminalCustody
	d := strings.Repeat("a", 64)
	run.TerminalFailure = &domain.RunTerminalFailureV1{Version: 1, Scope: c.Plan.Scope, ParentDigest: d, Plan: c.Plan, CapsuleSchema: "sessionless.terminal-failure-custody.v1", AccountingDigest: d, EventsDigest: d, RetainedBodiesDigest: d, RetiredAllocationDigest: d, CapturedAt: at, Stop: domain.KnownMCPServiceErrorV1, Cleanup: "joined_retained"}
	if run.Validate() != nil {
		t.Fatal("structural failure payload DATA")
	}
	return run
}

func TestLocalFailureGenericRunWriterPreservesAndCannotMint(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for _, mode := range []string{"preserve", "mint", "add", "drop", "edit"} {
			t.Run(map[bool]string{false: "ordinary", true: "owned"}[owned]+"/"+mode, func(t *testing.T) {
				store, f, run := runPayloadFixture(t)
				run = failureRunPayloadData(t, run)
				old := run.Clone()
				switch mode {
				case "mint":
					f.rows["runs"] = nil
					f.rows["run_idempotency"] = nil
				case "add":
					old.TerminalFailure = nil
					ratedFixturePut(t, f.ratedSQLFixture, "runs", old)
				default:
					ratedFixturePut(t, f.ratedSQLFixture, "runs", old)
				}
				switch mode {
				case "drop":
					run.TerminalFailure = nil
				case "edit":
					run.TerminalFailure.EventsDigest = strings.Repeat("b", 64)
				}
				if run.Validate() != nil {
					t.Fatal("changed DATA must remain structurally valid")
				}
				err := runPayloadTransaction(t, store, run.TenantID, owned, func(ctx context.Context, tx ports.StateTx) error { return tx.PutRun(ctx, run) })
				if mode == "preserve" {
					if err != nil || f.writes != 3 || f.commits != 1 {
						t.Fatal("exact F1 preservation", err, f.writes, f.commits)
					}
					var got domain.Run
					if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &got) != nil || !domain.EqualRunTerminalFailureV1(got.TerminalFailure, run.TerminalFailure) {
						t.Fatal("generic write lost F1")
					}
				} else if err == nil || f.writes != 0 || f.commits != 0 {
					t.Fatal("generic mutation minted/changed F1", mode, err, f.writes)
				}
			})
		}
	}
}
func TestLocalFailureRunReaderFull32KiBCap(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for _, size := range []int{ownedAuthorityHeadBytes, ownedAuthorityHeadBytes + 1} {
			store, f, run := runPayloadFixture(t)
			run = failureRunPayloadData(t, run)
			raw, _ := json.Marshal(run)
			raw = append(raw, []byte(strings.Repeat(" ", size-len(raw)))...)
			f.rows["runs"] = []driver.Value{string(raw)}
			err := runPayloadTransaction(t, store, run.TenantID, owned, func(ctx context.Context, tx ports.StateTx) error {
				got, found, err := tx.GetRun(ctx, run.ID)
				if err == nil && (!found || !domain.EqualRunTerminalFailureV1(got.TerminalFailure, run.TerminalFailure)) {
					t.Fatal("complete F1 read changed metadata")
				}
				return err
			})
			if (err == nil) != (size == ownedAuthorityHeadBytes) || f.writes != 0 {
				t.Fatal("F1 complete Run cap changed", owned, size, err)
			}
		}
	}
}
