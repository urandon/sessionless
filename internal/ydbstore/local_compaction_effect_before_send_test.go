package ydbstore

import (
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// SQL current-state rows are scripted DATA; the original Source/factory,
// calibration, Store Reserve/BeforeSend and issuer consumption are real. This
// does not claim keeper socket, model execution or native YDB evidence.
func TestLocalCompactionEffectBeforeSendRejoinsCurrentRun(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "unchanged_control"
		if changed {
			name = "changed_after_reserve"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := preparedCompactionEffectFixture(t, false)
				reserved, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: g.operation})
				if err != nil || reserved.Status != ports.AttemptOperationReservedV1 || reserved.Capability == nil || reserved.Capability.Validate() != nil || engine.writes != 1 {
					t.Fatalf("genuine factory -> actual public Reserve: err=%v status=%v capability=%t writes=%d", err, reserved.Status, reserved.Capability != nil, engine.writes)
				}
				if f.store.attemptOperationIssuer != f.bootstrap.operate {
					t.Fatal("capability assertion must use the original Store issuer")
				}
				if changed {
					var run domain.Run
					if err := json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run); err != nil {
						t.Fatalf("decode current Run: %v", err)
					}
					run.UpdatedAt = run.UpdatedAt.Add(1)
					if err := run.Validate(); err != nil {
						t.Fatalf("changed current Run must remain structurally valid: %v", err)
					}
					ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
				}
				err = f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), reserved.Capability.Clone())
				if changed {
					if !errors.Is(err, domain.ErrMembershipDenied) {
						t.Fatalf("current Run changed after Reserve: BeforeSend got=%v want=%v", err, domain.ErrMembershipDenied)
					}
					// Direct original-issuer consume is an assertion only; it sends
					// nothing and proves the denied Store gate did not burn it.
					if err := f.bootstrap.operate.ConsumeAttemptOperationCapability(reserved.Capability.Clone()); err != nil {
						t.Fatalf("denied BeforeSend consumed original capability: %v", err)
					}
				} else if err != nil {
					t.Fatalf("unchanged current Run: BeforeSend got=%v want=nil", err)
				}
				if err := f.bootstrap.operate.ConsumeAttemptOperationCapability(reserved.Capability.Clone()); err == nil {
					t.Fatal("original one-use capability remained reusable after successful consumption")
				}
				if engine.writes != 1 || engine.maxStatements > 32 {
					t.Fatalf("BeforeSend changed reservation or exceeded original SQL budget: writes=%d maxStatements=%d", engine.writes, engine.maxStatements)
				}
			})
		})
	}
}
