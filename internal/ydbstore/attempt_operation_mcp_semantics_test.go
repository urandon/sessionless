package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type ownedMCPSemanticSQLFixture struct {
	*ratedSQLFixture
	at                      time.Time
	scope                   domain.AttemptOperationScopeV1
	rows                    [][]driver.Value
	reads, closed, readRows int
}

func (f *ownedMCPSemanticSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *ownedMCPSemanticSQLFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("semantic fixture requires bounded context")
	}
	if statement == `SELECT CurrentUtcTimestamp()` {
		return &explanationFixtureRows{values: []driver.Value{f.at}}, nil
	}
	if statement != ownedMCPSemanticInventorySQL || len(args) != 3 || args[0].Value != string(f.scope.TenantID) || args[1].Value != string(f.scope.AttemptID) || args[2].Value != int64(13) {
		return nil, errors.New("semantic fixture rejects foreign/unbounded read")
	}
	f.reads++
	return &ownedMCPSemanticRows{fixture: f, rows: f.rows}, nil
}

type ownedMCPSemanticRows struct {
	fixture *ownedMCPSemanticSQLFixture
	rows    [][]driver.Value
}

func (*ownedMCPSemanticRows) Columns() []string {
	return []string{"sequence", "record", "digest", "operation_digest", "call_id"}
}
func (r *ownedMCPSemanticRows) Close() error { r.fixture.closed++; return nil }
func (r *ownedMCPSemanticRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	r.fixture.readRows++
	return nil
}

func semanticReservationRow(r domain.AttemptOperationReservationV1) []driver.Value {
	raw, _ := json.Marshal(r)
	digest, _ := r.Digest()
	return []driver.Value{r.Operation.Sequence, string(raw), string(digest), string(r.OperationDigest), r.Operation.CallID}
}

// Scripted SQL controls prove the actual bounded reader, canonical validation
// and semantic predicate. They do NOT prove native isolation or authenticated
// model/response custody; the joined native fixture covers that separately.
func ownedMCPSemanticFixture(t *testing.T) (ports.OwnedOperationResidualInventoryV1, []domain.AttemptOperationReservationV1, domain.AttemptOperationV1) {
	t.Helper()
	v := ownedResidualInventoryFixture(t)
	first := v.Operations[0].Reservation
	tool := first.Clone()
	op := first.Operation.Clone()
	op.Sequence, op.Kind, op.CallID = 2, domain.AttemptOperationMCPToolV1, "old-call-id"
	op.PredecessorObservationDigest, _ = v.Operations[0].Observation.Digest()
	op.ModelResponseDigest = v.Operations[0].Observation.ResponseDigest
	args := []byte(`{"operation":"fixture"}`)
	op.RequestRef, op.RequestDigest, op.ArgumentsDigest = "old-tool-args", ports.OwnedFixtureBytesDigestV1(args), ports.OwnedFixtureBytesDigestV1(args)
	op.RequestBytes, op.ContextBytes = uint64(len(args)), 0
	cost := v.Parent.Authority.AdmissionCostCeiling.MCPToolCost
	op.ToolGrantDigest, op.ExternalChargeEvidenceDigest = cost.ScopedGrantDigest, cost.PriceEvidenceDigest
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, BodyBytes: uint64(len(args)) + 512, ResponseBytes: 512, MCPOutboundMessages: 1, MCPInboundFrames: 1, MCPInboundBytes: 512, AmountMicrounits: *cost.MaxAmountMicrounits, ActiveDuration: 5 * time.Second}
	tool.Operation, tool.OperationDigest = op, domain.AttemptOperationDigestV1("")
	tool.OperationDigest, _ = op.Digest()
	tool.AggregateDebit, _ = first.AggregateDebit.Add(op.Debit)
	tool.ComponentAmounts[2] = op.Debit.AmountMicrounits
	model := first.Clone()
	model.Operation.Sequence, model.Operation.CallID = 3, "new-model"
	model.Operation.PredecessorObservationDigest = op.PredecessorObservationDigest
	model.OperationDigest, _ = model.Operation.Digest()
	model.AggregateDebit, _ = tool.AggregateDebit.Add(model.Operation.Debit)
	model.ComponentAmounts = tool.ComponentAmounts
	for _, r := range []domain.AttemptOperationReservationV1{first, tool, model} {
		if err := r.ValidateForParent(v.Parent.Authority, v.Parent.Reservation); err != nil {
			t.Fatal("valid semantic structural row", err)
		}
	}
	candidate := op.Clone()
	candidate.Sequence, candidate.CallID = 4, "regenerated-call-id"
	candidate.ModelResponseDigest = ports.OwnedFixtureBytesDigestV1([]byte("new authenticated model response"))
	candidate.PredecessorObservationDigest = op.PredecessorObservationDigest
	return v, []domain.AttemptOperationReservationV1{first, tool, model}, candidate
}

func TestOwnedMCPSemanticDuplicateCannotUseRegeneratedCallID(t *testing.T) {
	v, history, candidate := ownedMCPSemanticFixture(t)
	for _, tc := range []struct {
		name   string
		change func(*domain.AttemptOperationV1, *[]domain.AttemptOperationReservationV1)
		deny   bool
	}{
		{name: "same-arguments-new-call-id-and-model", deny: true},
		{name: "different-canonical-arguments", change: func(op *domain.AttemptOperationV1, _ *[]domain.AttemptOperationReservationV1) {
			op.ArgumentsDigest = ports.OwnedFixtureBytesDigestV1([]byte(`{"operation":"fixture","value":"different"}`))
			op.RequestDigest = op.ArgumentsDigest
		}, deny: false},
		{name: "absent-history", change: func(_ *domain.AttemptOperationV1, rows *[]domain.AttemptOperationReservationV1) { *rows = (*rows)[:2] }, deny: true},
		{name: "wrong-original-scope", change: func(_ *domain.AttemptOperationV1, rows *[]domain.AttemptOperationReservationV1) {
			(*rows)[1].Operation.Scope.SessionID = "foreign-session"
		}, deny: true},
		{name: "exact-current-reserved-row", change: func(op *domain.AttemptOperationV1, rows *[]domain.AttemptOperationReservationV1) {
			op.ArgumentsDigest = ports.OwnedFixtureBytesDigestV1([]byte(`{"operation":"fixture","value":"different"}`))
			op.RequestDigest = op.ArgumentsDigest
			r := (*rows)[1].Clone()
			r.Operation = *op
			r.OperationDigest, _ = op.Digest()
			r.AggregateDebit, _ = (*rows)[2].AggregateDebit.Add(op.Debit)
			r.ComponentAmounts[2] += op.Debit.AmountMicrounits
			*rows = append(*rows, r)
		}, deny: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := candidate.Clone()
			rows := make([]domain.AttemptOperationReservationV1, len(history))
			for i, r := range history {
				rows[i] = r.Clone()
			}
			if tc.change != nil {
				tc.change(&op, &rows)
			}
			f := &ownedMCPSemanticSQLFixture{ratedSQLFixture: &ratedSQLFixture{rows: make(map[string][]driver.Value)}, at: v.Parent.Reservation.ReservedAt.Add(time.Microsecond), scope: op.Scope}
			for _, r := range rows {
				f.rows = append(f.rows, semanticReservationRow(r))
			}
			db := sql.OpenDB(f)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			store := &Store{db: db}
			err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				return denyOwnedMCPSemanticRepeat(ctx, q, op, v.Parent)
			})
			if (err != nil) != tc.deny || f.reads != 1 || f.closed != 1 || f.writes != 0 {
				t.Fatalf("semantic protection deny=%t gotErr=%v reads=%d closed=%d writes=%d", tc.deny, err, f.reads, f.closed, f.writes)
			}
		})
	}
}
