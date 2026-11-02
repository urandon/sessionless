package attachedworkerstack_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// This is a structural regression for the controlled peer's chosen schedule,
// not a reproduction or qualification of YDB lock-conflict/retry behavior.
// Both HTTP exchange and the real admission must own the same fixture turn;
// sampling before acquiring that turn could otherwise retain an older clock.
func TestJoinedGenerationAdmissionOwnsControlledWireTurn(t *testing.T) {
	source, err := parser.ParseFile(token.NewFileSet(), "native_generation_ydbintegration_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	methods := make(map[string]*ast.FuncDecl)
	for _, declaration := range source.Decls {
		if method, ok := declaration.(*ast.FuncDecl); ok && method.Recv != nil {
			methods[method.Name.Name] = method
		}
	}
	for _, check := range []struct {
		method, lock, sample, operation string
	}{
		{"admitOutbox", "f.wire.mu", "f.wire.sampleLocked", "store.AdmitDispatch"},
		{"RoundTrip", "w.mu", "w.sampleLocked", "handler.ServeHTTP"},
	} {
		method := methods[check.method]
		if method == nil || method.Body == nil {
			t.Fatalf("missing real fixture method %s", check.method)
		}
		lock, unlock, sample, operation := -1, -1, -1, -1
		for i, statement := range method.Body.List {
			if call, ok := statement.(*ast.ExprStmt); ok {
				if call, ok := call.X.(*ast.CallExpr); ok && joinedGenerationSelector(call.Fun) == check.lock+".Lock" {
					lock = i
				}
			}
			if deferred, ok := statement.(*ast.DeferStmt); ok && joinedGenerationSelector(deferred.Call.Fun) == check.lock+".Unlock" {
				unlock = i
			}
			ast.Inspect(statement, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					switch joinedGenerationSelector(call.Fun) {
					case check.sample:
						if sample < 0 {
							sample = i
						}
					case check.operation:
						operation = i
					}
				}
				return true
			})
		}
		if lock < 0 || unlock <= lock || sample <= unlock || operation <= sample {
			t.Errorf("%s must lock/defer unlock the SAME wire turn before DB sampling and the real operation: lock=%d unlock=%d sample=%d operation=%d", check.method, lock, unlock, sample, operation)
		}
	}
}

func joinedGenerationSelector(expression ast.Expr) string {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return joinedGenerationSelector(value.X) + "." + value.Sel.Name
	default:
		return ""
	}
}
