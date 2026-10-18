package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Structural seeded DATA, not proof of real generation or fenced publication.
// The actual Store transaction and physical PutRun path are exercised below.
func runPayloadMetadata(t *testing.T, run domain.Run) *domain.RunDerivedContextV1 {
	t.Helper()
	h := strings.Repeat("a", 64)
	scope := domain.AttemptOperationScopeV1{TenantID: run.TenantID, SessionID: run.SessionID, OwnerUserID: "user-test", RunID: run.ID, AttemptID: "attempt-test", LeaseID: "lease-test", FenceToken: 1, PhysicalInvocationClaimID: "claim-test", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}
	v := &domain.RunDerivedContextV1{Version: 1}
	for generation := uint64(1); generation <= 2; generation++ {
		p := domain.DerivedContextPlanV1{Version: 1, Scope: scope, Generation: generation, OperationSequence: generation * 2, ReservationDigest: domain.AttemptOperationReservationDigestV1(h), ObservationDigest: domain.AttemptOperationObservationDigestV1(h), NormalizedResponseDigest: h, SourceViewDigest: h, Summary: domain.BlobRef{TenantID: run.TenantID, Size: 200, SHA256: strings.Repeat("b", 64)}, Manifest: domain.BlobRef{TenantID: run.TenantID, Size: 300, SHA256: strings.Repeat("c", 64)}}
		var err error
		p.Summary.Key, err = domain.DerivedContextObjectKeyV1(scope, generation, "summary", p.Summary.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		p.Manifest.Key, err = domain.DerivedContextObjectKeyV1(scope, generation, "manifest", p.Manifest.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		v.Inventory = append(v.Inventory, p)
	}
	digest, err := v.Inventory[1].Digest()
	if err != nil {
		t.Fatal(err)
	}
	v.Head = &domain.DerivedContextHeadV1{Version: 1, PlanDigest: digest}
	if err := v.ValidateForRun(run); err != nil {
		t.Fatal(err)
	}
	return v
}

type runPayloadSQLFixture struct{ *canonicalBudgetFixture }

func (f *runPayloadSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *runPayloadSQLFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	normalized := strings.Join(strings.Fields(statement), " ")
	for query, table := range map[string]string{
		"SELECT run_id FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2": "run_idempotency",
		"SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":    "session_deletions",
	} {
		if normalized == query {
			f.statements = append(f.statements, statement)
			f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
			return &explanationFixtureRows{values: f.rows[table]}, nil
		}
	}
	return f.canonicalBudgetFixture.QueryContext(ctx, statement, args)
}
func (f *runPayloadSQLFixture) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	result, err := f.canonicalBudgetFixture.ExecContext(ctx, statement, args)
	if err == nil && strings.HasPrefix(strings.TrimSpace(statement), "UPSERT INTO runs\n") {
		f.rows["runs"] = []driver.Value{args[8].Value}
	}
	return result, err
}

func runPayloadFixture(t *testing.T) (*Store, *canonicalBudgetFixture, domain.Run) {
	t.Helper()
	store, f, state := canonicalFixture(t)
	db := sql.OpenDB(&runPayloadSQLFixture{f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	return store, f, state.Run
}

func runPayloadTransaction(t *testing.T, store *Store, tenant domain.TenantID, owned bool, fn func(context.Context, ports.StateTx) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if owned {
		return store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			return fn(ctx, q.canonicalState(store, tenant))
		})
	}
	return store.Transact(ctx, tenant, func(tx ports.StateTx) error { return fn(ctx, tx) })
}

func TestRunPayloadGenericWritersPreserveCompleteMetadata(t *testing.T) {
	for _, owned := range []bool{false, true} {
		mode := "ordinary"
		if owned {
			mode = "owned"
		}
		t.Run(mode, func(t *testing.T) {
			for _, status := range []domain.RunStatus{domain.RunRunning, domain.RunCancelled, domain.RunFailed, domain.RunSucceeded} {
				t.Run(string(status), func(t *testing.T) {
					store, f, run := runPayloadFixture(t)
					run.DerivedContext = runPayloadMetadata(t, run)
					ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
					original := run.Clone()
					err := runPayloadTransaction(t, store, run.TenantID, owned, func(ctx context.Context, tx ports.StateTx) error {
						got, found, err := tx.GetRun(ctx, run.ID)
						if err != nil || !found {
							t.Fatalf("get original: found=%t err=%v", found, err)
						}
						at := run.UpdatedAt.Add(time.Second)
						if status != domain.RunRunning {
							if err := got.Transition(status, at); err != nil {
								return err
							}
						} else {
							got.CancellationRequestedAt = &at
							got.UpdatedAt = at
						}
						if err := tx.PutRun(ctx, got); err != nil {
							return err
						}
						again, found, err := tx.GetRun(ctx, run.ID)
						if err != nil || !found || again.Status != got.Status || !domain.EqualRunDerivedContextV1(again.DerivedContext, original.DerivedContext) {
							t.Fatalf("read-your-write lost metadata/status: found=%t status=%s want=%s err=%v", found, again.Status, got.Status, err)
						}
						got.DerivedContext.Inventory[0].Summary.Size++
						if !domain.EqualRunDerivedContextV1(again.DerivedContext, original.DerivedContext) {
							t.Fatal("caller aliases retained state")
						}
						return nil
					})
					if err != nil || f.writes != 3 || f.persistedWrites != 3 || f.commits != 1 {
						t.Fatalf("err=%v writes=%d persisted=%d commits=%d", err, f.writes, f.persistedWrites, f.commits)
					}
				})
			}
		})
	}
}

func TestRunPayloadGenericWritersRejectMetadataChangesBeforeMutation(t *testing.T) {
	for _, name := range []string{"creation_injection", "addition", "removal", "head_replacement", "inventory_edit", "stale_inventory", "invalid_inventory"} {
		t.Run(name, func(t *testing.T) {
			for _, owned := range []bool{false, true} {
				store, f, run := runPayloadFixture(t)
				run.DerivedContext = runPayloadMetadata(t, run)
				ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
				switch name {
				case "creation_injection":
					f.rows["runs"] = nil
					f.rows["run_idempotency"] = nil
				case "addition":
					old := run.Clone()
					old.DerivedContext = nil
					ratedFixturePut(t, f.ratedSQLFixture, "runs", old)
				case "removal":
					run.DerivedContext = nil
				case "head_replacement":
					run.DerivedContext.Head.PlanDigest, _ = run.DerivedContext.Inventory[0].Digest()
				case "inventory_edit":
					run.DerivedContext.Inventory[0].Summary.Size++
				case "stale_inventory":
					run.DerivedContext.Head = nil
					run.DerivedContext.Inventory = run.DerivedContext.Inventory[:1]
				case "invalid_inventory":
					run.DerivedContext.Inventory = append(run.DerivedContext.Inventory, run.DerivedContext.Inventory[0])
				}
				before := append([]driver.Value(nil), f.rows["runs"]...)
				err := runPayloadTransaction(t, store, run.TenantID, owned, func(ctx context.Context, tx ports.StateTx) error { return tx.PutRun(ctx, run) })
				if err == nil || f.writes != 0 || f.persistedWrites != 0 || f.commits != 0 {
					t.Fatalf("case=%s owned=%t err=%v writes=%d persisted=%d commits=%d", name, owned, err, f.writes, f.persistedWrites, f.commits)
				}
				if len(before) != len(f.rows["runs"]) || len(before) > 0 && before[0] != f.rows["runs"][0] {
					t.Fatal("denied update changed source")
				}
				for _, statement := range f.statements {
					if strings.Contains(statement, "run_explanation_heads_v1") {
						t.Fatal("metadata denial occurred after explanation invalidation began")
					}
				}
			}
		})
	}
}

func TestRunPayloadCodecCapsCompleteEncodedRun(t *testing.T) {
	// Exercise the byte codec itself, independently of the stricter domain ID
	// bound, so neither a metadata-only check nor domain denial can mask it.
	run := domain.Run{}
	empty, err := marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range []int{0, 1} {
		run.IdempotencyKey = domain.IdempotencyKey(strings.Repeat("x", ownedAuthorityHeadBytes-len(empty)+extra))
		raw, err := marshalRunPayload(run)
		if extra == 0 && (err != nil || len(raw) != ownedAuthorityHeadBytes) {
			t.Fatalf("exact complete Run cap: len=%d err=%v", len(raw), err)
		}
		if extra == 1 && (err == nil || raw != "") {
			t.Fatalf("first byte over complete cap accepted: len=%d err=%v", len(raw), err)
		}
	}
}

func TestRunPayloadLegacyCreationStillHasNoMetadata(t *testing.T) {
	for _, owned := range []bool{false, true} {
		store, f, run := runPayloadFixture(t)
		f.rows["runs"], f.rows["run_idempotency"] = nil, nil
		err := runPayloadTransaction(t, store, run.TenantID, owned, func(ctx context.Context, tx ports.StateTx) error { return tx.PutRun(ctx, run) })
		if err != nil || f.writes != 3 || f.persistedWrites != 3 || f.commits != 1 {
			t.Fatalf("owned=%t err=%v writes=%d persisted=%d commits=%d", owned, err, f.writes, f.persistedWrites, f.commits)
		}
		var got domain.Run
		if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &got); err != nil || got.DerivedContext != nil {
			t.Fatalf("legacy create changed metadata: %v", err)
		}
	}
}

func TestRunPayloadOwnedDecodeDenialRemainsSticky(t *testing.T) {
	store, f, run := runPayloadFixture(t)
	run.DerivedContext = runPayloadMetadata(t, run)
	encoded, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	f.rows["runs"] = []driver.Value{strings.Replace(string(encoded), `"derived_context":{`, `"derived_context":{"unsupported_future":{},`, 1)}
	err = runPayloadTransaction(t, store, run.TenantID, true, func(ctx context.Context, tx ports.StateTx) error { _, _, _ = tx.GetRun(ctx, run.ID); return nil })
	if err == nil || f.commits != 0 || f.writes != 0 {
		t.Fatalf("ignored corrupt metadata committed: err=%v commits=%d writes=%d", err, f.commits, f.writes)
	}
}

func TestRunPayloadCommonReaderBoundsAndRejectsUnknownMetadata(t *testing.T) {
	for _, name := range []string{"absent", "exact_cap", "over_cap", "malformed", "unknown_metadata", "foreign_metadata", "valid_metadata"} {
		t.Run(name, func(t *testing.T) {
			for _, owned := range []bool{false, true} {
				store, f, run := runPayloadFixture(t)
				metadata := runPayloadMetadata(t, run)
				raw := f.rows["runs"][0].(string)
				wantErr := false
				switch name {
				case "absent":
					f.rows["runs"] = nil
				case "exact_cap", "over_cap":
					n := ownedAuthorityHeadBytes
					if name == "over_cap" {
						n++
						wantErr = true
					}
					raw += strings.Repeat(" ", n-len(raw))
				case "malformed":
					raw = "{"
					wantErr = true
				case "unknown_metadata":
					run.DerivedContext = metadata
					encoded, _ := json.Marshal(run)
					raw = strings.Replace(string(encoded), `"derived_context":{`, `"derived_context":{"terminal_future":{},`, 1)
					wantErr = true
				case "foreign_metadata":
					run.DerivedContext = metadata
					run.SessionID = "foreign-session"
					encoded, _ := json.Marshal(run)
					raw = string(encoded)
					wantErr = true
				case "valid_metadata":
					run.DerivedContext = metadata
					encoded, _ := json.Marshal(run)
					raw = string(encoded)
				}
				if name != "absent" {
					f.rows["runs"] = []driver.Value{raw}
				}
				err := runPayloadTransaction(t, store, run.TenantID, owned, func(ctx context.Context, tx ports.StateTx) error {
					got, found, err := tx.GetRun(ctx, run.ID)
					if wantErr {
						if err == nil {
							t.Fatalf("case=%s owned=%t invalid metadata accepted", name, owned)
						}
						return err
					}
					if err != nil || found != (name != "absent") {
						t.Fatalf("case=%s owned=%t found=%t err=%v", name, owned, found, err)
					}
					if name == "valid_metadata" && !domain.EqualRunDerivedContextV1(got.DerivedContext, metadata) {
						t.Fatal("full metadata lost in common reader")
					}
					return err
				})
				if (err != nil) != wantErr || f.writes != 0 {
					t.Fatalf("case=%s owned=%t err=%v writes=%d", name, owned, err, f.writes)
				}
			}
		})
	}
}
