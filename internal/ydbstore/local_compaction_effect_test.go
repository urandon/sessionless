package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Only the SQL engine/proxy rows are scripted. Preparation, actual retained
// calibration/invoice, original Store, debit, issuer and one-use consume run
// production code. No keeper preflight/socket/model/head/real YDB is claimed.
type compactionEffectSQL struct {
	*compactionSourceSQL
	pending           []driver.Value
	pendingProxy      []driver.Value
	pendingIndexed    bool
	writes, lostAcks  int
	observationWrites int
	loseCommit        string // persist or rollback, one retryable lost acknowledgement
}
type compactionEffectConnector struct{ f *compactionEffectSQL }

func (c compactionEffectConnector) Connect(context.Context) (driver.Conn, error) { return c.f, nil }
func (c compactionEffectConnector) Driver() driver.Driver {
	return compactionSourceConnector{c.f.compactionSourceSQL}.Driver()
}

type compactionEffectTx struct {
	driver.Tx
	f          *compactionEffectSQL
	accounting []driver.Value
	done       bool
}

func (f *compactionEffectSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, err := f.compactionSourceSQL.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	f.pending, f.pendingIndexed = nil, false
	f.pendingProxy = nil
	return &compactionEffectTx{Tx: tx, f: f, accounting: append([]driver.Value(nil), f.f.parent.accountingRow...)}, nil
}
func (tx *compactionEffectTx) Commit() error {
	f := tx.f
	reservationCommit := f.pending != nil && len(f.pending) == 8 && f.pending[4] == uint64(0) && f.pending[5] == uint64(0)
	if f.pending != nil && f.loseCommit == "rollback" {
		f.loseCommit = ""
		f.lostAcks++
		_ = tx.Rollback()
		return retry.RetryableError(errors.New("owned nonpersisted compaction ACK loss"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	tx.done = true
	if f.firstProxy == nil && f.row != nil {
		f.firstProxy = append([]driver.Value(nil), f.row...)
	}
	if f.pendingProxy != nil {
		f.proxies = append(f.proxies, append([]driver.Value(nil), f.pendingProxy...))
		f.pendingProxy = nil
	}
	if f.pending != nil {
		var r domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(f.pending[0].(string)), &r) != nil || !f.pendingIndexed {
			return errors.New("owned committed operation has no exact index")
		}
		row := append([]driver.Value{r.Operation.Sequence}, f.pending...)
		row = append(row, r.Operation.Sequence, string(r.OperationDigest))
		if r.Operation.Sequence == uint64(len(f.operations))+1 {
			f.operations = append(f.operations, row)
		} else if r.Operation.Sequence >= 1 && r.Operation.Sequence <= uint64(len(f.operations)) {
			f.operations[r.Operation.Sequence-1] = row
		} else {
			return errors.New("owned operation gap")
		}
		f.pending = nil
		if reservationCommit {
			f.compactionSourceSQL.retainedEntrySQL.loopbackOperationSQL.runAfterOperationReservationCommit()
		}
		if f.loseCommit == "persist" {
			f.loseCommit = ""
			f.lostAcks++
			return retry.RetryableError(errors.New("owned persisted compaction ACK loss"), retry.WithBackoff(retry.TypeNoBackoff))
		}
	}
	return nil
}
func (tx *compactionEffectTx) Rollback() error {
	if !tx.done {
		tx.f.pending = nil
		tx.f.pendingProxy = nil
		tx.f.pendingIndexed = false
		tx.f.f.parent.accountingRow = append([]driver.Value(nil), tx.accounting...)
	}
	tx.done = true
	return tx.Tx.Rollback()
}
func (f *compactionEffectSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	if s == ownedOperationProxyRegistrationSQL {
		f.statements++
		if len(args) != 3 {
			return nil, errors.New("owned exact proxy point keys")
		}
		rows := append([][]driver.Value(nil), f.proxies...)
		if f.pendingProxy != nil {
			rows = append(rows, f.pendingProxy)
		}
		for _, row := range rows {
			var proxy domain.RegisteredProxyAttestationV2
			if len(row) != 14 || json.Unmarshal([]byte(row[7].(string)), &proxy) != nil {
				return nil, errors.New("owned proxy point DATA")
			}
			if args[0].Value == string(proxy.Scope.TenantID) && args[1].Value == string(proxy.Scope.AttemptID) && args[2].Value == int64(proxy.OperationSequence) {
				return &explanationFixtureRows{values: append([]driver.Value(nil), row[1:]...)}, nil
			}
		}
		return &explanationFixtureRows{}, nil
	}
	if s != ownedOperationReadSQL && s != ownedOperationCallSQL {
		return f.compactionSourceSQL.QueryContext(ctx, s, args)
	}
	f.statements++
	if len(args) != 3 {
		return nil, errors.New("owned effect point keys")
	}
	rows := cloneGenerationRows(f.operations)
	if f.pending != nil {
		var r domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(f.pending[0].(string)), &r) != nil {
			return nil, errors.New("owned pending row")
		}
		row := append([]driver.Value{r.Operation.Sequence}, f.pending...)
		if f.pendingIndexed {
			row = append(row, r.Operation.Sequence, string(r.OperationDigest))
		} else {
			row = append(row, uint64(0), "")
		}
		if r.Operation.Sequence <= uint64(len(rows)) {
			rows[r.Operation.Sequence-1] = row
		} else {
			rows = append(rows, row)
		}
	}
	for _, row := range rows {
		var r domain.AttemptOperationReservationV1
		if len(row) != 11 || json.Unmarshal([]byte(row[1].(string)), &r) != nil || args[0].Value != string(r.Operation.Scope.TenantID) || args[1].Value != string(r.Operation.Scope.AttemptID) {
			return nil, errors.New("owned exact effect scope")
		}
		if s == ownedOperationReadSQL && args[2].Value == int64(r.Operation.Sequence) {
			return &explanationFixtureRows{values: append([]driver.Value(nil), row[1:9]...)}, nil
		}
		if s == ownedOperationCallSQL && args[2].Value == r.Operation.CallID && row[9] != uint64(0) {
			return &explanationFixtureRows{values: append([]driver.Value(nil), row[9:]...)}, nil
		}
	}
	return &explanationFixtureRows{}, nil
}
func (f *compactionEffectSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	if s == `INSERT INTO attempt_operation_proxies_v1 (tenant_id,attempt_id,sequence,service_kind,original_record,original_digest,current_record,current_digest,policy_body,attestation_body) VALUES ($1,$2,$3,$4,$5,$6,$5,$6,$7,$8)` {
		f.statements++
		if len(args) != 8 || f.pendingProxy != nil {
			return nil, errors.New("owned proxy fresh INSERT")
		}
		var proxy domain.RegisteredProxyAttestationV2
		if json.Unmarshal([]byte(args[4].Value.(string)), &proxy) != nil || args[0].Value != string(proxy.Scope.TenantID) || args[1].Value != string(proxy.Scope.AttemptID) || args[2].Value != int64(proxy.OperationSequence) || args[3].Value != string(proxy.ServiceKind) {
			return nil, errors.New("owned proxy exact INSERT identity")
		}
		for _, old := range f.proxies {
			if old[0] == proxy.OperationSequence {
				return nil, errors.New("owned duplicate proxy INSERT")
			}
		}
		f.pendingProxy = []driver.Value{proxy.OperationSequence, uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), args[4].Value, args[5].Value, args[4].Value, args[5].Value, args[6].Value, args[7].Value, args[3].Value}
		return driver.RowsAffected(1), nil
	}
	if s == loopbackObservationUpdateSQL {
		f.statements++
		if len(args) != 6 || f.pending != nil {
			return nil, errors.New("owned observation exact CAS")
		}
		for _, row := range f.operations {
			var r domain.AttemptOperationReservationV1
			if json.Unmarshal([]byte(row[1].(string)), &r) != nil {
				return nil, errors.New("owned reservation encoding")
			}
			if args[4].Value != int64(r.Operation.Sequence) {
				continue
			}
			var o domain.AttemptOperationObservationV1
			if row[5] != uint64(0) || row[6] != uint64(0) || args[2].Value != string(r.Operation.Scope.TenantID) || args[3].Value != string(r.Operation.Scope.AttemptID) || args[5].Value != row[2] || json.Unmarshal([]byte(args[0].Value.(string)), &o) != nil || o.ValidateForReservation(r) != nil {
				return nil, errors.New("owned observation scope/pending association")
			}
			d, _ := o.Digest()
			raw, _ := json.Marshal(o)
			if args[1].Value != string(d) || args[0].Value != string(raw) {
				return nil, errors.New("owned observation canonical identity")
			}
			f.pending = append([]driver.Value(nil), row[1:9]...)
			f.pendingIndexed = true
			f.pending[4], f.pending[5], f.pending[6], f.pending[7] = uint64(1), uint64(1), args[0].Value, args[1].Value
			f.observationWrites++
			return driver.RowsAffected(1), nil
		}
		return nil, errors.New("owned observation missing reservation")
	}
	if s != loopbackOperationInsertSQL && s != loopbackCallInsertSQL {
		return f.compactionSourceSQL.ExecContext(ctx, s, args)
	}
	f.statements++
	if s == loopbackOperationInsertSQL {
		if len(args) != 7 || f.pending != nil {
			return nil, errors.New("owned sole fresh pending operation")
		}
		var r domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(args[3].Value.(string)), &r) != nil || r.Validate() != nil || r.Operation.Sequence != uint64(len(f.operations))+1 {
			return nil, errors.New("owned fresh operation encoding")
		}
		d, _ := r.Digest()
		if args[0].Value != string(r.Operation.Scope.TenantID) || args[1].Value != string(r.Operation.Scope.AttemptID) || args[2].Value != int64(r.Operation.Sequence) || args[4].Value != string(d) || args[5].Value != string(r.OperationDigest) || args[6].Value != r.Operation.CallID {
			return nil, errors.New("owned operation identity")
		}
		f.pending = []driver.Value{args[3].Value, args[4].Value, args[5].Value, args[6].Value, uint64(0), uint64(0), "", ""}
		f.writes++
	} else {
		var r domain.AttemptOperationReservationV1
		if len(args) != 5 || f.pending == nil || f.pendingIndexed || json.Unmarshal([]byte(f.pending[0].(string)), &r) != nil || args[0].Value != string(r.Operation.Scope.TenantID) || args[1].Value != string(r.Operation.Scope.AttemptID) || args[2].Value != r.Operation.CallID || args[3].Value != int64(r.Operation.Sequence) || args[4].Value != string(r.OperationDigest) {
			return nil, errors.New("owned call identity")
		}
		f.pendingIndexed = true
	}
	return driver.RowsAffected(1), nil
}

func installCompactionFutureProxy(t *testing.T, f *compactionSourceFixture, g *localCompactionGenerationV1) {
	t.Helper()
	installPreparedFutureProxy(t, f, g.operation)
}
func installPreparedFutureProxy(t *testing.T, f *compactionSourceFixture, op domain.AttemptOperationV1) {
	t.Helper()
	at := f.parent.graph.at
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(f.actual.config)
	if err != nil {
		t.Fatal(err)
	}
	pd, _ := policy.Digest()
	peer, _ := policy.LiteralPeerDigest()
	runtime := f.entry.allocation.LocalRuntime
	att := ports.LocalLoopbackEgressAttestationV1{Version: 1, PolicyDigest: pd, ObservedAt: at, ExpiresAt: op.Deadline, Observation: domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}}
	pb, _ := json.Marshal(policy)
	ab, _ := json.Marshal(att)
	bodies, err := (ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2, ServiceKind: domain.RegisteredProxyProviderV1, ObservedAt: at, PolicyBody: pb, AttestationBody: ab}).Bodies()
	if err != nil {
		t.Fatal(err)
	}
	od, _ := op.Digest()
	proxy, err := buildOwnedProxyRecord(bodies, f.entry.allocation, f.entry.source, domain.RegisteredProxyProviderV1, pb, ab, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: op.Deadline}, op.Sequence, od)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := domain.EncodeRegisteredProxyAttestationV2(proxy)
	digest, _ := proxy.Digest()
	f.sql.row = []driver.Value{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), string(raw), string(digest), string(raw), string(digest), string(pb), string(ab)}
	if op.Sequence > 1 {
		f.queries.proxies = append(f.queries.proxies, append(append([]driver.Value{op.Sequence}, f.sql.row...), string(domain.RegisteredProxyProviderV1)))
	}
}
func preparedCompactionEffectFixture(t *testing.T, prefix bool) (*compactionSourceFixture, *localCompactionGenerationV1, *compactionEffectSQL) {
	t.Helper()
	f, source := prepareGenerationFixture(t)
	if prefix {
		source.Clear()
		op := f.operation(t, 1)
		op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
		f.seedCompleted(t, op, 1)
		var err error
		source, err = f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(source.Clear)
	}
	g, err := prepareLocalCompactionGeneration(t.Context(), source)
	if err != nil {
		t.Fatal(err)
	}
	installCompactionFutureProxy(t, f, g)
	engine := &compactionEffectSQL{compactionSourceSQL: f.queries}
	db := sql.OpenDB(compactionEffectConnector{engine})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store.db = db // same original Store; only the declared SQL model is extended
	return f, g, engine
}
func TestLocalCompactionEffectActualReserveAndBeforeSend(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		name := "zero_prefix"
		if prefix {
			name = "completed_model_prefix"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := preparedCompactionEffectFixture(t, prefix)
				reserved, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: g.operation})
				if err != nil || reserved.Status != ports.AttemptOperationReservedV1 || reserved.Capability == nil {
					t.Fatalf("genuine factory -> actual reserve: %v statements=%d max=%d", err, engine.statements, engine.maxStatements)
				}
				if !reflect.DeepEqual(g.reservation, reserved.Reservation) || !g.reservationAttempted || engine.writes != 1 {
					t.Fatal("confirmed original reservation/debit not retained exactly once")
				}
				reserveStatements := engine.statements
				if err := f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), reserved.Capability.Clone()); err != nil {
					t.Fatalf("same actual issuer -> beforeSend: %v statements=%d max=%d", err, engine.statements, engine.maxStatements)
				}
				if reserveStatements > 32 || engine.statements > 32 || engine.maxStatements > 32 {
					t.Fatal("original statement budget exceeded")
				}
				t.Logf("actual reserve statements=%d before-send=%d max=%d", reserveStatements, engine.statements, engine.maxStatements)
				if f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), reserved.Capability.Clone()) == nil {
					t.Fatal("original capability consumed twice")
				}
				if g.observation.Version != 0 {
					t.Fatal("reserve/consume invented completed observation")
				}
				refs := g.program.References()
				if len(refs) == 0 {
					t.Fatal("actual reference inventory missing")
				}
				summary, err := json.Marshal(map[string]any{"schema": "sessionless.compaction-summary.v1", "task": []any{map[string]any{"text": "Preserve the original task", "source_ids": []string{refs[0].SourceID}}}, "constraints": []any{}, "facts": []any{}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := g.program.ValidateSummary(summary); err != nil {
					t.Fatalf("actual strict summary fixture: %v", err)
				}
				wire, err := json.Marshal(map[string]any{"model": f.actual.config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": string(summary)}, "finish_reason": "stop"}}})
				if err != nil {
					t.Fatal(err)
				}
				normalized, err := g.frozen.NormalizeResponseBodyV1(wire, g.operation.Kind, g.operation.ResultBytes)
				if err != nil {
					t.Fatal(err)
				}
				ref, err := f.entry.bodies.Write(t.Context(), normalized)
				clear(normalized)
				if err != nil {
					t.Fatal(err)
				}
				rd, _ := reserved.Reservation.Digest()
				o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: reserved.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: f.parent.graph.at, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
				observed, err := f.bootstrap.operate.AuthenticateAttemptOperationObservation(reserved.ObservationGrant, o)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: reserved.ObservationGrant, Observation: observed}); err != nil {
					t.Fatalf("actual record normalized compaction: %v", err)
				}
				if g.observation.Version != 0 {
					t.Fatal("unread caller observation entered prepared state")
				}
				read, err := f.store.ReadAttemptOperationObservation(t.Context(), reserved.ObservationGrant)
				if err != nil || !reflect.DeepEqual(read, g.observation) || !reflect.DeepEqual(read.Observation, o) {
					t.Fatalf("actual durable/issuer read did not retain exact completed DATA: %v", err)
				}
				if _, err := generationJoin(t, g, localCompactionCompleted); err != nil {
					t.Fatalf("actual stored completed phase: %v", err)
				}
				if duplicate, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: g.operation}); err == nil || duplicate.Capability != nil || engine.writes != 1 {
					t.Fatal("claimed generation retried fresh reserve")
				}
			})
		})
	}
}
func TestLocalCompactionEffectLostCommitCannotRetry(t *testing.T) {
	for _, mode := range []string{"rollback", "persist"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := preparedCompactionEffectFixture(t, false)
				engine.loseCommit = mode
				result, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: g.operation})
				if err == nil || result.Capability != nil || engine.writes != 1 || engine.lostAcks != 1 || !g.reservationAttempted || g.reservation.Version != 0 {
					t.Fatalf("%s lost ACK became send/retry: err=%v writes=%d lost=%d", mode, err, engine.writes, engine.lostAcks)
				}
				if again, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: g.operation}); err == nil || again.Capability != nil || engine.writes != 1 {
					t.Fatal("new call reopened lost reserve")
				}
				if mode == "persist" {
					var actual domain.AttemptOperationReservationV1
					if len(engine.operations) != 1 || json.Unmarshal([]byte(engine.operations[0][1].(string)), &actual) != nil {
						t.Fatal("persisted lost-ACK row missing")
					}
					digest, _ := actual.Digest()
					recovered, err := f.store.RecoverAttemptOperation(t.Context(), ports.RecoverAttemptOperationRequestV1{Operation: g.operation, ExpectedReservationDigest: digest})
					if err != nil || recovered.Status != ports.AttemptOperationReconcileOnlyV1 || recovered.Capability != nil || g.reservation.Version != 0 {
						t.Fatalf("recovery restored send owner or lost reconcile DATA: %v", err)
					}
				}
			})
		})
	}
}

func TestLocalCompactionEffectDenialsBeforeWrite(t *testing.T) {
	for _, name := range []string{"copied_owner", "partial_owner", "foreign_operation", "changed_fit", "closed_source", "cancelled", "missing_future_proxy", "changed_run"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := preparedCompactionEffectFixture(t, false)
				op := g.operation.Clone()
				ctx := t.Context()
				switch name {
				case "copied_owner":
					copied := *g
					f.entry.compactionGeneration.Store(&copied)
				case "partial_owner":
					g.retained = nil
				case "foreign_operation":
					op.CallID = "foreign-compaction-call"
				case "changed_fit":
					g.fit.InputUpperBound++
				case "closed_source":
					if err := f.actual.source.Close(); err != nil {
						t.Fatal(err)
					}
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "missing_future_proxy":
					f.sql.row = nil
				case "changed_run":
					var run domain.Run
					if json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run) != nil {
						t.Fatal("original Run fixture")
					}
					run.UpdatedAt = run.UpdatedAt.Add(1)
					if run.Validate() != nil {
						t.Fatal("causal valid changed Run")
					}
					ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", run)
				}
				if op.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation) != nil {
					t.Fatal("negative must keep structurally valid operation")
				}
				before := f.canonical.calls
				result, err := f.store.ReserveAttemptOperation(ctx, ports.ReserveAttemptOperationRequestV1{Operation: op})
				if err == nil || result.Capability != nil || engine.writes != 0 || g.reservation.Version != 0 {
					t.Fatalf("%s minted/wrote: err=%v writes=%d", name, err, engine.writes)
				}
				if name != "missing_future_proxy" && name != "changed_run" && f.canonical.calls != before {
					t.Fatalf("%s read body before original-identity/lifetime denial", name)
				}
			})
		})
	}
}
