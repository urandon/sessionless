package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Original private-file Source, bootstrap/entry, issuers and real Store code;
// SQL isolation, proxy/reservation rows, funding/source verifier and blob bytes
// are SCRIPTED DATA. No actual generation, transport, summary fidelity, source
// closure, head publication, terminal custody or live compaction is claimed.
type retainedEntrySQL struct {
	*loopbackOperationSQL
	registrationWrites int
	committed          bool
}
type retainedEntryConnector struct{ f *retainedEntrySQL }

func (c retainedEntryConnector) Connect(context.Context) (driver.Conn, error) { return c.f, nil }
func (c retainedEntryConnector) Driver() driver.Driver {
	return loopbackOperationDriver{c.f.loopbackOperationSQL}
}

type retainedEntryTx struct {
	driver.Tx
	f      *retainedEntrySQL
	before int
}

func (f *retainedEntrySQL) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := f.loopbackOperationSQL.BeginTx(ctx, opts)
	return retainedEntryTx{tx, f, f.registrationWrites}, err
}
func (tx retainedEntryTx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil && tx.f.registrationWrites > tx.before {
		tx.f.committed = true
	}
	return err
}
func (f *retainedEntrySQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement == ownedOperationProxyRegistrationSQL {
		if len(args) != 3 {
			return nil, errors.New("retained proxy point keys")
		}
		var proxy domain.RegisteredProxyAttestationV2
		if len(f.row) != 12 || json.Unmarshal([]byte(f.row[6].(string)), &proxy) != nil || args[0].Value != string(proxy.Scope.TenantID) || args[1].Value != string(proxy.Scope.AttemptID) || args[2].Value != int64(proxy.OperationSequence) {
			return nil, errors.New("retained proxy crossed exact keys")
		}
		row := append(append([]driver.Value(nil), f.row...), string(domain.RegisteredProxyProviderV1))
		return &explanationFixtureRows{values: row}, nil
	}
	return f.loopbackOperationSQL.QueryContext(ctx, statement, args)
}
func (f *retainedEntrySQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement != localRetainedRunUpdateSQL {
		return f.loopbackOperationSQL.ExecContext(ctx, statement, args)
	}
	if len(args) != 3 {
		return nil, errors.New("retained update keys")
	}
	var old, next domain.Run
	if json.Unmarshal([]byte(f.f.parent.graph.rows["runs"][0].(string)), &old) != nil || json.Unmarshal([]byte(args[0].Value.(string)), &next) != nil || next.Validate() != nil || len(args[0].Value.(string)) > 32<<10 || args[1].Value != string(old.TenantID) || args[2].Value != string(old.ID) {
		return nil, errors.New("retained update invalid payload/scope")
	}
	base := next.Clone()
	base.DerivedContext = old.DerivedContext
	if !reflect.DeepEqual(base, old) {
		return nil, errors.New("retained update changed base Run")
	}
	f.f.parent.base.recordQuery(statement, args)
	f.committed = false
	f.registrationWrites++
	f.f.parent.graph.rows["runs"] = []driver.Value{args[0].Value}
	return driver.RowsAffected(1), nil
}

type retainedEntryBlobs struct {
	localEntryBlobs
	sql          *retainedEntrySQL
	retainedPuts int
	premature    bool
	ambiguous    bool
}

func (b *retainedEntryBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, raw io.Reader) (domain.BlobRef, error) {
	retained := strings.Contains(key, "/derived-context-v1/")
	if retained {
		b.retainedPuts++
		if !b.sql.committed || b.sql.f.parent.activeTransactions != 0 {
			b.premature = true
			return domain.BlobRef{}, errors.New("PUT preceded confirmed registration")
		}
	}
	ref, err := b.localEntryBlobs.Put(ctx, tenant, key, raw)
	if retained && b.ambiguous {
		return domain.BlobRef{}, errors.New("owned ambiguous PUT")
	}
	return ref, err
}

type retainedEntryFixture struct {
	localBootstrapFixture
	entry            *LocalInvocationEntryV1
	sql              *retainedEntrySQL
	blobs            *retainedEntryBlobs
	frozen           *configuredinference.FrozenRequestV1
	request          operationbody.ReferenceV1
	cancelInvocation context.CancelFunc
}

func newRetainedEntryFixture(t *testing.T) *retainedEntryFixture {
	t.Helper()
	f := newLocalBootstrapFixture(t)
	fixture := &retainedEntrySQL{loopbackOperationSQL: &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{&allocationSQLFixture{parent: f.parent.sql}}}}}
	db := sql.OpenDB(retainedEntryConnector{fixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store.db = db
	completeLocalEntryFixture(t, f)
	blobs := &retainedEntryBlobs{sql: fixture}
	invocation, cancelInvocation := context.WithCancel(f.actual.ctx)
	t.Cleanup(cancelInvocation)
	entry, err := f.bootstrap.StartLocalInvocation(invocation, f.parent.request, blobs)
	if err != nil {
		t.Fatalf("original retained entry: %v", err)
	}
	frozen, err := configuredinference.FreezeRequestV1(f.actual.config, f.actual.config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "scripted compaction DATA"}}, MaxOutputTokens: 1})
	if err != nil {
		t.Fatal(err)
	}
	request, err := entry.bodies.Write(t.Context(), frozen.Body())
	if err != nil {
		t.Fatal(err)
	}
	return &retainedEntryFixture{f, entry, fixture, blobs, frozen, request, cancelInvocation}
}

func (f *retainedEntryFixture) operation(t *testing.T, sequence uint64) domain.AttemptOperationV1 {
	t.Helper()
	cost := f.entry.parent.Authority.AdmissionCostCeiling
	cd, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	duration := min(5*time.Second, f.actual.config.Limits.OperationTimeout)
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(f.entry.parent), Sequence: sequence, Kind: domain.AttemptOperationCompactionV1, CallID: "retained-call-" + string(rune('0'+sequence)), RequestRef: f.request.Ref, RequestDigest: f.request.Digest, RequestBytes: f.request.Bytes, ResultBytes: 4096, ContextBytes: 1, SummaryBytes: 1024, Deadline: f.parent.graph.at.Add(duration), CleanupDuration: f.actual.config.Limits.CleanupTimeout, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, CompactionCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: f.request.Bytes + 4096 + operationbody.RetainedPairBytesV1, ResponseBytes: 4096, ActiveDuration: duration}}
	if sequence > 1 {
		op.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("a", 64))
	}
	if err := op.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation); err != nil {
		t.Fatalf("frozen compaction DATA: %v", err)
	}
	return op
}

func (f *retainedEntryFixture) complete(t *testing.T, op domain.AttemptOperationV1, generation uint64, overrun ...bool) (domain.DerivedContextPlanV1, ports.AuthenticatedAttemptOperationObservationV1, []byte, []byte) {
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
	opDigest, _ := op.Digest()
	bodies, err := (ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2, ServiceKind: domain.RegisteredProxyProviderV1, ObservedAt: at, PolicyBody: pb, AttestationBody: ab}).Bodies()
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := buildOwnedProxyRecord(bodies, f.entry.allocation, f.entry.source, domain.RegisteredProxyProviderV1, pb, ab, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: op.Deadline}, op.Sequence, opDigest)
	if err != nil {
		t.Fatalf("scripted proxy DATA: %v", err)
	}
	pr, _ := domain.EncodeRegisteredProxyAttestationV2(proxy)
	proxyDigest, _ := proxy.Digest()
	f.sql.row = []driver.Value{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), string(pr), string(proxyDigest), string(pr), string(proxyDigest), string(pb), string(ab)}
	allocationDigest, _ := f.entry.allocation.Digest()
	debit := op.Debit
	for i := uint64(1); i < op.Sequence; i++ {
		debit, err = debit.Add(op.Debit)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op.Clone(), OperationDigest: opDigest, ReservedAt: at, AggregateDebit: debit, RegisteredAllocationDigest: allocationDigest, RegisteredProxyDigest: proxyDigest}
	if err := r.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation); err != nil {
		t.Fatalf("scripted reservation DATA: %v", err)
	}
	// No usage report: successful known response and unknown token usage are
	// distinct. Unknown usage remains covered by the original worst-case debit.
	wireValue := map[string]any{"model": f.actual.config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "summary DATA"}, "finish_reason": "stop"}}}
	if len(overrun) > 0 && overrun[0] {
		wireValue["usage"] = map[string]uint64{"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3}
	}
	wire, _ := json.Marshal(wireValue)
	decoded, err := f.frozen.DecodeResponse(wire)
	if err != nil {
		t.Fatal(err)
	}
	usage, err := decoded.Usage.OperationUsage()
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := f.frozen.NormalizeResponseBodyV1(wire, domain.AttemptOperationCompactionV1, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.entry.bodies.Write(t.Context(), normalized)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Digest()
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: opDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: response.Ref, ResponseDigest: response.Digest, ResponseBytes: response.Bytes, ObservedAt: at, Usage: usage}
	grant, err := f.bootstrap.operate.MintAttemptOperationObservationGrant(r, at, at.Add(op.CleanupDuration))
	if err != nil {
		t.Fatalf("original issuer observation grant: %v", err)
	}
	authenticated, err := f.bootstrap.operate.AuthenticateAttemptOperationObservation(grant, o)
	if err != nil {
		t.Fatalf("original issuer observation authentication: %v", err)
	}
	rawReservation, _ := json.Marshal(r)
	rawObservation, _ := json.Marshal(o)
	od, _ := o.Digest()
	f.sql.operation = []driver.Value{string(rawReservation), string(rd), string(opDigest), op.CallID, uint64(1), uint64(1), string(rawObservation), string(od)}
	f.sql.call = []driver.Value{op.Sequence, string(opDigest)}
	summary, manifest := []byte(`{"summary":"DATA only"}`), []byte(`{"manifest":"DATA only"}`)
	plan := domain.DerivedContextPlanV1{Version: 1, Scope: op.Scope, Generation: generation, OperationSequence: op.Sequence, ReservationDigest: rd, ObservationDigest: od, NormalizedResponseDigest: response.Digest, SourceViewDigest: strings.Repeat("c", 64)}
	for i, body := range [][]byte{summary, manifest} {
		sum := sha256.Sum256(body)
		ref := domain.BlobRef{TenantID: op.Scope.TenantID, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
		role := "summary"
		if i == 1 {
			role = "manifest"
		}
		ref.Key, err = domain.DerivedContextObjectKeyV1(op.Scope, generation, role, ref.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			plan.Summary = ref
		} else {
			plan.Manifest = ref
		}
	}
	return plan, authenticated, summary, manifest
}

func TestLocalRetainedCompactionKnownResponseOverrunDenies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newRetainedEntryFixture(t)
		op := f.operation(t, 1)
		handle, err := f.entry.PrepareRetainedCompaction(t.Context(), op, 1)
		if err != nil {
			t.Fatal(err)
		}
		plan, observed, summary, manifest := f.complete(t, op, 1, true)
		if !observed.Observation.Usage.ExceedsReservation(op.Debit) || f.bootstrap.operate.VerifyAttemptOperationObservation(observed) != nil {
			t.Fatal("regression requires genuine authenticated overrun")
		}
		check, stop := ownedAuthorityCallContext(t.Context())
		defer stop()
		if err := f.store.validateLocalNormalizedModel(check, op, observed.Observation); err != nil {
			t.Fatalf("causal normalized overrun DATA failed codec/body verification: %v", err)
		}
		if err := handle.Upload(t.Context(), plan, observed, summary, manifest); err == nil {
			t.Fatal("authenticated normalized known response over reservation uploaded retained objects")
		}
		if f.blobs.retainedPuts != 0 || f.sql.registrationWrites != 0 {
			t.Fatal("overrun performed retained effects")
		}
	})
}

func (f *retainedEntryFixture) run(t *testing.T) domain.Run {
	t.Helper()
	var run domain.Run
	if err := json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestLocalRetainedCompactionRegistersBeforePUTAndPreservesRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newRetainedEntryFixture(t)
		original := f.run(t)
		var expectedHead *domain.DerivedContextHeadV1
		for generation := uint64(1); generation <= 2; generation++ {
			op := f.operation(t, generation)
			handle, err := f.entry.PrepareRetainedCompaction(t.Context(), op, generation)
			if err != nil {
				t.Fatalf("original Prepare generation%d: %v", generation, err)
			}
			plan, observed, summary, manifest := f.complete(t, op, generation)
			if err := handle.Upload(t.Context(), plan, observed, summary, manifest); err != nil {
				t.Fatalf("registered original Upload generation%d: %v", generation, err)
			}
			if f.blobs.premature || f.blobs.retainedPuts != int(2*generation) || f.sql.registrationWrites != int(generation) {
				t.Fatal("retained PUT did not follow exactly one confirmed inventory registration")
			}
			current := f.run(t)
			if len(current.DerivedContext.Inventory) != int(generation) || !reflect.DeepEqual(current.DerivedContext.Head, expectedHead) || !reflect.DeepEqual(current.DerivedContext.Inventory[generation-1], plan) {
				t.Fatal("registered pair/head changed")
			}
			if generation == 1 {
				// Seed a selected head as historical DATA only; this is not the
				// unimplemented source/custody publisher or its acceptance proof.
				digest, _ := plan.Digest()
				expectedHead = &domain.DerivedContextHeadV1{Version: 1, PlanDigest: digest}
				current.DerivedContext.Head = expectedHead
				raw, _ := json.Marshal(current)
				f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
			}
			current.DerivedContext = nil
			if !reflect.DeepEqual(current, original) {
				t.Fatal("payload-only registration changed base Run")
			}
			if handle.Upload(t.Context(), plan, observed, summary, manifest) == nil {
				t.Fatal("same handle retried retained pair")
			}
		}
	})
}

func TestLocalRetainedCompactionDeniesBeforeRetainedPUT(t *testing.T) {
	for _, name := range []string{"stale_metadata", "cancelled", "source_closed", "source_closed_after_commit", "invocation_cancelled", "foreign_scope", "wrong_observation", "oversize_authenticator", "missing_row", "wrong_durable_observation", "wrong_bytes", "unknown_commit", "lost_commit_not_persisted", "rollback", "ambiguous_put"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newRetainedEntryFixture(t)
				op := f.operation(t, 1)
				handle, err := f.entry.PrepareRetainedCompaction(t.Context(), op, 1)
				if err != nil {
					t.Fatal(err)
				}
				plan, observed, summary, manifest := f.complete(t, op, 1)
				switch name {
				case "stale_metadata":
					run := f.run(t)
					run.DerivedContext = runPayloadMetadata(t, run)
					raw, _ := json.Marshal(run)
					f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
				case "cancelled":
					run := f.run(t)
					at := f.parent.graph.at
					run.CancellationRequestedAt = &at
					raw, _ := json.Marshal(run)
					f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
				case "source_closed":
					if err := f.actual.source.Close(); err != nil {
						t.Fatal(err)
					}
				case "source_closed_after_commit":
					f.parent.graph.afterCommit = func() {
						if err := f.actual.source.Close(); err != nil {
							t.Error(err)
						}
					}
				case "invocation_cancelled":
					f.cancelInvocation()
				case "foreign_scope":
					plan.Scope.OwnerUserID = "foreign-owner"
					plan.Summary.Key, _ = domain.DerivedContextObjectKeyV1(plan.Scope, plan.Generation, "summary", plan.Summary.SHA256)
					plan.Manifest.Key, _ = domain.DerivedContextObjectKeyV1(plan.Scope, plan.Generation, "manifest", plan.Manifest.SHA256)
				case "wrong_observation":
					observed.Authenticator[0] ^= 1
				case "oversize_authenticator":
					observed.Authenticator = make([]byte, 1<<20)
				case "missing_row":
					f.sql.operation = nil
					f.sql.call = nil
				case "wrong_durable_observation":
					f.sql.operation[7] = strings.Repeat("f", 64)
				case "wrong_bytes":
					summary = []byte("wrong")
				case "unknown_commit":
					f.parent.graph.commitModes = []string{"ambiguous", "ambiguous", "ambiguous"}
				case "lost_commit_not_persisted":
					// The existing fixture restores the pre-transaction Run but
					// returns a retryable lost-ACK error. A second callback would
					// see unchanged metadata and confirm successfully if allowed.
					f.parent.graph.commitModes = []string{"rollback"}
				case "rollback":
					f.parent.graph.commitModes = []string{"rollback", "rollback", "rollback"}
				case "ambiguous_put":
					f.blobs.ambiguous = true
				}
				if err := handle.Upload(t.Context(), plan, observed, summary, manifest); err == nil {
					t.Fatalf("%s Upload succeeded: registration attempts=%d retained PUTs=%d", name, f.sql.registrationWrites, f.blobs.retainedPuts)
				}
				if name == "lost_commit_not_persisted" {
					if f.sql.registrationWrites != 1 || f.run(t).DerivedContext != nil {
						t.Fatalf("ambiguous nonpersisted registration retried: attempts=%d", f.sql.registrationWrites)
					}
				}
				want := 0
				if name == "ambiguous_put" {
					want = 1
				}
				if f.blobs.retainedPuts != want || f.blobs.premature {
					t.Fatalf("%s retained PUTs=%d want%d", name, f.blobs.retainedPuts, want)
				}
				if handle.Upload(t.Context(), plan, observed, summary, manifest) == nil || f.blobs.retainedPuts != want {
					t.Fatal("failed/ambiguous Upload retried")
				}
			})
		})
	}
}

func TestLocalRetainedCompactionPrepareDeniesBeforeGeneration(t *testing.T) {
	for _, name := range []string{"full_inventory", "underreserved_bytes", "non_compaction", "copied_entry", "cancelled", "source_closed"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newRetainedEntryFixture(t)
				op := f.operation(t, 1)
				entry := f.entry
				switch name {
				case "full_inventory":
					run := f.run(t)
					run.DerivedContext = runPayloadMetadata(t, run)
					raw, _ := json.Marshal(run)
					f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
				case "underreserved_bytes":
					op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
				case "non_compaction":
					op.Kind = domain.AttemptOperationModelV1
					op.Debit.CompactionCalls = 0
					op.SummaryBytes = 0
				case "copied_entry":
					copy := *f.entry
					entry = &copy
				case "cancelled":
					f.cancelInvocation()
				case "source_closed":
					if err := f.actual.source.Close(); err != nil {
						t.Fatal(err)
					}
				}
				before := f.blobs.calls
				if h, err := entry.PrepareRetainedCompaction(t.Context(), op, 1); err == nil || h != nil {
					t.Fatalf("%s prepared retained generation", name)
				}
				if f.blobs.calls != before || f.sql.registrationWrites != 0 {
					t.Fatal("denied Prepare performed blob/registration effects")
				}
				if name == "full_inventory" {
					run := f.run(t)
					run.DerivedContext = nil
					raw, _ := json.Marshal(run)
					f.parent.graph.rows["runs"] = []driver.Value{string(raw)}
					if h, err := f.entry.PrepareRetainedCompaction(t.Context(), op, 1); err != nil || h == nil {
						t.Fatalf("full inventory rejection consumed pre-generation slot: %v", err)
					}
				}
			})
		})
	}
}
