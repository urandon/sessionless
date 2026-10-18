package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Real original private Source, bootstrap, entry, bounded Store callbacks and
// original blob/body adapters. SQL rows/isolation and completed observations are
// SCRIPTED DATA, not native SQL, transport, summary fidelity or live compaction.
type compactionSourceSQL struct {
	*retainedEntrySQL
	events           [][]driver.Value
	snapshot         []driver.Value
	operations       [][]driver.Value
	proxies          [][]driver.Value
	firstProxy       []driver.Value
	historyRuns      map[domain.RunID]string // explicit canonical-only legacy DATA
	historyMarkers   map[domain.RunID][2]string
	historyManifests map[domain.ArtifactManifestID]string
	rangeReads       int
	maxStatements    int
	statements       int
	failClose        bool
}
type compactionSourceConnector struct{ f *compactionSourceSQL }

func (c compactionSourceConnector) Connect(context.Context) (driver.Conn, error) { return c.f, nil }
func (c compactionSourceConnector) Driver() driver.Driver {
	return retainedEntryConnector{c.f.retainedEntrySQL}.Driver()
}
func (f *compactionSourceSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if f.statements > f.maxStatements {
		f.maxStatements = f.statements
	}
	f.statements = 0
	return f.retainedEntrySQL.BeginTx(ctx, options)
}
func (f *compactionSourceSQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.statements++
	var originalJob domain.WorkerJob
	if len(f.retainedEntrySQL.loopbackOperationSQL.f.parent.graph.rows["worker_jobs"]) != 0 {
		_ = json.Unmarshal([]byte(f.retainedEntrySQL.loopbackOperationSQL.f.parent.graph.rows["worker_jobs"][0].(string)), &originalJob)
	}
	scopeTenant, scopeSession := originalJob.TenantID, originalJob.SessionID
	if statement == localTerminalHistoryRunsSQL(len(args)-3) {
		if len(args) < 4 || args[0].Value != string(scopeTenant) || args[1].Value != string(scopeSession) {
			return nil, errors.New("history discovery exact canonical scope")
		}
		cap, ok := args[len(args)-1].Value.(int64)
		if !ok || cap < 1 || cap > 32769 {
			return nil, errors.New("history discovery bounded cell")
		}
		ids := make([]string, 0, len(args)-3)
		for _, arg := range args[2 : len(args)-1] {
			id, ok := arg.Value.(string)
			if !ok {
				return nil, errors.New("history exact Run key")
			}
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var rows [][]driver.Value
		for _, id := range ids {
			if raw, exists := f.historyRuns[domain.RunID(id)]; exists {
				if int64(len(raw)) > cap {
					raw = raw[:cap]
				}
				marker := f.historyMarkers[domain.RunID(id)]
				rows = append(rows, []driver.Value{id, raw, marker[0], marker[1]})
			}
		}
		return &compactionSourceRows{rows: rows}, nil
	}
	if statement == localTerminalLegacyManifestsSQL(len(args)-2) {
		return canonicalHistoryManifestRows(scopeTenant, f.historyManifests, args)
	}
	var values [][]driver.Value
	switch statement {
	case ownedProxyRegistrationSQL:
		if f.firstProxy == nil {
			return f.retainedEntrySQL.QueryContext(ctx, statement, args)
		}
		if len(args) != 3 || args[0].Value != string(scopeTenant) || args[1].Value != string(originalJob.AttemptID) {
			return nil, errors.New("source original first proxy exact keys")
		}
		return &explanationFixtureRows{values: append([]driver.Value(nil), f.firstProxy...)}, nil
	case localCompactionEventsSQL:
		f.rangeReads++
		if len(args) != 6 || args[0].Value != string(scopeTenant) || args[1].Value != string(scopeSession) || args[4].Value != args[3].Value.(int64)-args[2].Value.(int64)+1 || args[5].Value.(int64) > 8193 || args[4].Value.(int64)*args[5].Value.(int64) > 1<<20 {
			return nil, errors.New("source range must bind exact original window and transfer cap")
		}
		for _, row := range f.events {
			raw := row[0].(string)
			if len(raw) > int(args[5].Value.(int64)) {
				raw = raw[:args[5].Value.(int64)]
			}
			values = append(values, []driver.Value{raw})
		}
	case localCompactionSnapshotSQL:
		if len(args) != 3 || args[0].Value != string(scopeTenant) || args[1].Value != string(scopeSession) {
			return nil, errors.New("source snapshot exact keys")
		}
		if f.snapshot != nil {
			values = [][]driver.Value{f.snapshot}
		}
	case localCompactionOperationsSQL, localCompactionProxiesSQL:
		if len(args) != 2 || args[0].Value != string(scopeTenant) || args[1].Value != string(originalJob.AttemptID) {
			return nil, errors.New("source inventory exact scope")
		}
		if statement == localCompactionOperationsSQL {
			values = f.operations
		} else {
			values = f.proxies
		}
	default:
		return f.retainedEntrySQL.QueryContext(ctx, statement, args)
	}
	return &compactionSourceRows{rows: values, failClose: f.failClose}, nil
}
func (f *compactionSourceSQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	f.statements++
	return f.retainedEntrySQL.ExecContext(ctx, statement, args)
}

type compactionSourceRows struct {
	rows      [][]driver.Value
	next      int
	failClose bool
}

func (r *compactionSourceRows) Columns() []string {
	n := 1
	if len(r.rows) > 0 {
		n = len(r.rows[0])
	}
	result := make([]string, n)
	for i := range result {
		result[i] = fmt.Sprint(i)
	}
	return result
}
func (r *compactionSourceRows) Next(dest []driver.Value) error {
	if r.next == len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}
func (r *compactionSourceRows) Close() error {
	if r.failClose {
		return errors.New("owned test close failure")
	}
	return nil
}

type compactionSourceBlobs struct {
	retainedEntryBlobs
	beforeOpen          func()
	badClose            bool
	openErrorWithReader bool
	opened              int
	closed              int
}

func (b *compactionSourceBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if b.beforeOpen != nil {
		hook := b.beforeOpen
		b.beforeOpen = nil
		hook()
	}
	r, err := b.localEntryBlobs.Open(ctx, tenant, ref)
	if err != nil {
		return nil, err
	}
	b.opened++
	reader := &compactionSourceReader{ReadCloser: r, b: b}
	if b.openErrorWithReader {
		return reader, errors.New("owned Open returned acquired reader and error")
	}
	return reader, nil
}

type compactionSourceReader struct {
	io.ReadCloser
	b *compactionSourceBlobs
}

func (r *compactionSourceReader) Close() error {
	r.b.closed++
	e := r.ReadCloser.Close()
	if r.b.badClose {
		return errors.New("owned blob close failure")
	}
	return e
}

type compactionSourceFixture struct {
	*retainedEntryFixture
	queries   *compactionSourceSQL
	canonical *compactionSourceBlobs
	records   []sessioncontext.EventPayload
}

func newCompactionSourceFixture(t *testing.T, toolHistory bool, pinned ...bool) *compactionSourceFixture {
	t.Helper()
	return newCompactionSourceFixtureWithPrebuild(t, toolHistory, localRuntimeActualPrebuild{}, pinned...)
}

func newCompactionSourceFixtureWithPrebuild(t *testing.T, toolHistory bool, prebuild localRuntimeActualPrebuild, pinned ...bool) *compactionSourceFixture {
	t.Helper()
	f := newLocalBootstrapFixtureWithPrebuild(t, prebuild)
	fixture := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{&allocationSQLFixture{parent: f.parent.sql}}}}}}
	db := sql.OpenDB(compactionSourceConnector{fixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store.db = db
	var run domain.Run
	var job domain.WorkerJob
	if json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &run) != nil || json.Unmarshal([]byte(f.parent.graph.rows["worker_jobs"][0].(string)), &job) != nil {
		t.Fatal("canonical fixture rows")
	}
	count := 1
	if toolHistory {
		count = 7
	}
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: uint64(count)}
	if len(pinned) > 0 && pinned[0] {
		version := uint64(3)
		job.ContextWindow.SnapshotVersion = &version
		job.ContextWindow.AfterSequence = 1
	}
	ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "worker_jobs", job)
	completeLocalEntryFixture(t, f)
	blobs := &compactionSourceBlobs{retainedEntryBlobs: retainedEntryBlobs{sql: fixture.retainedEntrySQL}}
	invocation, cancel := context.WithCancel(f.actual.ctx)
	t.Cleanup(cancel)
	entry, err := f.bootstrap.StartLocalInvocation(invocation, f.parent.request, blobs)
	if err != nil {
		t.Fatalf("original canonical-window entry: %v", err)
	}
	frozen, err := configuredinference.FreezeRequestV1(f.actual.config, f.actual.config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "original scripted context DATA"}}, MaxOutputTokens: 1})
	if err != nil {
		t.Fatal(err)
	}
	request, err := entry.bodies.Write(t.Context(), frozen.Body())
	if err != nil {
		t.Fatal(err)
	}
	base := &retainedEntryFixture{localBootstrapFixture: f, entry: entry, sql: fixture.retainedEntrySQL, blobs: &blobs.retainedEntryBlobs, frozen: frozen, request: request, cancelInvocation: cancel}
	result := &compactionSourceFixture{retainedEntryFixture: base, queries: fixture, canonical: blobs}
	author := job.CredentialOwnerUserID
	producingRun := domain.RunID("historical-run")
	for i := 1; i <= count; i++ {
		kind := domain.SessionEventUserMessage
		body := `{"version":1,"text":"original task with constraints"}`
		if toolHistory {
			switch i {
			case 1:
				kind = domain.SessionEventSystemNotice
				body = `{"schema":"sessionless.run-terminal-notice.v1","code":"historical_notice","cancelled":false}`
			case 2, 3, 4, 5:
				kind = domain.SessionEventToolCall
				if i >= 4 {
					kind = domain.SessionEventToolResult
				}
				call := "a"
				if i == 3 || i == 5 {
					call = "b"
				}
				body = fmt.Sprintf(`{"schema":"sessionless.tool-event.v1","call_id":%q,"tool_name":"history_tool","content":{"data":"exact history bytes"}}`, call)
			case 6:
				kind = domain.SessionEventAssistantMessage
				body = `{"schema":"sessionless.assistant-message.v1","summary":"historical answer","artifact_manifest_id":"historical-manifest"}`
			}
		}
		eventID := domain.SessionEventID(fmt.Sprintf("history-%d", i))
		if i == count {
			eventID = run.TriggerEventID
		}
		event := domain.SessionEvent{ID: eventID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: uint64(i), Kind: kind, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("history-key-%d", i)), CreatedAt: f.parent.graph.at}
		if kind == domain.SessionEventUserMessage {
			event.AuthorUserID = &author
		}
		if kind == domain.SessionEventToolCall || kind == domain.SessionEventToolResult || kind == domain.SessionEventAssistantMessage {
			event.RunID = &producingRun
		}
		ref, err := blobs.Put(t.Context(), run.TenantID, domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, eventID)+"message.json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		event.Payload = ref
		if err := event.Validate(); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(event)
		fixture.events = append(fixture.events, []driver.Value{string(raw)})
		result.records = append(result.records, sessioncontext.EventPayload{Event: event, Payload: []byte(body)})
	}
	seedCanonicalHistoryRuns(t, fixture, run, result.records)
	if job.ContextWindow.SnapshotVersion != nil {
		compressed, history, err := sessioncontext.EncodeSnapshot(result.records[:1])
		if err != nil {
			t.Fatal(err)
		}
		defer clear(compressed)
		defer clear(history)
		ref, err := blobs.Put(t.Context(), run.TenantID, domain.SessionSnapshotObjectKey(run.TenantID, run.SessionID, 3), bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		snapshot := domain.SessionSnapshot{ID: "original-snapshot", TenantID: run.TenantID, SessionID: run.SessionID, Version: 3, ThroughSequence: 1, EventCount: 1, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), Payload: ref, CreatedAt: f.parent.graph.at}
		if err := snapshot.Validate(); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(snapshot)
		fixture.snapshot = []driver.Value{string(raw)}
		fixture.events = fixture.events[1:]
	}
	return result
}

// Existing tool-heavy rows are explicit canonical V1 DATA, not an owned local
// producer. Seed the complete matching V1 manifest/seal BEFORE the Store is
// called; queries never invent missing output or authority. This is not a
// claim that the fixture executed a managed process or attached worker.
func seedCanonicalHistoryRuns(t *testing.T, fixture *compactionSourceSQL, current domain.Run, records []sessioncontext.EventPayload) {
	t.Helper()
	fixture.historyRuns = make(map[domain.RunID]string)
	fixture.historyMarkers = make(map[domain.RunID][2]string)
	fixture.historyManifests = make(map[domain.ArtifactManifestID]string)
	for _, row := range fixture.events {
		var e domain.SessionEvent
		if json.Unmarshal([]byte(row[0].(string)), &e) != nil {
			t.Fatal("canonical legacy fixture event")
		}
		if e.Kind != domain.SessionEventAssistantMessage || e.RunID == nil || *e.RunID == current.ID {
			continue
		}
		legacy := current.Clone()
		legacy.ID, legacy.TriggerEventID, legacy.IdempotencyKey = *e.RunID, "legacy-trigger", domain.IdempotencyKey("legacy-"+string(*e.RunID))
		legacy.Status, legacy.UpdatedAt = domain.RunSucceeded, e.CreatedAt
		finished := e.CreatedAt
		legacy.StartedAt, legacy.FinishedAt, legacy.CancellationRequestedAt = nil, &finished, nil
		legacy.DerivedContext, legacy.TerminalCustody = nil, nil
		if err := legacy.Validate(); err != nil {
			t.Fatalf("explicit canonical-only legacy Run DATA: %v", err)
		}
		raw, err := json.Marshal(legacy)
		if err != nil {
			t.Fatal(err)
		}
		fixture.historyRuns[legacy.ID] = string(raw)
		drafts, manifestID, err := markerLegacyDraftsData(t, legacy, records)
		if err != nil {
			t.Fatalf("complete canonical V1 fixture material: %v", err)
		}
		manifest := domain.ArtifactManifest{ID: manifestID, TenantID: legacy.TenantID, RunID: legacy.ID, CreatedAt: legacy.UpdatedAt}
		if err := manifest.ValidateForRun(legacy); err != nil {
			t.Fatal(err)
		}
		seal, err := runFinalizationDigest(domain.RunSucceeded, &manifest, drafts, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		manifestRaw, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		fixture.historyMarkers[legacy.ID] = [2]string{string(domain.RunSucceeded), seal}
		fixture.historyManifests[manifestID] = string(manifestRaw)
	}
}

func canonicalHistoryManifestRows(tenant domain.TenantID, manifests map[domain.ArtifactManifestID]string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) < 3 || args[0].Value != string(tenant) {
		return nil, errors.New("legacy manifest exact tenant")
	}
	cap, ok := args[len(args)-1].Value.(int64)
	if !ok || cap < 1 || cap > 32769 || (cap+161)*int64(len(args)-1) > 1<<20 {
		return nil, errors.New("legacy manifest bounded transfer")
	}
	var ids []string
	seen := make(map[string]bool)
	for _, arg := range args[1 : len(args)-1] {
		id, ok := arg.Value.(string)
		if !ok || seen[id] {
			return nil, errors.New("legacy manifest malformed/duplicate key")
		}
		seen[id], ids = true, append(ids, id)
	}
	sort.Strings(ids)
	var rows [][]driver.Value
	for _, id := range ids {
		if raw, exists := manifests[domain.ArtifactManifestID(id)]; exists {
			if int64(len(raw)) > cap {
				raw = raw[:cap]
			}
			rows = append(rows, []driver.Value{id, raw})
		}
	}
	return &compactionSourceRows{rows: rows}, nil
}

func (f *compactionSourceFixture) seedCompleted(t *testing.T, op domain.AttemptOperationV1, generation uint64, overrun ...bool) (domain.DerivedContextPlanV1, ports.AuthenticatedAttemptOperationObservationV1, []byte, []byte) {
	t.Helper()
	plan, observed, summary, manifest := f.complete(t, op, generation, overrun...)
	// Existing retained fixture omits setup accounting because its tests did
	// not consume the complete ledger. Here replay the real setup debit and
	// persist the exact full frontier consumed by Capture.
	var accounting ownedAttemptAccounting
	if json.Unmarshal([]byte(f.parent.sql.accountingRow[2].(string)), &accounting) != nil {
		t.Fatal("setup accounting")
	}
	next, err := debitOwnedOperation(accounting, op, f.entry.parent, f.entry.source, observed.Reservation.ReservedAt)
	if err != nil {
		t.Fatal(err)
	}
	r := observed.Reservation
	r.AggregateDebit = next.Aggregate
	r.ComponentAmounts = next.Amounts
	raw, _ := json.Marshal(r)
	rd, _ := r.Digest()
	o := observed.Observation
	o.ReservationDigest = rd
	if op.Kind == domain.AttemptOperationModelV1 {
		// The inherited retained DATA helper generates a compaction envelope;
		// this source fixture supplies an actual MODEL normalized body instead.
		wire := []byte(fmt.Sprintf(`{"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"summary DATA"},"finish_reason":"stop"}]}`, f.actual.config.ModelID))
		body, err := f.frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := f.entry.bodies.Write(t.Context(), body)
		clear(body)
		if err != nil {
			t.Fatal(err)
		}
		o.ResponseRef, o.ResponseDigest, o.ResponseBytes = ref.Ref, ref.Digest, ref.Bytes
	}
	oraw, _ := json.Marshal(o)
	od, _ := o.Digest()
	grant, err := f.bootstrap.operate.MintAttemptOperationObservationGrant(r, f.parent.graph.at, f.parent.graph.at.Add(op.CleanupDuration))
	if err != nil {
		t.Fatal(err)
	}
	observed, err = f.bootstrap.operate.AuthenticateAttemptOperationObservation(grant, o)
	if err != nil {
		t.Fatal(err)
	}
	f.sql.operation = []driver.Value{string(raw), string(rd), string(r.OperationDigest), op.CallID, uint64(1), uint64(1), string(oraw), string(od)}
	row := append([]driver.Value{op.Sequence}, f.sql.operation...)
	row = append(row, op.Sequence, string(r.OperationDigest))
	f.queries.operations = append(f.queries.operations, row)
	if op.Sequence == 1 {
		f.queries.firstProxy = append([]driver.Value(nil), f.sql.row...)
	}
	if op.Sequence > 1 {
		f.queries.proxies = append(f.queries.proxies, append(append([]driver.Value{op.Sequence}, f.sql.row...), string(domain.RegisteredProxyProviderV1)))
	}
	encoded, digest, err := accountingBytes(next)
	if err != nil {
		t.Fatal(err)
	}
	f.parent.sql.accountingRow = []driver.Value{uint64(1), uint64(1), string(encoded), digest}
	plan.ReservationDigest = rd
	plan.ObservationDigest = od
	return plan, observed, summary, manifest
}

func TestLocalCompactionSourceOriginalCanonicalToolsAndClear(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, true)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatalf("capture original historical multicall source: %v (range reads %d, last statements %d)", err, f.queries.rangeReads, f.queries.statements)
		}
		defer source.Clear()
		records := source.Records()
		if len(records) != 7 || !reflect.DeepEqual(records, f.records) || len(source.Digest()) != 64 {
			t.Fatalf("source changed original records: count=%d digest length=%d", len(records), len(source.Digest()))
		}
		units := source.Units()
		if len(units) != 3 || !units[0].Protected || units[1].Start != 1 || units[1].End != 6 || !units[2].Protected {
			t.Fatalf("historical paired units/protection: %+v", units)
		}
		// Original capture, pre-history join, then post-materialization join.
		// A fourth range read is still an unexpected retry/fallback.
		if f.queries.rangeReads != 3 || f.queries.maxStatements > 32 || f.queries.statements > 32 || f.canonical.opened != f.canonical.closed {
			t.Fatalf("bounded capture/history joins/close: range=%d statements=%d/%d open/close=%d/%d", f.queries.rangeReads, f.queries.maxStatements, f.queries.statements, f.canonical.opened, f.canonical.closed)
		}
		records[0].Payload[0] = '!'
		if source.Records()[0].Payload[0] == '!' {
			t.Fatal("caller payload aliases original source")
		}
		copySource := *source
		copySource.Clear()
		if source.Digest() != "" || source.Records() != nil || source.Operations() != nil {
			t.Fatal("Clear through wrapper copy did not retire original state")
		}
	})
}

func TestLocalCompactionSourceCompletedUnknownTokenUsage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, false)
		op := f.operation(t, 1)
		f.seedCompleted(t, op, 1)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatalf("known completed normalized response with unknown token usage: %v range=%d statements=%d", err, f.queries.rangeReads, f.queries.statements)
		}
		defer source.Clear()
		ops := source.Operations()
		if len(ops) != 1 || ops[0].Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 || !bytes.Equal(ops[0].Request, f.frozen.Body()) {
			t.Fatal("source lost normalized unknown-usage operation DATA")
		}
		ops[0].Request[0] = '!'
		if source.Operations()[0].Request[0] == '!' {
			t.Fatal("operation accessor aliases original bytes")
		}
	})
}

func TestLocalCompactionSourcePinnedSnapshotAndTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, true, true)
		s, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatalf("exact pinned snapshot plus tool-heavy tail: %v", err)
		}
		defer s.Clear()
		if !reflect.DeepEqual(s.Records(), f.records) {
			t.Fatal("snapshot/tail changed original canonical source")
		}
		if f.queries.rangeReads != 3 {
			t.Fatalf("capture plus pre/post history joins retried fallback: range reads=%d want=3", f.queries.rangeReads)
		}
	})
	for _, name := range []string{"missing_exact_snapshot", "wrong_pinned_version", "wrong_coverage"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newCompactionSourceFixture(t, true, true)
				opened := f.canonical.opened
				if name == "missing_exact_snapshot" {
					f.queries.snapshot = nil
				} else {
					var snapshot domain.SessionSnapshot
					json.Unmarshal([]byte(f.queries.snapshot[0].(string)), &snapshot)
					if name == "wrong_pinned_version" {
						snapshot.Version = 2
					} else {
						snapshot.ThroughSequence = 2
						snapshot.EventCount = 2
					}
					raw, _ := json.Marshal(snapshot)
					f.queries.snapshot[0] = string(raw)
				}
				s, err := f.entry.CaptureCompactionSource(t.Context())
				if err == nil || s != nil {
					t.Fatal("invalid exact pinned snapshot accepted/fell back")
				}
				if f.canonical.opened != opened {
					t.Fatal("invalid pinned snapshot opened body")
				}
			})
		})
	}
}

func TestLocalCompactionSourcePendingNormalizedToolCallDenied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, false)
		frozen, err := configuredinference.FreezeRequestV1(f.actual.config, f.actual.config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "original scripted context DATA"}}, Tools: []configuredinference.ToolV1{{Name: "lookup", Description: "scripted bounded tool", Parameters: []byte(`{"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":30}},"required":["query"],"additionalProperties":false}`)}}, MaxOutputTokens: 1})
		if err != nil {
			t.Fatal(err)
		}
		f.frozen = frozen
		f.request, err = f.entry.bodies.Write(t.Context(), frozen.Body())
		if err != nil {
			t.Fatal(err)
		}
		op := f.operation(t, 1)
		op.Kind = domain.AttemptOperationModelV1
		op.SummaryBytes = 0
		op.Debit.CompactionCalls = 0
		_, observed, _, _ := f.seedCompleted(t, op, 1)
		wire := []byte(fmt.Sprintf(`{"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"pending-call","type":"function","function":{"name":"lookup","arguments":"{\"query\":\"docs\"}"}}]},"finish_reason":"tool_calls"}]}`, f.actual.config.ModelID))
		normalized, err := frozen.NormalizeResponseBodyV1(wire, domain.AttemptOperationModelV1, op.ResultBytes)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(f.actual.config, frozen.Body(), normalized, op.Kind)
		if err != nil || decoded.Call == nil {
			t.Fatalf("causal pending-call fixture invalid: %v", err)
		}
		clear(decoded.Call.Arguments)
		ref, err := f.entry.bodies.Write(t.Context(), normalized)
		if err != nil {
			t.Fatal(err)
		}
		o := observed.Observation
		o.ResponseRef, o.ResponseDigest, o.ResponseBytes = ref.Ref, ref.Digest, ref.Bytes
		raw, _ := json.Marshal(o)
		digest, _ := o.Digest()
		f.queries.operations[0][7], f.queries.operations[0][8] = string(raw), string(digest)
		s, err := f.entry.CaptureCompactionSource(t.Context())
		if err == nil || s != nil {
			t.Fatal("known model effect with pending declared tool call admitted as compaction source")
		}
		if f.queries.rangeReads != 1 {
			t.Fatalf("pending call was not rejected after original body verification: range reads=%d", f.queries.rangeReads)
		}
	})
}

func TestLocalCompactionSourceDenies(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *compactionSourceFixture)
	}{
		{"missing_event", func(_ *testing.T, f *compactionSourceFixture) { f.queries.events = nil }},
		{"event_overflow", func(_ *testing.T, f *compactionSourceFixture) {
			f.queries.events = append(f.queries.events, f.queries.events[0])
		}},
		{"foreign_event", func(t *testing.T, f *compactionSourceFixture) {
			var e domain.SessionEvent
			json.Unmarshal([]byte(f.queries.events[0][0].(string)), &e)
			e.SessionID = "foreign"
			raw, _ := json.Marshal(e)
			f.queries.events[0][0] = string(raw)
		}},
		{"cell_overflow", func(_ *testing.T, f *compactionSourceFixture) { f.queries.events[0][0] = strings.Repeat("x", 8193) }},
		{"missing_blob", func(_ *testing.T, f *compactionSourceFixture) {
			delete(f.canonical.objects, f.records[0].Event.Payload.Key)
		}},
		{"changed_blob", func(_ *testing.T, f *compactionSourceFixture) {
			f.canonical.objects[f.records[0].Event.Payload.Key] = []byte("different")
		}},
		{"close_error", func(_ *testing.T, f *compactionSourceFixture) { f.canonical.badClose = true }},
		{"open_error_with_reader", func(_ *testing.T, f *compactionSourceFixture) { f.canonical.openErrorWithReader = true }},
		{"rows_close_error", func(_ *testing.T, f *compactionSourceFixture) { f.queries.failClose = true }},
		{"cancel_during_blobs", func(_ *testing.T, f *compactionSourceFixture) { f.canonical.beforeOpen = f.cancelInvocation }},
		{"run_changes_after_blob_read", func(t *testing.T, f *compactionSourceFixture) {
			f.canonical.beforeOpen = func() {
				var r domain.Run
				json.Unmarshal([]byte(f.parent.graph.rows["runs"][0].(string)), &r)
				at := f.parent.graph.at
				r.CancellationRequestedAt = &at
				ratedFixturePut(t, f.parent.graph.ratedSQLFixture, "runs", r)
			}
		}},
		{"frontier_changes_after_blob_read", func(t *testing.T, f *compactionSourceFixture) {
			f.canonical.beforeOpen = func() { f.seedCompleted(t, f.operation(t, 1), 1) }
		}},
		{"unobserved_operation", func(t *testing.T, f *compactionSourceFixture) {
			f.seedCompleted(t, f.operation(t, 1), 1)
			r := f.queries.operations[0]
			r[5], r[6], r[7], r[8] = uint64(0), uint64(0), "", ""
		}},
		{"unknown_effect", func(t *testing.T, f *compactionSourceFixture) {
			f.seedCompleted(t, f.operation(t, 1), 1)
			r := f.queries.operations[0]
			var o domain.AttemptOperationObservationV1
			json.Unmarshal([]byte(r[7].(string)), &o)
			o.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			o.Reason = domain.AttemptOperationReasonTransportUnknownV1
			o.ResponseRef, o.ResponseDigest, o.ResponseBytes = "", "", 0
			var reservation domain.AttemptOperationReservationV1
			if json.Unmarshal([]byte(r[1].(string)), &reservation) != nil || o.ValidateForReservation(reservation) != nil {
				t.Fatal("unknown-effect fixture must be a valid durable observation")
			}
			if ownedObservationAllowsSuccessor(reservation, o) {
				t.Fatal("well-formed unknown outcome unexpectedly permits successor")
			}
			raw, _ := json.Marshal(o)
			d, err := o.Digest()
			if err != nil {
				t.Fatal(err)
			}
			r[7], r[8] = string(raw), string(d)
		}},
		{"missing_operation", func(t *testing.T, f *compactionSourceFixture) {
			f.seedCompleted(t, f.operation(t, 1), 1)
			f.queries.operations = nil
		}},
		{"operation_gap", func(t *testing.T, f *compactionSourceFixture) {
			f.seedCompleted(t, f.operation(t, 1), 1)
			f.queries.operations[0][0] = uint64(2)
		}},
		{"known_overrun", func(t *testing.T, f *compactionSourceFixture) { f.seedCompleted(t, f.operation(t, 1), 1, true) }},
		{"trailing_operation", func(t *testing.T, f *compactionSourceFixture) {
			f.seedCompleted(t, f.operation(t, 1), 1)
			f.queries.operations = append(f.queries.operations, f.queries.operations[0])
		}},
		{"wrong_index", func(t *testing.T, f *compactionSourceFixture) {
			f.seedCompleted(t, f.operation(t, 1), 1)
			f.queries.operations[0][9] = uint64(2)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newCompactionSourceFixture(t, false)
				c.mutate(t, f)
				puts := f.canonical.calls
				opened := f.canonical.opened
				source, err := f.entry.CaptureCompactionSource(t.Context())
				if source != nil || !errors.Is(err, domain.ErrMembershipDenied) {
					if source != nil {
						source.Clear()
					}
					t.Fatalf("case %s source=%t err=%v, want nil denied", c.name, source != nil, err)
				}
				if f.sql.registrationWrites != 0 || f.canonical.retainedPuts != 0 || f.canonical.calls < puts {
					t.Fatal("denied capture mutated registration or retained storage")
				}
				if f.canonical.opened != f.canonical.closed {
					t.Fatalf("denied capture leaked acquired original readers: open=%d close=%d", f.canonical.opened, f.canonical.closed)
				}
				if c.name == "unknown_effect" && f.canonical.opened != opened {
					t.Fatal("well-formed unknown effect reached source blob materialization")
				}
			})
		})
	}
}

func TestLocalCompactionSourceConsumedBeforeRetainedRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, true)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer source.Clear()
		op := f.operation(t, 1)
		h, err := source.PrepareRetainedCompaction(t.Context(), op, 1)
		if err != nil {
			t.Fatalf("original source-bound pre-generation reserve: %v", err)
		}
		plan, observed, summary, manifest := f.seedCompleted(t, op, 1)
		plan.SourceViewDigest = source.Digest()
		if err := h.Upload(t.Context(), plan, observed, summary, manifest); err != nil {
			t.Fatalf("source-bound confirmed registration and retained write: %v (statements %d, max %d)", err, f.queries.statements, f.queries.maxStatements)
		}
		if f.sql.registrationWrites != 1 || !f.sql.committed || f.canonical.retainedPuts != 2 || f.canonical.premature || f.queries.statements > 32 || f.queries.maxStatements > 32 {
			t.Fatalf("source registration/physical boundary: writes=%d committed=%t PUTs=%d premature=%t statements=%d/%d", f.sql.registrationWrites, f.sql.committed, f.canonical.retainedPuts, f.canonical.premature, f.queries.statements, f.queries.maxStatements)
		}
		if err := h.Upload(t.Context(), plan, observed, summary, manifest); err == nil || f.canonical.retainedPuts != 2 {
			t.Fatal("source-bound handle replayed physical pair")
		}
		t.Logf("actual source path max statements per callback=%d, upload=%d", f.queries.maxStatements, f.queries.statements)
	})
}

func TestLocalCompactionSourceConsumedWithCompletedModelPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, true)
		model := f.operation(t, 1)
		model.Kind = domain.AttemptOperationModelV1
		model.SummaryBytes = 0
		model.Debit.CompactionCalls = 0
		_, originalModel, _, _ := f.seedCompleted(t, model, 1)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatalf("capture valid completed MODEL prefix: %v", err)
		}
		defer source.Clear()
		prefix := source.Operations()
		if len(prefix) != 1 || prefix[0].Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
			t.Fatal("missing original completed MODEL source")
		}
		decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(f.actual.config, prefix[0].Request, prefix[0].Response, model.Kind)
		if err != nil || decoded.Call != nil || decoded.Stop != configuredinference.StopFinalV1 {
			t.Fatalf("causal prefix must be genuine normalized final MODEL DATA: %v", err)
		}
		op := f.operation(t, 2)
		op.PredecessorObservationDigest, err = originalModel.Observation.Digest()
		if err != nil {
			t.Fatal(err)
		}
		h, err := source.PrepareRetainedCompaction(t.Context(), op, 1)
		if err != nil {
			t.Fatalf("source-bound reserve after original completed MODEL: %v", err)
		}
		plan, observed, summary, manifest := f.seedCompleted(t, op, 1)
		plan.SourceViewDigest = source.Digest()
		if err := h.Upload(t.Context(), plan, observed, summary, manifest); err != nil {
			t.Fatalf("exact prefix plus one compaction append registration/PUT: %v (queries=%d max=%d)", err, f.queries.statements, f.queries.maxStatements)
		}
		if f.sql.registrationWrites != 1 || f.canonical.retainedPuts != 2 || f.canonical.premature || f.queries.statements > 32 || f.queries.maxStatements > 32 || !reflect.DeepEqual(prefix, source.Operations()) {
			t.Fatal("nonempty-prefix rejoin lost immutable prefix/budgets or registered without exact source")
		}
		t.Logf("completed MODEL prefix + compaction: statements=%d max=%d", f.queries.statements, f.queries.maxStatements)
	})
}

func TestLocalCompactionSourceBoundUploadDeniesChangedSource(t *testing.T) {
	for _, name := range []string{"caller_digest", "source_cleared", "frontier_stale_before_prepare", "cancel_after_generation"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newCompactionSourceFixture(t, false)
				source, err := f.entry.CaptureCompactionSource(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer source.Clear()
				op := f.operation(t, 1)
				if name == "frontier_stale_before_prepare" {
					f.seedCompleted(t, op, 1)
					if h, e := source.PrepareRetainedCompaction(t.Context(), op, 1); e == nil || h != nil {
						t.Fatal("stale captured frontier reserved another physical pair")
					}
					return
				}
				h, err := source.PrepareRetainedCompaction(t.Context(), op, 1)
				if err != nil {
					t.Fatal(err)
				}
				plan, observed, summary, manifest := f.seedCompleted(t, op, 1)
				plan.SourceViewDigest = source.Digest()
				switch name {
				case "caller_digest":
					plan.SourceViewDigest = strings.Repeat("b", 64)
				case "source_cleared":
					source.Clear()
				case "cancel_after_generation":
					f.cancelInvocation()
				}
				if err := h.Upload(t.Context(), plan, observed, summary, manifest); err == nil || f.sql.registrationWrites != 0 || f.canonical.retainedPuts != 0 {
					t.Fatalf("case %s invalid source reached registration/PUT: err=%v writes=%d puts=%d", name, err, f.sql.registrationWrites, f.canonical.retainedPuts)
				}
			})
		})
	}
}

func TestLocalCompactionSourceDiagnosticsRedacted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, false)
		f.seedCompleted(t, f.operation(t, 1), 1)
		s, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer s.Clear()
		for _, v := range []any{s, s.Operations()[0]} {
			for _, format := range []string{"%v", "%+v", "%#v"} {
				if got := fmt.Sprintf(format, v); strings.Contains(got, "original scripted") || strings.Contains(got, "summary DATA") || strings.Contains(got, "request_digest") {
					t.Fatalf("diagnostics leaked private content via %s", format)
				}
			}
			body, err := json.Marshal(v)
			if err != nil || !strings.Contains(string(body), "[redacted]") || strings.Contains(string(body), "request_digest") {
				t.Fatalf("JSON diagnostics leaked private source: %s %v", body, err)
			}
		}
	})
}

func TestLocalCompactionSourceOriginalAttachmentsVerifiedAndProtected(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt_%t", corrupt), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newCompactionSourceFixture(t, false)
				event := f.records[0].Event
				attachment, err := f.canonical.Put(t.Context(), event.TenantID, domain.SessionEventObjectPrefix(event.TenantID, event.SessionID, event.ID)+"picture.png", strings.NewReader("scripted original image bytes"))
				if err != nil {
					t.Fatal(err)
				}
				payload, err := json.Marshal(struct {
					Version     int                            `json:"version"`
					Text        string                         `json:"text"`
					Attachments []sessioncontext.AttachmentRef `json:"attachments"`
				}{1, "original user constraints", []sessioncontext.AttachmentRef{{Name: "picture.png", MediaType: "image/png", Blob: attachment}}})
				if err != nil {
					t.Fatal(err)
				}
				ref, err := f.canonical.Put(t.Context(), event.TenantID, event.Payload.Key, bytes.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				event.Payload = ref
				f.records[0] = sessioncontext.EventPayload{Event: event, Payload: payload}
				raw, _ := json.Marshal(event)
				f.queries.events[0][0] = string(raw)
				if corrupt {
					f.canonical.objects[attachment.Key] = []byte("corrupt image")
				}
				s, err := f.entry.CaptureCompactionSource(t.Context())
				if corrupt {
					if err == nil || s != nil {
						t.Fatal("corrupt original attachment became source evidence")
					}
					return
				}
				if err != nil {
					t.Fatalf("original attachment source: %v", err)
				}
				defer s.Clear()
				if !reflect.DeepEqual(s.Records(), f.records) || len(s.Units()) != 1 || !s.Units()[0].Protected {
					t.Fatal("original attachment reference/payload not preserved and protected")
				}
				// Verified bytes are not a claim that this text-only configured model
				// viewed an image; rendering/token fit remain future original consumers.
			})
		})
	}
}

func TestLocalCompactionSourceRejectsReconstructedHandle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newCompactionSourceFixture(t, false)
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer source.Clear()
		copySource := *source
		if h, err := copySource.PrepareRetainedCompaction(t.Context(), f.operation(t, 1), 1); err == nil || h != nil {
			t.Fatal("copied source reconstructed original ownership")
		}
	})
}

// Compile-time checks keep the fixture's storage on the exact original port.
var _ ports.BlobStore = (*compactionSourceBlobs)(nil)
