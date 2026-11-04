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
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// Structural retained DATA over actual codecs/readers only. This fixture does
// not create a private serving holder, consent issuer, physical erasure receipt
// or successful original finalizer. Joined serving proof must supply those.
type terminalSearchV3Data struct {
	snapshot localTerminalHistorySnapshot
	capsule  localRetainedTerminalCapsuleV3
	body     []byte
	records  []sessioncontext.EventPayload
}

func terminalSearchV3Fixture(t *testing.T) terminalSearchV3Data {
	t.Helper()
	f := failureSearchV4Data(t, domain.ModelAfterSearchFailureV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
	parent, source := f.history.Parent, f.history.Admission.Source
	at := source.IssuedAt
	r := newOwnedReaderFixtureAt(t, at, "-failure-v4")
	config := syntheticLocalInstallationGrant(t, r.profile).LocalInstallation.Profile.Clone()
	frozen, err := configuredinference.FreezeRequestV1(config, config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "public documentation"}}, Tools: []configuredinference.ToolV1{websearch.ModelToolV1()}, MaxOutputTokens: 100})
	if err != nil {
		t.Fatal(err)
	}
	final, err := frozen.NormalizeResponseBodyV1([]byte(`{"model":"`+config.ModelID+`","choices":[{"index":0,"message":{"role":"assistant","content":"bounded final answer"},"finish_reason":"stop"}]}`), domain.AttemptOperationModelV1, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(final) })
	last := &f.capsule.Operations[2]
	last.Normalized = final
	last.Observation.Outcome, last.Observation.Reason = domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1
	last.Observation.ResponseDigest, last.Observation.ResponseBytes = rawLocalFailureHash(final), uint64(len(final))
	last.Observation.ResponseRef = operationbodyTokenV3(final)
	if last.Observation.ValidateForReservation(last.Reservation) != nil {
		t.Fatal("completed final MODEL DATA")
	}
	accounting := f.history.Accounting
	accounting.UpdatedAt = last.Reservation.ReservedAt
	_, digest, err := accountingBytes(accounting)
	if err != nil {
		t.Fatal(err)
	}
	p := source.Spending[0]
	d := ports.OwnedOperationAccountingDispositionV4{Version: 4, Scope: f.capsule.Accounting.Scope, AccountingDigest: digest, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts, ConsentReservedBytes: f.capsule.Accounting.ConsentReservedBytes, ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: source.Profile.Digest, TermsDigest: p.TermsEvidenceDigest, Revision: p.PriceRevision, Currency: source.Currency, IssuedAt: source.IssuedAt, ExpiresAt: source.ExpiresAt}}
	d.ConsentStatesDigest, err = ports.OwnedSearchConsentStatesDigestV4(f.capsule.Consents)
	if err != nil {
		t.Fatal(err)
	}
	var operations []LocalCompactionOperationV1
	var materials []localCapsuleOperationMaterial
	var catalogs []compactioncontext.ModelCatalogV3
	for _, item := range f.capsule.Operations {
		row, err := ports.BuildOwnedOperationAccountingRowV4(item.Reservation, item.Observation, parent, source)
		if err != nil {
			t.Fatal(err)
		}
		d.Operations = append(d.Operations, row)
		operations = append(operations, LocalCompactionOperationV1{Reservation: item.Reservation, Observation: item.Observation, Response: item.Normalized})
		materials = append(materials, localCapsuleOperationMaterial{reservation: item.Reservation, observation: item.Observation, normalized: item.Normalized, bytes: uint64(len(item.Normalized))})
	}
	for _, catalog := range f.capsule.ModelCatalogs {
		catalogs = append(catalogs, compactioncontext.ModelCatalogV3{Sequence: catalog.Sequence, Tools: bytes.Clone(catalog.Tools)})
	}
	if err := d.ValidateFor(parent, source); err != nil {
		t.Fatal("success accounting DATA", err)
	}
	run := f.history.Run.Clone()
	run.Status, run.TerminalFailure, run.TerminalCustody = domain.RunSucceeded, nil, nil
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(run.ID), TenantID: run.TenantID, RunID: run.ID, CreatedAt: run.UpdatedAt}
	plan, err := buildLocalCanonicalPlanV3(run, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: domain.MaxTerminalCapsuleBytesV1}, f.capsule.ProfileDigest, nil, source, operations, f.capsule.Consents, catalogs, manifest.ID)
	if err != nil {
		t.Fatal("actual closed success canonical codec", err)
	}
	defer plan.Release()
	var events []localCapsuleEventV1
	var records []sessioncontext.EventPayload
	var drafts []domain.SessionEventDraft
	for i, event := range plan.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, run.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
		draft.DisplayText = event.DisplayText
		drafts = append(drafts, draft)
		events = append(events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
		records = append(records, sessioncontext.EventPayload{Event: domain.SessionEvent{ID: draft.ID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: uint64(i + 1), Kind: draft.Kind, RunID: &run.ID, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, CreatedAt: run.UpdatedAt}, Payload: bytes.Clone(event.Payload)})
	}
	sink := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1, emit: true}
	if err := encodeLocalTerminalCapsuleV3To(&sink, d.Scope, f.capsule.ProfileDigest, f.capsule.FrontierDigest, materials, nil, events, 0, manifest, d, f.capsule.Consents, catalogs); err != nil {
		t.Fatal(err)
	}
	c, err := decodeLocalTerminalCapsuleV3(sink.body)
	if err != nil {
		t.Fatal("strict V3 roundtrip", err)
	}
	key, _ := domain.TerminalCustodyObjectKeyV1(d.Scope, rawLocalFailureHash(sink.body))
	ref := domain.BlobRef{TenantID: run.TenantID, Key: key, Size: int64(len(sink.body)), SHA256: rawLocalFailureHash(sink.body)}
	ad, _ := d.Digest()
	pd, _ := parent.Digest()
	sd, _ := source.Digest()
	od, _ := f.history.Original.Digest()
	rd, _ := f.history.Retired.Digest()
	evidence := domain.LocalExecutionEvidenceV1{Version: 1, Scope: d.Scope, ParentDigest: pd, SourceDigest: sd, ProfileDigest: c.ProfileDigest, OriginalAllocationDigest: string(od), RetiredAllocationDigest: string(rd), FrontierDigest: c.FrontierDigest, Capsule: ref, ResidualInventoryDigest: strings.Repeat("a", 64), ResidualReceiptDigest: strings.Repeat("b", 64), AccountingDispositionDigest: ad, StoppedAt: f.capsule.CapturedAt, RetiredAt: f.capsule.CapturedAt, ErasedAt: run.UpdatedAt}
	evidence.EvidenceDigest, err = evidence.Digest()
	if err != nil {
		t.Fatal(err)
	}
	canonicalDigest, _ := terminalCanonicalEventsDigestV1(drafts)
	derivedDigest, _ := domain.TerminalDerivedContextDigestV1(nil)
	run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: domain.TerminalCustodyPlanV1{Scope: d.Scope, Capsule: ref, FrontierDigest: c.FrontierDigest}, Correspondence: &domain.TerminalCorrespondenceV1{Version: 1, CapsuleDigest: ref.SHA256, TerminalAssistantEventID: records[len(records)-1].Event.ID, CanonicalEventsDigest: canonicalDigest, DerivedContextDigest: derivedDigest, AccountingDispositionDigest: ad}, LocalEvidence: &evidence}
	if err := run.Validate(); err != nil {
		t.Fatal("retained successful Run DATA", err)
	}
	v := localTerminalHistorySnapshot{Run: run, Anchor: records[len(records)-1].Event, Parent: parent, Admission: f.history.Admission, Evidence: domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceLocalV1, LocalV1: &evidence}, Manifest: manifest, Accounting: accounting}
	seal, err := runFinalizationDigestV2(run.Status, &manifest, drafts, &v.Evidence, nil, run.TerminalCustody)
	if err == nil {
		v.Seal, err = encodeLocalFinalizationMarkerV2(seal)
	}
	if err != nil || validateHistoricalCapsuleDataV3(v, c, records) != nil {
		t.Fatal("valid complete historical DATA", err)
	}
	return terminalSearchV3Data{snapshot: v, capsule: c, body: sink.body, records: records}
}

func operationbodyTokenV3(body []byte) string {
	// Existing scoped adapter token grammar; no body is written by this fixture.
	return "opbody-v1." + fmt.Sprint(len(body)) + "." + rawLocalFailureHash(body)
}

func TestLocalTerminalSearchV3OriginalTriggerIsInputNotOutput(t *testing.T) {
	f := terminalSearchV3Fixture(t)
	// The original sealed output is unchanged. Place it after its ordinary
	// canonical user trigger, as the genuine normal-Web writer actually does.
	for i := range f.records {
		f.records[i].Event.Sequence++
	}
	f.snapshot.Anchor = f.records[len(f.records)-1].Event
	if validateHistoricalCapsuleDataV3(f.snapshot, f.capsule, f.records) != nil {
		t.Fatal("valid sealed output baseline before adding original trigger")
	}
	payload := []byte(`{"text":"original public request"}`)
	run, owner := f.snapshot.Run, f.snapshot.Admission.Source.Scope.OwnerUserID
	trigger := domain.SessionEvent{ID: run.TriggerEventID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, RunID: &run.ID, IdempotencyKey: "original-search-trigger", CreatedAt: run.CreatedAt}
	trigger.Payload = domain.BlobRef{TenantID: run.TenantID, Key: domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, trigger.ID) + "payloads/sha256/" + rawLocalFailureHash(payload) + ".json", Size: int64(len(payload)), SHA256: rawLocalFailureHash(payload)}
	if trigger.Validate() != nil {
		t.Fatal("valid actual-scope original user trigger DATA")
	}
	if _, err := sessioncontext.EncodeRecord(trigger, payload); err != nil {
		t.Fatal("canonical trigger payload baseline", err)
	}
	records := append([]sessioncontext.EventPayload{{Event: trigger, Payload: payload}}, f.records...)
	if validateHistoricalCapsuleDataV3(f.snapshot, f.capsule, records) != nil {
		t.Fatal("original authenticated input trigger was mistaken for an unsealed output")
	}
	t.Run("other_run_input_before_atomic_output", func(t *testing.T) {
		// DATA-reader control only: another Run's canonical user may precede
		// these still-atomic outputs. This is not mounted asynchronous ingress
		// qualification of the separate pre-existing segment discovery gate.
		interleaved := append([]sessioncontext.EventPayload(nil), records...)
		for i := 1; i < len(interleaved); i++ {
			interleaved[i].Event.Sequence++
		}
		otherRun := domain.RunID("other-canonical-user-run")
		other := interleaved[0]
		other.Event.ID, other.Event.RunID, other.Event.Sequence = "other-canonical-user-input", &otherRun, 2
		other.Event.Payload.Key = domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, other.Event.ID) + "payloads/sha256/" + rawLocalFailureHash(payload) + ".json"
		if other.Event.Validate() != nil {
			t.Fatal("valid foreign Run input before output baseline")
		}
		interleaved = append(interleaved, other)
		snapshot := f.snapshot
		snapshot.Anchor = interleaved[len(interleaved)-2].Event
		if validateHistoricalCapsuleDataV3(snapshot, f.capsule, interleaved) != nil {
			t.Fatal("original input was incorrectly required to adjoin atomic output")
		}
	})
	for _, mode := range []string{"unknown_input", "wrong_kind", "foreign_author", "duplicate", "wrong_sequence", "body_digest"} {
		t.Run(mode, func(t *testing.T) {
			bad := append([]sessioncontext.EventPayload(nil), records...)
			switch mode {
			case "unknown_input":
				bad[0].Event.ID = "unsealed-user-input"
				bad[0].Event.Payload.Key = domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, bad[0].Event.ID) + "payloads/sha256/" + rawLocalFailureHash(payload) + ".json"
			case "wrong_kind":
				bad[0].Event.Kind = domain.SessionEventSystemNotice
			case "foreign_author":
				other := domain.UserID("foreign-trigger-author")
				bad[0].Event.AuthorUserID = &other
			case "duplicate":
				bad = append(bad, bad[0])
			case "wrong_sequence":
				bad[0].Event.Sequence++
			case "body_digest":
				bad[0].Payload = append(bytes.Clone(bad[0].Payload), ' ')
			}
			if validateHistoricalCapsuleDataV3(f.snapshot, f.capsule, bad) == nil {
				t.Fatal("non-original trigger accepted", mode)
			}
		})
	}
}

type terminalSearchV3SQL struct {
	values []driver.Value
	reads  int
}

type terminalSearchV3Conn struct{ fixture *terminalSearchV3SQL }

func (f *terminalSearchV3SQL) Connect(context.Context) (driver.Conn, error) {
	return &terminalSearchV3Conn{fixture: f}, nil
}
func (*terminalSearchV3SQL) Driver() driver.Driver { return ownedAdmissionJoinDriver{} }
func (*terminalSearchV3Conn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*terminalSearchV3Conn) Close() error              { return nil }
func (*terminalSearchV3Conn) Begin() (driver.Tx, error) { return ownedAdmissionJoinTx{}, nil }
func (*terminalSearchV3Conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return ownedAdmissionJoinTx{}, nil
}
func (c *terminalSearchV3Conn) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement != localTerminalHistorySQL || len(args) != 4 {
		return nil, errors.New("not the unchanged bounded eleven-column history query")
	}
	c.fixture.reads++
	return &retainedDeletionWriterRows{rows: [][]driver.Value{c.fixture.values}, columns: 11}, nil
}

func terminalSearchV3Read(t *testing.T, f terminalSearchV3Data, change func([]driver.Value)) error {
	t.Helper()
	v := f.snapshot
	raw := func(value any) string {
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	pd, _ := v.Parent.Digest()
	ad, _ := v.Admission.Digest()
	accounting, digest, _ := accountingBytes(v.Accounting)
	fixture := &terminalSearchV3SQL{values: []driver.Value{raw(v.Parent), pd, raw(v.Admission), ad, raw(v.Evidence), v.Evidence.LocalV1.EvidenceDigest, string(v.Run.Status), v.Seal, raw(v.Manifest), string(accounting), digest}}
	if change != nil {
		change(fixture.values)
	}
	db := sql.OpenDB(fixture)
	defer db.Close()
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	callback, err := budget.begin()
	if err != nil {
		return err
	}
	defer budget.finish(callback)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	q := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback, at: v.Run.UpdatedAt}
	actual, err := readHistoricalTerminalSnapshot(ctx, q, v.Run, v.Anchor)
	if err == nil {
		if !reflect.DeepEqual(actual, v) {
			return errors.New("history reader changed actual immutable snapshot")
		}
		var capsule localRetainedTerminalCapsuleV3
		capsule, err = decodeLocalTerminalCapsuleV3(f.body)
		if err == nil {
			err = validateHistoricalCapsuleDataV3(actual, capsule, f.records)
		}
		if err == nil {
			var history compactioncontext.HistoricalRunV3
			history, err = historicalCapsuleRunV3(capsule, actual.Anchor.ID)
			if err == nil && (history.Version != 3 || len(history.Tools) != 1 || history.Tools[0].Search == nil || len(history.ModelCatalogs) != 2) {
				return errors.New("mounted conversion lost SEARCH witness/catalogs")
			}
		}
	}
	if fixture.reads != 1 {
		t.Fatalf("query budget changed: reads=%d", fixture.reads)
	}
	return err
}

func TestLocalTerminalSearchV3MountedHistoryData(t *testing.T) {
	f := terminalSearchV3Fixture(t)
	if err := terminalSearchV3Read(t, f, nil); err != nil {
		t.Fatal("actual SQL/canonical success history DATA consumer", err)
	}
	for _, column := range []int{1, 3, 5, 7, 10} {
		t.Run(fmt.Sprint(column), func(t *testing.T) {
			if err := terminalSearchV3Read(t, f, func(values []driver.Value) { values[column] = strings.Repeat("e", len(values[column].(string))) }); err == nil {
				t.Fatal("crossed original history commitment accepted")
			}
		})
	}
	for _, replacement := range [][2]string{{"sessionless.terminal-custody.v3", "sessionless.terminal-custody.v2"}, {`"codec_version":3`, `"codec_version":2`}, {`"model_catalogs":`, `"MODEL_CATALOGS":`}} {
		if bytes.Count(f.body, []byte(replacement[0])) != 1 {
			t.Fatal("vacuous schema mutation")
		}
		bad := f
		bad.body = bytes.Replace(f.body, []byte(replacement[0]), []byte(replacement[1]), 1)
		if err := terminalSearchV3Read(t, bad, nil); err == nil {
			t.Fatal("crossed/aliased V3 language accepted")
		}
	}
	if _, err := decodeLocalTerminalCapsuleV2(f.body); err == nil {
		t.Fatal("V3 silently widened V2")
	}
}

type terminalSearchV3Blobs struct {
	ports.BlobStore
	ref   domain.BlobRef
	body  []byte
	reads int
}

func (b *terminalSearchV3Blobs) Open(_ context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.reads++
	if tenant != b.ref.TenantID || ref != b.ref {
		return nil, errors.New("foreign capsule reference")
	}
	return io.NopCloser(bytes.NewReader(b.body)), nil
}

func terminalSearchV3Materialize(t *testing.T, f terminalSearchV3Data) error {
	t.Helper()
	view, err := compactioncontext.BuildCanonicalV1(f.records, 1<<20, 12)
	if err != nil {
		t.Fatal("actual canonical view DATA", err)
	}
	blobs := &terminalSearchV3Blobs{ref: f.snapshot.Run.TerminalCustody.Plan.Capsule, body: f.body}
	// Scope is retained DATA only. This object cannot pass entry.original(),
	// prepare an operation, publish, erase or claim original serving ownership.
	entry := &LocalInvocationEntryV1{parent: f.snapshot.Parent, canonicalBlobs: blobs}
	source := &LocalCompactionSourceV1{entry: entry, state: &localCompactionSourceState{view: view, history: &localTerminalHistoryV1{snapshots: []localTerminalHistorySnapshot{f.snapshot}}}}
	source.self = source
	defer source.Clear()
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	err = source.materializeHistoryLocked(ctx)
	if blobs.reads != 1 {
		t.Fatalf("capsule body reads=%d", blobs.reads)
	}
	if err == nil && (len(source.state.history.runsV3) != 1 || len(source.state.history.runsV2) != 0 || len(source.state.history.runs) != 0 || source.state.history.runsV3[0].Version != 3) {
		return errors.New("actual mounted history dispatch lost explicit V3")
	}
	return err
}

func TestLocalTerminalSearchV3ActualMaterializationDispatchAndRawCustody(t *testing.T) {
	f := terminalSearchV3Fixture(t)
	if err := terminalSearchV3Materialize(t, f); err != nil {
		t.Fatal("actual ordinary history V3 materialization", err)
	}
	bad := f
	bad.body = bytes.Replace(f.body, []byte("bounded final answer"), []byte("changed final answer"), 1)
	if bytes.Equal(bad.body, f.body) {
		t.Fatal("vacuous raw capsule mutation")
	}
	if err := terminalSearchV3Materialize(t, bad); err == nil {
		t.Fatal("altered capsule escaped original raw custody reference")
	}
}

// Structural successful retained DATA, not an original serving/finalizer
// receipt. The actual original SQL readers still join every operation/call/
// proxy/reconciliation row before the V3 capsule/body/parent/seal consumer runs.
func terminalSearchV3DeletionData(t *testing.T, value terminalSearchV3Data) *failureSearchSQL {
	t.Helper()
	base := failureSearchV4Data(t, domain.ModelAfterSearchFailureV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
	if !reflect.DeepEqual(base.history.Parent, value.snapshot.Parent) || !reflect.DeepEqual(base.history.Admission, value.snapshot.Admission) {
		t.Fatal("success deletion DATA must retain the exact common original scope")
	}
	f := failureSearchSQLData(t, base)
	raw := func(v any) string {
		t.Helper()
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	for i, item := range value.capsule.Operations {
		row := f.graph.tables["attempt_operations_v1"][i]
		rd, _ := item.Reservation.Digest()
		od, _ := item.Observation.Digest()
		row["record"], row["digest"], row["operation_digest"] = raw(item.Reservation), string(rd), string(item.Reservation.OperationDigest)
		row["observation_record"], row["observation_digest"] = raw(item.Observation), string(od)
		f.graph.tables["attempt_operation_calls_v1"][i]["operation_digest"] = string(item.Reservation.OperationDigest)
	}
	parent := f.graph.tables["attempt_effect_reservations"][0]
	accounting, digest, err := accountingBytes(value.snapshot.Accounting)
	if err != nil {
		t.Fatal(err)
	}
	parent["operation_accounting_record"], parent["operation_accounting_digest"] = string(accounting), digest
	f.graph.tables["run_finalizations"][0]["terminal_status"] = string(domain.RunSucceeded)
	f.graph.tables["run_finalizations"][0]["content_digest"] = value.snapshot.Seal
	r := value.snapshot.Parent.Reservation
	f.graph.tables["substrate_execution_evidence"] = []map[string]driver.Value{{"tenant_id": string(r.TenantID), "attempt_id": string(r.AttemptID), "record": raw(value.snapshot.Evidence), "evidence_digest": value.snapshot.Evidence.LocalV1.EvidenceDigest}}
	f.graph.tables["artifact_manifests"] = []map[string]driver.Value{{"tenant_id": string(r.TenantID), "artifact_manifest_id": string(value.snapshot.Manifest.ID), "payload": raw(value.snapshot.Manifest)}}
	base.blobs[value.snapshot.Run.TerminalCustody.Plan.Capsule.Key] = bytes.Clone(value.body)
	for _, record := range value.records {
		base.blobs[record.Event.Payload.Key] = bytes.Clone(record.Payload)
	}
	return f
}

func terminalSearchV3ReadDeletion(t *testing.T, value terminalSearchV3Data, sqlData *failureSearchSQL) error {
	t.Helper()
	var writer retainedDeletionWriterData
	if err := failureSearchQuery(t, sqlData, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		n := uint64(len(value.capsule.Operations))
		writer, err = readRetainedDeletionWriter(ctx, q, value.snapshot.Run, value.snapshot.Anchor, retainedDeletionAttemptRows{parents: 1, evidence: 1, operations: n, calls: n, proxies: n - 1})
		return err
	}); err != nil {
		return err
	}
	ownership, err := newRetainedDeletionOwnershipData(value.snapshot.Run.TenantID, value.snapshot.Run.SessionID)
	if err != nil {
		return err
	}
	if err := ownership.addRun(value.snapshot.Run); err != nil {
		return err
	}
	for _, record := range value.records {
		if err := ownership.addEvent(record.Event); err != nil {
			return err
		}
	}
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	_, err = verifyRetainedDeletionBodiesWithWriter(ctx, &failureSearchBlobs{f: sqlData.data}, value.snapshot, ownership, nil, &writer)
	return err
}

func TestLocalTerminalSearchV3MountedFullDeletionData(t *testing.T) {
	for _, mode := range []string{"positive", "missing_search_call", "foreign_search_proxy", "changed_canonical_body", "wrong_accounting_tail"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				value := terminalSearchV3Fixture(t)
				f := terminalSearchV3DeletionData(t, value)
				switch mode {
				case "missing_search_call":
					f.graph.tables["attempt_operation_calls_v1"] = append(f.graph.tables["attempt_operation_calls_v1"][:1], f.graph.tables["attempt_operation_calls_v1"][2:]...)
				case "foreign_search_proxy":
					f.graph.tables["attempt_operation_proxies_v1"][0]["service_kind"] = string(domain.RegisteredProxyMCPToolV1)
				case "changed_canonical_body":
					f.data.blobs[value.records[0].Event.Payload.Key] = []byte("changed")
				case "wrong_accounting_tail":
					accounting := value.snapshot.Accounting
					accounting.UpdatedAt = accounting.UpdatedAt.Add(-time.Microsecond)
					b, d, _ := accountingBytes(accounting)
					f.graph.tables["attempt_effect_reservations"][0]["operation_accounting_record"], f.graph.tables["attempt_effect_reservations"][0]["operation_accounting_digest"] = string(b), d
				}
				err := terminalSearchV3ReadDeletion(t, value, f)
				if (mode == "positive") != (err == nil) {
					t.Fatalf("full original SQL/V3 body deletion DATA mode=%s err=%v", mode, err)
				}
				if f.writes != 0 {
					t.Fatal("retained DATA reader wrote state")
				}
			})
		})
	}
}

func TestLocalTerminalSearchV3ProspectiveUsesActualCapsuleEncoder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		value := terminalSearchV3Fixture(t)
		base := failureSearchV4Data(t, domain.ModelAfterSearchFailureV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
		source := value.snapshot.Admission.Source
		if !reflect.DeepEqual(source, base.history.Admission.Source) {
			t.Fatal("prospective DATA fixture original scope mismatch")
		}
		reader := newOwnedReaderFixtureAt(t, source.IssuedAt, "-failure-v4")
		configuration := syntheticLocalInstallationGrant(t, reader.profile).LocalInstallation.Profile.Clone()
		ops := make([]LocalCompactionOperationV1, len(value.capsule.Operations))
		for i, item := range value.capsule.Operations {
			op := item.Reservation.Operation
			ref, err := operationbody.RetainedOperationBodyRefV1(op.Scope, op.RequestRef, op.RequestBytes, op.RequestDigest)
			if err != nil {
				t.Fatal(err)
			}
			request := base.blobs[ref.Key]
			if !localTerminalMaterialBytesV3(request, op.RequestBytes, op.RequestDigest) {
				t.Fatal("actual original request DATA missing", i)
			}
			ops[i] = LocalCompactionOperationV1{Reservation: item.Reservation, Observation: item.Observation, Request: request, Response: item.NormalizedBody}
		}
		product := currentWorkerAuthorityFixture(t).Job.Limits
		product.MaxToolEvents, product.MaxToolEventBytes = 8, domain.MaxTerminalCapsuleBytesV1
		if err := product.Validate(); err != nil {
			t.Fatal("valid admitted product DATA", err)
		}
		for _, index := range []int{1, 2} {
			future := ops[index].Reservation.Operation
			n, err := localTerminalProspectiveBytesV3(value.snapshot.Run, product, configuration, source, ops[:index], future, value.capsule.Consents, configuration.Limits.ResponseBytes, domain.MaxTerminalCapsuleBytesV1)
			if err != nil || n < uint64(len(value.body)) {
				t.Fatalf("same V3 encoder future=%s upper=%d actual=%d err=%v", future.Kind, n, len(value.body), err)
			}
			if _, err := localTerminalProspectiveBytesV3(value.snapshot.Run, product, configuration, source, ops[:index], future, value.capsule.Consents, configuration.Limits.ResponseBytes, n-1); !errors.Is(err, ErrTerminalCapacity) {
				t.Fatalf("first-over V3 capsule capacity=%d err=%v", n-1, err)
			}
		}
		if _, err := localTerminalProspectiveBytesV2(value.snapshot.Run, product, value.capsule.ProfileDigest, configuration, source, ops[:1], ops[1].Reservation.Operation, configuration.Limits.ResponseBytes, domain.MaxTerminalCapsuleBytesV1); err == nil {
			t.Fatal("explicit SEARCH silently widened old V2 counter")
		}
	})
}

func TestLocalTerminalSearchV4FailureProspectiveUsesActualEncoder(t *testing.T) {
	for _, stop := range []string{domain.SearchConsentStoppedV4, domain.SearchRejectedBeforeSendV4, domain.SearchServiceErrorV4, domain.SearchOutcomeUnknownV4, domain.ModelAfterSearchFailureV4} {
		t.Run(stop, func(t *testing.T) {
			outcome := domain.AttemptOperationAcceptedOutcomeUnknownV1
			if stop == domain.SearchRejectedBeforeSendV4 {
				outcome = domain.AttemptOperationRejectedBeforeSendV1
			}
			f := failureSearchV4Data(t, stop, outcome)
			reader := newOwnedReaderFixtureAt(t, f.history.Admission.Source.IssuedAt, "-failure-v4")
			configuration := syntheticLocalInstallationGrant(t, reader.profile).LocalInstallation.Profile.Clone()
			var ops []LocalCompactionOperationV1
			for _, item := range f.capsule.Operations {
				op := item.Reservation.Operation
				ref, err := operationbody.RetainedOperationBodyRefV1(op.Scope, op.RequestRef, op.RequestBytes, op.RequestDigest)
				if err != nil || !localTerminalMaterialBytesV3(f.blobs[ref.Key], op.RequestBytes, op.RequestDigest) {
					t.Fatal("actual original request DATA missing", err)
				}
				ops = append(ops, LocalCompactionOperationV1{Reservation: item.Reservation, Observation: item.Observation, Request: f.blobs[ref.Key], Response: item.Normalized})
			}
			future := ops[len(ops)-1].Reservation.Operation
			prefix := ops[:len(ops)-1]
			if stop == domain.SearchConsentStoppedV4 {
				request := f.capsule.Consents[0].Request
				future = domain.AttemptOperationV1{Scope: future.Scope, Sequence: future.Sequence + 1, Kind: domain.AttemptOperationWebSearchV1, CallID: request.CallID, ModelResponseDigest: request.ModelResponseDigest, ArgumentsDigest: request.ArgumentsDigest, QueryDigest: request.QueryDigest}
				prefix = ops // no hypothetical SEARCH operation for this tail
			}
			product := currentWorkerAuthorityFixture(t).Job.Limits
			product.MaxToolEvents, product.MaxToolEventBytes = 8, domain.MaxTerminalCapsuleBytesV1
			if product.Validate() != nil {
				t.Fatal("valid admitted product DATA")
			}
			actual := f.blobs[f.history.Run.TerminalCustody.Plan.Capsule.Key]
			n, err := localSearchFailureProspectiveBytesV4(f.history.Run, product, configuration, f.history.Parent, f.history.Admission.Source, prefix, future, f.capsule.Consents, stop, operationbody.LocalTerminalCapsuleBytesV1)
			if err != nil || n < uint64(len(actual)) {
				t.Fatalf("same V4 serializer stop=%s upper=%d actual=%d err=%v", stop, n, len(actual), err)
			}
			if _, err := localSearchFailureProspectiveBytesV4(f.history.Run, product, configuration, f.history.Parent, f.history.Admission.Source, prefix, future, f.capsule.Consents, stop, n-1); !errors.Is(err, ErrTerminalCapacity) {
				t.Fatalf("first-over V4 capsule bound=%d err=%v", n-1, err)
			}
			if stop != domain.SearchConsentStoppedV4 {
				product.MaxToolEventBytes = 1
				if _, err := localSearchFailureProspectiveBytesV4(f.history.Run, product, configuration, f.history.Parent, f.history.Admission.Source, prefix, future, f.capsule.Consents, stop, operationbody.LocalTerminalCapsuleBytesV1); !errors.Is(err, ErrTerminalCapacity) {
					t.Fatal("unfit canonical tool bytes admitted", err)
				}
			} else if len(f.capsule.Operations) != 1 || f.capsule.Accounting.AggregateDebit.WebSearches != 0 {
				t.Fatal("consent-stop DATA invented a SEARCH row/debit")
			}
		})
	}
}

func TestLocalTerminalSearchNoAuthUsesOriginalWholeCleanupDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		peers := localBootstrapInertCredentialPeers{}
		credentials, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: peers, IDs: peers}, peers, f.store, peers)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.bootstrap.Complete(f.bridge, credentials); err != nil {
			t.Fatal(err)
		}
		entry, err := f.bootstrap.StartLocalInvocation(f.actual.ctx, f.parent.request, &localEntryBlobs{})
		if err != nil || !entry.hasCredentialLifecycle() || entry.owner.configuration.Auth != domain.ConfiguredInferenceAuthNoneV1 {
			t.Fatal("actual auth-none entry with original service owner", err)
		}
		active, err := entry.PrepareCredentialedExecution(f.actual.ctx)
		if err != nil || !entry.credentialExecutionActive() {
			t.Fatal("original whole-engine metadata", err)
		}
		deadline, err := entry.BeginCredentialedCleanup()
		if err != nil || deadline.After(active.Add(entry.credentialCleanup.duration)) || entry.credentialExecutionActive() {
			t.Fatal("original U narrowed to final D", err)
		}
		<-time.After(time.Millisecond)
		again, err := entry.BeginCredentialedCleanup()
		if err != nil || !again.Equal(deadline) {
			t.Fatal("whole cleanup deadline renewed", err)
		}
		terminal, err := entry.ClaimTerminal(f.actual.ctx)
		if err != nil || terminal == nil {
			t.Fatal("original terminal claim", err)
		}
		actual, err := terminal.Deadline()
		if err != nil || !actual.Equal(deadline) {
			t.Fatal("auth-none service terminal allocated a fresh cleanup window", err)
		}
	})
}
