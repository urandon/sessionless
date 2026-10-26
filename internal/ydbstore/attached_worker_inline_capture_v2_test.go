package ydbstore

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This joins the actual schema, database/sql scan, shared capture ledger and
// original observeRow using the existing SQL driver fixture. It proves bounded
// DATA capture only, not native SQL, deletion authority or physical deletion.
type inlineReceiptCaptureFixtureV2 struct {
	sql      *deletionCaptureFixture
	graph    *deletionCaptureGraph
	scope    deletionCaptureScope
	observed int
}

func inlineReceiptCaptureRowV2(r AttachedWorkerOutputReceiptV2, payload []byte) []driver.Value {
	a := r.Binding
	return []driver.Value{string(a.TenantID), string(a.RunID), string(a.OwnerUserID), string(a.WorkerID), string(a.AttemptID), a.LeaseGeneration,
		string(r.CanonicalDigest), r.CandidateFingerprint, r.CreatedAt, string(payload)}
}

func newInlineReceiptCaptureFixtureV2(t *testing.T, r AttachedWorkerOutputReceiptV2, row []driver.Value) *inlineReceiptCaptureFixtureV2 {
	t.Helper()
	spec := deletionCaptureSpecs["attached_worker_output_receipts"]
	f := &inlineReceiptCaptureFixtureV2{sql: &deletionCaptureFixture{spec: spec, values: [][]driver.Value{row}}}
	db := captureFixtureDB(t, f.sql)
	f.graph = deletionCaptureTestGraph(t, db)
	g := f.graph
	g.inventory.TenantID, g.inventory.SessionID = r.Binding.TenantID, r.Producer.SessionID
	var err error
	g.ownership, err = newRetainedDeletionOwnershipData(g.inventory.TenantID, g.inventory.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	_, loaded, _ := inlineReceiptFixtureV2(t)
	if loaded.Run.TenantID != r.Binding.TenantID || loaded.Run.SessionID != r.Producer.SessionID || loaded.Run.ID != r.Binding.RunID {
		t.Fatal("capture fixture must retain the original Run owner")
	}
	if err := g.ownership.addRun(loaded.Run); err != nil {
		t.Fatal("original Run ownership", err)
	}
	digest, err := deletionRunDigest(loaded.Run)
	if err != nil {
		t.Fatal(err)
	}
	g.runDigests = map[domain.RunID]string{loaded.Run.ID: digest}
	f.scope = deletionCaptureScope{spec: spec, prefix: []any{r.Binding.TenantID, r.Binding.RunID}, query: g.query,
		observe: func(spec *deletionCaptureSpec, cells []deletionCaptureCell) error {
			f.observed++
			return g.observeRow(spec, cells)
		}}
	return f
}

func TestInlineReceiptCaptureV2SchemaLimitsAreColumnSpecific(t *testing.T) {
	for table, spec := range deletionCaptureSpecs {
		for _, column := range spec.columns {
			want := uint64(ownedAuthorityHeadBytes)
			if table == "attached_worker_output_receipts" && column.name == "payload" && column.kind == 'j' {
				want = maxInlineReceiptStoredBytesV2
			}
			if column.limit != want {
				t.Errorf("capture limit table=%s column=%s got=%d want=%d", table, column.name, column.limit, want)
			}
		}
	}
	spec := deletionCaptureSpecs["attached_worker_output_receipts"]
	statement, err := spec.sql(2)
	if err != nil || !strings.Contains(statement, "SUBSTRING(CAST(payload AS String),0,524331)") {
		t.Fatal("receipt SQL lacks the exact codec overflow sentinel", statement, err)
	}
	for _, column := range spec.columns {
		if column.kind == 's' && !strings.Contains(statement, fmt.Sprintf("SUBSTRING(CAST(%s AS String),0,32769)", column.name)) {
			t.Fatal("receipt scalar SQL lost its unchanged 32 KiB sentinel", column.name)
		}
	}
	unrelated, err := deletionCaptureSpecs["artifact_manifests"].sql(1)
	if err != nil || !strings.Contains(unrelated, "SUBSTRING(CAST(payload AS String),0,32769)") {
		t.Fatal("unrelated JSON SQL inherited the receipt allowance", unrelated, err)
	}
}

func TestInlineReceiptCaptureV2MaximumReadyReachesActualObserver(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	r.Stage = attachedworkeroutput.InlineReadyV2
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	if len(raw) <= ownedAuthorityHeadBytes {
		t.Fatal("maximum answer fixture does not cross the old generic column ceiling", len(raw))
	}
	padded := append(bytes.Clone(raw), bytes.Repeat([]byte(" "), maxInlineReceiptStoredBytesV2-len(raw))...)
	defer clear(padded)
	for name, payload := range map[string][]byte{"maximum": raw, "exact_private_frame_limit": padded} {
		t.Run(name, func(t *testing.T) {
			f := newInlineReceiptCaptureFixtureV2(t, r, inlineReceiptCaptureRowV2(r, payload))
			digest, rows, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{f.scope}, ownedAuthorityMaxBytes)
			if err != nil || len(digest) != 64 || rows != 1 || f.observed != 1 || f.sql.queries != 1 || f.sql.pulls != 1 ||
				f.graph.inventory.ReceiptRows != 1 || len(f.graph.objects) != len((attachedWorkerReceiptEnvelope{v2: &r}).objectRefs()) {
				t.Fatalf("maximum READY did not reach original capture observer: bytes=%d rows=%d observed=%d queries=%d pulls=%d receipts=%d objects=%d err=%v", len(payload), rows, f.observed, f.sql.queries, f.sql.pulls, f.graph.inventory.ReceiptRows, len(f.graph.objects), err)
			}
			for _, ref := range (attachedWorkerReceiptEnvelope{v2: &r}).objectRefs() {
				if f.graph.objects[ref.Key] != ref {
					t.Fatal("actual observer changed original READY custody reference", ref.Key)
				}
			}
			q := f.graph.query.(*deletionCaptureQuery)
			if q.budget.rows != 1 || q.budget.bytes < uint64(len(payload)) || q.budget.ceiling != ownedAuthorityMaxBytes {
				t.Fatal("capture did not charge original scanned bytes to its unchanged ledger", q.budget)
			}
		})
	}
}

func TestInlineReceiptCaptureV2OverflowDeniedBeforeObserver(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	r.Stage = attachedworkeroutput.InlineReadyV2
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	overflow := append(bytes.Clone(raw), bytes.Repeat([]byte(" "), maxInlineReceiptStoredBytesV2+1-len(raw))...)
	defer clear(overflow)
	for _, name := range []string{"receipt_payload_cap_plus_one", "scalar_32KiB_plus_one", "unrelated_JSON_32KiB_plus_one"} {
		t.Run(name, func(t *testing.T) {
			row := inlineReceiptCaptureRowV2(r, raw)
			if name == "receipt_payload_cap_plus_one" {
				row[9] = string(overflow)
			} else if name == "scalar_32KiB_plus_one" {
				row[2] = strings.Repeat("o", ownedAuthorityHeadBytes+1)
			}
			f := newInlineReceiptCaptureFixtureV2(t, r, row)
			if name == "unrelated_JSON_32KiB_plus_one" {
				spec := deletionCaptureSpecs["artifact_manifests"]
				f.sql.spec = spec
				f.sql.values = [][]driver.Value{{string(r.Binding.TenantID), "manifest-inline-capture", string(r.Binding.RunID), r.CreatedAt, "{}" + strings.Repeat(" ", ownedAuthorityHeadBytes-1)}}
				f.scope.spec, f.scope.prefix = spec, []any{r.Binding.TenantID}
			}
			if digest, rows, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{f.scope}, ownedAuthorityMaxBytes); err == nil || digest != "" || rows != 0 || f.observed != 0 || f.graph.inventory.ReceiptRows != 0 || len(f.graph.objects) != 0 {
				t.Fatalf("overflow crossed scan/observer boundary: rows=%d observed=%d err=%v", rows, f.observed, err)
			}
		})
	}
}

func TestInlineReceiptCaptureV2SharesPreSpentAggregateWithoutReset(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	r.Stage = attachedworkeroutput.InlineReadyV2
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	f := newInlineReceiptCaptureFixtureV2(t, r, inlineReceiptCaptureRowV2(r, raw))
	q := f.graph.query.(*deletionCaptureQuery)
	spent := uint64(ownedAuthorityMaxBytes - 1)
	if err := chargeDeletionCaptureSQL(q, spent); err != nil {
		t.Fatal("pre-spend original shared material ledger", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, _, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{f.scope}, ownedAuthorityMaxBytes); err == nil || f.observed != 0 ||
			!q.budget.failed || q.budget.bytes != spent || q.budget.ceiling != ownedAuthorityMaxBytes || f.sql.queries != 1 || f.graph.inventory.ReceiptRows != 0 || len(f.graph.objects) != 0 {
			t.Fatalf("capture reset or bypassed sticky original aggregate: attempt=%d bytes=%d ceiling=%d queries=%d observed=%d err=%v", attempt, q.budget.bytes, q.budget.ceiling, f.sql.queries, f.observed, err)
		}
	}
}

func TestInlineReceiptCaptureV2OriginalObserverRejectsMalformedOrForeignRow(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	r.Stage = attachedworkeroutput.InlineReadyV2
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	for _, name := range []string{"wrong_encoding", "foreign_scalar_owner"} {
		t.Run(name, func(t *testing.T) {
			row := inlineReceiptCaptureRowV2(r, raw)
			if name == "wrong_encoding" {
				row[9] = strings.Replace(string(raw), `"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":"unsupported"`, 1)
				if row[9] == string(raw) {
					t.Fatal("encoding mutation did not change its exact target")
				}
			} else {
				row[2] = "foreign-capture-owner"
			}
			f := newInlineReceiptCaptureFixtureV2(t, r, row)
			if _, _, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{f.scope}, ownedAuthorityMaxBytes); err == nil || f.observed != 1 || f.graph.inventory.ReceiptRows != 0 || len(f.graph.objects) != 0 {
				t.Fatalf("original observer accepted malformed/foreign receipt: observed=%d err=%v", f.observed, err)
			}
		})
	}
}
