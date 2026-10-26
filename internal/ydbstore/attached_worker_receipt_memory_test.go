package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestPlannedReceiptBlobsKnownSizeDoesNotGrow(t *testing.T) {
	for _, size := range []int{1, 32 << 10, 192_000, 256 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			body := bytes.Repeat([]byte("x"), size)
			planned := new(plannedReceiptBlobs)
			ref, err := planned.Put(t.Context(), "receipt-memory-tenant", "tenants/receipt-memory-tenant/result", bytes.NewReader(body))
			if err != nil || len(planned.items) != 1 || ref.Size != int64(size) {
				t.Fatal("known-size canonical planning failed", err)
			}
			if got := planned.items[0].body; len(got) != size || cap(got) != size {
				t.Fatalf("known-size planner retained growth capacity: len=%d cap=%d declared=%d", len(got), cap(got), size)
			}
			clear(body)
			if !bytes.Equal(planned.items[0].body, bytes.Repeat([]byte("x"), size)) {
				t.Fatal("planner borrowed mutable producer bytes instead of owning its copy")
			}
		})
	}
}

type receiptExactReader struct {
	contents   []byte
	finalErr   error
	afterErr   error
	cancel     context.CancelFunc
	reads      int
	noProgress bool
}

func (r *receiptExactReader) Read(p []byte) (int, error) {
	r.reads++
	if r.noProgress {
		return 0, nil
	}
	if len(r.contents) == 0 {
		if r.afterErr != nil {
			return 0, r.afterErr
		}
		return 0, io.EOF
	}
	n := copy(p, r.contents)
	r.contents = r.contents[n:]
	if r.cancel != nil {
		r.cancel()
	}
	if len(r.contents) == 0 {
		return n, r.finalErr
	}
	return n, nil
}

func TestExactReceiptBytesPreservesFinalErrorsAndBounds(t *testing.T) {
	for _, tc := range []struct {
		name, input        string
		size, limit        int64
		finalErr           error
		afterErr           error
		cancel, noProgress bool
		wantErr            error
	}{
		{name: "exact_eof", input: "body", size: 4, limit: 4, finalErr: io.EOF},
		{name: "separate_eof", input: "body", size: 4, limit: 4},
		{name: "final_bytes_error", input: "body", size: 4, limit: 4, finalErr: io.ErrClosedPipe, wantErr: io.ErrClosedPipe},
		{name: "probe_error", input: "body", size: 4, limit: 4, afterErr: io.ErrClosedPipe, wantErr: io.ErrClosedPipe},
		{name: "short", input: "bod", size: 4, limit: 4, finalErr: io.EOF, wantErr: io.EOF},
		{name: "trailing", input: "body!", size: 4, limit: 4, finalErr: io.EOF, wantErr: attachedworkeroutput.ErrCandidateInvalid},
		{name: "oversized_before_read", input: "body", size: 4, limit: 3, wantErr: attachedworkeroutput.ErrCandidateInvalid},
		{name: "cancel_final_eof", input: "body", size: 4, limit: 4, finalErr: io.EOF, cancel: true, wantErr: context.Canceled},
		{name: "no_progress", size: 4, limit: 4, noProgress: true, wantErr: io.ErrNoProgress},
		{name: "empty_probe_no_progress", size: 0, limit: 4, noProgress: true, wantErr: io.ErrNoProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader := &receiptExactReader{contents: []byte(tc.input), finalErr: tc.finalErr, afterErr: tc.afterErr, noProgress: tc.noProgress}
			if tc.cancel {
				reader.cancel = cancel
			}
			observe := &receiptMemoryReadback{reader: reader}
			got, err := readExactReceiptBytes(ctx, observe, tc.size, tc.limit)
			if tc.wantErr == nil {
				if err != nil || string(got) != tc.input || cap(got) != len(got) {
					t.Fatal("exact reader changed canonical bytes/capacity", err)
				}
			} else {
				if got != nil || !errors.Is(err, tc.wantErr) || !errors.Is(err, attachedworkeroutput.ErrCandidateInvalid) {
					t.Fatalf("body error changed: got=%d err=%v want=%v", len(got), err, tc.wantErr)
				}
				for _, raw := range observe.borrowed {
					if !bytes.Equal(raw, make([]byte, len(raw))) {
						t.Fatal("denied temporary body retained content")
					}
				}
			}
			if tc.size > tc.limit && reader.reads != 0 || tc.noProgress && reader.reads != 100 || observe.closed {
				t.Fatalf("reader admission/progress/ownership changed: reads=%d closed=%t", reader.reads, observe.closed)
			}
		})
	}
	// The explicit past deadline is already expired before admission; no
	// elapsed-time assertion, sleep or scheduling race decides this negative.
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(1, 0))
	defer cancel()
	reader := &receiptExactReader{contents: []byte("body"), finalErr: io.EOF}
	if got, err := readExactReceiptBytes(ctx, reader, 4, 4); got != nil || !errors.Is(err, context.DeadlineExceeded) || reader.reads != 0 {
		t.Fatal("expired context acquired body read/allocation authority", err, reader.reads)
	}
}

type receiptSizedReader struct {
	*receiptExactReader
	size int
}

func (r receiptSizedReader) Len() int { return r.size }

func TestInlineReceiptPlannerAdmitsSizeBeforeRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		unknown bool
		size    int
	}{
		{name: "unknown_size", unknown: true, size: 4},
		{name: "oversized", size: 5},
		{name: "negative", size: -1},
		{name: "exact", size: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			planned := &plannedReceiptBlobs{maxBytes: 4}
			r := &receiptExactReader{contents: []byte("body"), finalErr: io.EOF}
			var input io.Reader = receiptSizedReader{r, tc.size}
			if tc.unknown {
				input = r
			}
			_, err := planned.Put(t.Context(), "receipt-memory-tenant", "tenants/receipt-memory-tenant/result", input)
			if tc.name == "exact" {
				if err != nil || planned.total != 4 || len(planned.items) != 1 || cap(planned.items[0].body) != 4 {
					t.Fatal("exact V2 cap rejected", err)
				}
			} else if !errors.Is(err, attachedworkeroutput.ErrCandidateInvalid) || r.reads != 0 || planned.total != 0 || len(planned.items) != 0 {
				t.Fatal("V2 admitted unknown/oversize content before prospective cap", err, r.reads)
			}
		})
	}
	// The old V1 zero-default still accepts an unknown-size stream.
	legacy := new(plannedReceiptBlobs)
	if _, err := legacy.Put(t.Context(), "receipt-memory-tenant", "tenants/receipt-memory-tenant/legacy", &receiptExactReader{contents: []byte("body"), finalErr: io.EOF}); err != nil {
		t.Fatal("legacy stream semantics changed", err)
	}
}

type receiptMemorySQL struct {
	*ratedSQLFixture
	payload    []byte
	written    []byte
	execErr    error
	execCalls  int
	duringExec func([]byte) error
	discard    bool
}

func (f *receiptMemorySQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *receiptMemorySQL) QueryContext(_ context.Context, statement string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(statement, "SELECT payload FROM attached_worker_output_receipts") {
		return nil, errors.New("unexpected SQL read")
	}
	return &explanationFixtureRows{values: []driver.Value{f.payload}}, nil
}
func (f *receiptMemorySQL) ExecContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.Contains(statement, "attached_worker_output_receipts") || !strings.Contains(statement, "AS JsonDocument)") {
		return nil, errors.New("unexpected SQL write")
	}
	f.execCalls++
	payload, ok := args[len(args)-1].Value.([]byte)
	if !ok {
		return nil, errors.New("receipt payload acquired an extra string representation")
	}
	// The driver borrows this parameter only until Exec returns. Retain a
	// test-owned copy, never the Encoder's pooled buffer or an interior view.
	if !f.discard {
		f.written = bytes.Clone(payload)
	}
	if f.duringExec != nil {
		if err := f.duringExec(payload); err != nil {
			return nil, err
		}
	}
	if f.execErr != nil {
		return nil, f.execErr
	}
	return driver.RowsAffected(1), nil
}

func TestInlineReceiptSQLUsesOwnedBytePayload(t *testing.T) {
	r := inlineCanonicalFixtureV2(t)
	raw := marshalInlineStoredReceiptTestV2(t, r)
	fixture := &receiptMemorySQL{ratedSQLFixture: new(ratedSQLFixture), payload: raw}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	state := &stateTx{sqlTx: tx, tenantID: r.Binding.TenantID}
	a := r.Binding
	got, found, err := readAttachedWorkerReceiptEnvelopeTx(t.Context(), state, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration)
	if err != nil || !found || got.v2 == nil || !sameInlineReceiptV2(r, *got.v2) || !bytes.Equal(raw, fixture.payload) {
		t.Fatal("SQL byte scan changed valid receipt or borrowed driver buffer", err)
	}
	// The fake driver deliberately returns the same valid private row even
	// for a foreign query. Codec self-identity DATA cannot replace these actual
	// caller-key joins in the shared normal/finalizer/inventory reader.
	for _, name := range []string{"tenant", "run", "owner", "worker", "attempt", "generation"} {
		t.Run("foreign_"+name, func(t *testing.T) {
			query := *state
			run, owner, worker, attempt, generation := a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration
			switch name {
			case "tenant":
				query.tenantID = "foreign-tenant"
			case "run":
				run = "foreign-run"
			case "owner":
				owner = "foreign-owner"
			case "worker":
				worker = "foreign-worker"
			case "attempt":
				attempt = "foreign-attempt"
			case "generation":
				generation++
			}
			value, found, err := readAttachedWorkerReceiptEnvelopeTx(t.Context(), &query, run, owner, worker, attempt, generation)
			if !errors.Is(err, ErrAttachedWorkerAttemptConflict) || found || value.v1 != nil || value.v2 != nil || !bytes.Equal(raw, fixture.payload) {
				t.Fatal("valid stored DATA authorized a foreign SQL scope or changed driver bytes", err, found)
			}
		})
	}
	for i, insert := range []bool{true, false} {
		if err := writeInlineReceiptTxV2(t.Context(), state, r, insert); err != nil {
			t.Fatal("byte receipt write failed", err)
		}
		if !bytes.Equal(fixture.written, raw) || fixture.execCalls != i+1 {
			t.Fatal("SQL payload semantics or single-Exec ownership changed")
		}
	}
	fixture.execErr = io.ErrClosedPipe
	if err := writeInlineReceiptTxV2(t.Context(), state, r, false); !errors.Is(err, io.ErrClosedPipe) || fixture.execCalls != 3 {
		t.Fatal("failed SQL write lost cause or repeated Exec", err)
	}
	clear(fixture.payload)
	if got.v2.Candidate.Summary != r.Candidate.Summary || got.v2.Materialization.Completion.Events[0].DisplayText != r.Materialization.Completion.Events[0].DisplayText {
		t.Fatal("typed receipt retained driver payload aliases")
	}
	legacy := AttachedWorkerOutputReceiptV1{Version: 1, Ready: true, Binding: r.Binding, Nonce: r.Nonce,
		CandidateFingerprint: r.CandidateFingerprint, Observation: r.Observation, ObservationDigest: r.ObservationDigest,
		CredentialRequired: r.CredentialRequired, HarnessBindingDigest: r.Producer.HarnessBindingDigest,
		Status: domain.AttachedWorkerTerminalSucceeded, CanonicalDigest: r.CanonicalDigest, Materialization: r.Materialization, CreatedAt: r.CreatedAt}
	fixture.payload, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	old, found, err := readAttachedWorkerReceiptEnvelopeTx(t.Context(), state, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration)
	if err != nil || !found || old.v1 == nil || old.v2 != nil || !old.ready() || old.v1.CanonicalDigest != legacy.CanonicalDigest {
		t.Fatal("owned SQL byte scan changed V1 compatibility", err)
	}
	clear(fixture.payload)
	if old.v1.Materialization.Completion.Events[0].DisplayText != r.Candidate.Summary {
		t.Fatal("legacy typed receipt borrowed driver payload content")
	}
}

func openReceiptMemoryWriterTx(t *testing.T) (*receiptMemorySQL, *stateTx) {
	t.Helper()
	f := &receiptMemorySQL{ratedSQLFixture: new(ratedSQLFixture)}
	db := sql.OpenDB(f)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return f, &stateTx{sqlTx: tx}
}

func maximumInlineArtifactSQLFixtureV2(t *testing.T) AttachedWorkerOutputReceiptV2 {
	t.Helper()
	r := maximumInlineIdentityFixtureV2(t)
	_, loaded, _ := inlineReceiptFixtureV2(t)
	body := make([]byte, 0, attachedworkeroutput.MaxInlineTotalBytesV2)
	for i, name := range []string{strings.Repeat("<", 255), strings.Repeat(">", 255)} {
		content := bytes.Repeat([]byte{byte('a' + i)}, attachedworkeroutput.MaxInlineArtifactBytesV2)
		sum := sha256.Sum256(content)
		r.Candidate.Artifacts = append(r.Candidate.Artifacts, attachedworkeroutput.InlineArtifactV2{Name: name,
			MediaType: "text/plain", Size: uint64(len(content)), SHA256: hex.EncodeToString(sum[:])})
		body = append(body, content...)
		clear(content)
	}
	defer clear(body)
	verified, err := attachedworkeroutput.VerifyInlineV2(r.Candidate, body)
	if err != nil {
		t.Fatal("maximum two-artifact input", err)
	}
	defer verified.Clear()
	planned := new(plannedReceiptBlobs)
	t.Cleanup(func() { planned.clearBodies() })
	r.Materialization, err = attachedworkeroutput.MaterializeInlineV2(t.Context(), planned, loaded, r.Binding.LeaseID, r.Binding.LeaseGeneration, verified, r.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	r.CandidateFingerprint, err = inlineReceiptPlanFingerprintV2(r)
	if err != nil {
		t.Fatal(err)
	}
	r.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
	if err != nil {
		t.Fatal(err)
	}
	r.Materialization.EvidenceDigest = r.CanonicalDigest
	if err := validateInlineFixtureV2(r); err != nil {
		t.Fatal("maximum two-artifact receipt is not identity-valid", err)
	}
	return r
}

func TestInlineReceiptSQLEncoderMatchesMarshal(t *testing.T) {
	for _, name := range []string{"prepared", "claimed", "copying", "ready", "retired", "maximum", "maximum_artifacts", "encoding_error", "oversized"} {
		t.Run(name, func(t *testing.T) {
			r := inlineCanonicalFixtureV2(t)
			switch name {
			case "prepared", "claimed", "retired":
				r, _, _ = inlineReceiptFixtureV2(t)
				if name != "prepared" {
					r.Stage, r.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, strings.Repeat("c", 64)
				}
				if name == "retired" {
					r.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
				}
			case "ready":
				r.Stage = attachedworkeroutput.InlineReadyV2
			case "maximum":
				r = maximumInlineIdentityFixtureV2(t)
			case "maximum_artifacts":
				r = maximumInlineArtifactSQLFixtureV2(t)
			case "encoding_error":
				r.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "oversized":
				r.Candidate.Summary = strings.Repeat("<", maxInlineReceiptRecordBytesV2/6)
			}
			full, marshalErr := json.Marshal(r)
			var raw []byte
			if marshalErr == nil && len(full) <= maxInlineReceiptRecordBytesV2 {
				raw = marshalInlineStoredReceiptTestV2(t, r)
			}
			f, state := openReceiptMemoryWriterTx(t)
			for _, insert := range []bool{true, false} {
				before := f.execCalls
				err := writeInlineReceiptTxV2(t.Context(), state, r, insert)
				if marshalErr != nil || len(full) > maxInlineReceiptRecordBytesV2 {
					if !errors.Is(err, ErrAttachedWorkerAttemptConflict) || f.execCalls != before {
						t.Fatal("encoding/size denial reached SQL", err, f.execCalls-before)
					}
				} else if err != nil || f.execCalls != before+1 || !bytes.Equal(f.written, raw) {
					t.Fatal("Encoder changed private stored bytes or single Exec", err, f.execCalls-before)
				}
			}
		})
	}
}

func TestInlineReceiptSQLWriterBorrowAndOneShot(t *testing.T) {
	for _, name := range []string{"exact_cap", "oversized", "missing_lf", "empty", "sql_error", "canceled"} {
		t.Run(name, func(t *testing.T) {
			f, state := openReceiptMemoryWriterTx(t)
			p := []byte("{\"answer\":\"<>&\\n\\u2028\"}\n")
			switch name {
			case "exact_cap", "oversized":
				size := maxInlineReceiptStoredBytesV2
				if name == "oversized" {
					size++
				}
				p = []byte("\"" + strings.Repeat("x", size-2) + "\"\n")
			case "missing_lf":
				p = p[:len(p)-1]
			case "empty":
				p = nil
			case "sql_error":
				f.execErr = io.ErrClosedPipe
			}
			original := bytes.Clone(p)
			f.duringExec = func(payload []byte) error {
				if len(payload) != len(p)-1 || &payload[0] != &p[0] || !bytes.Equal(payload, original[:len(original)-1]) {
					return errors.New("SQL copied or modified the borrowed complete payload")
				}
				return nil
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if name == "canceled" {
				cancel()
			}
			w := inlineReceiptSQLWriterV2{ctx: ctx, tx: state}
			n, err := w.Write(p)
			wantCalls := 1
			var wantErr error
			switch name {
			case "oversized", "missing_lf", "empty":
				wantCalls, wantErr = 0, ErrAttachedWorkerAttemptConflict
			case "sql_error":
				wantErr = io.ErrClosedPipe
			case "canceled":
				wantCalls, wantErr = 0, context.Canceled
			}
			if !errors.Is(err, wantErr) || f.execCalls != wantCalls || !bytes.Equal(p, original) || (err == nil && n != len(p)) || (err != nil && n != 0) {
				t.Fatalf("borrowed write: n=%d err=%v calls=%d wantErr=%v wantCalls=%d", n, err, f.execCalls, wantErr, wantCalls)
			}
			if n, err := w.Write([]byte("{}\n")); n != 0 || !errors.Is(err, ErrAttachedWorkerAttemptConflict) || f.execCalls != wantCalls {
				t.Fatal("second Write reacquired SQL mutation", n, err, f.execCalls)
			}
		})
	}
}

func TestInlineReceiptSQLSerializationAllocationEvidence(t *testing.T) {
	r := maximumInlineArtifactSQLFixtureV2(t)
	f, state := openReceiptMemoryWriterTx(t)
	f.discard = true
	var bytesSeen int
	f.duringExec = func(p []byte) error {
		bytesSeen = len(p)
		_ = sha256.Sum256(p)
		return nil
	}
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := writeInlineReceiptTxV2(t.Context(), state, r, false); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Evidence only: pooled capacity depends on other JSON and race-detector
	// pool eviction. This is not an aggregate peak or zeroization assertion.
	t.Logf("SQL serialization allocated=%d B/op, payload=%d B; not aggregate memory proof", result.AllocedBytesPerOp(), bytesSeen)
}

type receiptMemoryReadback struct {
	reader   io.Reader
	borrowed [][]byte
	closed   bool
	closeErr error
}

func (r *receiptMemoryReadback) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.borrowed = append(r.borrowed, p[:n])
	}
	return n, err
}

func (r *receiptMemoryReadback) Close() error { r.closed = true; return r.closeErr }

type receiptMemoryBlobs struct {
	reader *receiptMemoryReadback
	puts   int
	ref    domain.BlobRef
}

func (b *receiptMemoryBlobs) Put(_ context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	b.puts++
	body, err := io.ReadAll(r)
	if err != nil {
		return domain.BlobRef{}, err
	}
	hash := sha256.Sum256(body)
	b.ref = domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(hash[:])}
	clear(body)
	return b.ref, nil
}

func (b *receiptMemoryBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	return b.reader, nil
}

func (*receiptMemoryBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return errors.New("unexpected object deletion")
}

func TestPlannedReceiptBlobsClearsReadbackCopy(t *testing.T) {
	for _, test := range []struct {
		name, contents string
		closeErr       error
		wantErr        bool
	}{
		{name: "exact", contents: "canonical"},
		{name: "short", contents: "canon", wantErr: true},
		{name: "trailing", contents: "canonical!", wantErr: true},
		{name: "changed", contents: "CANONICAL", wantErr: true},
		{name: "close_error", contents: "canonical", closeErr: io.ErrClosedPipe, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			readback := &receiptMemoryReadback{reader: bytes.NewBufferString(test.contents), closeErr: test.closeErr}
			upstream := &receiptMemoryBlobs{reader: readback}
			planned := &plannedReceiptBlobs{upstream: upstream}
			if _, err := planned.Put(t.Context(), "receipt-memory-tenant", "tenants/receipt-memory-tenant/result", bytes.NewBufferString("canonical")); err != nil {
				t.Fatal("fixture planner Put", err)
			}
			err := planned.Commit(t.Context())
			if test.wantErr != (err != nil) || err != nil && !errors.Is(err, attachedworkeroutput.ErrCandidateInvalid) || !readback.closed || upstream.puts != 1 {
				t.Fatalf("readback outcome: err=%v closed=%t puts=%d", err, readback.closed, upstream.puts)
			}
			for _, borrowed := range readback.borrowed {
				if !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
					t.Fatal("temporary canonical reread bytes remained live after Commit returned")
				}
			}
			// Temporary reread cleanup does not retire the exact planned bytes
			// or prove remote custody/quiescence after a failed/ambiguous write.
			if string(planned.items[0].body) != "canonical" || planned.items[0].ref != upstream.ref {
				t.Fatal("readback cleanup changed canonical plan/custody identity")
			}
		})
	}
}
