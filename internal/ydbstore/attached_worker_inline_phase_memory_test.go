package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	Ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	YdbOperations "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	YdbTable "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// Phase-local, compositional payload evidence, not process-wide heap/allocs,
// an authenticated native producer, real SQL/S3, or whole-issue acceptance.
// No arbitrary idle JSON-pool backing tails/history are charged to an attempt.
// Active JSON encoded prefixes and returned copies ARE distinct representations.
// Source/lifetime map (pinned dependencies must be rechecked on upgrades):
//   - receipt/inline_http_v2.go: raw exact allocation; inlineDecodeV2 retains a
//     standard Decoder frame through Decode/EOF, then Marshal canonical check.
//     raw is cleared before service.Prepare, not carried into materialization.
//   - inline_store_v2.go: raw body -> verified clone, raw cleared; materialize
//     into planner; verified.Clear before copying SQL; P cleared before READY.
//   - output/materialize.go: event Marshal -> planner owned copy -> readVerified
//     io.ReadAll. Original+copy+reread overlap, not with later SQL encoding.
//   - receipt_blobs.go: exact planner and Commit reread allocations; upstream
//     s3store/store.go Put uses io.ReadAll before its synchronous network call.
//   - inline_store_v2.go: writeInlineReceiptTxV2 lends exact Q through Exec and
//     clears Q afterward. JSON answer-free metadata is transient, not a cap on
//     old 512KiB Usage DATA. Typed prior SQL rows can own a second Summary.
//   - SDK v3.135.0 internal/table/session.go executeDataQuery: response Any
//     UnmarshalTo decodes JsonDocument TextValue; scanner aliases that text
//     as bytes, then database/sql Scan copies into []byte. Codec receive
//     fragments/concatenation precede this SDK stage.
//     The separate ydbclient codec tests qualify its private exact allocator;
//     this file neither exports nor copies that codec or calls it an integration.
// Input fixtures/oracle clones below are test scaffolding, not added runtime
// owners. Phase sums describe named production representations, not test heap.

func inlinePhaseScopeV2(t *testing.T, long bool) (AttachedWorkerOutputReceiptV2, ports.WorkerJobState) {
	t.Helper()
	r, loaded, unused := inlineReceiptFixtureV2(t)
	clear(unused)
	if !long {
		return r, loaded
	}
	// Max admitted opaque-ID bytes, UTC nanosecond spelling and generation
	// widths. Canonical manifest/event IDs below are STILL generated normally,
	// never replaced by longer self-valid DATA-only IDs.
	id := strings.Repeat("i", 160)
	at := loaded.Run.CreatedAt.Add(123456789 * time.Nanosecond)
	loaded.Run.ID, loaded.Run.TenantID, loaded.Run.SessionID = domain.RunID(id), domain.TenantID(id), domain.SessionID(id)
	loaded.Run.TriggerEventID, loaded.Run.SubscriptionConnectionID, loaded.Run.IdempotencyKey = domain.SessionEventID(id), domain.SubscriptionConnectionID(id), domain.IdempotencyKey(id)
	loaded.Run.CreatedAt, loaded.Run.UpdatedAt = at, at
	j := &loaded.Job
	j.TenantID, j.RunID, j.SessionID, j.TriggerEventID = loaded.Run.TenantID, loaded.Run.ID, loaded.Run.SessionID, loaded.Run.TriggerEventID
	j.AttemptID, j.ReservationID, j.InputManifestID, j.CredentialOwnerUserID = domain.AttemptID(id), domain.QuotaReservationID(id), domain.ArtifactManifestID(id), domain.UserID(id)
	j.CreatedAt, j.DeliveryChat.TenantID = at, loaded.Run.TenantID
	j.ContextSnapshot.TenantID, j.ContextSnapshot.Key = loaded.Run.TenantID, domain.SessionObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID)+"context"
	j.ExecutionPlacementV2.OwnerUserID, j.ExecutionPlacementV2.WorkerID = j.CredentialOwnerUserID, domain.AttachedWorkerID(id)
	pd, err := domain.ExecutionPlacementDigest(j.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	h := &j.HarnessBinding
	h.TenantID, h.OwnerUserID, h.RunID, h.AttemptID = j.TenantID, j.CredentialOwnerUserID, j.RunID, j.AttemptID
	h.Resource.OwnerUserID, h.ExecutionPlacementDigest = j.CredentialOwnerUserID, string(pd)
	loaded.Attempt.ID, loaded.Attempt.TenantID, loaded.Attempt.RunID, loaded.Attempt.WorkerID = j.AttemptID, j.TenantID, j.RunID, id
	loaded.Attempt.CreatedAt, loaded.Attempt.UpdatedAt = at, at
	loaded.Reservation.ID, loaded.Reservation.TenantID, loaded.Reservation.RunID, loaded.Reservation.SubscriptionConnectionID = j.ReservationID, j.TenantID, j.RunID, loaded.Run.SubscriptionConnectionID
	loaded.Reservation.HeldAt, loaded.Reservation.UpdatedAt, loaded.Reservation.ExpiresAt = at, at, at.Add(time.Minute)
	loaded.InputManifest.ID, loaded.InputManifest.TenantID, loaded.InputManifest.RunID, loaded.InputManifest.CreatedAt = j.InputManifestID, j.TenantID, j.RunID, at
	if err := validateLoadedWorkerJob(loaded); err != nil {
		t.Fatal("maximum scoped fixture must satisfy original aggregate validation", err)
	}
	a := &r.Binding
	a.TenantID, a.OwnerUserID, a.WorkerID, a.ConnectionID = j.TenantID, j.CredentialOwnerUserID, j.ExecutionPlacementV2.WorkerID, domain.AttachedWorkerConnectionID(id)
	a.RunID, a.AttemptID, a.LeaseID = j.RunID, j.AttemptID, domain.LeaseID(id)
	a.EnrollmentGeneration, a.ConnectionGeneration, a.LeaseGeneration = ^uint64(0), ^uint64(0), ^uint64(0)
	a.LeaseExpiresAtUnixMicro = at.Add(time.Minute).UnixMicro()
	a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	a.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(loaded.Job, loaded.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	r.Nonce, r.Producer.SessionID, r.Producer.ReservationID = domain.IdempotencyKey(id), loaded.Run.SessionID, j.ReservationID
	r.Producer.HarnessBindingDigest, err = h.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r.Producer.AcceptedAt, r.CreatedAt, r.ExpiresAt = at, at.Add(time.Second), at.Add(6*time.Second)
	return r, loaded
}

func TestInlineNormalProducerPhasePayloadRepresentationsV2(t *testing.T) {
	for _, name := range []string{"small-generated", "maximum-answer-artifacts", "maximum-answer-artifacts-scope"} {
		t.Run(name, func(t *testing.T) {
			r, loaded := inlinePhaseScopeV2(t, name == "maximum-answer-artifacts-scope")
			r.Candidate = attachedworkeroutput.InlineCandidateV2{Summary: "small answer"}
			body := make([]byte, 0)
			if name != "small-generated" {
				r.Candidate.Summary = strings.Repeat("<", 15808) + strings.Repeat("\u2028", 384) + strings.Repeat("<", 15808)
				body = make([]byte, 64<<10)
				copy(body, strings.Repeat("a", 32<<10))
				body[32<<10] = '0'
				for i := (32 << 10) + 1; i < len(body); i++ {
					body[i] = ' '
				}
				for i, media := range []string{"text/plain", "application/json"} {
					sum := sha256.Sum256(body[i*(32<<10) : (i+1)*(32<<10)])
					r.Candidate.Artifacts = append(r.Candidate.Artifacts, attachedworkeroutput.InlineArtifactV2{Name: strings.Repeat([]string{"<", ">"}[i], 255), MediaType: media, Size: 32 << 10, SHA256: hex.EncodeToString(sum[:])})
				}
				if len(r.Candidate.Summary) != 32<<10 || utf8.RuneCountInString(r.Candidate.Summary) != 32000 {
					t.Fatal("fixture did not reach both admitted answer maxima")
				}
			}
			t.Cleanup(func() { clear(body) })
			// The semantic maxima must also fit one original final, using a
			// legitimate literal-HTML spelling rather than Marshal's optional
			// HTML escapes. This is representability, not a private native witness.
			var final bytes.Buffer
			enc := json.NewEncoder(&final)
			enc.SetEscapeHTML(false)
			type finalArtifact struct {
				Name      string `json:"name"`
				MediaType string `json:"media_type"`
				Content   string `json:"content"`
			}
			artifacts := make([]finalArtifact, len(r.Candidate.Artifacts))
			for i, artifact := range r.Candidate.Artifacts {
				artifacts[i].Name, artifacts[i].MediaType, artifacts[i].Content = artifact.Name, artifact.MediaType, string(body[i*(32<<10):(i+1)*(32<<10)])
			}
			if err := enc.Encode(struct {
				Answer    string          `json:"answer"`
				Artifacts []finalArtifact `json:"artifacts"`
			}{r.Candidate.Summary, artifacts}); err != nil || final.Len()-1 > 128<<10 {
				t.Fatal("maximum semantic fixture cannot fit original final", final.Len(), err)
			}
			finalDigest := sha256.Sum256(bytes.TrimSuffix(final.Bytes(), []byte{'\n'}))
			r.Producer.FinalDigest = hex.EncodeToString(finalDigest[:])
			clear(final.Bytes()) // Test-only original-final spelling, not server ownership.
			final.Reset()
			for i := range artifacts {
				artifacts[i].Content = ""
			}
			resealInlineCodecFixtureV2(t, &r)
			observeInlinePreparePhaseV2(t, r)
			cloneProbe := bytes.Clone(body) // Same library operation, not private owner access.
			verifiedCapacity := cap(cloneProbe)
			clear(cloneProbe)
			cloneProbe = nil
			verified, err := attachedworkeroutput.VerifyInlineV2(r.Candidate, body)
			if err != nil {
				t.Fatal("original inline verifier", err)
			}
			t.Cleanup(verified.Clear)
			// VerifyInlineV2 bytes.Clone is a SECOND body; raw is released before
			// Materialize in production. No private owner fields/accessors needed.
			t.Logf("body verification: raw=%d verified semantic bytes=%d same-bytes.Clone capacity probe=%d answer=%d (private clone capacity composed, not directly observed)", cap(body), len(body), verifiedCapacity, len(r.Candidate.Summary))
			clear(body)
			body = nil
			planned := &plannedReceiptBlobs{maxBytes: 320 << 10}
			t.Cleanup(planned.clearBodies)
			r.Materialization, err = attachedworkeroutput.MaterializeInlineV2(t.Context(), planned, loaded, r.Binding.LeaseID, r.Binding.LeaseGeneration, verified, r.CreatedAt)
			if err != nil {
				t.Fatal("actual canonical generator/planner", err)
			}
			if r.Materialization.Completion.Usage != nil || r.Materialization.Completion.SubstrateEvidence != nil || len(planned.items) != len(r.Candidate.Artifacts)+1 {
				t.Fatal("fixture left the generated normal-producer cohort")
			}
			verified.Clear() // Actual server lifetime: do not add this clone to SQL.
			r.Stage, r.BodyClaimID, r.CopyOwnerID = attachedworkeroutput.InlineCanonicalCopyingV2, strings.Repeat("c", 64), strings.Repeat("d", 64)
			resealInlineCodecFixtureV2(t, &r)
			p, largest := 0, 0
			for _, item := range planned.items {
				if cap(item.body) != len(item.body) {
					t.Fatal("planner retained growth capacity")
				}
				p += cap(item.body)
				if len(item.body) > largest {
					largest = len(item.body)
				}
				reader, err := planned.Open(t.Context(), item.ref.TenantID, item.ref)
				if err != nil {
					t.Fatal(err)
				}
				// Same standard ReadAll path as materialize.readVerified and
				// s3store.Put; no S3 constructor/client/network is invoked.
				reread, readErr := io.ReadAll(io.LimitReader(reader, item.ref.Size+1))
				closeErr := reader.Close()
				if readErr != nil || closeErr != nil || !bytes.Equal(reread, item.body) {
					t.Fatal("planned canonical reread", readErr, closeErr)
				}
				t.Logf("readback representation: bytes=%d ReadAll capacity=%d (separate from SQL phase)", len(item.body), cap(reread))
				if item.ref == r.Materialization.Completion.Events[0].Payload {
					original, err := json.Marshal(struct {
						Schema     string                    `json:"schema"`
						Summary    string                    `json:"summary"`
						ManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
					}{"sessionless.assistant-message.v1", r.Candidate.Summary, r.Materialization.Completion.Manifest.ID})
					if err != nil || !bytes.Equal(original, item.body) {
						t.Fatal("original materializer payload spelling", err)
					}
					phase := p + len(r.Candidate.Summary) + verifiedCapacity + cap(original) + cap(reread)
					t.Logf("materialization compositional overlap P=%d answer=%d verifiedCloneComposedCap=%d originalEventCap=%d rereadCap=%d sum=%d; planner clone and reread overlap original, no later SQL buffers", p, len(r.Candidate.Summary), verifiedCapacity, cap(original), cap(reread), phase)
					clear(original)
				}
				clear(reread)
			}
			if p > 320<<10 || int64(p) != planned.total {
				t.Fatalf("generated plan escaped existing cohort: capacity=%d total=%d", p, planned.total)
			}
			// The copying transition reads the prior body_claimed row while P
			// exists; it does not read a READY row in that phase.
			prior := r
			prior.Stage, prior.CopyOwnerID, prior.CanonicalDigest = attachedworkeroutput.InlineBodyClaimedV2, "", ""
			prior.Materialization = ports.AttachedWorkerTerminalMaterialization{}
			observeInlineReceivePhaseV2(t, prior, p)
			observeInlineSQLPhaseV2(t, r, p)
			// Commit owns upstream/S3/reread lifetimes not exercised here. Releasing
			// server planning bytes after acknowledged Commit is source evidence,
			// not permission to retire ambiguous custody or original native bytes.
			planned.clearBodies()
			for _, item := range planned.items {
				if item.body != nil {
					t.Fatal("explicit planner release retained a body")
				}
			}
			ready := r
			ready.Stage = attachedworkeroutput.InlineReadyV2
			observeInlineReceivePhaseV2(t, ready, 0)
			t.Logf("generated IDs manifest=%d event=%d, largest canonical object=%d; no bound on all unrelated metadata/Usage DATA", len(r.Materialization.Completion.Manifest.ID), len(r.Materialization.Completion.Events[0].ID), largest)
		})
	}
}

type inlinePhaseReadV2 struct {
	*bytes.Reader
	maxCapacity int
}

func (r *inlinePhaseReadV2) Read(p []byte) (int, error) {
	if cap(p) > r.maxCapacity {
		r.maxCapacity = cap(p)
	}
	return r.Reader.Read(p)
}

func observeInlinePreparePhaseV2(t *testing.T, r AttachedWorkerOutputReceiptV2) {
	t.Helper()
	input := attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: r.Binding, Nonce: r.Nonce, Candidate: r.Candidate, Producer: r.Producer, Observation: r.Observation, ExpiresAt: r.ExpiresAt}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > 256<<10 {
		t.Fatal("maximum Prepare frame cannot be represented", len(raw), err)
	}
	defer clear(raw)
	if err := strictjson.ValidateDocument(raw, 256<<10, 16, 64); err != nil {
		t.Fatal("actual Prepare lexical bounds rejected generated frame", err)
	}
	reader := &inlinePhaseReadV2{Reader: bytes.NewReader(raw)}
	d := json.NewDecoder(reader)
	d.DisallowUnknownFields()
	var decoded attachedworkeroutput.AttachedWorkerInlinePrepareV2
	if d.Decode(&decoded) != nil || d.Decode(new(any)) != io.EOF {
		t.Fatal("same strict typed Decode/EOF path rejected generated Prepare")
	}
	canonical, err := json.Marshal(&decoded)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatal("Prepare canonical check changed semantic frame", err)
	}
	defer clear(canonical)
	// The handler raw buffer is exact declared length, unlike this test's
	// Marshal scaffold. On EOF refill Decoder supplies its available frame;
	// maxCapacity observes standard-library frame growth, not a fake 256K cap.
	// Marshal temporarily owns an active encoded prefix AND its returned copy.
	count := inlineReceiptStringCountV2{}
	if err := streamInlineReceiptJSONStringV2(&count, r.Candidate.Summary); err != nil {
		t.Fatal(err)
	}
	metadata := len(raw) - count.bytes + 2
	phase := len(raw) + reader.maxCapacity + len(canonical) + cap(canonical) + len(decoded.Candidate.Summary) + metadata
	t.Logf("Prepare compositional overlap rawExact=%d decoderFrameObserved=%d canonicalActivePrefix=%d canonicalReturnedCap=%d answer=%d conservativeMetadata=%d sum=%d; helper is private, not actual handler integration", len(raw), reader.maxCapacity, len(canonical), cap(canonical), len(decoded.Candidate.Summary), metadata, phase)
	// This observed fixture must fit; no new public metadata ceiling follows.
	if phase > 1<<20 {
		t.Fatalf("observed generated Prepare representations exceed original phase bound: %d", phase)
	}
}

func observeInlineSQLPhaseV2(t *testing.T, r AttachedWorkerOutputReceiptV2, p int) {
	t.Helper()
	plan, err := planInlineReceiptEncodingV2(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(plan.metadata)
	t.Logf("SQL encoding subphases: P=%d typedAnswers=%d typedMetadataBound=%d metadataMarshalActivePrefix=%d metadataReturnedCap=%d laterExactQ=%d; active prefix ends before Q/protobuf send, idle pool tails excluded", p, 2*len(r.Candidate.Summary), 2*len(plan.metadata), len(plan.metadata), cap(plan.metadata), plan.storedBytes)
	f, state := openInlinePhaseSQLV2(t)
	f.discard = true
	var borrowed []byte
	f.duringExec = func(q []byte) error {
		borrowed = q // Observation only; never change the synchronous parameter.
		a := r.Binding
		params, err := table.NewQueryParameters(
			table.ValueParam("$p1", types.TextValue(string(a.TenantID))), table.ValueParam("$p2", types.TextValue(string(a.RunID))),
			table.ValueParam("$p3", types.TextValue(string(a.OwnerUserID))), table.ValueParam("$p4", types.TextValue(string(a.WorkerID))),
			table.ValueParam("$p5", types.TextValue(string(a.AttemptID))), table.ValueParam("$p6", types.Uint64Value(a.LeaseGeneration)),
			table.ValueParam("$p7", types.TextValue(string(r.CanonicalDigest))), table.ValueParam("$p8", types.BytesValue(q)),
		).ToYDB()
		if err != nil {
			return err
		}
		request := &YdbTable.ExecuteDataQueryRequest{SessionId: strings.Repeat("s", 160), Query: &YdbTable.Query{Query: &YdbTable.Query_YqlText{YqlText: "UPDATE attached_worker_output_receipts SET canonical_digest=$p7,payload=CAST($p8 AS JsonDocument) WHERE tenant_id=$p1 AND run_id=$p2 AND owner_user_id=$p3 AND worker_id=$p4 AND attempt_id=$p5 AND lease_generation=$p6"}}, Parameters: params}
		// Standard pinned protobuf composition, NOT a copied/called private
		// ydbclient codec or a physical exchange. Parameter conversion is actual
		// public SDK code; auto-declare/session/operation wrappers are not fully
		// reproduced and remain an explicit W-envelope qualification gap.
		wire, err := proto.Marshal(request)
		if err != nil {
			return err
		}
		defer clear(wire)
		var decoded YdbTable.ExecuteDataQueryRequest
		if proto.Unmarshal(wire, &decoded) != nil || !proto.Equal(request, &decoded) || len(wire) != proto.Size(request) || len(q) != cap(q) {
			return fmt.Errorf("generated request/protobuf/exact SQL capacity mismatch")
		}
		// Two typed answers conservatively include the independently decoded
		// prior SQL row. DisplayText in this generated completion aliases Summary.
		// Metadata bytes overestimate retained typed string content; this fixture
		// does not cap arbitrary valid Usage nor idle JSON pool backing tails.
		sum := p + 2*len(r.Candidate.Summary) + 2*len(plan.metadata) + cap(q) + proto.Size(request)
		t.Logf("SQL compositional overlap P=%d twoTypedAnswers=%d typedMetadataBound=%d Qexact=%d protobufSize=%d standardMarshalCapObserved=%d sum=%d; private codec capacity qualified separately", p, 2*len(r.Candidate.Summary), 2*len(plan.metadata), cap(q), proto.Size(request), cap(wire), sum)
		if sum > 1<<20 {
			return fmt.Errorf("observed generated SQL payload representations exceed bound: %d", sum)
		}
		return nil
	}
	if err := writeInlineReceiptTxV2(t.Context(), state, r, false); err != nil || f.execCalls != 1 {
		t.Fatal("actual synchronous SQL writer phase", err, f.execCalls)
	}
	if borrowed == nil || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Fatal("SQL parameter was not released/cleared after synchronous Exec")
	}
}

func observeInlineReceivePhaseV2(t *testing.T, r AttachedWorkerOutputReceiptV2, p int) {
	t.Helper()
	q, err := encodeInlineReceiptSQLBytesV2(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(q) // Test-only source row, not an outgoing SQL send in this phase.
	result := &YdbTable.ExecuteQueryResult{ResultSets: []*Ydb.ResultSet{{Columns: []*Ydb.Column{{Name: "payload", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT}}}}, Rows: []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_TextValue{TextValue: string(q)}}}}}}}}
	wrapped, err := anypb.New(result)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(wrapped.Value)
	response := &YdbTable.ExecuteDataQueryResponse{Operation: &YdbOperations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: wrapped}}
	wire, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(wire)
	var received YdbTable.ExecuteDataQueryResponse
	if proto.Unmarshal(wire, &received) != nil {
		t.Fatal("generated response decode")
	}
	defer clear(received.Operation.Result.Value)
	var got YdbTable.ExecuteQueryResult
	if received.Operation.Result.UnmarshalTo(&got) != nil {
		t.Fatal("standard SDK-shaped Any UnmarshalTo")
	}
	cellText := got.ResultSets[0].Rows[0].Items[0].GetTextValue()
	// SDK scanner/rawConverter.JSONDocument aliases its TextValue via
	// xstring.ToBytes. This owned conversion is ONLY fake-driver scaffolding,
	// not evidence of an additional SDK cell copy or a string-wiping claim.
	cell := []byte(cellText)
	defer clear(cell)
	f, state := openInlinePhaseSQLV2(t)
	f.payload, state.tenantID = cell, r.Binding.TenantID
	rows, err := state.sqlTx.QueryContext(t.Context(), "SELECT payload FROM attached_worker_output_receipts")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rows.Close() })
	var scanned []byte
	if !rows.Next() {
		t.Fatal("actual database/sql Scan fixture returned no row", rows.Err())
	}
	if err := rows.Scan(&scanned); err != nil {
		t.Fatal(err)
	}
	defer clear(scanned)
	if rows.Next() || rows.Err() != nil {
		t.Fatal("SQL scan row/EOF contract", rows.Err())
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(cell) > 0 && &scanned[0] == &cell[0] {
		t.Fatal("database/sql Scan did not own its independent byte copy")
	}
	decoded, err := decodeInlineStoredReceiptV2(scanned)
	if err != nil || !sameInlineReceiptV2(r, decoded) || !bytes.Equal(q, cell) {
		t.Fatal("generated Any/cell/receipt DATA composition", err)
	}
	a := r.Binding
	envelope, found, err := readAttachedWorkerReceiptEnvelopeTx(t.Context(), state, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration)
	if err != nil || !found || envelope.v2 == nil || !sameInlineReceiptV2(r, *envelope.v2) {
		t.Fatal("actual SQL envelope reader DATA/scope composition", err)
	}
	// Genuine receive peak has buffered transport fragments + possible
	// contiguous materialization + decoded Any. Their private allocator is
	// composed from ydbclient tests, not observed here. Later Any->cell and
	// database/sql Scan ownership must not all be summed with that earlier
	// transport phase. No whole-phase receive bound is asserted from this log.
	t.Logf("receive evidence stage=%s P=%d wireLen/cap=%d/%d AnyLen/cap=%d/%d JsonDocumentTextBytes=%d ScanLen/cap=%d/%d decodedAnswer=%d; codec fragments, lexical/unquote transient, actual SDK scanner alias and return lifetimes remain separate source obligations; these observations are NOT all simultaneous", r.Stage, p, len(wire), cap(wire), len(received.Operation.Result.Value), cap(received.Operation.Result.Value), len(cellText), len(scanned), cap(scanned), len(decoded.Candidate.Summary))
}

// The real pinned YDB xsql.Conn.CheckNamedValue accepts typed values before
// SDK binding, including uint64's high bit. The shared fake otherwise invokes
// database/sql's signed default conversion and rejects this admitted scope
// before QueryContext/ExecContext. Adapt only this phase fixture, not production
// or shared helpers; all other ordinary values retain their default conversion.
type inlinePhaseSQLV2 struct{ *receiptMemorySQL }

func (f *inlinePhaseSQLV2) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*inlinePhaseSQLV2) CheckNamedValue(value *driver.NamedValue) error {
	if _, ok := value.Value.(uint64); ok {
		return nil
	}
	return driver.ErrSkip
}

func openInlinePhaseSQLV2(t *testing.T) (*receiptMemorySQL, *stateTx) {
	t.Helper()
	f := &receiptMemorySQL{ratedSQLFixture: new(ratedSQLFixture)}
	db := sql.OpenDB(&inlinePhaseSQLV2{receiptMemorySQL: f})
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

func TestInlinePhaseSQLV2RetainsFullUint64Scope(t *testing.T) {
	f := new(inlinePhaseSQLV2)
	for _, generation := range []uint64{1, uint64(1) << 63, ^uint64(0)} {
		value := driver.NamedValue{Ordinal: 6, Value: generation}
		if err := f.CheckNamedValue(&value); err != nil || value.Value != generation {
			t.Fatalf("typed generation narrowed or rejected: got=%v want=%d err=%v", value.Value, generation, err)
		}
	}
	value := driver.NamedValue{Value: "ordinary-string"}
	if err := f.CheckNamedValue(&value); err != driver.ErrSkip || value.Value != "ordinary-string" {
		t.Fatalf("non-uint64 bypassed ordinary conversion: value=%v err=%v", value.Value, err)
	}
}
