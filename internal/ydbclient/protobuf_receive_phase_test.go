package ydbclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	Ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	YdbOperations "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	YdbTable "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This is a compositional body_claimed row, not a second receipt codec or an
// authority fixture. The field names/types match AttachedWorkerOutputReceiptV2
// and its single-answer-v1 SQL projection; opaque digests here are declarations,
// not minting proof of an invocation or a valid persisted receipt identity.
// The ydbstore TestInlineNormalProducerPhasePayloadRepresentationsV2 separately
// exercises the actual writer/reader and canonical materializer, including READY.
// Importing that store into this same-package private-codec test would cycle.
func codecNormalInlineRow(t *testing.T) []byte {
	t.Helper()
	answer := strings.Repeat("<", 15808) + strings.Repeat("\u2028", 384) + strings.Repeat("<", 15808)
	if len(answer) != attachedworkeroutput.MaxInlineAnswerBytesV2 || utf8.RuneCountInString(answer) != 32000 {
		t.Fatal("normal-answer fixture does not reach both admitted maxima")
	}
	body := make([]byte, attachedworkeroutput.MaxInlineTotalBytesV2)
	copy(body, strings.Repeat("a", attachedworkeroutput.MaxInlineArtifactBytesV2))
	body[attachedworkeroutput.MaxInlineArtifactBytesV2] = '0'
	for i := attachedworkeroutput.MaxInlineArtifactBytesV2 + 1; i < len(body); i++ {
		body[i] = ' '
	}
	defer clear(body)
	candidate := attachedworkeroutput.InlineCandidateV2{Summary: answer}
	for i, media := range []string{"text/plain", "application/json"} {
		digest := sha256.Sum256(body[i*(32<<10) : (i+1)*(32<<10)])
		candidate.Artifacts = append(candidate.Artifacts, attachedworkeroutput.InlineArtifactV2{
			Name: strings.Repeat([]string{"<", ">"}[i], 255), MediaType: media,
			Size: 32 << 10, SHA256: hex.EncodeToString(digest[:]),
		})
	}
	verified, err := attachedworkeroutput.VerifyInlineV2(candidate, body)
	if err != nil {
		t.Fatal("actual normal candidate/body verifier rejected semantic maxima", err)
	}
	verified.Clear()
	id, digest := strings.Repeat("i", 160), strings.Repeat("a", 64)
	at := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	observation := attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}
	od, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	auth := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: domain.TenantID(id), OwnerUserID: domain.UserID(id), WorkerID: domain.AttachedWorkerID(id),
		ConnectionID: domain.AttachedWorkerConnectionID(id), EnrollmentGeneration: ^uint64(0), ConnectionGeneration: ^uint64(0),
		RunID: domain.RunID(id), AttemptID: domain.AttemptID(id), AttemptSequence: 1,
		LeaseID: domain.LeaseID(id), LeaseGeneration: ^uint64(0), FenceToken: domain.AttachedWorkerFenceToken(digest),
		LeaseExpiresAtUnixMicro: at.Add(time.Minute).UnixMicro(), ContextDigest: domain.AttachedWorkerContextDigest(digest),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(digest), PolicyDigest: domain.AttachedWorkerPolicyDigest(digest),
	}
	row := struct {
		PersistenceEncoding  string                                           `json:"persistence_encoding"`
		Version              uint32                                           `json:"version"`
		Stage                attachedworkeroutput.AttachedWorkerInlineStageV2 `json:"stage"`
		Binding              ports.AttachedWorkerSealedInputAuthorization     `json:"binding"`
		Nonce                domain.IdempotencyKey                            `json:"nonce"`
		CandidateFingerprint string                                           `json:"candidate_fingerprint"`
		Candidate            attachedworkeroutput.InlineCandidateV2           `json:"candidate"`
		Producer             attachedworkeroutput.InlineProducerBindingV2     `json:"producer"`
		Observation          attachedworkeroutput.ProcessObservationV1        `json:"observation"`
		ObservationDigest    string                                           `json:"observation_digest"`
		CredentialRequired   bool                                             `json:"credential_required"`
		BodyClaimID          string                                           `json:"body_claim_id"`
		CopyOwnerID          string                                           `json:"copy_owner_id"`
		CanonicalDigest      domain.AttachedWorkerTerminalEvidenceDigest      `json:"canonical_digest"`
		Materialization      ports.AttachedWorkerTerminalMaterialization      `json:"materialization"`
		CreatedAt            time.Time                                        `json:"created_at"`
		ExpiresAt            time.Time                                        `json:"expires_at"`
	}{
		PersistenceEncoding: "single-answer-v1", Version: 2, Stage: attachedworkeroutput.InlineBodyClaimedV2,
		Binding: auth, Nonce: domain.IdempotencyKey(id), CandidateFingerprint: digest, Candidate: candidate,
		Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: domain.SessionID(id), HarnessBindingDigest: domain.HarnessBindingDigestV2(digest),
			ReservationID: domain.QuotaReservationID(id), AcceptedAt: at, SchemaDigest: digest, FinalDigest: digest, AnswerDigest: digest},
		Observation: observation, ObservationDigest: od, BodyClaimID: digest,
		CreatedAt: at.Add(time.Second), ExpiresAt: at.Add(6 * time.Second),
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"display_text"`)) || len(raw) <= 192000 || len(raw) > (256<<10) {
		t.Fatalf("compositional single-answer normal row has wrong shape/size: %d", len(raw))
	}
	return raw
}

// Match NopBufferPool's exact allocation, but observe transport-owned releases.
// This is not the codec's private concatenation pool. Clearing at Put makes an
// accidental borrowed decode observable without inspecting private refcounts.
type codecReceivePool struct{ puts, releasedCapacity int }

func (*codecReceivePool) Get(size int) *[]byte { b := make([]byte, size); return &b }
func (p *codecReceivePool) Put(b *[]byte) {
	p.puts++
	p.releasedCapacity += cap(*b)
	clear(*b)
}

func TestYDBProtobufCodecOperationalInlineReceivePhases(t *testing.T) {
	row := codecNormalInlineRow(t)
	t.Cleanup(func() { clear(row) })
	for _, count := range []int{1, 3} {
		name := "single-fragment"
		if count == 3 {
			name = "three-fragments"
		}
		t.Run(name, func(t *testing.T) {
			// These are the operational Table RPC messages, not a bare query
			// result. No query stats are requested on the ordinary SQL path;
			// transaction ID is opaque server metadata, not answer content.
			result := &YdbTable.ExecuteQueryResult{
				ResultSets: []*Ydb.ResultSet{{Columns: []*Ydb.Column{{Name: "payload", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_JSON_DOCUMENT}}}},
					Rows: []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_TextValue{TextValue: string(row)}}}}}}},
				TxMeta: &YdbTable.TransactionMeta{Id: "synthetic-operational-transaction"},
			}
			wrapped, err := anypb.New(result)
			if err != nil {
				t.Fatal(err)
			}
			response := &YdbTable.ExecuteDataQueryResponse{Operation: &YdbOperations.Operation{Ready: true, Status: Ydb.StatusIds_SUCCESS, Result: wrapped}}
			wire, err := proto.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(wire)
			defer clear(wrapped.Value)
			pool := new(codecReceivePool)
			var fragments mem.BufferSlice
			released := false
			t.Cleanup(func() {
				if !released {
					fragments.Free()
				}
			})
			fragmentCapacity := 0
			for i := range count {
				start, end := i*len(wire)/count, (i+1)*len(wire)/count
				owned := pool.Get(end - start)
				copy(*owned, wire[start:end])
				fragmentCapacity += cap(*owned)
				fragments = append(fragments, mem.NewBuffer(owned, pool))
			}
			var received YdbTable.ExecuteDataQueryResponse
			if err := (exactCapacityProtobufCodec{}).Unmarshal(fragments, &received); err != nil || !proto.Equal(response, &received) {
				t.Fatal("private codec operational response mismatch", err)
			}
			if pool.puts != 0 || fragmentCapacity != len(wire) {
				t.Fatalf("codec retired references or fixture has unexpected transport capacity: puts=%d capacity=%d wire=%d", pool.puts, fragmentCapacity, len(wire))
			}
			for i, fragment := range fragments {
				start, end := i*len(wire)/count, (i+1)*len(wire)/count
				if !bytes.Equal(fragment.ReadOnlyData(), wire[start:end]) {
					t.Fatalf("codec mutated borrowed fragment %d", i)
				}
			}
			anyLength, anyCapacity := len(received.GetOperation().GetResult().GetValue()), cap(received.GetOperation().GetResult().GetValue())
			// Pinned grpc rpc_util.go recv defers data.Free around Unmarshal.
			// Its return occurs BEFORE SDK internal/table/session.go performs
			// Operation.Result.UnmarshalTo. Reproduce that ownership boundary;
			// do not claim the private codec owns transport references itself.
			fragments.Free()
			released = true
			fragments = nil
			if pool.puts != count || pool.releasedCapacity != fragmentCapacity {
				t.Fatalf("receive references retained after transport release: puts=%d want=%d capacity=%d want=%d", pool.puts, count, pool.releasedCapacity, fragmentCapacity)
			}
			var decoded YdbTable.ExecuteQueryResult
			if err := received.GetOperation().GetResult().UnmarshalTo(&decoded); err != nil || !proto.Equal(result, &decoded) {
				t.Fatal("SDK-shaped Any decode after receive-reference release", err)
			}
			cell := decoded.ResultSets[0].Rows[0].Items[0].GetTextValue()
			if cell != string(row) {
				t.Fatal("maximum escaped normal row changed after fragments were released/cleared")
			}
			// Nop concatenation is source-proven in exactCapacityProtobufCodec:
			// MaterializeToBuffer borrows one fragment; for >1 it allocates
			// exactly BufferSlice.Len() and defers Free before returning.
			// It is not directly instrumented and is not an observed heap peak.
			concatSourceBytes := 0
			if count > 1 {
				concatSourceBytes = len(wire)
			}
			t.Logf("codec phase: row=%d fragment count=%d owned capacities=%d concat source-derived capacity=%d decoded Any len/cap=%d/%d operational outer bytes=%d; fragment+concat+Any overlap only inside Unmarshal", len(row), count, fragmentCapacity, concatSourceBytes, anyLength, anyCapacity, len(wire)-anyLength)
			t.Logf("SDK Any phase: transport fragments released=%d/%d bytes, codec concat no longer owned; Any cap=%d decoded JsonDocument TextValue bytes=%d tx ID bytes=%d; no database/sql Scan or receipt decoder is running here", pool.puts, pool.releasedCapacity, anyCapacity, len(cell), len(decoded.GetTxMeta().GetId()))
			clear(received.Operation.Result.Value)
		})
	}
}
