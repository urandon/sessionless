//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func canonicalDataIdentity(input *attachedworkerdaemon.VerifiedAttachedInputV1) attachedworkerdaemon.InvocationIdentity {
	a := input.AcceptedAttempt
	return attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID,
		WorkerID: a.WorkerID, RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
}

func TestCanonicalMediaDataCompleteOriginalHistoryAndSnapshot(t *testing.T) {
	tail, snapshot := canonicalMediaFixture(t, false), canonicalMediaFixture(t, true)
	t.Cleanup(tail.Clear)
	t.Cleanup(snapshot.Clear)
	a, err := ProjectCanonicalMediaDataV1(context.Background(), canonicalDataIdentity(tail), tail, canonicalTestLedger(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Clear)
	b, err := ProjectCanonicalMediaDataV1(context.Background(), canonicalDataIdentity(snapshot), snapshot, canonicalTestLedger(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Clear)
	if !bytes.Equal(a.FramesV1(), b.FramesV1()) || a.AssociationDigestV1() != b.AssociationDigestV1() ||
		!reflect.DeepEqual(a.SourcesV1(), b.SourcesV1()) {
		t.Fatal("complete snapshot/tail representation differs")
	}
	sources := a.SourcesV1()
	if len(sources) != 4 || sources[0].Name != sources[2].Name || sources[1].Name != sources[3].Name ||
		sources[0].EventID == sources[2].EventID || sources[1].Width != 3000 || sources[1].Height != 1000 ||
		sources[1].ImageOrdinal != 1 || sources[3].ImageOrdinal != 2 {
		t.Fatal("complete source cohort/association missing")
	}
	for index, source := range sources {
		original := tail.CanonicalContext.Attachments[index]
		if source.Blob != original.Blob || !bytes.Equal(source.Body, original.Body) {
			t.Fatalf("source %d changed", index)
		}
	}
	if !bytes.Contains(a.FramesV1(), []byte("90071992547409931234567890123")) ||
		bytes.Contains(a.FramesV1(), []byte("tenants/")) || bytes.Contains(a.FramesV1(), []byte(MediaProfileVersionV1)) {
		t.Fatal("numeric tokens/storage separation/backend-neutral data label differs")
	}
	printed := fmt.Sprintf("%+v %#v %+v %#v", a, a, sources, sources)
	if strings.Contains(printed, "same.png") || strings.Contains(printed, "900719925474") ||
		strings.Contains(printed, "tenants/") {
		t.Fatal("private data reached generic formatting")
	}
	frames := a.FramesV1()
	a.Clear()
	a.Clear() // Idempotent owned cleanup; the original caller input is preserved.
	if a.FramesV1() != nil || a.SourcesV1() != nil || a.AssociationDigestV1() != [32]byte{} {
		t.Fatal("cleared data retained an accessor")
	}
	for _, value := range frames {
		if value != 0 {
			t.Fatal("owned frames not cleared")
		}
	}
	for _, source := range sources {
		for _, value := range source.Body {
			if value != 0 {
				t.Fatal("owned original body not cleared")
			}
		}
	}
	if len(tail.CanonicalContext.Attachments[1].Body) == 0 || tail.CanonicalContext.Attachments[1].Body[0] != 0x89 {
		t.Fatal("projection cleanup mutated caller's canonical proof")
	}
}

func TestCanonicalMediaDataCallerIdentityAndOriginalAssociationDenials(t *testing.T) {
	for _, name := range []string{"owner", "tenant", "run", "fence", "missing", "swapped", "corrupt-body", "wire"} {
		t.Run(name, func(t *testing.T) {
			input := canonicalMediaFixture(t, false)
			t.Cleanup(input.Clear)
			identity := canonicalDataIdentity(input)
			switch name {
			case "owner":
				identity.OwnerUserID = "foreign-owner"
			case "tenant":
				identity.TenantID = "foreign-tenant"
			case "run":
				identity.RunID = "foreign-run"
			case "fence":
				identity.FenceToken++
			case "missing":
				input.CanonicalContext.Attachments = input.CanonicalContext.Attachments[:3]
			case "swapped":
				input.CanonicalContext.Attachments[0], input.CanonicalContext.Attachments[1] = input.CanonicalContext.Attachments[1], input.CanonicalContext.Attachments[0]
			case "corrupt-body":
				input.CanonicalContext.Attachments[1].Body[0] ^= 1
			case "wire":
				original := input.CanonicalContext.Attachments[1].Body
				padded := append(bytes.Clone(original[:len(original)-12]), pngChunk("tEXt", bytes.Repeat([]byte("x"), 786432))...)
				padded = append(padded, original[len(original)-12:]...)
				replaceCanonicalMediaBody(t, input, 1, padded)
				if len(padded) > mediaImageBytes {
					t.Fatal("wire negative accidentally exceeds original image ceiling")
				}
			}
			if value, err := ProjectCanonicalMediaDataV1(context.Background(), identity, input, canonicalTestLedger(t)); err == nil || value != nil {
				value.Clear()
				t.Fatalf("%s admitted original invalid/caller-foreign source", name)
			}
			control := canonicalMediaFixture(t, false)
			t.Cleanup(control.Clear)
			value, err := ProjectCanonicalMediaDataV1(context.Background(), canonicalDataIdentity(control), control, canonicalTestLedger(t))
			if err != nil || value == nil {
				t.Fatalf("restored legitimate control failed: %v", err)
			}
			t.Cleanup(value.Clear)
		})
	}
}

func TestCanonicalMediaDataTypedOriginalOrderAndMaximumText(t *testing.T) {
	input := canonicalMediaFixture(t, false)
	t.Cleanup(input.Clear)
	// Original maximum cohort, not a smaller newest-image-only fixture.
	jsonBody := []byte(`{"n":90071992547409931234567890}`)
	jsonBody = append(jsonBody, bytes.Repeat([]byte(" "), mediaTextBytes-len(jsonBody))...)
	// The original replacement helper operates on tail events, so prepare the
	// coherent original full history first; snapshot equivalence is above.
	replaceCanonicalMediaBody(t, input, 0, jsonBody)
	replaceCanonicalMediaBody(t, input, 2, bytes.Repeat([]byte("x"), mediaTextBytes))
	data, err := ProjectCanonicalMediaDataV1(context.Background(), canonicalDataIdentity(input), input, canonicalTestLedger(t))
	if err != nil {
		t.Fatalf("original maximum data rejected: %v", err)
	}
	t.Cleanup(data.Clear)
	messages, err := data.OriginalMessagesV1()
	if err != nil || len(messages) != 1 || messages[0].Role != "user" {
		t.Fatalf("typed original maximum: %v", err)
	}
	encoded, err := json.Marshal(messages)
	if err != nil || len(encoded) > 1<<20 {
		t.Fatalf("typed input wire exceeded inherited cap: %v", err)
	}
	// Decode actual bytes using the actual SDK field spelling (not an alias).
	var wire []struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
			MIME string `json:"mimeType"`
			Data string `json:"data"`
		} `json:"content"`
	}
	if json.Unmarshal(encoded, &wire) != nil {
		t.Fatal("actual typed JSON malformed")
	}
	images := 0
	for index, block := range wire[0].Content {
		if block.Type != "image" {
			continue
		}
		images++
		if index == 0 || wire[0].Content[index-1].Type != "text" {
			t.Fatal("image lost adjacent source label")
		}
		var label struct {
			Source struct {
				EventID      string `json:"event_id"`
				ImageOrdinal int    `json:"image_ordinal"`
				SHA256       string `json:"sha256"`
			} `json:"source"`
		}
		if json.Unmarshal([]byte(wire[0].Content[index-1].Text), &label) != nil || label.Source.ImageOrdinal != images {
			t.Fatal("source/image ordinal mismatch")
		}
		original := input.CanonicalContext.Attachments[2*images-1]
		decoded, err := base64.StdEncoding.DecodeString(block.Data)
		if err != nil || !bytes.Equal(decoded, original.Body) || block.MIME != original.MediaType ||
			label.Source.SHA256 != original.Blob.SHA256 {
			t.Fatal("original typed image transformed or reassociated")
		}
	}
	if images != 2 || !bytes.Contains(encoded, []byte("90071992547409931234567890")) || bytes.Contains(encoded, []byte("tenants/")) {
		t.Fatal("maximum complete original typed cohort lost history/tokens or exposed storage capability")
	}
	limit := data.input.Job.Limits.MaxInputBytes
	data.input.Job.Limits.MaxInputBytes = uint64(len(encoded) - 1)
	if value, err := data.OriginalMessagesV1(); err == nil || value != nil {
		t.Fatal("one-byte typed wire overflow admitted")
	}
	data.input.Job.Limits.MaxInputBytes = limit
	if _, err := data.OriginalMessagesV1(); err != nil {
		t.Fatalf("restored genuine control failed: %v", err)
	}
}

func TestCanonicalMediaDataBorrowedMutationDeniesTypedProjection(t *testing.T) {
	for _, name := range []string{"frames", "text-original", "image-original"} {
		t.Run(name, func(t *testing.T) {
			input := canonicalMediaFixture(t, false)
			t.Cleanup(input.Clear)
			data, err := ProjectCanonicalMediaDataV1(context.Background(), canonicalDataIdentity(input), input, canonicalTestLedger(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(data.Clear)
			if _, err := data.OriginalMessagesV1(); err != nil {
				t.Fatalf("initial genuine control: %v", err)
			}
			switch name {
			case "frames":
				data.FramesV1()[0] ^= 1
			case "text-original":
				data.SourcesV1()[0].Body[0] ^= 1
			case "image-original":
				data.SourcesV1()[1].Body[0] ^= 1
			}
			if value, err := data.OriginalMessagesV1(); err == nil || value != nil {
				t.Fatal("borrowed mutable data silently became an original typed projection")
			}
		})
	}
}

func canonicalTestLedger(t *testing.T) *CanonicalMediaLedgerV1 {
	t.Helper()
	// Declared synthetic logical budgets; not an eligible native P/profile.
	ledger, err := NewCanonicalMediaLedgerV1(domain.AttachedPiSDKNativeLimitsV1{PreSendAllocationBytes: 128 << 20, ProcessMemoryBytes: 256 << 20, JSONNodes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}
