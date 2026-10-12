//go:build darwin || linux

package codexexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func replaceCanonicalMediaBody(t *testing.T, input *attachedworkerdaemon.VerifiedAttachedInputV1, index int, body []byte) {
	t.Helper()
	proof := input.CanonicalContext
	eventIndex, ordinal := index/2, index%2
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(proof.EventBodies[eventIndex], &envelope); err != nil {
		t.Fatal(err)
	}
	var refs []sessioncontext.AttachmentRef
	if err := json.Unmarshal(envelope["attachments"], &refs); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	refs[ordinal].Blob.Size = int64(len(body))
	refs[ordinal].Blob.SHA256 = hex.EncodeToString(digest[:])
	clear(proof.Attachments[index].Body)
	proof.Attachments[index].Body = body
	proof.Attachments[index].Blob = refs[ordinal].Blob
	envelope["attachments"], _ = json.Marshal(refs)
	clear(proof.EventBodies[eventIndex])
	proof.EventBodies[eventIndex], _ = json.Marshal(envelope)
	digest = sha256.Sum256(proof.EventBodies[eventIndex])
	proof.Input.Events[eventIndex].Payload.Size = int64(len(proof.EventBodies[eventIndex]))
	proof.Input.Events[eventIndex].Payload.SHA256 = hex.EncodeToString(digest[:])
	clear(input.Context)
	var err error
	input.Context, _, err = sessioncontext.ProjectCanonical(input.Job, proof, maxInstructionBytes)
	if err != nil {
		t.Fatal(err)
	}
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMediaMaximumTextAndInheritedWireDenial(t *testing.T) {
	input := canonicalMediaFixture(t, false)
	jsonBody := []byte(`{"n":90071992547409931234567890}`)
	jsonBody = append(jsonBody, bytes.Repeat([]byte(" "), mediaTextBytes-len(jsonBody))...)
	replaceCanonicalMediaBody(t, input, 0, jsonBody)
	replaceCanonicalMediaBody(t, input, 2, []byte(strings.Repeat("x", mediaTextBytes)))
	clone, err := input.Clone()
	if err != nil {
		t.Fatalf("admitted two text/JSON individual+aggregate maximum does not fit tighter complete copy: %v", err)
	}
	defer clone.Clear()
	media, err := prepareMediaInput(clone)
	if err != nil {
		t.Fatal(err)
	}
	defer media.clear()
	if len(media.sources) != 4 || len(media.sources[0].Body) != mediaTextBytes || len(media.sources[2].Body) != mediaTextBytes {
		t.Fatal("maximum mixed cohort silently lost material")
	}
	oversize := bytes.Clone(jsonBody)
	oversize = append(oversize, ' ')
	replaceCanonicalMediaBody(t, input, 0, oversize)
	if result, err := prepareMediaInput(input); err == nil {
		result.clear()
		t.Fatal("one-byte text excess admitted")
	}
	// A compressed original can fit the per-image profile while complete
	// repeated source/base64 framing exceeds the inherited 1MiB copy cap.
	wire := canonicalMediaFixture(t, false)
	original := wire.CanonicalContext.Attachments[1].Body
	padded := append(bytes.Clone(original[:len(original)-12]), pngChunk("tEXt", bytes.Repeat([]byte("x"), 786432))...)
	padded = append(padded, original[len(original)-12:]...)
	replaceCanonicalMediaBody(t, wire, 1, padded)
	if len(padded) > mediaImageBytes {
		t.Fatal("wire negative accidentally exceeds original image profile")
	}
	if clone, err := wire.Clone(); err == nil {
		clone.Clear()
		t.Fatal("raw-fitting image bypassed complete inherited JSON/base64 copy bound")
	}
}

func canonicalMediaFixture(t *testing.T, snapshot bool) *attachedworkerdaemon.VerifiedAttachedInputV1 {
	t.Helper()
	request, authority := validDriverRequest(t)
	input := preparedOwnerTypedInput(t, request, authority)
	job := input.Job
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 3}
	job.TriggerEventID = "media-event-3"
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: job.TenantID, SessionID: job.SessionID}}
	bodies := [][]byte{[]byte(` {"huge":90071992547409931234567890123} `), mediaImageFixture(t, "image/png", 3000, 1000), []byte("quoted task data\n"), mediaImageFixture(t, "image/jpeg", 24, 12)}
	names := []string{"same.json", "same.png", "same.json", "same.png"}
	types := []string{"application/json", "image/png", "text/plain", "image/jpeg"}
	var records []sessioncontext.EventPayload
	for sequence := 1; sequence <= 3; sequence++ {
		eventID := domain.SessionEventID(fmt.Sprintf("media-event-%d", sequence))
		event := domain.SessionEvent{ID: eventID, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: uint64(sequence), Kind: domain.SessionEventUserMessage, AuthorUserID: &job.CredentialOwnerUserID, RunID: &job.RunID, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("media-key-%d", sequence)), CreatedAt: driverNow}
		var attachments []sessioncontext.AttachmentRef
		if sequence <= 2 {
			for i := (sequence - 1) * 2; i < sequence*2; i++ {
				digest := sha256.Sum256(bodies[i])
				ref := sessioncontext.AttachmentRef{Name: names[i], MediaType: types[i], Blob: domain.BlobRef{TenantID: job.TenantID, Key: domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, eventID) + fmt.Sprintf("attachment-%d", i), Size: int64(len(bodies[i])), SHA256: hex.EncodeToString(digest[:])}}
				attachments = append(attachments, ref)
				qualified := ref
				qualified.Name = fmt.Sprintf("%020d-%02d-%s", sequence, len(attachments), ref.Name)
				proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: qualified, Body: bodies[i]})
			}
		}
		payload, err := json.Marshal(struct {
			Version     uint32                         `json:"version"`
			Text        string                         `json:"text"`
			Attachments []sessioncontext.AttachmentRef `json:"attachments,omitempty"`
		}{1, fmt.Sprintf("turn %d", sequence), attachments})
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(payload)
		event.Payload = domain.BlobRef{TenantID: job.TenantID, Key: domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, eventID) + "message.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:])}
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: payload})
	}
	if snapshot {
		compressed, history, err := sessioncontext.EncodeSnapshot(records[:2])
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(compressed)
		version := uint64(1)
		snapshot := domain.SessionSnapshot{ID: "media-snapshot", TenantID: job.TenantID, SessionID: job.SessionID, Version: version, ThroughSequence: 2, EventCount: 2, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), Payload: domain.BlobRef{TenantID: job.TenantID, Key: domain.SessionSnapshotObjectKey(job.TenantID, job.SessionID, version), Size: int64(len(compressed)), SHA256: hex.EncodeToString(digest[:])}, CreatedAt: driverNow}
		proof.Input.Snapshot = &snapshot
		proof.SnapshotBytes = compressed
		job.ContextWindow.AfterSequence = 2
		job.ContextWindow.SnapshotVersion = &version
		records = records[2:]
	}
	for _, record := range records {
		proof.Input.Events = append(proof.Input.Events, record.Event)
		proof.EventBodies = append(proof.EventBodies, record.Payload)
	}
	history, _, err := sessioncontext.ProjectCanonical(job, proof, maxInstructionBytes)
	if err != nil {
		t.Fatal(err)
	}
	input.CanonicalContext.Clear()
	clear(input.Context)
	input.Job = job
	input.CanonicalContext = proof
	input.Context = history
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func TestMediaCanonicalHistorySnapshotTailAndRepeatedNames(t *testing.T) {
	tail := canonicalMediaFixture(t, false)
	snapshot := canonicalMediaFixture(t, true)
	a, err := prepareMediaInput(tail)
	if err != nil {
		t.Fatal(err)
	}
	defer a.clear()
	b, err := prepareMediaInput(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer b.clear()
	if !bytes.Equal(a.prompt, b.prompt) || a.associationDigest != b.associationDigest || !reflect.DeepEqual(a.sources, b.sources) {
		t.Fatal("snapshot/tail changed original ordered association or prompt")
	}
	if len(a.sources) != 4 || a.sources[0].Name != a.sources[2].Name || a.sources[1].Name != a.sources[3].Name || a.sources[0].EventID == a.sources[2].EventID || a.sources[1].ImageOrdinal != 1 || a.sources[3].ImageOrdinal != 2 || a.sources[1].Width != 3000 {
		t.Fatalf("original associations=%+v", a.sources)
	}
	if !bytes.Contains(a.prompt, []byte("90071992547409931234567890123")) || bytes.Contains(a.prompt, []byte("tenants/")) {
		t.Fatal("JSON tokens changed or storage coordinate reached prompt")
	}
	for _, source := range a.sources {
		if !mediaBodyMatches(source) {
			t.Fatal("original bytes changed")
		}
	}
}

func TestMediaCanonicalAssociationDenials(t *testing.T) {
	for _, name := range []string{"missing", "extra", "swapped", "body", "ref", "unsupported", "count"} {
		t.Run(name, func(t *testing.T) {
			input := canonicalMediaFixture(t, false)
			proof := input.CanonicalContext
			switch name {
			case "missing":
				proof.Attachments = proof.Attachments[:3]
			case "extra":
				proof.Attachments = append(proof.Attachments, proof.Attachments[0])
			case "swapped":
				proof.Attachments[0], proof.Attachments[1] = proof.Attachments[1], proof.Attachments[0]
			case "body":
				proof.Attachments[1].Body[0] ^= 1
			case "ref":
				proof.Attachments[1].Name = "foreign.png"
			case "unsupported", "count":
				// Keep the original immutable proof coherent: modify the original
				// descriptor, event digest and projected history, not just a DTO.
				var envelope map[string]json.RawMessage
				if json.Unmarshal(proof.EventBodies[0], &envelope) != nil {
					t.Fatal("envelope")
				}
				var refs []sessioncontext.AttachmentRef
				if json.Unmarshal(envelope["attachments"], &refs) != nil {
					t.Fatal("refs")
				}
				if name == "unsupported" {
					refs[0].MediaType = "application/pdf"
				} else {
					refs[1].MediaType = "text/plain"
				}
				envelope["attachments"], _ = json.Marshal(refs)
				proof.EventBodies[0], _ = json.Marshal(envelope)
				digest := sha256.Sum256(proof.EventBodies[0])
				proof.Input.Events[0].Payload.Size = int64(len(proof.EventBodies[0]))
				proof.Input.Events[0].Payload.SHA256 = hex.EncodeToString(digest[:])
				proof.Attachments[0].MediaType = refs[0].MediaType
				proof.Attachments[1].MediaType = refs[1].MediaType
				clear(input.Context)
				var err error
				input.Context, _, err = sessioncontext.ProjectCanonical(input.Job, proof, maxInstructionBytes)
				if err != nil {
					t.Fatal(err)
				}
			}
			if result, err := prepareMediaInput(input); err == nil {
				result.clear()
				t.Fatal("association/format violation accepted")
			}
		})
	}
}

func TestMediaOriginalImageStagingAndInodeBoundCleanup(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replace-%t", replace), func(t *testing.T) {
			input := canonicalMediaFixture(t, false)
			media, err := prepareMediaInput(input)
			if err != nil {
				t.Fatal(err)
			}
			defer media.clear()
			stage, err := stagePreparedHistory(privatePreparedScratch(t), input.Context)
			if err != nil {
				t.Fatal(err)
			}
			defer stage.remove()
			if err = stage.stageImages(media); err != nil {
				t.Fatal(err)
			}
			paths := stage.imagePaths()
			if len(paths) != 2 {
				t.Fatalf("image paths=%v", paths)
			}
			for i, path := range paths {
				body, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(body, stage.images[i].source.Body) {
					t.Fatalf("original staged[%d] differs err=%v", i, err)
				}
				info, err := os.Lstat(path)
				if err != nil || info.Mode().Perm() != 0o400 || filepath.Base(path) == stage.images[i].source.Name {
					t.Fatalf("ordinal readonly stage[%d] info=%v err=%v", i, info, err)
				}
			}
			if replace {
				// Move the owned inode aside before replacement, so the filesystem
				// cannot recycle its inode and make this control nondeterministic.
				moved := paths[0] + "-original"
				if os.Rename(paths[0], moved) != nil {
					t.Fatal("rename owned inode")
				}
				if os.WriteFile(paths[0], []byte("replacement"), 0o400) != nil {
					t.Fatal("replacement")
				}
				if stage.verifyImages(media) == nil {
					t.Fatal("replaced source passed consumption check")
				}
				if stage.remove() == nil {
					t.Fatal("cleanup accepted replacement")
				}
				body, err := os.ReadFile(paths[0])
				if err != nil || string(body) != "replacement" {
					t.Fatal("cleanup removed substituted inode")
				}
				// t.TempDir owns both objects. Production must not unlink them.
			} else if err = stage.remove(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
