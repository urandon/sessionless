//go:build darwin || linux

package attachedworkerstack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// The canonical ingress envelope is the same immutable trigger body already
// projected into history, not an attachment or caller-selected prompt.
func addPreparedCanonicalEnvelope(t *testing.T, invocation *attachedworkerdaemon.Invocation, snapshot bool) {
	t.Helper()
	input := invocation.VerifiedInput
	event := &input.CanonicalContext.Input.Events[0]
	run := input.Job.RunID
	event.RunID = &run
	input.Manifest.Artifacts = []domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: event.Payload}}
	input.Artifacts = []attachedworkerdaemon.VerifiedAttachedArtifactV1{{Name: "message.json", Body: append([]byte(nil), input.CanonicalContext.EventBodies[0]...)}}
	if snapshot {
		compressed, history, err := sessioncontext.EncodeSnapshot([]sessioncontext.EventPayload{{Event: *event, Payload: input.Artifacts[0].Body}})
		if err != nil {
			t.Fatal(err)
		}
		defer clear(history)
		version := uint64(1)
		hash := sha256.Sum256(compressed)
		input.Job.ContextWindow = &domain.SessionContextWindow{SnapshotVersion: &version, AfterSequence: 1, ThroughSequence: 1}
		input.CanonicalContext = &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: input.Job.TenantID, SessionID: input.Job.SessionID,
			Snapshot: &domain.SessionSnapshot{ID: "prepared-envelope-snapshot", TenantID: input.Job.TenantID, SessionID: input.Job.SessionID,
				Version: version, ThroughSequence: 1, EventCount: 1, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), CreatedAt: input.Manifest.CreatedAt,
				Payload: domain.BlobRef{TenantID: input.Job.TenantID, Key: domain.SessionSnapshotObjectKey(input.Job.TenantID, input.Job.SessionID, version), Size: int64(len(compressed)), SHA256: hex.EncodeToString(hash[:])}}}, SnapshotBytes: compressed}
	}
	reprojectPreparedEnvelope(t, invocation)
}

func reprojectPreparedEnvelope(t *testing.T, invocation *attachedworkerdaemon.Invocation) {
	t.Helper()
	input := invocation.VerifiedInput
	history, _, err := sessioncontext.ProjectCanonical(input.Job, input.CanonicalContext, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	clear(input.Context)
	input.Context = history
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := input.ValidateFor(invocation.Identity); err != nil {
		t.Fatalf("coherent canonical envelope: %v", err)
	}
}

func TestNormalStackPreparedCodexCanonicalEnvelope(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "tail", true: "snapshot-only"}[snapshot], func(t *testing.T) {
			stack, invocation, owner, launcher := newPreparedStackFixture(t)
			addPreparedCanonicalEnvelope(t, &invocation, snapshot)
			result, err := stack.Run(context.Background(), invocation)
			if !result.Succeeded(err) {
				t.Fatalf("mandatory canonical envelope denied: %v", err)
			}
			candidate, err := stack.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded)
			if err != nil || candidate.Summary != "normal prepared answer" {
				t.Fatalf("original finalized candidate: %v", err)
			}
			if !reflect.DeepEqual(owner.order, []string{"issue", "materialize", "writeback", "release"}) || launcher.calls != 1 {
				t.Fatal("not the sole original lifecycle")
			}
		})
	}
}

func TestNormalStackPreparedCodexCanonicalEnvelopeDeniesBeforeIssue(t *testing.T) {
	for _, name := range []string{"name", "media", "ref", "body", "attachment", "extra-artifact", "missing-artifact", "foreign-author", "foreign-run", "missing-run", "kind", "trigger", "window", "extra-record", "foreign-session", "foreign-tenant", "oversized"} {
		t.Run(name, func(t *testing.T) {
			stack, invocation, owner, launcher := newPreparedStackFixture(t)
			addPreparedCanonicalEnvelope(t, &invocation, false)
			input := invocation.VerifiedInput
			coherent := true
			switch name {
			case "name":
				input.Artifacts[0].Name, input.Manifest.Artifacts[0].Name = "attachment.json", "attachment.json"
			case "media":
				input.Manifest.Artifacts[0].MediaType = "image/png"
			case "ref":
				input.Manifest.Artifacts[0].Blob.Key += ".other"
			case "body":
				input.Artifacts[0].Body = []byte(`{"version":1,"text":"different original body"}`)
				hash := sha256.Sum256(input.Artifacts[0].Body)
				input.Manifest.Artifacts[0].Blob.Size = int64(len(input.Artifacts[0].Body))
				input.Manifest.Artifacts[0].Blob.SHA256 = hex.EncodeToString(hash[:])
			case "attachment":
				body := []byte("bounded fixture image")
				hash := sha256.Sum256(body)
				ref := sessioncontext.AttachmentRef{Name: "image.png", MediaType: "image/png", Blob: domain.BlobRef{TenantID: input.Job.TenantID,
					Key: domain.SessionEventObjectPrefix(input.Job.TenantID, input.Job.SessionID, input.Job.TriggerEventID) + "image.png", Size: int64(len(body)), SHA256: hex.EncodeToString(hash[:])}}
				payload, err := json.Marshal(struct {
					Version     int                            `json:"version"`
					Text        string                         `json:"text"`
					Attachments []sessioncontext.AttachmentRef `json:"attachments"`
				}{Version: 1, Text: "bounded image input", Attachments: []sessioncontext.AttachmentRef{ref}})
				if err != nil {
					t.Fatal(err)
				}
				hash = sha256.Sum256(payload)
				input.CanonicalContext.Input.Events[0].Payload.Size = int64(len(payload))
				input.CanonicalContext.Input.Events[0].Payload.SHA256 = hex.EncodeToString(hash[:])
				input.CanonicalContext.EventBodies[0] = payload
				input.Manifest.Artifacts[0].Blob = input.CanonicalContext.Input.Events[0].Payload
				input.Artifacts[0].Body = append([]byte(nil), payload...)
				ref.Name = "00000000000000000001-01-image.png"
				input.CanonicalContext.Attachments = []sessioncontext.AttachmentPayload{{AttachmentRef: ref, Body: body}}
			case "extra-artifact":
				input.Manifest.Artifacts = append(input.Manifest.Artifacts, domain.Artifact{Name: "extra.json", MediaType: "application/json", Blob: input.Manifest.Artifacts[0].Blob})
				input.Artifacts = append(input.Artifacts, attachedworkerdaemon.VerifiedAttachedArtifactV1{Name: "extra.json", Body: append([]byte(nil), input.Artifacts[0].Body...)})
			case "missing-artifact":
				input.Artifacts = nil
				coherent = false
			case "foreign-author":
				user := domain.UserID("foreign-user")
				input.CanonicalContext.Input.Events[0].AuthorUserID = &user
			case "foreign-run":
				run := domain.RunID("foreign-run")
				input.CanonicalContext.Input.Events[0].RunID = &run
			case "missing-run":
				input.CanonicalContext.Input.Events[0].RunID = nil
			case "kind":
				input.CanonicalContext.Input.Events[0].Kind = domain.SessionEventAssistantMessage
			case "trigger":
				input.Job.TriggerEventID = "foreign-trigger"
				coherent = false
			case "window":
				input.Job.ContextWindow.ThroughSequence++
				coherent = false
			case "extra-record":
				input.Context = append(input.Context, input.Context...)
				coherent = false
			case "foreign-session":
				input.CanonicalContext.Input.Events[0].SessionID = "foreign-session"
				coherent = false
			case "foreign-tenant":
				input.CanonicalContext.Input.Events[0].TenantID = "foreign-tenant"
				coherent = false
			case "oversized":
				input.Artifacts[0].Body = make([]byte, (1<<20)+1)
				coherent = false
			}
			if coherent {
				reprojectPreparedEnvelope(t, &invocation)
			} else if input.ValidateFor(invocation.Identity) == nil {
				t.Fatal("invalid original proof unexpectedly verified")
			}
			if _, err := stack.Run(context.Background(), invocation); err == nil {
				t.Fatal("mismatched envelope accepted")
			}
			if len(owner.order) != 0 || launcher.calls != 0 {
				t.Fatal("envelope denial followed physical effects")
			}
		})
	}
}
