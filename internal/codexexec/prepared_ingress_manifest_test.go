//go:build darwin || linux

package codexexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// This is coherent immutable input DATA only. It does not create a Session,
// issue credentials, admit work, run a process or access a state store.
func preparedIngressManifestFixture(t *testing.T, older, current int) *attachedworkerdaemon.VerifiedAttachedInputV1 {
	t.Helper()
	request, authority := validDriverRequest(t)
	input := preparedOwnerTypedInput(t, request, authority)
	input.CanonicalContext.Clear()
	clear(input.Context)
	input.Job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 2}
	input.Job.TriggerEventID = "manifest-event-2"
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: input.Job.TenantID, SessionID: input.Job.SessionID}}
	bodies := [][]byte{[]byte("original text\n"), []byte(` {"n":9007199254740993123456789} `), mediaImageFixture(t, "image/png", 1, 1), mediaImageFixture(t, "image/jpeg", 1, 1)}
	names := []string{"request.txt", "request.json", "request.png", "request.jpg"}
	media := []string{"text/plain", "application/json", "image/png", "image/jpeg"}
	for index, count := range []int{older, current} {
		sequence := uint64(index + 1)
		eventID := domain.SessionEventID(fmt.Sprintf("manifest-event-%d", sequence))
		event := domain.SessionEvent{ID: eventID, TenantID: input.Job.TenantID, SessionID: input.Job.SessionID, Sequence: sequence,
			Kind: domain.SessionEventUserMessage, AuthorUserID: &input.Job.CredentialOwnerUserID, RunID: &input.Job.RunID,
			IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("manifest-key-%d", sequence)), CreatedAt: driverNow}
		var descriptors []sessioncontext.AttachmentRef
		var uploads []domain.Artifact
		var uploaded []attachedworkerdaemon.VerifiedAttachedArtifactV1
		for ordinal := range count {
			bodyIndex := ordinal
			if older == 2 && current == 2 && index == 1 {
				bodyIndex += 2
			}
			body := bytes.Clone(bodies[bodyIndex])
			digest := sha256.Sum256(body)
			ref := sessioncontext.AttachmentRef{Name: names[bodyIndex], MediaType: media[bodyIndex], Blob: domain.BlobRef{TenantID: event.TenantID,
				Key:  domain.SessionEventObjectPrefix(event.TenantID, event.SessionID, event.ID) + fmt.Sprintf("attachments/%02d-%s", ordinal+1, names[bodyIndex]),
				Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}}
			descriptors = append(descriptors, ref)
			qualified := ref
			qualified.Name = fmt.Sprintf("%020d-%02d-%s", sequence, ordinal+1, ref.Name)
			proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: qualified, Body: body})
			name := fmt.Sprintf("attachment-%02d-%s", ordinal+1, ref.Name)
			uploads = append(uploads, domain.Artifact{Name: name, MediaType: ref.MediaType, Blob: ref.Blob})
			uploaded = append(uploaded, attachedworkerdaemon.VerifiedAttachedArtifactV1{Name: name, Body: bytes.Clone(body)})
		}
		payload, err := json.Marshal(struct {
			Version     uint32                         `json:"version"`
			Text        string                         `json:"text"`
			Attachments []sessioncontext.AttachmentRef `json:"attachments"`
		}{1, "use the original files", descriptors})
		if err != nil {
			t.Fatal("encode fixed canonical user envelope", err)
		}
		digest := sha256.Sum256(payload)
		event.Payload = domain.BlobRef{TenantID: event.TenantID, Key: domain.SessionEventObjectPrefix(event.TenantID, event.SessionID, event.ID) + "message.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:])}
		proof.Input.Events = append(proof.Input.Events, event)
		proof.EventBodies = append(proof.EventBodies, payload)
		if index == 1 {
			input.Manifest.Artifacts = append([]domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: event.Payload}}, uploads...)
			input.Artifacts = append([]attachedworkerdaemon.VerifiedAttachedArtifactV1{{Name: "message.json", Body: bytes.Clone(payload)}}, uploaded...)
		}
	}
	input.CanonicalContext = proof
	var err error
	input.Context, _, err = sessioncontext.ProjectCanonical(input.Job, proof, maxInstructionBytes)
	if err != nil {
		t.Fatal("project original canonical input", err)
	}
	preparedIngressManifestDigest(t, input)
	if err := input.ValidateFor(preparedIngressManifestIdentity(input)); err != nil {
		t.Fatal("fixed input must satisfy the existing verified-input contract", err)
	}
	return input
}

func preparedIngressManifestIdentity(input *attachedworkerdaemon.VerifiedAttachedInputV1) attachedworkerdaemon.InvocationIdentity {
	a := input.AcceptedAttempt
	return attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
		RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
}

func preparedIngressManifestDigest(t *testing.T, input *attachedworkerdaemon.VerifiedAttachedInputV1) {
	t.Helper()
	var err error
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal("bind exact manifest to original accepted input", err)
	}
}

func TestPreparedIngressManifestOriginalAttachments(t *testing.T) {
	for _, tc := range []struct {
		name           string
		older, current int
	}{
		{name: "normal five-entry manifest", current: 4},
		{name: "current subset after older history", older: 2, current: 2},
		{name: "text successor retains four older attachments", older: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := preparedIngressManifestFixture(t, tc.older, tc.current)
			if !preparedIngressEnvelope(input) {
				t.Fatalf("exact current-trigger manifest rejected: manifest=%d current=%d history=%d", len(input.Manifest.Artifacts), tc.current, len(input.CanonicalContext.Attachments))
			}
			media, err := prepareMediaInput(input)
			if err != nil {
				t.Fatal("original bounded canonical media rejected", err)
			}
			defer media.clear()
			if len(media.sources) != tc.older+tc.current {
				t.Errorf("historical media stripped: got=%d want=%d", len(media.sources), tc.older+tc.current)
			}
		})
	}
}

func TestPreparedIngressManifestDeniesUnboundArtifacts(t *testing.T) {
	for _, fault := range []string{"missing", "extra", "duplicate", "foreign blob", "name", "media", "body", "proof body", "message body", "message first", "older as current"} {
		t.Run(fault, func(t *testing.T) {
			input := preparedIngressManifestFixture(t, 2, 2)
			switch fault {
			case "missing":
				input.Manifest.Artifacts = input.Manifest.Artifacts[:2]
				input.Artifacts = input.Artifacts[:2]
			case "extra", "duplicate":
				ref, body := input.Manifest.Artifacts[1], input.Artifacts[1]
				if fault == "extra" {
					ref.Name, body.Name = "unrequested.bin", "unrequested.bin"
				}
				input.Manifest.Artifacts = append(input.Manifest.Artifacts, ref)
				input.Artifacts = append(input.Artifacts, body)
			case "foreign blob":
				input.Manifest.Artifacts[1].Blob.Key = "tenants/" + string(input.Job.TenantID) + "/foreign-object"
			case "name":
				input.Manifest.Artifacts[1].Name, input.Artifacts[1].Name = "attachment-01-foreign.png", "attachment-01-foreign.png"
			case "media":
				input.Manifest.Artifacts[1].MediaType = "text/plain"
			case "body":
				input.Artifacts[1].Body[0] ^= 1
			case "proof body":
				input.CanonicalContext.Attachments[2].Body[0] ^= 1
			case "message body":
				input.Artifacts[0].Body[0] ^= 1
			case "message first":
				input.Artifacts[0], input.Artifacts[1] = input.Artifacts[1], input.Artifacts[0]
				input.Manifest.Artifacts[0], input.Manifest.Artifacts[1] = input.Manifest.Artifacts[1], input.Manifest.Artifacts[0]
			case "older as current":
				input.Manifest.Artifacts[1].Blob = input.CanonicalContext.Attachments[0].Blob
				input.Artifacts[1].Body = bytes.Clone(input.CanonicalContext.Attachments[0].Body)
			}
			if fault != "duplicate" { // The canonical job digest already rejects duplicate artifact names.
				preparedIngressManifestDigest(t, input)
			}
			if preparedIngressEnvelope(input) {
				t.Fatalf("unbound current-trigger material accepted: fault=%s", fault)
			}
		})
	}
}
