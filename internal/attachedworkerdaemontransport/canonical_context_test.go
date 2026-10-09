package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func canonicalMaterializerFixture(t *testing.T) (*BoundMaterializer, *sealedSourceFixture, MaterializationRequestV1) {
	t.Helper()
	materializer, source, request := sealedMaterializerFixture(t)
	materializer.maxBytes = 32 << 10
	job := &source.input.Job
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 2}
	job.TriggerEventID = "event-2"
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: job.TenantID, SessionID: job.SessionID}}
	owner := job.CredentialOwnerUserID
	for sequence := uint64(1); sequence <= 2; sequence++ {
		id := domain.SessionEventID(fmt.Sprintf("event-%d", sequence))
		body := []byte(fmt.Sprintf("file body %d", sequence))
		ref := sealedBlob(job.TenantID, "placeholder", body)
		ref.Key = domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id) + "file.txt"
		attachment := sessioncontext.AttachmentRef{Name: "file.txt", MediaType: "text/plain", Blob: ref}
		payload, err := json.Marshal(struct {
			Attachments []sessioncontext.AttachmentRef `json:"attachments"`
		}{Attachments: []sessioncontext.AttachmentRef{attachment}})
		if err != nil {
			t.Fatal(err)
		}
		payloadRef := sealedBlob(job.TenantID, "placeholder", payload)
		payloadRef.Key = domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id) + "payload.json"
		event := domain.SessionEvent{ID: id, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: sequence,
			Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, IdempotencyKey: domain.IdempotencyKey(id), Payload: payloadRef, CreatedAt: time.Unix(int64(sequence), 0).UTC()}
		proof.Input.Events = append(proof.Input.Events, event)
		proof.EventBodies = append(proof.EventBodies, payload)
		attachment.Name = fmt.Sprintf("%020d-01-file.txt", sequence)
		proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: attachment, Body: body})
	}
	history, _, err := sessioncontext.ProjectCanonical(*job, proof, uint64(materializer.maxBytes))
	if err != nil {
		t.Fatal(err)
	}
	source.input.Context, source.input.CanonicalContext = history, proof
	digest, err := domain.AttachedWorkerJobContextDigestV1(*job, source.input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
	return materializer, source, request
}

func TestCanonicalMaterializerVerifiesHistoryAndPreservesHistoricalFiles(t *testing.T) {
	materializer, source, request := canonicalMaterializerFixture(t)
	proof := source.input.CanonicalContext
	firstBody := proof.EventBodies[0]
	fileBody := proof.Attachments[0].Body
	result, err := materializer.Materialize(context.Background(), request)
	if err != nil {
		t.Fatalf("Materialize canonical history: %v", err)
	}
	defer clearBytes(result.Stdin)
	var envelope sealedEnvelopeV1
	if err := json.Unmarshal(result.Stdin, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Kind != "sessionless.attached-worker.canonical-input.v1" || len(envelope.ContextAttachments) != 2 || len(envelope.Artifacts) != 1 {
		t.Fatalf("canonical input envelope lost files: kind=%q files=%d artifacts=%d", envelope.Kind, len(envelope.ContextAttachments), len(envelope.Artifacts))
	}
	for index, file := range envelope.ContextAttachments {
		if file.Name != fmt.Sprintf("%020d-01-file.txt", index+1) || string(file.Body) != fmt.Sprintf("file body %d", index+1) {
			t.Errorf("historical file %d: name=%q body=%q", index, file.Name, file.Body)
		}
	}
	if !bytes.Equal(firstBody, make([]byte, len(firstBody))) || !bytes.Equal(fileBody, make([]byte, len(fileBody))) || len(proof.EventBodies) != 0 {
		t.Fatal("transferred canonical proof buffers not cleared")
	}
}

func TestCanonicalMaterializerRejectsProofTamperingAndCleansBuffers(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*SealedInputV1)
	}{
		{name: "history-drift", mutate: func(input *SealedInputV1) { input.Context[0] ^= 1 }},
		{name: "payload-drift", mutate: func(input *SealedInputV1) { input.CanonicalContext.EventBodies[0][0] ^= 1 }},
		{name: "attachment-drift", mutate: func(input *SealedInputV1) { input.CanonicalContext.Attachments[0].Body[0] ^= 1 }},
		{name: "attachment-drop", mutate: func(input *SealedInputV1) {
			input.CanonicalContext.Attachments = input.CanonicalContext.Attachments[:1]
		}},
		{name: "cross-session", mutate: func(input *SealedInputV1) { input.CanonicalContext.Input.SessionID = "other" }},
		{name: "cross-tenant", mutate: func(input *SealedInputV1) { input.CanonicalContext.Input.TenantID = "other" }},
		{name: "wrong-trigger", mutate: func(input *SealedInputV1) { input.CanonicalContext.Input.Events[1].ID = "wrong" }},
		{name: "noncontiguous", mutate: func(input *SealedInputV1) { input.CanonicalContext.Input.Events[1].Sequence = 3 }},
		{name: "truncated", mutate: func(input *SealedInputV1) {
			input.CanonicalContext.Input.Events = input.CanonicalContext.Input.Events[:1]
			input.CanonicalContext.EventBodies = input.CanonicalContext.EventBodies[:1]
		}},
		{name: "proof-missing", mutate: func(input *SealedInputV1) { input.CanonicalContext = nil }},
		{name: "proof-too-large", mutate: func(input *SealedInputV1) { input.CanonicalContext.EventBodies[0] = make([]byte, 33<<10) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			materializer, source, request := canonicalMaterializerFixture(t)
			test.mutate(&source.input)
			history := source.input.Context
			result, err := materializer.Materialize(context.Background(), request)
			if !errors.Is(err, ErrSealedInputInvalid) || len(result.Stdin) != 0 || !bytes.Equal(history, make([]byte, len(history))) {
				t.Fatalf("tampered %s released history: err=%v stdin=%d", test.name, err, len(result.Stdin))
			}
		})
	}
}

func TestCanonicalMaterializerEnforcesToolAndSerializedBudgets(t *testing.T) {
	for _, name := range []string{"tool-count", "tool-bytes", "serialized-input"} {
		t.Run(name, func(t *testing.T) {
			for _, reduced := range []bool{false, true} {
				t.Run(fmt.Sprintf("reduced-%t", reduced), func(t *testing.T) {
					materializer, source, request := canonicalMaterializerFixture(t)
					job, proof := &source.input.Job, source.input.CanonicalContext
					// Construct a valid tool history before changing any budget.
					run := job.RunID
					for index := range proof.Input.Events {
						proof.Input.Events[index].Kind = domain.SessionEventToolCall
						proof.Input.Events[index].RunID = &run
					}
					proof.Attachments = nil
					history, _, err := sessioncontext.ProjectCanonical(*job, proof, uint64(materializer.maxBytes))
					if err != nil {
						t.Fatalf("construct admitted tool projection: %v", err)
					}
					clearBytes(source.input.Context)
					source.input.Context = history
					if reduced {
						switch name {
						case "tool-count":
							job.Limits.MaxToolEvents = 1
						case "tool-bytes":
							job.Limits.MaxToolEventBytes = 1
						case "serialized-input":
							job.Limits.MaxInputBytes = 100
						}
					}
					digest, err := domain.AttachedWorkerJobContextDigestV1(*job, source.input.Manifest)
					if err != nil {
						t.Fatal(err)
					}
					request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
					result, err := materializer.Materialize(context.Background(), request)
					defer clearBytes(result.Stdin)
					if reduced {
						if !errors.Is(err, ErrSealedInputInvalid) {
							t.Fatalf("reduced %s did not reject valid tool projection: %v", name, err)
						}
					} else if err != nil || len(result.Stdin) == 0 {
						t.Fatalf("admitted %s tool projection failed positive baseline: %v", name, err)
					}
				})
			}
		})
	}
}

func TestCanonicalMaterializerAcceptsMoreThanArtifactCountOfHistoryRecords(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot-%t", snapshot), func(t *testing.T) {
			materializer, source, request := sealedMaterializerFixture(t)
			materializer.maxBytes = defaultMaxSealedInputBytes
			job := &source.input.Job
			job.Limits.MaxContextEvents = 512
			job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 65}
			job.TriggerEventID = "event-65"
			proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: job.TenantID, SessionID: job.SessionID}}
			owner := job.CredentialOwnerUserID
			var records []sessioncontext.EventPayload
			for sequence := uint64(1); sequence <= 65; sequence++ {
				id := domain.SessionEventID(fmt.Sprintf("event-%d", sequence))
				payload := []byte(fmt.Sprintf(`{"version":1,"text":"turn %d"}`, sequence))
				ref := sealedBlob(job.TenantID, "placeholder", payload)
				ref.Key = domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id) + "payload.json"
				event := domain.SessionEvent{ID: id, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: sequence,
					Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, IdempotencyKey: domain.IdempotencyKey(id), Payload: ref, CreatedAt: time.Unix(int64(sequence), 0).UTC()}
				proof.Input.Events = append(proof.Input.Events, event)
				proof.EventBodies = append(proof.EventBodies, payload)
				records = append(records, sessioncontext.EventPayload{Event: event, Payload: payload})
			}
			if snapshot {
				compressed, history, err := sessioncontext.EncodeSnapshot(records[:64])
				if err != nil {
					t.Fatal(err)
				}
				version := uint64(4)
				ref := sealedBlob(job.TenantID, "placeholder", compressed)
				ref.Key = domain.SessionSnapshotObjectKey(job.TenantID, job.SessionID, version)
				proof.Input.Snapshot = &domain.SessionSnapshot{ID: "snapshot-4", TenantID: job.TenantID, SessionID: job.SessionID,
					Version: version, ThroughSequence: 64, EventCount: 64, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), Payload: ref, CreatedAt: time.Unix(66, 0).UTC()}
				proof.SnapshotBytes = compressed
				proof.Input.Events = proof.Input.Events[64:]
				proof.EventBodies = proof.EventBodies[64:]
				job.ContextWindow.SnapshotVersion, job.ContextWindow.AfterSequence = &version, 64
			}
			history, _, err := sessioncontext.ProjectCanonical(*job, proof, uint64(materializer.maxBytes))
			if err != nil {
				t.Fatalf("prepare admitted 65-event history: %v", err)
			}
			clearBytes(source.input.Context)
			source.input.Context, source.input.CanonicalContext = history, proof
			digest, err := domain.AttachedWorkerJobContextDigestV1(*job, source.input.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
			result, err := materializer.Materialize(context.Background(), request)
			if err != nil {
				t.Fatalf("materialize 65-event history, snapshot=%t: %v", snapshot, err)
			}
			defer clearBytes(result.Stdin)
			var envelope sealedEnvelopeV1
			if err := json.Unmarshal(result.Stdin, &envelope); err != nil {
				t.Fatal(err)
			}
			decoded, err := sessioncontext.DecodeJSONL(envelope.Context, job.TenantID, job.SessionID)
			if err != nil || len(decoded) != 65 || decoded[64].Event.ID != "event-65" {
				t.Fatalf("65-event canonical boundary lost: events=%d err=%v", len(decoded), err)
			}
		})
	}
}
