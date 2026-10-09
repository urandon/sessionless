package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type canonicalJobFixture struct {
	*jobFixture
	input        domain.SessionContextInput
	contextCalls int
	request      ports.WorkerContextRequest
}

func (fixture *canonicalJobFixture) LoadWorkerContext(_ context.Context, request ports.WorkerContextRequest) (domain.SessionContextInput, error) {
	fixture.contextCalls++
	fixture.request = request
	return fixture.input, nil
}

func canonicalFixture(t *testing.T) (*Service, *authorizerFixture, *canonicalJobFixture, *blobFixture, attachedworkerdaemontransport.MaterializationRequestV1) {
	t.Helper()
	service, authorizer, jobs, blobs, request := fixtureInput(t)
	job := &jobs.state.Job
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 2}
	job.TriggerEventID = "event-2"
	canonical := &canonicalJobFixture{jobFixture: jobs, input: domain.SessionContextInput{TenantID: job.TenantID, SessionID: job.SessionID}}
	owner := job.CredentialOwnerUserID
	for sequence := uint64(1); sequence <= 2; sequence++ {
		id := domain.SessionEventID(fmt.Sprintf("event-%d", sequence))
		attachmentBody := []byte(fmt.Sprintf("image bytes %d", sequence))
		attachment := testBlob(job.TenantID, "placeholder", attachmentBody)
		attachment.Key = domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id) + "image.png"
		payload, err := json.Marshal(struct {
			Version     uint32                         `json:"version"`
			Text        string                         `json:"text"`
			Attachments []sessioncontext.AttachmentRef `json:"attachments"`
		}{Version: 1, Text: fmt.Sprintf("turn %d", sequence), Attachments: []sessioncontext.AttachmentRef{{Name: "image.png", MediaType: "image/png", Blob: attachment}}})
		if err != nil {
			t.Fatal(err)
		}
		ref := testBlob(job.TenantID, "placeholder", payload)
		ref.Key = domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id) + "payload.json"
		event := domain.SessionEvent{ID: id, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: sequence,
			Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, IdempotencyKey: domain.IdempotencyKey(id), Payload: ref, CreatedAt: time.Unix(int64(sequence), 0).UTC()}
		canonical.input.Events = append(canonical.input.Events, event)
		blobs.contents[ref.Key], blobs.contents[attachment.Key] = payload, attachmentBody
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(*job, jobs.state.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt.ContextDigest = decodeDigest(t, string(digest))
	service.jobs = canonical
	return service, authorizer, canonical, blobs, request
}

func TestCanonicalSealedInputPreservesHistoryAndHistoricalImages(t *testing.T) {
	service, authorizer, jobs, blobs, request := canonicalFixture(t)
	input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if err != nil {
		t.Fatalf("Load canonical window: %v", err)
	}
	defer clearResult(&input)
	if input.Job.ContextWindow == nil || input.Job.ContextWindow.ThroughSequence != 2 || input.CanonicalContext == nil ||
		len(input.CanonicalContext.Attachments) != 2 || authorizer.calls != 2 || jobs.contextCalls != 1 || blobs.calls != 5 {
		t.Fatalf("window/proof/read mismatch: window=%+v proof=%+v auth=%d context=%d blobs=%d", input.Job.ContextWindow, input.CanonicalContext, authorizer.calls, jobs.contextCalls, blobs.calls)
	}
	if jobs.request.TenantID != request.TenantID || jobs.request.SessionID != input.Job.SessionID ||
		jobs.request.TriggerEventID != input.Job.TriggerEventID || jobs.request.ThroughSequence != 2 || jobs.request.AtOrBeforeSnapshotVersion != nil {
		t.Fatalf("unbound canonical context request: %+v", jobs.request)
	}
	records, err := sessioncontext.DecodeJSONL(input.Context, input.Job.TenantID, input.Job.SessionID)
	if err != nil || len(records) != 2 || records[1].Event.ID != input.Job.TriggerEventID {
		t.Fatalf("history boundary: records=%d err=%v", len(records), err)
	}
	for index, attachment := range input.CanonicalContext.Attachments {
		wantName := fmt.Sprintf("%020d-01-image.png", index+1)
		if attachment.Name != wantName || !bytes.Equal(attachment.Body, []byte(fmt.Sprintf("image bytes %d", index+1))) {
			t.Errorf("historical image %d: name=%q body=%q", index, attachment.Name, attachment.Body)
		}
	}
}

func TestCanonicalSealedInputRejectsScopeBoundaryDriftAndLimits(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*canonicalJobFixture, *blobFixture)
		want   error
	}{
		{name: "cross-session", mutate: func(j *canonicalJobFixture, _ *blobFixture) { j.input.SessionID = "foreign" }, want: ErrInvalid},
		{name: "cross-tenant", mutate: func(j *canonicalJobFixture, _ *blobFixture) { j.input.TenantID = "foreign" }, want: ErrInvalid},
		{name: "truncated", mutate: func(j *canonicalJobFixture, _ *blobFixture) { j.input.Events = j.input.Events[:1] }, want: ErrInvalid},
		{name: "wrong-trigger", mutate: func(j *canonicalJobFixture, _ *blobFixture) { j.input.Events[1].ID = "other" }, want: ErrInvalid},
		{name: "tampered-payload", mutate: func(j *canonicalJobFixture, b *blobFixture) {
			b.contents[j.input.Events[0].Payload.Key] = []byte(`{"text":"tampered"}`)
		}, want: ErrInvalid},
		{name: "tampered-image", mutate: func(j *canonicalJobFixture, b *blobFixture) {
			b.contents[domain.SessionEventObjectPrefix(j.input.TenantID, j.input.SessionID, j.input.Events[0].ID)+"image.png"] = []byte("different")
		}, want: ErrInvalid},
		{name: "byte-limit", mutate: func(j *canonicalJobFixture, _ *blobFixture) { j.state.Job.Limits.MaxContextBytes = 1 }, want: ErrInvalid},
		{name: "event-limit", mutate: func(j *canonicalJobFixture, _ *blobFixture) { j.state.Job.Limits.MaxContextEvents = 1 }, want: ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, _, jobs, blobs, request := canonicalFixture(t)
			test.mutate(jobs, blobs)
			digest, err := domain.AttachedWorkerJobContextDigestV1(jobs.state.Job, jobs.state.InputManifest)
			if err != nil {
				t.Fatal(err)
			}
			request.Attempt.ContextDigest = decodeDigest(t, string(digest))
			input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
			if !errors.Is(err, test.want) || len(input.Context) != 0 || input.CanonicalContext != nil || len(input.Artifacts) != 0 {
				t.Fatalf("%s released context: err=%v input=%+v", test.name, err, input)
			}
		})
	}
}

func TestCanonicalSealedInputRevocationGatesContextAndReleasesProof(t *testing.T) {
	for _, denyAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("gate-%d", denyAt), func(t *testing.T) {
			service, authorizer, jobs, blobs, request := canonicalFixture(t)
			authorizer.denyAt = denyAt
			input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
			if !errors.Is(err, ErrUnauthorized) || input.CanonicalContext != nil || len(input.Context) != 0 {
				t.Fatalf("revocation leaked proof: err=%v input=%+v", err, input)
			}
			if denyAt == 1 && (jobs.contextCalls != 0 || blobs.calls != 0) {
				t.Fatalf("unauthorized reads: context=%d blobs=%d", jobs.contextCalls, blobs.calls)
			}
			if denyAt == 2 && (jobs.contextCalls != 1 || blobs.calls != 5 || authorizer.requests[1].ExpectedAttemptRevision != 7) {
				t.Fatalf("post-read revision check: context=%d blobs=%d requests=%+v", jobs.contextCalls, blobs.calls, authorizer.requests)
			}
		})
	}
}

func TestCanonicalSealedInputRequiresExactPinnedSnapshot(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt-%t", corrupt), func(t *testing.T) {
			service, _, jobs, blobs, request := canonicalFixture(t)
			first := jobs.input.Events[0]
			compressed, history, err := sessioncontext.EncodeSnapshot([]sessioncontext.EventPayload{{Event: first, Payload: blobs.contents[first.Payload.Key]}})
			if err != nil {
				t.Fatal(err)
			}
			version := uint64(3)
			ref := testBlob(first.TenantID, "placeholder", compressed)
			ref.Key = domain.SessionSnapshotObjectKey(first.TenantID, first.SessionID, version)
			jobs.input.Snapshot = &domain.SessionSnapshot{ID: "snapshot-3", TenantID: first.TenantID, SessionID: first.SessionID,
				Version: version, ThroughSequence: 1, EventCount: 1, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), Payload: ref, CreatedAt: time.Unix(5, 0).UTC()}
			jobs.input.Events = jobs.input.Events[1:]
			jobs.state.Job.ContextWindow = &domain.SessionContextWindow{SnapshotVersion: &version, AfterSequence: 1, ThroughSequence: 2}
			blobs.contents[ref.Key] = compressed
			if corrupt {
				jobs.input.Snapshot.Version = 2
			}
			digest, err := domain.AttachedWorkerJobContextDigestV1(jobs.state.Job, jobs.state.InputManifest)
			if err != nil {
				t.Fatal(err)
			}
			request.Attempt.ContextDigest = decodeDigest(t, string(digest))
			input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
			if corrupt {
				if !errors.Is(err, ErrInvalid) || blobs.calls != 0 {
					t.Fatalf("older snapshot used: err=%v reads=%d", err, blobs.calls)
				}
				return
			}
			if err != nil || input.CanonicalContext == nil || len(input.CanonicalContext.Attachments) != 2 || jobs.request.AtOrBeforeSnapshotVersion == nil || *jobs.request.AtOrBeforeSnapshotVersion != 3 {
				t.Fatalf("pinned snapshot failed: err=%v request=%+v", err, jobs.request)
			}
			defer clearResult(&input)
		})
	}
}

func TestCanonicalHTTPSourceToBoundMaterializerIncludesHistoricalImages(t *testing.T) {
	service, authorizer, _, blobs, request := canonicalFixture(t)
	server := httptest.NewTLSServer(Handler(service))
	t.Cleanup(server.Close)
	source, err := NewClientSource(server.URL+PathV1, server.Client(), []byte("test-connection-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	materializer, err := attachedworkerdaemontransport.NewBoundMaterializer(source, 32<<10, func() time.Time { return time.Unix(20, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	result, err := materializer.Materialize(context.Background(), request)
	if err != nil {
		t.Fatalf("HTTPS canonical input materialization: %v", err)
	}
	t.Cleanup(func() { clearBytes(result.Stdin) })
	var envelope struct {
		Kind               string `json:"kind"`
		Context            []byte `json:"context"`
		ContextAttachments []struct {
			Name      string `json:"name"`
			MediaType string `json:"media_type"`
			Body      []byte `json:"body"`
		} `json:"context_attachments"`
	}
	if err := json.Unmarshal(result.Stdin, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Kind != "sessionless.attached-worker.canonical-input.v1" || len(envelope.ContextAttachments) != 2 || authorizer.calls != 2 || blobs.calls != 5 || result.Credential != nil || len(result.ReadRoots) != 0 {
		t.Fatalf("HTTPS context lost binding or images: kind=%q images=%d auth=%d blobs=%d credential=%v roots=%d", envelope.Kind, len(envelope.ContextAttachments), authorizer.calls, blobs.calls, result.Credential != nil, len(result.ReadRoots))
	}
	for index, attachment := range envelope.ContextAttachments {
		if attachment.Name != fmt.Sprintf("%020d-01-image.png", index+1) || attachment.MediaType != "image/png" || string(attachment.Body) != fmt.Sprintf("image bytes %d", index+1) {
			t.Errorf("HTTPS image %d lost bytes or identity: %+v", index, attachment)
		}
	}
	records, err := sessioncontext.DecodeJSONL(envelope.Context, request.TenantID, "session-1")
	if err != nil || len(records) != 2 || records[1].Event.ID != "event-2" {
		t.Fatalf("HTTPS canonical history boundary: records=%d err=%v", len(records), err)
	}
}

func TestCanonicalSealedInputAccepts65HistoryRecordsWithinAdmittedLimits(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot-%t", snapshot), func(t *testing.T) {
			service, authorizer, baseJobs, blobs, request := fixtureInput(t)
			jobs := &canonicalJobFixture{jobFixture: baseJobs}
			job := &jobs.state.Job
			job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 65}
			job.TriggerEventID, job.Limits.MaxContextEvents = "event-65", 512
			jobs.input = domain.SessionContextInput{TenantID: job.TenantID, SessionID: job.SessionID}
			owner := job.CredentialOwnerUserID
			var records []sessioncontext.EventPayload
			for sequence := uint64(1); sequence <= 65; sequence++ {
				id := domain.SessionEventID(fmt.Sprintf("event-%d", sequence))
				payload := []byte(fmt.Sprintf(`{"version":1,"text":"turn %d"}`, sequence))
				ref := testBlob(job.TenantID, "placeholder", payload)
				ref.Key = domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, id) + "payload.json"
				event := domain.SessionEvent{ID: id, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: sequence,
					Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, IdempotencyKey: domain.IdempotencyKey(id), Payload: ref, CreatedAt: time.Unix(int64(sequence), 0).UTC()}
				jobs.input.Events = append(jobs.input.Events, event)
				blobs.contents[ref.Key] = payload
				records = append(records, sessioncontext.EventPayload{Event: event, Payload: payload})
			}
			wantBlobs := 66
			if snapshot {
				compressed, history, err := sessioncontext.EncodeSnapshot(records[:64])
				if err != nil {
					t.Fatal(err)
				}
				version := uint64(4)
				ref := testBlob(job.TenantID, "placeholder", compressed)
				ref.Key = domain.SessionSnapshotObjectKey(job.TenantID, job.SessionID, version)
				jobs.input.Snapshot = &domain.SessionSnapshot{ID: "snapshot-4", TenantID: job.TenantID, SessionID: job.SessionID,
					Version: version, ThroughSequence: 64, EventCount: 64, FormatVersion: 1, Compression: "zstd", UncompressedSize: uint64(len(history)), Payload: ref, CreatedAt: time.Unix(66, 0).UTC()}
				blobs.contents[ref.Key] = compressed
				jobs.input.Events = jobs.input.Events[64:]
				job.ContextWindow.SnapshotVersion, job.ContextWindow.AfterSequence = &version, 64
				wantBlobs = 3
			}
			service.jobs = jobs
			digest, err := domain.AttachedWorkerJobContextDigestV1(*job, jobs.state.InputManifest)
			if err != nil {
				t.Fatal(err)
			}
			request.Attempt.ContextDigest = decodeDigest(t, string(digest))
			input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
			if err != nil {
				t.Fatalf("Load admitted 65-event history, snapshot=%t: %v", snapshot, err)
			}
			defer clearResult(&input)
			decoded, err := sessioncontext.DecodeJSONL(input.Context, job.TenantID, job.SessionID)
			if err != nil || len(decoded) != 65 || decoded[64].Event.ID != "event-65" || jobs.request.MaxEvents != 512 || jobs.request.ThroughSequence != 65 || authorizer.calls != 2 || blobs.calls != wantBlobs {
				t.Fatalf("65-event history lost boundary/authority: events=%d err=%v request=%+v auth=%d blobs=%d want=%d", len(decoded), err, jobs.request, authorizer.calls, blobs.calls, wantBlobs)
			}
		})
	}
}
