package sessioningress_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// Observe material DURING the actual producer→store boundary, not through a
// pointer retained after producer cleanup. This is not SQL authority evidence.
type preparedIngressStore struct {
	*memoryCanonicalStore
	t        *testing.T
	blobs    *memoryBlobs
	prepared *sessionmaterial.PreparedV1
	clone    *sessionmaterial.PreparedV1
	final    domain.SessionEvent
	measures sessionmaterial.MeasuresV1
	fail     error
}

func (store *preparedIngressStore) CommitCanonicalUserEvent(ctx context.Context, request ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	store.t.Helper()
	store.prepared = request.PreparedMaterial
	user, run := request.UserID, request.RunID
	store.final = domain.SessionEvent{ID: request.EventID, TenantID: request.TenantID, SessionID: store.bindings[request.BindingID].SessionID,
		Sequence: 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &user, RunID: &run, IdempotencyKey: request.IdempotencyKey, Payload: request.Payload, CreatedAt: request.CommittedAt}
	body, measures, err := request.PreparedMaterial.CopyForFinalV1(store.final)
	defer clear(body)
	if err != nil || !bytes.Equal(body, store.blobs.objects[request.Payload.Key]) {
		store.t.Fatalf("actual ingress did not prepare its exact stored envelope: %v", err)
	}
	store.measures = measures
	store.clone, err = request.PreparedMaterial.Clone()
	if err != nil {
		store.t.Fatal(err)
	}
	store.t.Cleanup(store.clone.Release)
	if store.fail != nil {
		return ports.CanonicalUserEventResult{}, store.fail
	}
	return store.memoryCanonicalStore.CommitCanonicalUserEvent(ctx, request)
}

func TestCanonicalIngressActualBothProducersPrepareOrderedAttachmentsAndRelease(t *testing.T) {
	for _, path := range []string{"ingest", "bound"} {
		for _, outcome := range []string{"success", "commit error"} {
			t.Run(path+"/"+outcome, func(t *testing.T) {
				blobs := newMemoryBlobs()
				store := &preparedIngressStore{memoryCanonicalStore: newMemoryCanonicalStore(), t: t, blobs: blobs}
				if outcome == "commit error" {
					store.fail = errors.New("test canonical commit failure")
				}
				service, err := sessioningress.New(testIngressConfig("material"), store, blobs)
				if err != nil {
					t.Fatal(err)
				}
				at := time.Date(2026, 10, 11, 1, 2, 3, 0, time.UTC)
				actor := sessioningress.Actor{TenantID: "tenant-a", UserID: "user-a", Frontend: "web", ExternalConversationID: "material-conversation"}
				if path == "ingest" {
					_, err = service.Ingest(t.Context(), sessioningress.UserInput{Actor: actor, ExternalEventID: "material-event", ReceivedAt: at, Text: "actual private text", SubscriptionConnectionID: "subscription-a",
						Attachments: []sessioningress.Attachment{{Name: "one.txt", MediaType: "text/plain", Body: strings.NewReader("first")}, {Name: "two.txt", MediaType: "text/plain", Body: strings.NewReader("second")}}})
				} else {
					state, setupErr := service.EnsureSession(t.Context(), actor, at)
					if setupErr != nil {
						t.Fatal(setupErr)
					}
					input := sessioningress.BoundUserInput{Actor: actor, Binding: state.Binding, ExternalEventID: "material-event", ReceivedAt: at, Text: "actual private text", SubscriptionConnectionID: "subscription-a"}
					plan, setupErr := service.PlanBound(input)
					if setupErr != nil {
						t.Fatal(setupErr)
					}
					for index, text := range []string{"first", "second"} {
						name := []string{"one.txt", "two.txt"}[index]
						key, setupErr := plan.AttachmentObjectKey("prepared-upload", index, name)
						if setupErr != nil {
							t.Fatal(setupErr)
						}
						ref, setupErr := blobs.Put(t.Context(), actor.TenantID, key, strings.NewReader(text))
						if setupErr != nil {
							t.Fatal(setupErr)
						}
						input.Attachments = append(input.Attachments, sessioningress.StoredAttachment{Name: name, MediaType: "text/plain", Blob: ref})
					}
					_, err = service.IngestBound(t.Context(), input)
				}
				if (outcome == "success" && err != nil) || (outcome == "commit error" && !errors.Is(err, store.fail)) {
					t.Fatalf("producer outcome=%s err=%v", outcome, err)
				}
				if store.prepared == nil || store.measures.AttachmentCount != 2 || store.measures.ReferencedAttachmentBytes != 11 || store.measures.CodexCompatible {
					t.Fatalf("actual historical attachment measures: count=%d bytes=%d compatible=%v prepared=%v", store.measures.AttachmentCount, store.measures.ReferencedAttachmentBytes, store.measures.CodexCompatible, store.prepared != nil)
				}
				if _, live := store.prepared.OwnedBytesV1(); live {
					t.Fatal("producer retained live private preparation after return")
				}
				body, _, copyErr := store.clone.CopyForFinalV1(store.final)
				defer clear(body)
				if copyErr != nil || len(body) == 0 {
					t.Fatalf("explicit independent clone did not retain its lifetime: %v", copyErr)
				}
			})
		}
	}
}

type mismatchedIngressBlobs struct {
	*memoryBlobs
	mutate func(*domain.BlobRef)
}

func (blobs mismatchedIngressBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	ref, err := blobs.memoryBlobs.Put(ctx, tenant, key, body)
	if err == nil {
		blobs.mutate(&ref)
	}
	return ref, err
}

func TestCanonicalIngressPutMetadataMismatchCannotCommit(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.BlobRef)
	}{
		{"tenant", func(r *domain.BlobRef) { r.TenantID = "foreign-tenant" }},
		{"key", func(r *domain.BlobRef) { r.Key += "-wrong" }},
		{"hash", func(r *domain.BlobRef) { r.SHA256 = strings.Repeat("f", 64) }},
		{"size", func(r *domain.BlobRef) { r.Size++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMemoryCanonicalStore()
			service, err := sessioningress.New(testIngressConfig("mismatch"), store, mismatchedIngressBlobs{newMemoryBlobs(), test.mutate})
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Ingest(t.Context(), sessioningress.UserInput{Actor: sessioningress.Actor{TenantID: "tenant-a", UserID: "user-a", Frontend: "web", ExternalConversationID: "metadata-mismatch"}, ExternalEventID: "mismatch", ReceivedAt: time.Date(2026, 10, 11, 1, 2, 3, 0, time.UTC), Text: "private text", SubscriptionConnectionID: "subscription-a"})
			if !errors.Is(err, sessionmaterial.ErrStoredPayloadMismatch) || len(store.commits) != 0 {
				t.Fatalf("mismatched Put response committed: commits=%d err=%v", len(store.commits), err)
			}
		})
	}
}
