//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
)

// This strict peer captures only the real canonical ingress commit request
// and the final event bytes released through its prepared-material contract.
type preparedIngressServiceStore struct {
	blobs   *preparedIngressServiceBlobs
	session domain.Session
	binding domain.FrontendBinding
	commits []preparedIngressServiceCommit
}

type preparedIngressServiceCommit struct {
	request ports.CanonicalUserEventCommit
	event   domain.SessionEvent
	body    []byte
}

func (store *preparedIngressServiceStore) EnsureFrontendSession(_ context.Context, request ports.FrontendSessionRequest) (ports.FrontendSessionState, error) {
	if request.Validate() != nil {
		return ports.FrontendSessionState{}, errors.New("invalid fixture session request")
	}
	if store.session.ID != "" {
		if store.binding.ID != request.BindingID || store.session.ID != request.SessionID || store.binding.ExternalConversationID != request.ExternalConversationID {
			return ports.FrontendSessionState{}, errors.New("fixture session scope changed")
		}
		return ports.FrontendSessionState{Session: store.session, Binding: store.binding}, nil
	}
	store.session = domain.Session{ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
		Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At}
	store.binding = domain.FrontendBinding{ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
		ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
		Revision: 1, CreatedAt: request.At, UpdatedAt: request.At}
	return ports.FrontendSessionState{Session: store.session, Binding: store.binding}, nil
}

func (store *preparedIngressServiceStore) CreateAndSwitchFrontendSession(context.Context, ports.CanonicalSessionSwitchRequest) (ports.FrontendSessionState, error) {
	return ports.FrontendSessionState{}, errors.New("unexpected session switch in ingress fixture")
}

func (store *preparedIngressServiceStore) LookupCanonicalUserEvent(_ context.Context, request ports.CanonicalUserEventLookup) (ports.CanonicalUserEventLookupResult, error) {
	if request.TenantID != store.session.TenantID || request.BindingID != store.binding.ID ||
		request.ExternalConversationID != store.binding.ExternalConversationID || request.UserID != store.session.CreatedBy ||
		request.EventID == "" || request.RunID == "" || request.IdempotencyKey == "" {
		return ports.CanonicalUserEventLookupResult{}, errors.New("invalid fixture event lookup")
	}
	return ports.CanonicalUserEventLookupResult{}, nil
}

func (store *preparedIngressServiceStore) CommitCanonicalUserEvent(ctx context.Context, request ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	if ctx.Err() != nil || request.PreparedMaterial == nil || request.TenantID != store.session.TenantID ||
		request.SessionID != store.session.ID || request.BindingID != store.binding.ID ||
		request.ExpectedBindingRevision != store.binding.Revision || request.UserID != store.session.CreatedBy ||
		request.HarnessBinding.Resource.ResourceID != "subscription-a" || request.RunID == "" || request.AttemptID == "" {
		return ports.CanonicalUserEventResult{}, errors.New("fixture commit request scope mismatch")
	}
	sequence := uint64(len(store.commits) + 1)
	author, runID := request.UserID, request.RunID
	event := domain.SessionEvent{ID: request.EventID, TenantID: request.TenantID, SessionID: request.SessionID,
		Sequence: sequence, Kind: domain.SessionEventUserMessage, AuthorUserID: &author, RunID: &runID,
		IdempotencyKey: request.IdempotencyKey, CreatedAt: request.CommittedAt, Payload: request.Payload}
	payload, _, err := request.PreparedMaterial.CopyForFinalV1(event)
	if err != nil {
		return ports.CanonicalUserEventResult{}, errors.New("fixture prepared canonical event rejected")
	}
	defer clear(payload)
	reader, err := store.blobs.Open(ctx, request.TenantID, request.Payload)
	if err != nil {
		return ports.CanonicalUserEventResult{}, errors.New("fixture canonical event object unavailable")
	}
	stored, readErr := io.ReadAll(io.LimitReader(reader, 1<<20+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(stored) > 1<<20 || !bytes.Equal(stored, payload) {
		clear(stored)
		return ports.CanonicalUserEventResult{}, errors.New("fixture canonical event bytes differ from committed object")
	}
	copyRequest := request
	copyRequest.Artifacts = append([]domain.Artifact(nil), request.Artifacts...)
	store.commits = append(store.commits, preparedIngressServiceCommit{request: copyRequest, event: event, body: stored})
	return ports.CanonicalUserEventResult{SessionID: request.SessionID, EventID: request.EventID, Sequence: sequence,
		RunID: request.RunID, Created: true}, nil
}

type preparedIngressServiceBlobs struct {
	objects map[string][]byte
	bytes   int
}

func (blobs *preparedIngressServiceBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, source io.Reader) (domain.BlobRef, error) {
	if ctx.Err() != nil || tenant.Validate() != nil || !strings.HasPrefix(key, "tenants/"+string(tenant)+"/") {
		return domain.BlobRef{}, errors.New("fixture immutable object scope denied")
	}
	raw, err := io.ReadAll(io.LimitReader(source, 1<<20+1))
	if err != nil || ctx.Err() != nil || len(raw) > 1<<20 || len(blobs.objects) >= 32 || blobs.bytes+len(raw) > 4<<20 {
		clear(raw)
		return domain.BlobRef{}, errors.New("fixture immutable object bound exceeded")
	}
	if prior, found := blobs.objects[key]; found {
		if !bytes.Equal(prior, raw) {
			clear(raw)
			return domain.BlobRef{}, errors.New("fixture immutable replacement denied")
		}
		clear(raw)
		return preparedIngressBlobRef(tenant, key, prior), nil
	}
	blobs.objects[key] = bytes.Clone(raw)
	blobs.bytes += len(raw)
	ref := preparedIngressBlobRef(tenant, key, raw)
	clear(raw)
	return ref, nil
}

func (blobs *preparedIngressServiceBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx.Err() != nil || tenant != ref.TenantID || ref.Validate() != nil {
		return nil, errors.New("fixture immutable object read denied")
	}
	raw, found := blobs.objects[ref.Key]
	if !found || preparedIngressBlobRef(tenant, ref.Key, raw) != ref {
		return nil, errors.New("fixture immutable object absent or changed")
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(raw))), nil
}

func (blobs *preparedIngressServiceBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || tenant != ref.TenantID || ref.Validate() != nil {
		return errors.New("fixture immutable object deletion denied")
	}
	raw, found := blobs.objects[ref.Key]
	if !found || preparedIngressBlobRef(tenant, ref.Key, raw) != ref {
		return errors.New("fixture immutable object delete mismatch")
	}
	clear(raw)
	blobs.bytes -= len(raw)
	delete(blobs.objects, ref.Key)
	return nil
}

func preparedIngressBlobRef(tenant domain.TenantID, key string, body []byte) domain.BlobRef {
	digest := sha256.Sum256(body)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
}

type preparedIngressServiceBinder struct {
	placement domain.ExecutionPlacementV2
	binding   domain.HarnessBindingV2
}

func (binder *preparedIngressServiceBinder) BindHarness(ctx context.Context, request ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	if ctx.Err() != nil || request.TenantID != binder.binding.TenantID || request.OwnerUserID != binder.binding.OwnerUserID ||
		request.SubscriptionConnectionID != domain.SubscriptionConnectionID(binder.binding.Resource.ResourceID) {
		return ports.ExecutionAuthorityV2{}, errors.New("fixture binder scope mismatch")
	}
	binding := binder.binding.Clone()
	binding.RunID, binding.AttemptID = request.RunID, request.AttemptID
	binding.Resource.ResourceID = string(request.SubscriptionConnectionID)
	binding.EffectivePolicyDigest = string(binder.placement.PolicyDigest)
	return ports.ExecutionAuthorityV2{ExecutionPlacementV2: binder.placement, HarnessBinding: binding}, nil
}

func TestPreparedIngressEnvelopeFromActualCanonicalIngress(t *testing.T) {
	request, authority := validDriverRequest(t)
	binding := request.HarnessBinding.Clone()
	placement := request.ExecutionPlacementV2
	binder := &preparedIngressServiceBinder{placement: placement, binding: binding}
	blobs := &preparedIngressServiceBlobs{objects: make(map[string][]byte)}
	store := &preparedIngressServiceStore{blobs: blobs}
	service, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{0x5a}, 32), HarnessBinder: binder}, store, blobs)
	if err != nil {
		t.Fatal("construct actual canonical ingress service")
	}
	ctx := context.Background()
	at := driverNow
	actor := sessioningress.Actor{TenantID: request.TenantID, UserID: request.OwnerUserID,
		Frontend: domain.FrontendWeb, ExternalConversationID: "actual-ingress-contract"}
	var pngBody, jpegBody bytes.Buffer
	if err := png.Encode(&pngBody, image.NewNRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		t.Fatal("encode fixture PNG")
	}
	if err := jpeg.Encode(&jpegBody, image.NewNRGBA(image.Rect(0, 0, 1, 1)), &jpeg.Options{Quality: 80}); err != nil {
		t.Fatal("encode fixture JPEG")
	}
	inputs := []sessioningress.UserInput{
		{Actor: actor, ExternalEventID: "generation-1", ReceivedAt: at, Text: "Return two files",
			SubscriptionConnectionID: "subscription-a", Attachments: []sessioningress.Attachment{
				{Name: "request.txt", MediaType: "text/plain", Body: strings.NewReader("original bounded request")},
				{Name: "request.json", MediaType: "application/json", Body: strings.NewReader(` { "n": 9007199254740993123456789 } `)},
				{Name: "request.png", MediaType: "image/png", Body: bytes.NewReader(pngBody.Bytes())},
				{Name: "request.jpg", MediaType: "image/jpeg", Body: bytes.NewReader(jpegBody.Bytes())},
			}},
		{Actor: actor, ExternalEventID: "generation-2", ReceivedAt: at.Add(time.Minute), Text: "Return the two original requested files",
			SubscriptionConnectionID: "subscription-a"},
	}
	for _, input := range inputs {
		if _, err := service.Ingest(ctx, input); err != nil {
			t.Fatal("actual sessioningress.Ingest", err)
		}
	}
	if len(store.commits) != 2 || store.commits[0].event.Sequence != 1 || store.commits[1].event.Sequence != 2 {
		t.Fatalf("captured canonical commit count/sequence: %d", len(store.commits))
	}
	for index, tc := range []struct {
		name          string
		artifactCount int
	}{
		{name: "normal message with four uploads", artifactCount: 5},
		{name: "text successor with original full history", artifactCount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := preparedIngressServiceVerifiedInput(t, request, authority, blobs, store.commits, index)
			if err := input.ValidateFor(preparedIngressManifestIdentity(input)); err != nil {
				t.Fatalf("actual-ingress input must pass ValidateFor before target predicate: %v", err)
			}
			if got := len(input.Manifest.Artifacts); got != tc.artifactCount {
				t.Fatalf("captured manifest count %d, want %d", got, tc.artifactCount)
			}
			if stage := preparedIngressContractStage(input); stage != 0 || !preparedIngressEnvelope(input) {
				t.Errorf("actual canonical ingress output rejected at ingress stage (manifest=%d, artifacts=%d, historyAttachments=%d)",
					len(input.Manifest.Artifacts), len(input.Artifacts), len(input.CanonicalContext.Attachments))
			}
		})
	}
}

func preparedIngressServiceVerifiedInput(t *testing.T, baseRequest ports.ExecutionRequest, baseAuthority AuthorityV1,
	blobs *preparedIngressServiceBlobs, commits []preparedIngressServiceCommit, target int) *attachedworkerdaemon.VerifiedAttachedInputV1 {
	t.Helper()
	if target < 0 || target >= len(commits) {
		t.Fatal("fixture target commit outside captured history")
	}
	input := preparedOwnerTypedInput(t, baseRequest, baseAuthority)
	input.CanonicalContext.Clear()
	clear(input.Context)
	targetCommit := commits[target]
	request := targetCommit.request
	input.Job = domain.WorkerJob{TenantID: request.TenantID, RunID: request.RunID, SessionID: request.SessionID,
		TriggerEventID: request.EventID, AttemptID: request.AttemptID, ReservationID: baseAuthority.ReservationID,
		InputManifestID: request.ManifestID, ContextWindow: &domain.SessionContextWindow{ThroughSequence: targetCommit.event.Sequence},
		CredentialOwnerUserID: request.UserID, ExecutionPlacementV2: request.ExecutionPlacementV2,
		HarnessBinding: request.HarnessBinding.Clone(), Limits: baseRequestLimits(baseRequest),
		Origin: cloneFrontendOrigin(&request.Origin), CreatedAt: request.CommittedAt}
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: request.TenantID, SessionID: request.SessionID}}
	for _, captured := range commits[:target+1] {
		proof.Input.Events = append(proof.Input.Events, captured.event)
		proof.EventBodies = append(proof.EventBodies, bytes.Clone(captured.body))
		for index, artifact := range captured.request.Artifacts {
			if index == 0 {
				continue // The canonical JSON message is an artifact, not an attachment.
			}
			body, err := preparedIngressReadBlob(t.Context(), blobs, captured.request.TenantID, artifact.Blob)
			if err != nil {
				t.Fatal("read immutable actual-ingress attachment")
			}
			ref := sessioncontext.AttachmentRef{Name: strings.TrimPrefix(artifact.Name, fmt.Sprintf("attachment-%02d-", index)),
				MediaType: artifact.MediaType, Blob: artifact.Blob}
			ref.Name = fmt.Sprintf("%020d-%02d-%s", captured.event.Sequence, index, ref.Name)
			proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: ref, Body: body})
		}
	}
	input.CanonicalContext = proof
	input.Manifest = domain.ArtifactManifest{ID: request.ManifestID, TenantID: request.TenantID, RunID: request.RunID,
		CreatedAt: request.CommittedAt, Artifacts: append([]domain.Artifact(nil), request.Artifacts...)}
	for _, artifact := range request.Artifacts {
		body, err := preparedIngressReadBlob(t.Context(), blobs, request.TenantID, artifact.Blob)
		if err != nil {
			t.Fatal("read immutable actual-ingress manifest artifact")
		}
		input.Artifacts = append(input.Artifacts, attachedworkerdaemon.VerifiedAttachedArtifactV1{Name: artifact.Name, Body: body})
	}
	var err error
	input.Context, _, err = sessioncontext.ProjectCanonical(input.Job, proof, maxInstructionBytes)
	if err != nil {
		t.Fatal("project exact captured canonical history")
	}
	authority := baseAuthority
	authority.RunID, authority.AttemptID = request.RunID, request.AttemptID
	authority.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(authority.TenantID, authority.OwnerUserID,
		authority.WorkerID, request.RunID, request.AttemptID, authority.LeaseID, authority.LeaseGeneration)
	if err != nil {
		t.Fatal("derive exact accepted-attempt fence for captured run")
	}
	authority.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal("bind actual commit context and manifest digest")
	}
	input.AcceptedAttempt = attemptForAuthority(authority, domain.AttachedWorkerAttemptClaimed)
	if err := input.ValidateFor(preparedIngressManifestIdentity(input)); err != nil {
		t.Fatalf("constructed actual-ingress verified input failed contract: %v", err)
	}
	return input
}

func preparedIngressReadBlob(ctx context.Context, blobs *preparedIngressServiceBlobs, tenant domain.TenantID, ref domain.BlobRef) ([]byte, error) {
	reader, err := blobs.Open(ctx, tenant, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(io.LimitReader(reader, 1<<20+1))
}

func baseRequestLimits(request ports.ExecutionRequest) domain.ProductLimits {
	return domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 1,
		MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 10,
		MaxToolEvents: 10, MaxToolEventBytes: 4096}
}

func cloneFrontendOrigin(origin *domain.FrontendEventOrigin) *domain.FrontendEventOrigin {
	if origin == nil {
		return nil
	}
	copy := *origin
	return &copy
}

var (
	_ ports.CanonicalIngressStore = (*preparedIngressServiceStore)(nil)
	_ ports.BlobStore             = (*preparedIngressServiceBlobs)(nil)
	_ ports.HarnessBinder         = (*preparedIngressServiceBinder)(nil)
)
