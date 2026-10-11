package attachedworkerdaemon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Unit-only admitted fixture. This is not a production authority producer or
// proof that the concrete driver/stack has been activated.
func validPreparedCredentialInvocation(t *testing.T) Invocation {
	t.Helper()
	input := validCredentialInvocation(t)
	i, request := input.Identity, input.Credential.IssueRequest
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: i.OwnerUserID, WorkerID: i.WorkerID,
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	expires := request.Lease.ExpiresAt
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2,
		TenantID: i.TenantID, OwnerUserID: i.OwnerUserID, RunID: i.RunID, AttemptID: i.AttemptID,
		Backend: domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1",
			BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1,
			ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "codex-jsonl.v1", BackendProfileDigest: strings.Repeat("2", 64),
			ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1},
		Resource: request.ProviderResource, ModelVendorID: "openai", ModelID: "fixture",
		InputDataClass: domain.ProviderDataPrivateV1, ProviderCatalogDigest: strings.Repeat("1", 64),
		ProviderRouteDigest: strings.Repeat("2", 64), PrivacyPolicyDigest: strings.Repeat("3", 64),
		CapabilityEvidenceDigest: strings.Repeat("4", 64), EffectivePolicyDigest: strings.Repeat("5", 64),
		ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires}
	body := []byte("private typed context")
	digest := sha256.Sum256(body)
	job := domain.WorkerJob{TenantID: i.TenantID, RunID: i.RunID, AttemptID: i.AttemptID,
		SessionID: request.Run.SessionID, TriggerEventID: request.Run.TriggerEventID, ReservationID: "reservation-a", InputManifestID: "manifest-a",
		CredentialOwnerUserID: i.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: binding,
		ContextSnapshot: domain.BlobRef{TenantID: i.TenantID, Key: "tenants/tenant-a/context", Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])},
		Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 1,
			MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 10, MaxToolEvents: 10, MaxToolEventBytes: 4096}}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: i.TenantID, RunID: i.RunID, CreatedAt: request.Lease.AcquiredAt}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(i.TenantID, i.OwnerUserID, i.WorkerID, i.RunID, i.AttemptID, i.LeaseID, i.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	input.VerifiedInput = &VerifiedAttachedInputV1{AcceptedAttempt: domain.AttachedWorkerAttemptV1{Version: 1,
		TenantID: i.TenantID, OwnerUserID: i.OwnerUserID, WorkerID: i.WorkerID, ConnectionID: "attached-connection-a",
		RunID: i.RunID, AttemptID: i.AttemptID, ReservationID: job.ReservationID, LeaseID: i.LeaseID,
		LeaseGeneration: i.FenceToken, FenceToken: fence, EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ContextDigest: contextDigest, CapabilityDigest: placement.CapabilityDigest, PolicyDigest: placement.PolicyDigest,
		State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1,
		CreatedAt: request.Lease.AcquiredAt, UpdatedAt: request.Lease.AcquiredAt, LeaseExpiresAt: expires, Revision: 7},
		Job: job, Manifest: manifest, Context: body}
	if err := input.VerifiedInput.validateCredential(i, input.Credential); err != nil {
		t.Fatalf("typed fixture invalid: %v", err)
	}
	return input
}

func canonicalVerifiedInvocation(t *testing.T) Invocation {
	t.Helper()
	input := validPreparedCredentialInvocation(t)
	v := input.VerifiedInput
	job := &v.Job
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 1}
	blob := func(key string, body []byte) domain.BlobRef {
		digest := sha256.Sum256(body)
		return domain.BlobRef{TenantID: job.TenantID, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
	}
	attachmentBody := []byte("private original attachment")
	attachment := sessioncontext.AttachmentRef{Name: "input.txt", MediaType: "text/plain",
		Blob: blob(domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, job.TriggerEventID)+"input.txt", attachmentBody)}
	payload, err := json.Marshal(struct {
		Text        string                         `json:"text"`
		Attachments []sessioncontext.AttachmentRef `json:"attachments"`
	}{
		Text: "private canonical message", Attachments: []sessioncontext.AttachmentRef{attachment}})
	if err != nil {
		t.Fatal(err)
	}
	owner := job.CredentialOwnerUserID
	event := domain.SessionEvent{ID: job.TriggerEventID, TenantID: job.TenantID, SessionID: job.SessionID, Sequence: 1,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &owner, IdempotencyKey: "canonical-fixture-event",
		Payload:   blob(domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, job.TriggerEventID)+"message.json", payload),
		CreatedAt: v.AcceptedAttempt.CreatedAt}
	attachment.Name = "00000000000000000001-01-input.txt"
	v.CanonicalContext = &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: job.TenantID,
		SessionID: job.SessionID, Events: []domain.SessionEvent{event}}, EventBodies: [][]byte{payload},
		Attachments: []sessioncontext.AttachmentPayload{{AttachmentRef: attachment, Body: attachmentBody}}}
	v.Context, _, err = sessioncontext.ProjectCanonical(*job, v.CanonicalContext, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("private original artifact")
	v.Artifacts = []VerifiedAttachedArtifactV1{{Name: "alpha", Body: body}}
	v.Manifest.Artifacts = []domain.Artifact{{Name: "alpha", MediaType: "text/plain", Blob: blob("tenants/tenant-a/alpha", body)}}
	v.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(*job, v.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.ValidateFor(input.Identity); err != nil {
		t.Fatalf("canonical fixture invalid: %v", err)
	}
	t.Cleanup(v.Clear)
	return input
}

func TestVerifiedAttachedInputDeepCloneIncludesCanonicalProofAndMetadata(t *testing.T) {
	input := canonicalVerifiedInvocation(t)
	cloned, err := input.VerifiedInput.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer cloned.Clear()
	if cloned.ValidateFor(input.Identity) != nil {
		t.Fatal("clone lost valid canonical input")
	}
	cloned.Job.ContextWindow.ThroughSequence++
	*cloned.CanonicalContext.Input.Events[0].AuthorUserID = "foreign-owner"
	cloned.Artifacts[0].Body[0] ^= 1
	cloned.CanonicalContext.EventBodies[0][0] ^= 1
	cloned.CanonicalContext.Attachments[0].Body[0] ^= 1
	if input.VerifiedInput.ValidateFor(input.Identity) != nil {
		t.Fatal("deep copy mutation reached original proof/job/artifacts")
	}
	borrowed := [][]byte{cloned.Context, cloned.Artifacts[0].Body, cloned.CanonicalContext.EventBodies[0], cloned.CanonicalContext.Attachments[0].Body}
	cloned.Clear()
	for index, body := range borrowed {
		if !bytes.Equal(body, make([]byte, len(body))) {
			t.Fatalf("clone body %d survived clear", index)
		}
	}
}

func TestVerifiedAttachedInputBoundsBeforeClone(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*VerifiedAttachedInputV1)
	}{
		{name: "body", mutate: func(input *VerifiedAttachedInputV1) { input.Context = make([]byte, maxVerifiedAttachedInputBytes+1) }},
		{name: "metadata", mutate: func(input *VerifiedAttachedInputV1) { input.Job.OwnedExecutionSourceRecord = strings.Repeat("x", 4097) }},
		{name: "collection", mutate: func(input *VerifiedAttachedInputV1) { input.Artifacts = make([]VerifiedAttachedArtifactV1, 65) }},
		{name: "serialized input", mutate: func(input *VerifiedAttachedInputV1) { input.Job.Limits.MaxInputBytes = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := validPreparedCredentialInvocation(t)
			test.mutate(input.VerifiedInput)
			if cloned, err := input.VerifiedInput.Clone(); err == nil || cloned != nil {
				t.Fatalf("unbounded clone=%t err=%v", cloned != nil, err)
			}
		})
	}
}

func TestVerifiedAttachedInputCloneAndPrivacy(t *testing.T) {
	input := validPreparedCredentialInvocation(t)
	cloned, err := input.VerifiedInput.Clone()
	if err != nil {
		t.Fatal(err)
	}
	borrowed := cloned.Context
	cloned.Context[0] ^= 1
	if bytes.Equal(cloned.Context, input.VerifiedInput.Context) {
		t.Fatal("clone aliases source context")
	}
	cloned.Clear()
	if !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Fatal("borrowed clone survived clear")
	}
	encoded, err := json.Marshal(input.VerifiedInput)
	if err != nil || string(encoded) != "{}" {
		t.Fatalf("private carrier JSON=%q err=%v", encoded, err)
	}
	for _, text := range []string{fmt.Sprint(input.VerifiedInput), fmt.Sprintf("%#v", input.VerifiedInput)} {
		if strings.Contains(text, "tenant-a") || strings.Contains(text, "typed context") {
			t.Fatalf("carrier leaked: %s", text)
		}
	}
}
