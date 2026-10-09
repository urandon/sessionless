package attachedworkersealedinput

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type canonicalContextStore interface {
	LoadWorkerContext(context.Context, ports.WorkerContextRequest) (domain.SessionContextInput, error)
}

func (service *Service) canonicalContext(ctx context.Context, job domain.WorkerJob) (history []byte, proof *sessioncontext.CanonicalProof, err error) {
	store, ok := service.jobs.(canonicalContextStore)
	if !ok {
		return nil, nil, ErrUnsupported
	}
	window := job.ContextWindow
	if window == nil || window.Validate() != nil || window.ThroughSequence > job.Limits.EffectiveMaxContextEvents() || window.ThroughSequence > attachedworkerdaemontransport.MaxSealedContextRecordsV1 {
		return nil, nil, ErrInvalid
	}
	// Do not hand the store the job's mutable pointer to the pinned version.
	var version *uint64
	if window.SnapshotVersion != nil {
		copy := *window.SnapshotVersion
		version = &copy
	}
	maxEvents := job.Limits.EffectiveMaxContextEvents()
	if maxEvents > attachedworkerdaemontransport.MaxSealedContextRecordsV1 {
		maxEvents = attachedworkerdaemontransport.MaxSealedContextRecordsV1
	}
	input, loadErr := store.LoadWorkerContext(ctx, ports.WorkerContextRequest{
		TenantID: job.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID,
		ThroughSequence: window.ThroughSequence, AtOrBeforeSnapshotVersion: version,
		MaxEvents: maxEvents,
	})
	if loadErr != nil {
		return nil, nil, ErrUnavailable
	}
	proof = &sessioncontext.CanonicalProof{Input: input}
	defer func() {
		if err != nil {
			clearBytes(history)
			proof.Clear()
			history = nil
			proof = nil
		}
	}()
	if !attachedworkerdaemontransport.BoundedCanonicalInputMetadataV1(input, maxInputBytes) || input.Validate() != nil ||
		input.TenantID != job.TenantID || input.SessionID != job.SessionID ||
		window.SnapshotVersion == nil && input.Snapshot != nil ||
		window.SnapshotVersion != nil && (input.Snapshot == nil || input.Snapshot.Version != *window.SnapshotVersion || input.Snapshot.ThroughSequence != window.AfterSequence) {
		return nil, proof, ErrInvalid
	}
	remaining := int64(maxInputBytes)
	if input.Snapshot != nil {
		proof.SnapshotBytes, err = service.readExact(ctx, job.TenantID, input.Snapshot.Payload, remaining)
		if err != nil {
			return nil, proof, err
		}
		remaining -= int64(len(proof.SnapshotBytes))
	}
	for _, event := range input.Events {
		body, readErr := service.readExact(ctx, job.TenantID, event.Payload, remaining)
		if readErr != nil {
			return nil, proof, readErr
		}
		proof.EventBodies = append(proof.EventBodies, body)
		remaining -= int64(len(body))
	}
	contextLimit := job.Limits.MaxContextBytes
	if contextLimit > maxInputBytes {
		contextLimit = maxInputBytes
	}
	var refs []sessioncontext.AttachmentRef
	history, refs, err = sessioncontext.ProjectCanonical(job, proof, contextLimit)
	if err != nil {
		return history, proof, ErrInvalid
	}
	contextRemaining := int64(contextLimit) - int64(len(history))
	for _, ref := range refs {
		if ref.Blob.Size > contextRemaining {
			return history, proof, ErrInvalid
		}
		body, readErr := service.readExact(ctx, job.TenantID, ref.Blob, remaining)
		if readErr != nil {
			return history, proof, readErr
		}
		proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: ref, Body: body})
		remaining -= int64(len(body))
		contextRemaining -= int64(len(body))
	}
	if sessioncontext.VerifyCanonicalAttachments(history, refs, proof, contextLimit) != nil ||
		!attachedworkerdaemontransport.BoundedCanonicalProofV1(proof, maxInputBytes) {
		return history, proof, ErrInvalid
	}
	return history, proof, nil
}
