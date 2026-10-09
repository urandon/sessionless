package sessioncontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// CanonicalProof carries immutable source bytes, not a second history format.
// Both ends reconstruct history with the canonical codec before releasing it.
type CanonicalProof struct {
	Input         domain.SessionContextInput `json:"input"`
	SnapshotBytes []byte                     `json:"snapshot_bytes,omitempty"`
	EventBodies   [][]byte                   `json:"event_bodies"`
	Attachments   []AttachmentPayload        `json:"attachments,omitempty"`
}

type AttachmentRef struct {
	Name      string         `json:"name"`
	MediaType string         `json:"media_type"`
	Blob      domain.BlobRef `json:"blob"`
}

type AttachmentPayload struct {
	AttachmentRef
	Body []byte `json:"body"`
}

// ProjectCanonical applies the same snapshot/record codec and boundary/tool
// limits used by worker materialization. An admitted snapshot is exact: this
// bounded attached path does not negotiate older snapshots or partial history.
func ProjectCanonical(job domain.WorkerJob, proof *CanonicalProof, maxBytes uint64) (output []byte, outputRefs []AttachmentRef, outputErr error) {
	invalid := func(reason string) ([]byte, []AttachmentRef, error) {
		return nil, nil, domain.ValidationError{Field: "worker_context", Reason: reason}
	}
	window := job.ContextWindow
	if proof == nil || window == nil || window.Validate() != nil || proof.Input.Validate() != nil ||
		proof.Input.TenantID != job.TenantID || proof.Input.SessionID != job.SessionID ||
		len(proof.EventBodies) != len(proof.Input.Events) {
		return invalid("does not match the admitted scope and window")
	}
	if maxBytes > job.Limits.MaxContextBytes {
		maxBytes = job.Limits.MaxContextBytes
	}
	if maxBytes == 0 || window.ThroughSequence > job.Limits.EffectiveMaxContextEvents() {
		return invalid("exceeds the admitted context limits")
	}
	var records []EventPayload
	var history []byte
	snapshotRecords := 0
	defer func() {
		// DecodeSnapshot owns freshly allocated record payloads. Tail payloads
		// remain proof-owned and are cleared by CanonicalProof.Clear instead.
		for index := 0; index < snapshotRecords; index++ {
			clear(records[index].Payload)
		}
		if outputErr != nil {
			clear(history)
		}
	}()
	if window.SnapshotVersion == nil {
		if proof.Input.Snapshot != nil || len(proof.SnapshotBytes) != 0 {
			return invalid("unexpected snapshot")
		}
	} else {
		snapshot := proof.Input.Snapshot
		if snapshot == nil || snapshot.Version != *window.SnapshotVersion || snapshot.ThroughSequence != window.AfterSequence ||
			snapshot.EventCount > job.Limits.EffectiveMaxContextEvents() || !matchesBlob(snapshot.Payload, proof.SnapshotBytes) {
			return invalid("snapshot does not match its pinned immutable reference")
		}
		var err error
		records, history, err = DecodeSnapshot(proof.SnapshotBytes, *snapshot, maxBytes)
		snapshotRecords = len(records)
		if err != nil {
			return nil, nil, err
		}
	}
	for index, event := range proof.Input.Events {
		line, err := EncodeRecord(event, proof.EventBodies[index])
		if err != nil {
			return nil, nil, err
		}
		if uint64(len(line)) > maxBytes-uint64(len(history)) {
			clear(line)
			return invalid("exceeds the admitted context byte limit")
		}
		history = append(history, line...)
		clear(line)
		records = append(records, EventPayload{Event: event, Payload: proof.EventBodies[index]})
	}
	if len(records) == 0 || uint64(len(records)) != window.ThroughSequence ||
		records[len(records)-1].Event.ID != job.TriggerEventID || records[len(records)-1].Event.Sequence != window.ThroughSequence {
		return invalid("does not reach the exact admitted trigger boundary")
	}
	maxTools, maxToolBytes := job.Limits.EffectiveToolEventLimits()
	var tools, toolBytes uint64
	var attachments []AttachmentRef
	for index, record := range records {
		if record.Event.Sequence != uint64(index+1) || record.Event.TenantID != job.TenantID || record.Event.SessionID != job.SessionID {
			return invalid("does not form the contiguous admitted history")
		}
		if record.Event.Kind == domain.SessionEventToolCall || record.Event.Kind == domain.SessionEventToolResult {
			tools++
			if uint64(len(record.Payload)) > maxToolBytes-toolBytes {
				return invalid("exceeds the admitted tool-event byte limit")
			}
			toolBytes += uint64(len(record.Payload))
		}
		if record.Event.Kind != domain.SessionEventUserMessage {
			continue
		}
		var envelope struct {
			Attachments json.RawMessage `json:"attachments"`
		}
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return nil, nil, err
		}
		if len(envelope.Attachments) == 0 || bytes.Equal(envelope.Attachments, []byte("null")) {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(envelope.Attachments))
		token, err := decoder.Token()
		if err != nil || token != json.Delim('[') {
			return invalid("attachments must be an array")
		}
		attachmentIndex := 0
		for decoder.More() {
			if uint64(len(attachments)) >= uint64(job.Limits.MaxArtifacts) || len(attachments) >= 64 {
				return invalid("exceeds the admitted attachment limit")
			}
			var attachment AttachmentRef
			if err := decoder.Decode(&attachment); err != nil {
				return nil, nil, err
			}
			if strings.TrimSpace(attachment.Name) == "" || attachment.Name == "." || attachment.Name == ".." ||
				strings.ContainsAny(attachment.Name, `/\\`) || len(attachment.Name) > 4072 || len(attachment.MediaType) > 4096 || len(attachment.Blob.Key) > 4096 ||
				domain.ValidateSessionEventBlob(job.TenantID, job.SessionID, record.Event.ID, attachment.Blob) != nil {
				return invalid("attachment crosses its immutable event boundary or has an unsafe name")
			}
			// Sequence is canonical event identity within this exact session. Names
			// stay unique even when several historical events upload image.png.
			attachment.Name = fmt.Sprintf("%020d-%02d-%s", record.Event.Sequence, attachmentIndex+1, attachment.Name)
			attachments = append(attachments, attachment)
			attachmentIndex++
		}
		if _, err := decoder.Token(); err != nil {
			return nil, nil, err
		}
	}
	if tools > uint64(maxTools) {
		return invalid("exceeds the admitted tool-event count limit")
	}
	return history, attachments, nil
}

func VerifyCanonicalAttachments(history []byte, expected []AttachmentRef, proof *CanonicalProof, maxBytes uint64) error {
	if proof == nil || len(expected) != len(proof.Attachments) || uint64(len(history)) > maxBytes {
		return domain.ValidationError{Field: "worker_context.attachments", Reason: "does not match canonical history"}
	}
	used := uint64(len(history))
	for index, attachment := range expected {
		body := proof.Attachments[index]
		if body.AttachmentRef != attachment || uint64(len(body.Body)) > maxBytes-used || !matchesBlob(attachment.Blob, body.Body) {
			return domain.ValidationError{Field: "worker_context.attachments", Reason: "does not match the immutable reference or byte limit"}
		}
		used += uint64(len(body.Body))
	}
	return nil
}

func matchesBlob(ref domain.BlobRef, body []byte) bool {
	digest := sha256.Sum256(body)
	return ref.Validate() == nil && ref.Size == int64(len(body)) && ref.SHA256 == hex.EncodeToString(digest[:])
}

// Clear relinquishes all proof payload buffers on every terminal path.
func (proof *CanonicalProof) Clear() {
	if proof == nil {
		return
	}
	clear(proof.SnapshotBytes)
	for _, body := range proof.EventBodies {
		clear(body)
	}
	for _, attachment := range proof.Attachments {
		clear(attachment.Body)
	}
	*proof = CanonicalProof{}
}
