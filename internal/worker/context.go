package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type contextAttachment struct {
	Name      string         `json:"name"`
	MediaType string         `json:"media_type"`
	Blob      domain.BlobRef `json:"blob"`
}

type contextUserEnvelope struct {
	Version     uint32              `json:"version"`
	Attachments []contextAttachment `json:"attachments,omitempty"`
}

func (manager *Manager) materializeCanonicalContext(
	ctx context.Context,
	loaded ports.WorkerJobState,
	workDir string,
) error {
	window := loaded.Job.ContextWindow
	if window == nil {
		return domain.ValidationError{Field: "worker_job.context_window", Reason: "must be present"}
	}
	maxBytes := loaded.Job.Limits.MaxContextBytes
	if uint64(manager.config.MaxMaterializedBytes) < maxBytes {
		maxBytes = uint64(manager.config.MaxMaterializedBytes)
	}
	maxEvents := loaded.Job.Limits.EffectiveMaxContextEvents()
	version := cloneUint64(window.SnapshotVersion)
	var fallbacks uint32
	for {
		input, err := manager.state.LoadWorkerContext(ctx, ports.WorkerContextRequest{
			TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
			TriggerEventID:            loaded.Run.TriggerEventID,
			AtOrBeforeSnapshotVersion: version,
			ThroughSequence:           window.ThroughSequence,
			MaxEvents:                 maxEvents,
		})
		if err != nil {
			return err
		}
		records, history, snapshotErr := manager.materializeContextAttempt(
			ctx, loaded, input, maxBytes, version, window,
		)
		if snapshotErr == nil {
			if err := validateContextBoundary(records, loaded.Run.TriggerEventID, window.ThroughSequence); err != nil {
				return err
			}
			if err := validateContextToolLimit(records, loaded.Job.Limits); err != nil {
				return err
			}
			used := uint64(len(history))
			if err := writeExclusive(filepath.Join(workDir, "context", "history.jsonl"), history); err != nil {
				return err
			}
			return manager.materializeContextAttachments(ctx, workDir, records, &used, maxBytes)
		}
		if input.Snapshot == nil {
			return snapshotErr
		}
		fallbacks++
		if fallbacks >= manager.config.MaxSnapshotFallbacks || input.Snapshot.Version <= 1 {
			version = nil
		} else {
			fallback := input.Snapshot.Version - 1
			version = &fallback
		}
	}
}

func (manager *Manager) materializeContextAttempt(
	ctx context.Context,
	loaded ports.WorkerJobState,
	input domain.SessionContextInput,
	maxBytes uint64,
	requestedVersion *uint64,
	window *domain.SessionContextWindow,
) ([]sessioncontext.EventPayload, []byte, error) {
	if err := input.Validate(); err != nil {
		return nil, nil, err
	}
	if input.TenantID != loaded.Run.TenantID || input.SessionID != loaded.Run.SessionID {
		return nil, nil, domain.ValidationError{
			Field: "worker_context", Reason: "crosses the admitted tenant or session boundary",
		}
	}
	records := make([]sessioncontext.EventPayload, 0, len(input.Events))
	var history []byte
	if input.Snapshot != nil {
		if requestedVersion != nil && input.Snapshot.Version == *requestedVersion &&
			window.SnapshotVersion != nil && *requestedVersion == *window.SnapshotVersion &&
			input.Snapshot.ThroughSequence != window.AfterSequence {
			return nil, nil, fmt.Errorf("pinned snapshot coverage does not match the admitted context window")
		}
		compressed, err := manager.readBlob(
			ctx, loaded.Run.TenantID, input.Snapshot.Payload, manager.config.MaxMaterializedBytes,
		)
		if err != nil {
			return nil, nil, err
		}
		records, history, err = sessioncontext.DecodeSnapshot(compressed, *input.Snapshot, maxBytes)
		if err != nil {
			return nil, nil, err
		}
	}
	for _, event := range input.Events {
		remaining := maxBytes - uint64(len(history))
		payload, err := manager.readBlob(ctx, loaded.Run.TenantID, event.Payload, int64(remaining))
		if err != nil {
			return nil, nil, err
		}
		line, err := sessioncontext.EncodeRecord(event, payload)
		if err != nil {
			return nil, nil, err
		}
		if uint64(len(line)) > remaining {
			return nil, nil, domain.ValidationError{
				Field: "worker_context.bytes", Reason: "exceeds the admitted context limit",
			}
		}
		history = append(history, line...)
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: payload})
	}
	if uint64(len(records)) > loaded.Job.Limits.EffectiveMaxContextEvents() {
		return nil, nil, domain.ValidationError{
			Field: "worker_context.events", Reason: "exceeds the admitted event limit",
		}
	}
	return records, history, nil
}

func (manager *Manager) materializeContextAttachments(
	ctx context.Context,
	workDir string,
	records []sessioncontext.EventPayload,
	used *uint64,
	maxBytes uint64,
) error {
	for _, record := range records {
		if record.Event.Kind != domain.SessionEventUserMessage {
			continue
		}
		var envelope contextUserEnvelope
		if err := json.Unmarshal(record.Payload, &envelope); err != nil {
			return fmt.Errorf("decode user event attachments: %w", err)
		}
		for index, attachment := range envelope.Attachments {
			if err := validateFilename(attachment.Name); err != nil {
				return err
			}
			if err := domain.ValidateSessionEventBlob(
				record.Event.TenantID, record.Event.SessionID, record.Event.ID, attachment.Blob,
			); err != nil {
				return err
			}
			if attachment.Blob.Size < 0 || uint64(attachment.Blob.Size) > maxBytes-*used {
				return domain.ValidationError{
					Field: "worker_context.attachments", Reason: "exceeds the admitted context limit",
				}
			}
			payload, err := manager.readBlob(
				ctx, record.Event.TenantID, attachment.Blob, int64(maxBytes-*used),
			)
			if err != nil {
				return err
			}
			name := fmt.Sprintf("%02d-%s", index+1, attachment.Name)
			target := filepath.Join(
				workDir, "context", "attachments", fmt.Sprintf("%020d", record.Event.Sequence), name,
			)
			if err := writeExclusive(target, payload); err != nil {
				return err
			}
			*used += uint64(len(payload))
		}
	}
	return nil
}

func validateContextBoundary(
	records []sessioncontext.EventPayload,
	triggerEventID domain.SessionEventID,
	throughSequence uint64,
) error {
	if len(records) == 0 || records[len(records)-1].Event.Sequence != throughSequence {
		return domain.ValidationError{Field: "worker_context.events", Reason: "does not reach the pinned boundary"}
	}
	if records[len(records)-1].Event.ID != triggerEventID {
		return domain.ValidationError{Field: "worker_context.trigger_event_id", Reason: "does not match the pinned boundary event"}
	}
	return nil
}

func validateContextToolLimit(records []sessioncontext.EventPayload, limits domain.ProductLimits) error {
	maxEvents, maxBytes := limits.EffectiveToolEventLimits()
	var count, payloadBytes uint64
	for _, record := range records {
		if record.Event.Kind == domain.SessionEventToolCall || record.Event.Kind == domain.SessionEventToolResult {
			count++
			if uint64(len(record.Payload)) > maxBytes-payloadBytes {
				return domain.ValidationError{
					Field: "worker_context.tool_event_bytes", Reason: "exceeds the admitted tool-event byte limit",
				}
			}
			payloadBytes += uint64(len(record.Payload))
		}
	}
	if count > uint64(maxEvents) {
		return domain.ValidationError{
			Field: "worker_context.tool_events", Reason: "exceeds the admitted tool-event limit",
		}
	}
	return nil
}

func writeExclusive(target string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := bytes.NewReader(body).WriteTo(file)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
