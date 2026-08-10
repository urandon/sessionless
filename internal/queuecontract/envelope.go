// Package queuecontract defines the versioned, payload-free messages exchanged
// by the serverless control plane and worker queues.
package queuecontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const SchemaV1 = "sessionless.queue.v1"

type Kind string

const (
	KindDispatchRun     Kind = "dispatch.run"
	KindDeliverTelegram Kind = "deliver.telegram"
	KindWakeDispatch    Kind = "wake.dispatch"
	KindWakeTelegram    Kind = "wake.telegram"
)

func (kind Kind) Valid() bool {
	return kind == KindDispatchRun || kind == KindDeliverTelegram ||
		kind == KindWakeDispatch || kind == KindWakeTelegram
}

// Envelope intentionally contains opaque identifiers only. Prompts,
// attachments, credentials, and generated content must be resolved from
// tenant-scoped stores after authorization.
type Envelope struct {
	Schema     string           `json:"schema"`
	MessageID  domain.MessageID `json:"message_id"`
	Kind       Kind             `json:"kind"`
	TenantID   domain.TenantID  `json:"tenant_id"`
	SubjectID  string           `json:"subject_id"`
	EnqueuedAt time.Time        `json:"enqueued_at"`
}

func (envelope Envelope) Validate() error {
	if envelope.Schema != SchemaV1 {
		return domain.ValidationError{Field: "queue.schema", Reason: "unsupported schema version"}
	}
	if err := envelope.MessageID.Validate(); err != nil {
		return err
	}
	if !envelope.Kind.Valid() {
		return domain.ValidationError{Field: "queue.kind", Reason: "is unknown"}
	}
	if err := envelope.TenantID.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("queue.subject_id", envelope.SubjectID); err != nil {
		return err
	}
	if envelope.EnqueuedAt.IsZero() {
		return domain.ValidationError{Field: "queue.enqueued_at", Reason: "must not be zero"}
	}
	return nil
}

func Encode(envelope Envelope) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal queue envelope: %w", err)
	}
	return data, nil
}

func Decode(data []byte) (Envelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode queue envelope: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Envelope{}, err
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode queue envelope: trailing JSON value")
		}
		return fmt.Errorf("decode queue envelope trailing data: %w", err)
	}
	return nil
}
