package ydbstore

import (
	"encoding/json"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const (
	inlineReceiptPersistenceEncodingV2 = "single-answer-v1"
	maxInlineReceiptStoredBytesV2      = maxInlineReceiptRecordBytesV2 + 42
)

// This discriminator belongs only to private SQL DATA. It is neither a public
// receipt version nor authority to use the receipt's claimed binding.
type storedInlineReceiptV2 struct {
	PersistenceEncoding string `json:"persistence_encoding"`
	AttachedWorkerOutputReceiptV2
}

func encodeInlineReceiptProjectionV2(r AttachedWorkerOutputReceiptV2) (storedInlineReceiptV2, error) {
	if validateInlineReceiptSelfV2(r) != nil {
		return storedInlineReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	// Preserve the old full-domain Marshal byte ceiling exactly. Encoder adds
	// one newline; the count includes it only for checking/removing framing.
	// Encoder buffers before Write and uses a shared pool: this avoids a
	// returned full copy, NOT an aggregate allocation/capacity/zeroization proof.
	count := inlineReceiptDomainCountV2{}
	if json.NewEncoder(&count).Encode(r) != nil || count.bytes == 0 || count.last != '\n' || count.bytes-1 > maxInlineReceiptRecordBytesV2 {
		return storedInlineReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	if original := r.Materialization.Completion; original != nil {
		completion := *original
		completion.Events = make([]domain.SessionEventDraft, len(original.Events))
		copy(completion.Events, original.Events)
		for i := range completion.Events {
			completion.Events[i].DisplayText = ""
		}
		r.Materialization.Completion = &completion
	}
	return storedInlineReceiptV2{PersistenceEncoding: inlineReceiptPersistenceEncodingV2, AttachedWorkerOutputReceiptV2: r}, nil
}

type inlineReceiptDomainCountV2 struct {
	bytes int
	last  byte
}

func (w *inlineReceiptDomainCountV2) Write(p []byte) (int, error) {
	if len(p) > maxInlineReceiptRecordBytesV2+1-w.bytes {
		return 0, ErrAttachedWorkerAttemptConflict
	}
	w.bytes += len(p)
	if len(p) != 0 {
		w.last = p[len(p)-1]
	}
	return len(p), nil
}

func decodeInlineStoredReceiptV2(payload []byte) (AttachedWorkerOutputReceiptV2, error) {
	if len(payload) > maxInlineReceiptStoredBytesV2 {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	metadata, original, err := strictjson.ProjectStrings(payload, maxInlineReceiptStoredBytesV2, []strictjson.StringProjection{
		{Path: []string{"candidate", "summary"}, MaxBytes: attachedworkeroutput.MaxInlineAnswerBytesV2},
		{Path: []string{"materialization", "Completion", "Events", "0", "display_text"}, MaxBytes: 0},
	})
	if err != nil {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	defer func() { clear(metadata) }()
	var stored storedInlineReceiptV2
	// With omitempty DisplayText still empty, this closed shape rejects ANY
	// display member, including empty/null/case aliases. ProjectStrings rejects
	// a nonempty original display token before allocating the metadata copy.
	if decodeOwnedEffectCanonicalJSON(string(metadata), &stored) != nil || stored.PersistenceEncoding != inlineReceiptPersistenceEncodingV2 {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	clear(metadata)
	metadata = nil
	r := stored.AttachedWorkerOutputReceiptV2
	if original[0] != nil && json.Unmarshal(original[0], &r.Candidate.Summary) != nil {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	if r.Stage == attachedworkeroutput.InlineCanonicalCopyingV2 || r.Stage == attachedworkeroutput.InlineReadyV2 {
		if r.Materialization.Completion == nil || len(r.Materialization.Completion.Events) != 1 {
			return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
		}
		r.Materialization.Completion.Events[0].DisplayText = strings.TrimSpace(r.Candidate.Summary)
	}
	if validateInlineReceiptSelfV2(r) != nil {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	return r, nil
}

// Only structural DATA identity is checked here. SQL consumers independently
// join the actual tenant/owner/attempt and denormalized cells/current authority.
func validateInlineReceiptSelfV2(r AttachedWorkerOutputReceiptV2) error {
	a := r.Binding
	return (attachedWorkerReceiptEnvelope{v2: &r}).validateIdentity(a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration)
}
