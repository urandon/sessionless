package ydbstore

import (
	"bytes"
	"encoding/json"
	"io"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// encodeInlineReceiptSQLBytesV2 returns one exact-capacity owned SQL parameter,
// with the same bytes as Marshal of the private projection and no trailing LF.
// The synchronous SQL caller owns clearing it after Exec returns. Metadata
// Marshal still uses JSON's shared pool; its transients and SDK copies remain
// separate obligations, not whole-phase capacity or zeroization guarantees.
func encodeInlineReceiptSQLBytesV2(r AttachedWorkerOutputReceiptV2) ([]byte, error) {
	plan, err := planInlineReceiptEncodingV2(r)
	if err != nil {
		return nil, ErrAttachedWorkerAttemptConflict
	}
	defer clear(plan.metadata)
	encoded := make([]byte, plan.storedBytes)
	w := inlineReceiptFixedWriterV2{buffer: encoded}
	if plan.writeTo(&w) != nil || w.written != len(encoded) {
		clear(encoded)
		return nil, ErrAttachedWorkerAttemptConflict
	}
	return encoded, nil
}

type inlineReceiptEncodingPlanV2 struct {
	projected   storedInlineReceiptV2
	metadata    []byte
	valueStart  int
	valueEnd    int
	storedBytes int
	fullBytes   int
}

func planInlineReceiptEncodingV2(r AttachedWorkerOutputReceiptV2) (inlineReceiptEncodingPlanV2, error) {
	var zero inlineReceiptEncodingPlanV2
	// Validate the original before omission can hide any contradictory display,
	// identity, digest, stage or canonical materialization.
	if validateInlineReceiptSelfV2(r) != nil {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	summaryCount := inlineReceiptStringCountV2{}
	if streamInlineReceiptJSONStringV2(&summaryCount, r.Candidate.Summary) != nil {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	displayBytes := 0
	if original := r.Materialization.Completion; original != nil {
		completion := *original
		completion.Events = make([]domain.SessionEventDraft, len(original.Events))
		copy(completion.Events, original.Events)
		for i := range completion.Events {
			display := completion.Events[i].DisplayText
			if display != "" {
				count := inlineReceiptStringCountV2{}
				if streamInlineReceiptJSONStringV2(&count, display) != nil {
					return zero, ErrAttachedWorkerAttemptConflict
				}
				// Omitting this omitempty member removes its key/colon,
				// quoted value and exactly one separating comma. Original
				// validation requires a single populated canonical event.
				displayBytes += len(`"display_text":`) + count.bytes + 1
			}
			completion.Events[i].DisplayText = ""
		}
		r.Materialization.Completion = &completion
	}
	projected := storedInlineReceiptV2{PersistenceEncoding: inlineReceiptPersistenceEncodingV2, AttachedWorkerOutputReceiptV2: r}
	placeholder := projected
	placeholder.Candidate.Summary = inlineReceiptHashMarkerV2
	metadata, err := json.Marshal(placeholder)
	if err != nil {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	keep := false
	defer func() {
		if !keep {
			clear(metadata)
		}
	}()
	// The marker is a fixed simple string; json.Marshal's spelling has no
	// escapes. Only typed metadata is searched, never untrusted raw JSON.
	const marker = `"` + inlineReceiptHashMarkerV2 + `"`
	const token = `"summary":` + marker
	if bytes.Count(metadata, []byte(token)) != 1 {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	// This derived precheck cannot narrow old valid DATA: even an empty quoted
	// answer could remove only the marker's bytes. It avoids arithmetic growth;
	// it is not a new Usage-count or decoded-metadata ceiling.
	if len(metadata) > maxInlineReceiptStoredBytesV2+len(marker) {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	storedBytes := len(metadata) - len(marker) + summaryCount.bytes
	// The fixed discriminator adds exactly 42 bytes. Reinsert the original
	// omitted display member to recover the old full-domain Marshal length.
	fullBytes := storedBytes - (maxInlineReceiptStoredBytesV2 - maxInlineReceiptRecordBytesV2) + displayBytes
	if fullBytes <= 0 || fullBytes > maxInlineReceiptRecordBytesV2 || storedBytes > maxInlineReceiptStoredBytesV2 {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	start := bytes.Index(metadata, []byte(token)) + len(`"summary":`)
	keep = true
	return inlineReceiptEncodingPlanV2{projected: projected, metadata: metadata, valueStart: start, valueEnd: start + len(marker), storedBytes: storedBytes, fullBytes: fullBytes}, nil
}

func (plan inlineReceiptEncodingPlanV2) writeTo(w io.Writer) error {
	if err := writeInlineReceiptBytesV2(w, plan.metadata[:plan.valueStart]); err != nil {
		return err
	}
	if err := streamInlineReceiptJSONStringV2(w, plan.projected.Candidate.Summary); err != nil {
		return err
	}
	return writeInlineReceiptBytesV2(w, plan.metadata[plan.valueEnd:])
}

func writeInlineReceiptBytesV2(w io.Writer, data []byte) error {
	if w == nil {
		return ErrAttachedWorkerAttemptConflict
	}
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

type inlineReceiptStringCountV2 struct{ bytes int }

func (w *inlineReceiptStringCountV2) Write(data []byte) (int, error) {
	if len(data) > maxInlineReceiptRecordBytesV2-w.bytes {
		return 0, ErrAttachedWorkerAttemptConflict
	}
	w.bytes += len(data)
	return len(data), nil
}

type inlineReceiptFixedWriterV2 struct {
	buffer  []byte
	written int
}

func (w *inlineReceiptFixedWriterV2) Write(data []byte) (int, error) {
	if len(data) > len(w.buffer)-w.written {
		return 0, io.ErrShortWrite
	}
	copy(w.buffer[w.written:], data)
	w.written += len(data)
	return len(data), nil
}
