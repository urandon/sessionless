package attachedworkerreceipt

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Borrow the exact buffer passed to request.Body.Read. This test-only observer
// keeps it reachable to inspect erasure at store entry, not to measure heap or
// prove that the test itself has released this borrowed reference.
type inlineReadBufferObserverV2 struct {
	*bytes.Reader
	borrowed []byte
}

func (r *inlineReadBufferObserverV2) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 && r.borrowed == nil {
		r.borrowed = p[:n]
	}
	return n, err
}

func (*inlineReadBufferObserverV2) Close() error { return nil }

type inlineErasedControlStoreV2 struct {
	*inlineStoreFixtureV2
	reader  *inlineReadBufferObserverV2
	want    string
	checked bool
	erased  bool
}

func (s *inlineErasedControlStoreV2) checkBuffer() {
	s.checked, s.erased = true, len(s.reader.borrowed) > 0
	for _, b := range s.reader.borrowed {
		if b != 0 {
			s.erased = false
			break
		}
	}
}

func (s *inlineErasedControlStoreV2) PrepareAttachedWorkerInlineOutput(ctx context.Context, blobs ports.BlobStore, input attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.checkBuffer()
	if input.Candidate.Summary != s.want {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, io.ErrUnexpectedEOF
	}
	return s.inlineStoreFixtureV2.PrepareAttachedWorkerInlineOutput(ctx, blobs, input)
}

func (s *inlineErasedControlStoreV2) ReconcileAttachedWorkerInlineOutput(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	s.checkBuffer()
	return s.inlineStoreFixtureV2.ReconcileAttachedWorkerInlineOutput(ctx, input)
}

func TestInlineV2ErasesControlReadBufferBeforeStore(t *testing.T) {
	for _, path := range []string{PathInlinePrepareV2, PathInlineReconcileV2} {
		t.Run(path, func(t *testing.T) {
			service, base := inlineFixtureV2(t)
			answer := strings.Repeat("<", 15_808) + strings.Repeat("\u2028", 384) + strings.Repeat("<", 15_808)
			candidate := attachedworkeroutput.InlineCandidateV2{Summary: answer}
			if candidate.Validate() != nil {
				t.Fatal("maximum valid answer fixture")
			}
			expires := time.Unix(2_000_000_000, 0).UTC()
			var input any = attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: inlineAuthV2(), Candidate: candidate, ExpiresAt: expires}
			if path == PathInlineReconcileV2 {
				input = attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: inlineAuthV2()}
			}
			raw := inlineMarshalV2(t, input)
			defer clear(raw)
			reader := &inlineReadBufferObserverV2{Reader: bytes.NewReader(raw)}
			store := &inlineErasedControlStoreV2{inlineStoreFixtureV2: base, reader: reader, want: answer}
			service.store = store
			request := inlineControlRequestV2(t, path, raw)
			request.Body = reader
			writer := &inlineDeadlineWriterV2{ResponseRecorder: httptest.NewRecorder()}
			HandlerInlineV2(service).ServeHTTP(writer, request)
			if writer.Code != http.StatusOK || base.calls != 1 || !store.checked || !store.erased || len(reader.borrowed) != len(raw) {
				t.Fatalf("control buffer at store entry: status=%d calls=%d checked=%t erased=%t borrowed=%d wire=%d", writer.Code, base.calls, store.checked, store.erased, len(reader.borrowed), len(raw))
			}
		})
	}
}
