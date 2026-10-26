package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// These independent Marshal oracles exercise persistence DATA and exact output
// capacity, not actual producer authority, SQL ownership or whole-phase memory.
func originalInlineStoredMarshalV2(t *testing.T, r AttachedWorkerOutputReceiptV2) []byte {
	t.Helper()
	if c := r.Materialization.Completion; c != nil {
		clone := *c
		clone.Events = append([]domain.SessionEventDraft(nil), c.Events...)
		for i := range clone.Events {
			clone.Events[i].DisplayText = ""
		}
		r.Materialization.Completion = &clone
	}
	raw, err := json.Marshal(storedInlineReceiptV2{PersistenceEncoding: inlineReceiptPersistenceEncodingV2, AttachedWorkerOutputReceiptV2: r})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func requireInlineSQLEncodingV2(t *testing.T, r AttachedWorkerOutputReceiptV2) {
	t.Helper()
	before, err := json.Marshal(r)
	if err != nil || len(before) > maxInlineReceiptRecordBytesV2 {
		t.Fatalf("original full-domain fixture size=%d err=%v", len(before), err)
	}
	defer clear(before)
	want := originalInlineStoredMarshalV2(t, r)
	defer clear(want)
	plan, err := planInlineReceiptEncodingV2(r)
	if err != nil {
		t.Fatal("plan original valid DATA", err)
	}
	defer clear(plan.metadata)
	if plan.fullBytes != len(before) || plan.storedBytes != len(want) {
		t.Fatalf("exact byte accounting full=%d/%d stored=%d/%d", plan.fullBytes, len(before), plan.storedBytes, len(want))
	}
	got, err := encodeInlineReceiptSQLBytesV2(r)
	if err != nil {
		t.Fatal("encode original valid DATA", err)
	}
	defer clear(got)
	if len(got) != cap(got) || !bytes.Equal(got, want) || len(got) > maxInlineReceiptStoredBytesV2 || got[len(got)-1] != '}' {
		t.Fatalf("SQL output not exact owned Marshal bytes: len=%d cap=%d want=%d", len(got), cap(got), len(want))
	}
	projection, err := encodeInlineReceiptProjectionV2(r)
	if err != nil {
		t.Fatal("existing projection API denied valid DATA", err)
	}
	projected, err := json.Marshal(projection)
	defer clear(projected)
	if err != nil || !bytes.Equal(projected, want) {
		t.Fatal("existing projection API changed", err)
	}
	restored, err := decodeInlineStoredReceiptV2(got)
	if err != nil || !sameInlineReceiptV2(r, restored) {
		t.Fatal("SQL bytes lost original DATA identity", err)
	}
	// Destroying the returned owned SQL parameter cannot damage borrowed input.
	clear(got)
	after, err := json.Marshal(r)
	defer clear(after)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("encoding/clearing output mutated caller's receipt", err)
	}
}

func TestInlineSQLEncodingV2AllStagesAndContainerShapes(t *testing.T) {
	prepared, _, _ := inlineReceiptFixtureV2(t)
	claimed, retired := prepared, prepared
	claimed.Stage, claimed.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, strings.Repeat("c", 64)
	retired.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	copying := inlineCanonicalFixtureV2(t)
	ready := inlineCanonicalFixtureV2(t)
	ready.Stage = attachedworkeroutput.InlineReadyV2
	for _, r := range []AttachedWorkerOutputReceiptV2{prepared, claimed, retired, copying, ready} {
		t.Run(string(r.Stage), func(t *testing.T) { requireInlineSQLEncodingV2(t, r) })
	}
	t.Run("empty-artifacts", func(t *testing.T) {
		r := prepared
		r.Candidate.Artifacts = []attachedworkeroutput.InlineArtifactV2{}
		resealInlineCodecFixtureV2(t, &r)
		requireInlineSQLEncodingV2(t, r)
	})
	t.Run("empty-usage-and-substrate-DATA", func(t *testing.T) {
		r := inlineCanonicalFixtureV2(t)
		r.Materialization.Completion.Usage = []domain.UsageObservation{}
		r.Materialization.Completion.SubstrateEvidence = &domain.SubstrateExecutionEvidenceV1{Version: 1}
		resealInlineCodecFixtureV2(t, &r)
		requireInlineSQLEncodingV2(t, r)
	})
}

func TestInlineSQLEncodingV2EscapingMaximumAndCompatibilityBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(*testing.T) AttachedWorkerOutputReceiptV2
	}{
		{name: "maximum-answer", make: maximumInlineIdentityFixtureV2},
		{name: "maximum-answer-two-artifacts", make: maximumInlineArtifactSQLFixtureV2},
		{name: "old-512KiB-Usage-DATA", make: nearLimitInlineCodecFixtureV2},
	} {
		t.Run(tc.name, func(t *testing.T) { requireInlineSQLEncodingV2(t, tc.make(t)) })
	}
	for _, answer := range []string{" \tOriginal answer\n", "<>&\\\"\b\f\n\r\t\x01\x1f\u2028\u2029🙂", inlineReceiptHashMarkerV2, strings.Repeat("🙂", 8192)} {
		t.Run(fmt.Sprintf("answer-bytes-%d", len(answer)), func(t *testing.T) {
			r := inlineCanonicalFixtureV2(t)
			r.Candidate.Summary = answer
			r.Materialization.Completion.Events[0].DisplayText = strings.TrimSpace(answer)
			resealInlineCodecFixtureV2(t, &r)
			requireInlineSQLEncodingV2(t, r)
		})
	}
	t.Run("timestamp-precision-and-offset", func(t *testing.T) {
		r := inlineCanonicalFixtureV2(t)
		zone := time.FixedZone("fixture", 23*3600+59*60)
		r.CreatedAt, r.ExpiresAt, r.Producer.AcceptedAt = r.CreatedAt.In(zone), r.ExpiresAt.In(zone), r.Producer.AcceptedAt.In(zone)
		c := r.Materialization.Completion
		c.At, c.Manifest.CreatedAt, c.Events[0].CreatedAt = c.At.In(zone), c.Manifest.CreatedAt.In(zone), c.Events[0].CreatedAt.In(zone)
		resealInlineCodecFixtureV2(t, &r)
		requireInlineSQLEncodingV2(t, r)
	})
}

func TestInlineSQLEncodingV2RejectsOriginalOverflowAndContradiction(t *testing.T) {
	t.Run("old-limit-plus-one", func(t *testing.T) {
		r := nearLimitInlineCodecFixtureV2(t)
		c := r.Materialization.Completion
		c.Usage[len(c.Usage)-1].ID += "u"
		resealInlineCodecFixtureV2(t, &r)
		before, err := json.Marshal(r)
		if err != nil || len(before) != maxInlineReceiptRecordBytesV2+1 {
			t.Fatalf("overflow control size=%d err=%v", len(before), err)
		}
		defer clear(before)
		if got, err := encodeInlineReceiptSQLBytesV2(r); got != nil || !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
			t.Fatal("SQL encoder hid full-domain overflow", err)
		}
		if _, err := encodeInlineReceiptProjectionV2(r); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
			t.Fatal("projection API hid full-domain overflow", err)
		}
		after, err := json.Marshal(r)
		defer clear(after)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("overflow denial mutated caller", err)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(*AttachedWorkerOutputReceiptV2)
	}{
		{name: "display-mismatch", mutate: func(r *AttachedWorkerOutputReceiptV2) {
			r.Materialization.Completion.Events[0].DisplayText = "contrary"
		}},
		{name: "digest-mismatch", mutate: func(r *AttachedWorkerOutputReceiptV2) {
			r.CanonicalDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("0", 64))
		}},
		{name: "invalid-time", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.ExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{name: "invalid-unicode", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Candidate.Summary = "bad\xff" }},
		{name: "embedded-nul", mutate: func(r *AttachedWorkerOutputReceiptV2) { r.Candidate.Summary = "bad\x00answer" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := inlineCanonicalFixtureV2(t)
			tc.mutate(&r)
			summary, display, expiry, digest := r.Candidate.Summary, r.Materialization.Completion.Events[0].DisplayText, r.ExpiresAt, r.CanonicalDigest
			if got, err := encodeInlineReceiptSQLBytesV2(r); got != nil || !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Fatal("projection hid original contradiction", err)
			}
			if r.Candidate.Summary != summary || r.Materialization.Completion.Events[0].DisplayText != display || r.ExpiresAt != expiry || r.CanonicalDigest != digest {
				t.Fatal("denial mutated the original contradictory receipt")
			}
		})
	}
}

type inlineReceiptEncodeFaultWriterV2 struct {
	calls  int
	failAt int
	err    error
}

func (w *inlineReceiptEncodeFaultWriterV2) Write(data []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		if w.err != nil {
			return 0, w.err
		}
		return len(data) - 1, nil
	}
	return len(data), nil
}

func TestInlineSQLEncodingV2FixedWriterAndFailurePropagation(t *testing.T) {
	plan, err := planInlineReceiptEncodingV2(inlineCanonicalFixtureV2(t))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(plan.metadata)
	for _, size := range []int{0, plan.storedBytes - 1, plan.storedBytes} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			owned := make([]byte, size)
			defer clear(owned)
			w := inlineReceiptFixedWriterV2{buffer: owned}
			err := plan.writeTo(&w)
			if size == plan.storedBytes {
				if err != nil || w.written != size {
					t.Fatalf("exact writer rejected full output: written=%d size=%d err=%v", w.written, size, err)
				}
			} else if !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("undersized writer did not deny without growth: %v", err)
			}
			if len(w.buffer) != size || cap(w.buffer) != size {
				t.Fatal("fixed writer grew its owned allocation")
			}
		})
	}
	for _, phase := range []int{1, 2, 3} {
		for _, cause := range []error{nil, context.Canceled} {
			t.Run(fmt.Sprintf("phase-%d-cancel-%t", phase, cause != nil), func(t *testing.T) {
				w := inlineReceiptEncodeFaultWriterV2{failAt: phase, err: cause}
				want := cause
				if want == nil {
					want = io.ErrShortWrite
				}
				if err := plan.writeTo(&w); !errors.Is(err, want) || w.calls != phase {
					t.Fatalf("failure lost or continued writing: phase=%d calls=%d got=%v want=%v", phase, w.calls, err, want)
				}
			})
		}
	}
}
