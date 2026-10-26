package attachedworkeroutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func inlineFixtureV2() (InlineCandidateV2, []byte) {
	texts := []string{"Useful original notes\n", " {\"n\":9007199254740993123456789,\"exp\":1e999} \n"}
	candidate := InlineCandidateV2{Summary: "ready"}
	var body []byte
	for i, text := range texts {
		digest := sha256.Sum256([]byte(text))
		candidate.Artifacts = append(candidate.Artifacts, InlineArtifactV2{
			Name: []string{"notes.txt", "report.json"}[i], MediaType: []string{"text/plain", "application/json"}[i],
			Size: uint64(len(text)), SHA256: hex.EncodeToString(digest[:]),
		})
		body = append(body, text...)
	}
	return candidate, body
}

func TestInlineMaterializeUsesOriginalBytesAndSameCanonicalCore(t *testing.T) {
	loaded, blobs, lease, at := outputFixture(t)
	candidate, body := inlineFixtureV2()
	verified, err := VerifyInlineV2(candidate, body)
	if err != nil {
		t.Fatal(err)
	}
	// Neither mutation of the wire buffer nor its declaration slice may change
	// the private, already-verified source.
	original := bytes.Clone(body)
	clear(body)
	candidate.Artifacts[0].Name = "changed.txt"
	result, err := MaterializeInlineV2(t.Context(), blobs, loaded, lease, 7, verified, at)
	if err != nil {
		t.Fatal(err)
	}
	if result.Completion == nil || len(result.Completion.Manifest.Artifacts) != 2 {
		t.Fatalf("useful canonical artifact result missing: %+v", result)
	}
	stored := Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "ready"}
	// V1 consumes actual source objects; V2 never fabricated these objects.
	offset := 0
	for i, artifact := range result.Completion.Manifest.Artifacts {
		size := int(artifact.Blob.Size)
		want := original[offset : offset+size]
		if !bytes.Equal(blobs.values[artifact.Blob.Key], want) || artifact.Name != []string{"notes.txt", "report.json"}[i] {
			t.Errorf("canonical artifact %d changed semantic bytes/name", i)
		}
		ref, err := blobs.Put(t.Context(), loaded.Run.TenantID, candidatePrefix(loaded)+artifact.Name, bytes.NewReader(want))
		if err != nil {
			t.Fatal(err)
		}
		stored.Artifacts = append(stored.Artifacts, ArtifactCandidate{Name: artifact.Name, MediaType: artifact.MediaType, Source: ref})
		offset += size
	}
	v1, err := Materialize(t.Context(), blobs, loaded, lease, 7, stored, at)
	if err != nil || !reflect.DeepEqual(v1, result) {
		t.Errorf("inline and storage canonical semantics differ: err=%v\nv1=%+v\nv2=%+v", err, v1, result)
	}
}

func TestInlineRejectsWholeSubmissionBeforeAnyWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*InlineCandidateV2, *[]byte)
	}{
		{"wrong hash", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[1].SHA256 = strings.Repeat("0", 64) }},
		{"trailing bytes", func(_ *InlineCandidateV2, b *[]byte) { *b = append(*b, 'x') }},
		{"missing bytes", func(_ *InlineCandidateV2, b *[]byte) { *b = (*b)[:len(*b)-1] }},
		{"reordered metadata", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0], c.Artifacts[1] = c.Artifacts[1], c.Artifacts[0] }},
		{"name collision", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[1].Name = "NOTES.TXT" }},
		{"path name", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].Name = "../notes.txt" }},
		{"control name", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].Name = "notes\n.txt" }},
		{"bad name unicode", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].Name = "\xff" }},
		{"wrong media type", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].MediaType = "image/png" }},
		{"empty body", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].Size = 0 }},
		{"oversized declaration", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].Size = MaxInlineArtifactBytesV2 + 1 }},
		{"overflow declaration", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts[0].Size = ^uint64(0) }},
		{"too many artifacts", func(c *InlineCandidateV2, _ *[]byte) { c.Artifacts = append(c.Artifacts, c.Artifacts[0]) }},
		{"empty answer", func(c *InlineCandidateV2, _ *[]byte) { c.Summary = " \n" }},
		{"answer byte ceiling", func(c *InlineCandidateV2, _ *[]byte) { c.Summary = strings.Repeat("🙂", 9000) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded, blobs, lease, at := outputFixture(t)
			candidate, body := inlineFixtureV2()
			tc.change(&candidate, &body)
			before := len(blobs.values)
			verified, err := VerifyInlineV2(candidate, body)
			if !errors.Is(err, ErrCandidateInvalid) {
				t.Fatalf("invalid inline submission accepted: err=%v", err)
			}
			if _, err := MaterializeInlineV2(t.Context(), blobs, loaded, lease, 7, verified, at); !errors.Is(err, ErrCandidateInvalid) {
				t.Errorf("failed verification minted materializable bytes: %v", err)
			}
			if len(blobs.values) != before {
				t.Error("partial invalid submission caused canonical write")
			}
		})
	}
}

func TestInlineRejectsInvalidSemanticContent(t *testing.T) {
	for _, tc := range []struct{ name, mediaType, body string }{
		{"invalid UTF8", "text/plain", "\xff"},
		{"NUL text", "text/plain", "a\x00b"},
		{"invalid JSON", "application/json", "{oops}"},
		{"JSON duplicate", "application/json", `{"x":1,"\u0078":2}`},
		{"JSON surrogate", "application/json", `"\ud800"`},
		{"JSON depth", "application/json", strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			digest := sha256.Sum256([]byte(tc.body))
			candidate := InlineCandidateV2{Summary: "ready", Artifacts: []InlineArtifactV2{{Name: "out.txt", MediaType: tc.mediaType, Size: uint64(len(tc.body)), SHA256: hex.EncodeToString(digest[:])}}}
			if _, err := VerifyInlineV2(candidate, []byte(tc.body)); !errors.Is(err, ErrCandidateInvalid) {
				t.Errorf("invalid original content accepted: %v", err)
			}
		})
	}
}

type inlineCountingBlobs struct {
	*memoryBlobs
	puts    int
	corrupt bool
}

func (b *inlineCountingBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, reader io.Reader) (domain.BlobRef, error) {
	b.puts++
	ref, err := b.memoryBlobs.Put(ctx, tenant, key, reader)
	if b.corrupt && err == nil {
		b.values[key] = []byte("corrupt canonical copy")
	}
	return ref, err
}

func TestInlineCanonicalScopeLimitsAndReread(t *testing.T) {
	for _, name := range []string{"scope", "artifact count", "byte budget", "cancel", "corrupt copy"} {
		t.Run(name, func(t *testing.T) {
			loaded, memory, lease, at := outputFixture(t)
			blobs := &inlineCountingBlobs{memoryBlobs: memory}
			candidate, body := inlineFixtureV2()
			verified, err := VerifyInlineV2(candidate, body)
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			switch name {
			case "scope":
				loaded.Job.TenantID = "foreign"
			case "artifact count":
				loaded.Job.Limits.MaxArtifacts = 1
			case "byte budget":
				loaded.Job.Limits.MaxInputBytes = 1
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "corrupt copy":
				blobs.corrupt = true
			}
			if _, err := MaterializeInlineV2(ctx, blobs, loaded, lease, 7, verified, at); !errors.Is(err, ErrCandidateInvalid) {
				t.Errorf("invalid canonical operation accepted: %v", err)
			}
			if name != "corrupt copy" && blobs.puts != 0 {
				t.Errorf("preflight failure dispatched %d writes", blobs.puts)
			}
			if name == "corrupt copy" && blobs.puts != 1 {
				t.Errorf("corrupt canonical reread did not stop at first Put: %d", blobs.puts)
			}
		})
	}
}

func TestInlineExactMaximumAndAnswerOnly(t *testing.T) {
	for _, count := range []int{0, 2} {
		loaded, blobs, lease, at := outputFixture(t)
		loaded.Job.Limits.MaxInputBytes = MaxInlineTotalBytesV2
		candidate := InlineCandidateV2{Summary: strings.Repeat("a", 32_000)}
		var body []byte
		for i := 0; i < count; i++ {
			content := bytes.Repeat([]byte("x"), MaxInlineArtifactBytesV2)
			digest := sha256.Sum256(content)
			candidate.Artifacts = append(candidate.Artifacts, InlineArtifactV2{Name: []string{"one.txt", "two.txt"}[i], MediaType: "text/plain", Size: uint64(len(content)), SHA256: hex.EncodeToString(digest[:])})
			body = append(body, content...)
		}
		verified, err := VerifyInlineV2(candidate, body)
		if err != nil {
			t.Fatalf("count %d exact maximum rejected: %v", count, err)
		}
		result, err := MaterializeInlineV2(t.Context(), blobs, loaded, lease, 7, verified, at)
		if err != nil || result.Completion == nil || len(result.Completion.Manifest.Artifacts) != count {
			t.Fatalf("count %d canonical maximum/answer-only failed: result=%+v err=%v", count, result, err)
		}
	}
}

func TestInlineRetirementInvalidatesCopiesBeforeWrite(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run([]string{"answer-only", "unused", "artifacts"}[count], func(t *testing.T) {
			loaded, memory, lease, at := outputFixture(t)
			blobs := &inlineCountingBlobs{memoryBlobs: memory}
			candidate, body := inlineFixtureV2()
			if count == 0 {
				candidate.Artifacts, body = nil, nil
			}
			verified, err := VerifyInlineV2(candidate, body)
			if err != nil {
				t.Fatal(err)
			}
			copy := verified
			verified.Clear()
			verified.Clear()
			for _, source := range []VerifiedInlineV2{verified, copy, {}} {
				if _, err := MaterializeInlineV2(t.Context(), blobs, loaded, lease, 7, source, at); !errors.Is(err, ErrCandidateInvalid) {
					t.Errorf("retired or forged source admitted: %v", err)
				}
			}
			if blobs.puts != 0 {
				t.Errorf("retired content dispatched %d writes", blobs.puts)
			}
		})
	}
}

func TestInlinePreservesNonNULControlsAndMaximumEscapedAnswer(t *testing.T) {
	loaded, blobs, lease, at := outputFixture(t)
	content := []byte("page one\fpage two\b\x7f\n")
	digest := sha256.Sum256(content)
	candidate := InlineCandidateV2{Summary: "x" + strings.Repeat("\x01", 31_999), Artifacts: []InlineArtifactV2{{Name: "controls.txt", MediaType: "text/plain", Size: uint64(len(content)), SHA256: hex.EncodeToString(digest[:])}}}
	encoded, err := json.Marshal(candidate)
	if err != nil || len(encoded) <= 128<<10 || len(encoded) > 256<<10 {
		t.Fatalf("maximum escaped answer fixture outside V2 wire budgets: size=%d err=%v", len(encoded), err)
	}
	verified, err := VerifyInlineV2(candidate, content)
	if err != nil {
		t.Fatalf("original UTF8/no-NUL semantics rejected: %v", err)
	}
	result, err := MaterializeInlineV2(t.Context(), blobs, loaded, lease, 7, verified, at)
	if err != nil || result.Completion == nil {
		t.Fatalf("escaped answer materialization failed: %v", err)
	}
	artifact := result.Completion.Manifest.Artifacts[0]
	if !bytes.Equal(blobs.values[artifact.Blob.Key], content) || result.Completion.Events[0].DisplayText != candidate.Summary {
		t.Error("original control bytes or maximum answer changed")
	}
}

type blockingInlineBlobs struct {
	*memoryBlobs
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingInlineBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, reader io.Reader) (domain.BlobRef, error) {
	b.once.Do(func() {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
	if err := ctx.Err(); err != nil {
		return domain.BlobRef{}, err
	}
	return b.memoryBlobs.Put(ctx, tenant, key, reader)
}

func TestInlineClearSerializesWithOriginalMaterialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	loaded, memory, lease, at := outputFixture(t)
	blobs := &blockingInlineBlobs{memoryBlobs: memory, entered: make(chan struct{}), release: make(chan struct{})}
	candidate, body := inlineFixtureV2()
	verified, err := VerifyInlineV2(candidate, body)
	if err != nil {
		t.Fatal(err)
	}
	copy := verified
	finished := make(chan error, 1)
	done := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blobs.release) }) }
	t.Cleanup(func() {
		cancel()
		release()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			t.Error("owned materialization goroutine did not stop during cleanup")
		}
	})
	go func() {
		defer close(done)
		_, err := MaterializeInlineV2(ctx, blobs, loaded, lease, 7, verified, at)
		finished <- err
	}()
	select {
	case <-blobs.entered:
	case <-ctx.Done():
		t.Fatalf("materialization never reached first Put: %v", ctx.Err())
	}
	// The first Put is parked before consuming its source. Its carrier must
	// remain exclusively owned until the complete canonical operation ends.
	if verified.owner.mu.TryLock() {
		verified.owner.mu.Unlock()
		t.Fatal("source ownership not held during canonical Put")
	}
	cleared := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		release()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-cleared:
		case <-timer.C:
			t.Error("owned retirement goroutine did not stop during cleanup")
		}
	})
	go func() { copy.Clear(); close(cleared) }()
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Errorf("original materialization changed during Clear: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("materialization failed to finish after release: %v", ctx.Err())
	}
	select {
	case <-cleared:
	case <-ctx.Done():
		t.Fatalf("Clear failed to retire after materialization: %v", ctx.Err())
	}
	if _, err := MaterializeInlineV2(t.Context(), blobs, loaded, lease, 7, verified, at); !errors.Is(err, ErrCandidateInvalid) {
		t.Errorf("cleared alias admitted: %v", err)
	}
}
