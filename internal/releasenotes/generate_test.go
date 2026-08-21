package releasenotes

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	shaC = "cccccccccccccccccccccccccccccccccccccccc"
	shaD = "dddddddddddddddddddddddddddddddddddddddd"
	shaE = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

type fakeRepository struct {
	resolved map[string]string
	history  []Commit
	tags     []VersionTag
	blobs    map[string][]byte
}

func (f *fakeRepository) ResolveCommit(_ context.Context, revision string) (string, error) {
	commit, ok := f.resolved[revision]
	if !ok {
		return "", errors.New("unknown revision " + revision)
	}
	return commit, nil
}

func (f *fakeRepository) FirstParentHistory(context.Context, string) ([]Commit, error) {
	return append([]Commit(nil), f.history...), nil
}

func (f *fakeRepository) VersionTags(context.Context) ([]VersionTag, error) {
	return append([]VersionTag(nil), f.tags...), nil
}

func (f *fakeRepository) BlobAt(_ context.Context, commit, path string) ([]byte, bool, error) {
	data, ok := f.blobs[commit+":"+path]
	return append([]byte(nil), data...), ok, nil
}

func TestGenerateGoldenAndDeterministic(t *testing.T) {
	t.Parallel()
	repository := releaseFixture()
	options := Options{Tag: "v1.2.0", SourceSHA: strings.ToUpper(shaD)}
	first, err := Generate(context.Background(), repository, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate(context.Background(), repository, options)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("same release evidence produced different notes")
	}
	want, err := os.ReadFile(filepath.Join("testdata", "v1.2.0.golden.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, want) {
		t.Fatalf("release notes differ from golden\n--- got ---\n%s\n--- want ---\n%s", first, want)
	}
}

func TestGenerateInitialReleaseIncludesRoot(t *testing.T) {
	t.Parallel()
	repository := &fakeRepository{
		resolved: map[string]string{"HEAD": shaA, "v0.1.0": shaA},
		history:  []Commit{{SHA: shaA, Subject: "feat: establish repository", Paths: []string{"go.mod"}}},
		blobs:    map[string][]byte{},
	}
	notes, err := Generate(context.Background(), repository, Options{Tag: "v0.1.0"})
	if err != nil {
		t.Fatal(err)
	}
	text := string(notes)
	if !strings.Contains(text, "Compared range: `repository root..v0.1.0`") ||
		!strings.Contains(text, "establish repository") {
		t.Fatalf("initial release omitted its root commit:\n%s", text)
	}
}

func TestGenerateRejectsExplicitTagOutsideFirstParent(t *testing.T) {
	t.Parallel()
	repository := releaseFixture()
	repository.resolved["v1.1.1"] = shaE
	_, err := Generate(context.Background(), repository, Options{Tag: "v1.2.0", PreviousTag: "v1.1.1"})
	if err == nil || !strings.Contains(err.Error(), "strict first-parent ancestor") {
		t.Fatalf("Generate error = %v", err)
	}
}

func TestGenerateRejectsHeadMismatch(t *testing.T) {
	t.Parallel()
	repository := releaseFixture()
	repository.resolved["HEAD"] = shaC
	_, err := Generate(context.Background(), repository, Options{Tag: "v1.2.0"})
	if err == nil || !strings.Contains(err.Error(), "does not match release tag") {
		t.Fatalf("Generate error = %v", err)
	}
}

func TestGenerateRejectsVersionRegressionAnywhereOnLineage(t *testing.T) {
	t.Parallel()
	repository := releaseFixture()
	repository.tags = append(repository.tags, VersionTag{Name: "v2.0.0", Commit: shaA})
	_, err := Generate(context.Background(), repository, Options{Tag: "v1.2.0"})
	if err == nil || !strings.Contains(err.Error(), "non-older tag v2.0.0") {
		t.Fatalf("Generate error = %v", err)
	}
}

func TestIssueReferencesExcludeCodeAndGitHubRunLinks(t *testing.T) {
	t.Parallel()
	message := "Closes #43 and relates to [#42](https://gitcode.com/urandon/sessionless/issues/42).\n" +
		"Verified by CI [#88](https://github.com/urandon/sessionless/actions/runs/88).\n" +
		"`Closes #99`\n```text\nFixes #100\n```"
	issues := extractIssues(message)
	if got, want := intsText(issues), "42,43"; got != want {
		t.Fatalf("issues = %s, want %s", got, want)
	}
}

func TestLocalRepositoryPeelsAnnotatedTagAndReadsTaggedBlob(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is unavailable")
	}
	root := t.TempDir()
	runGitTest(t, root, "init", "-q")
	runGitTest(t, root, "config", "user.name", "Release Test")
	runGitTest(t, root, "config", "user.email", "release@example.invalid")
	path := filepath.Join(root, ".github", "release-notes", "v1.0.0.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tagged supplement\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, root, "add", ".")
	runGitTest(t, root, "commit", "-q", "-m", "feat: initial release")
	runGitTest(t, root, "tag", "-a", "v1.0.0", "-m", "release")
	if err := os.WriteFile(path, []byte("dirty supplement\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repository, err := NewLocalRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := repository.ResolveCommit(context.Background(), "v1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	head, err := repository.ResolveCommit(context.Background(), "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if commit != head {
		t.Fatalf("annotated tag peeled to %s, want %s", commit, head)
	}
	data, found, err := repository.BlobAt(context.Background(), commit, ".github/release-notes/v1.0.0.md")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(data) != "tagged supplement\n" {
		t.Fatalf("BlobAt = %q, %t", data, found)
	}
	history, err := repository.FirstParentHistory(context.Background(), commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].SHA != commit {
		t.Fatalf("history = %+v", history)
	}
}

func releaseFixture() *fakeRepository {
	supplementPath := shaD + ":.github/release-notes/v1.2.0.md"
	return &fakeRepository{
		resolved: map[string]string{"HEAD": shaD, "v1.2.0": shaD, "v1.1.0": shaA},
		history: []Commit{
			{SHA: shaA, Subject: "feat: previous release", Paths: []string{"go.mod"}},
			{
				SHA: shaB, Parents: []string{shaA, shaE},
				Subject: "Merge MR !40: Build responsive [WebUI]",
				Body:    "Implements #43 and [#30](https://gitcode.com/urandon/sessionless/issues/30).",
				Paths:   []string{"web/src/routes/+page.svelte"},
			},
			{
				SHA: shaC, Parents: []string{shaB, shaE},
				Subject: "!35 merge ai/fix-auth into main",
				Body:    "Fix login redirect\n\nCloses #31.\nVerified by CI [#9](https://github.com/urandon/sessionless/actions/runs/9).",
				Paths:   []string{"internal/webbff/handler.go"},
			},
			{SHA: shaD, Parents: []string{shaC}, Subject: "docs: Explain releases", Paths: []string{"docs/development.md"}},
		},
		tags:  []VersionTag{{Name: "v1.1.0", Commit: shaA}, {Name: "v1.2.0", Commit: shaD}},
		blobs: map[string][]byte{supplementPath: []byte("Reviewed highlights\r\n\r\nShip safely.\r\n")},
	}
}

func intsText(values []int) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}

func runGitTest(t *testing.T, root string, arguments ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	command.Env = append(os.Environ(), "LC_ALL=C", "TZ=UTC")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", arguments, err, output)
	}
}
