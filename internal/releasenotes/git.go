package releasenotes

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"gitcode.com/urandon/sessionless/internal/releaseversion"
)

// LocalRepository reads an existing Git checkout without network access.
type LocalRepository struct {
	root string
}

// NewLocalRepository creates a local Git reader rooted at path.
func NewLocalRepository(path string) (*LocalRepository, error) {
	root, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	return &LocalRepository{root: root}, nil
}

func (r *LocalRepository) ResolveCommit(ctx context.Context, revision string) (string, error) {
	if revision == "" || strings.HasPrefix(revision, "-") {
		return "", fmt.Errorf("invalid Git revision %q", revision)
	}
	output, err := r.git(ctx, "rev-parse", "--verify", "--end-of-options", revision+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve Git revision %q: %w", revision, err)
	}
	sha := strings.TrimSpace(string(output))
	if !fullSHA.MatchString(sha) {
		return "", fmt.Errorf("Git revision %q resolved to invalid commit %q", revision, sha)
	}
	return strings.ToLower(sha), nil
}

func (r *LocalRepository) FirstParentHistory(ctx context.Context, commit string) ([]Commit, error) {
	output, err := r.git(ctx, "rev-list", "--first-parent", "--reverse", commit)
	if err != nil {
		return nil, fmt.Errorf("read first-parent history: %w", err)
	}
	lines := strings.Fields(string(output))
	commits := make([]Commit, 0, len(lines))
	for _, sha := range lines {
		item, err := r.readCommit(ctx, sha)
		if err != nil {
			return nil, err
		}
		commits = append(commits, item)
	}
	return commits, nil
}

func (r *LocalRepository) VersionTags(ctx context.Context) ([]VersionTag, error) {
	output, err := r.git(ctx, "tag", "--list", "v*")
	if err != nil {
		return nil, fmt.Errorf("list release tags: %w", err)
	}
	names := strings.Fields(string(output))
	sort.Strings(names)
	tags := make([]VersionTag, 0, len(names))
	for _, name := range names {
		if _, err := releaseversion.ParseTag(name); err != nil {
			continue
		}
		commit, err := r.ResolveCommit(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("peel release tag %q: %w", name, err)
		}
		tags = append(tags, VersionTag{Name: name, Commit: commit})
	}
	return tags, nil
}

func (r *LocalRepository) BlobAt(ctx context.Context, commit, path string) ([]byte, bool, error) {
	if path == "" || strings.HasPrefix(path, "-") || filepath.IsAbs(path) {
		return nil, false, fmt.Errorf("invalid Git tree path %q", path)
	}
	output, err := r.git(ctx, "ls-tree", "-z", commit, "--", path)
	if err != nil {
		return nil, false, fmt.Errorf("inspect %s at %s: %w", path, commit, err)
	}
	if len(output) == 0 {
		return nil, false, nil
	}
	records := bytes.Split(bytes.TrimSuffix(output, []byte{0}), []byte{0})
	if len(records) != 1 {
		return nil, false, fmt.Errorf("expected one Git object for %s, got %d", path, len(records))
	}
	metadata, _, ok := bytes.Cut(records[0], []byte{'\t'})
	if !ok {
		return nil, false, fmt.Errorf("malformed Git tree entry for %s", path)
	}
	fields := strings.Fields(string(metadata))
	if len(fields) != 3 || fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755") {
		return nil, false, fmt.Errorf("release-note supplement %s is not a regular file", path)
	}
	data, err := r.git(ctx, "cat-file", "blob", fields[2])
	if err != nil {
		return nil, false, fmt.Errorf("read %s at %s: %w", path, commit, err)
	}
	if len(data) > maxSupplementBytes {
		return nil, false, fmt.Errorf("release-note supplement %s exceeds %d bytes", path, maxSupplementBytes)
	}
	return data, true, nil
}

func (r *LocalRepository) readCommit(ctx context.Context, sha string) (Commit, error) {
	output, err := r.git(ctx, "show", "-s", "--format=%H%x00%P%x00%s%x00%B%x00", sha)
	if err != nil {
		return Commit{}, fmt.Errorf("read commit %s: %w", sha, err)
	}
	fields := bytes.SplitN(output, []byte{0}, 5)
	if len(fields) != 5 {
		return Commit{}, fmt.Errorf("malformed Git metadata for commit %s", sha)
	}
	item := Commit{
		SHA:     strings.ToLower(string(fields[0])),
		Parents: strings.Fields(string(fields[1])),
		Subject: string(fields[2]),
		Body:    strings.TrimSuffix(string(fields[3]), "\n"),
	}
	var paths []byte
	if len(item.Parents) == 0 {
		paths, err = r.git(ctx, "diff-tree", "--root", "--no-commit-id", "--name-only", "-r", "-z", sha)
	} else {
		paths, err = r.git(ctx, "diff", "--no-ext-diff", "--name-only", "-z", item.Parents[0], sha)
	}
	if err != nil {
		return Commit{}, fmt.Errorf("read changed paths for commit %s: %w", sha, err)
	}
	for _, path := range bytes.Split(bytes.TrimSuffix(paths, []byte{0}), []byte{0}) {
		if len(path) != 0 {
			item.Paths = append(item.Paths, string(path))
		}
	}
	return item, nil
}

func (r *LocalRepository) git(ctx context.Context, arguments ...string) ([]byte, error) {
	args := append([]string{"-C", r.root}, arguments...)
	command := exec.CommandContext(ctx, "git", args...)
	command.Env = append(command.Environ(), "LC_ALL=C", "TZ=UTC")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return nil, errors.New(message)
	}
	return output, nil
}
