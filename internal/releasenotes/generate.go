package releasenotes

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/releaseversion"
)

// Generate renders deterministic Markdown for opts.Tag from repository-local
// Git evidence. The tag and HEAD must identify the same commit.
func Generate(ctx context.Context, repository Repository, opts Options) ([]byte, error) {
	currentVersion, err := releaseversion.ParseTag(opts.Tag)
	if err != nil {
		return nil, err
	}
	currentCommit, err := repository.ResolveCommit(ctx, opts.Tag)
	if err != nil {
		return nil, err
	}
	headCommit, err := repository.ResolveCommit(ctx, "HEAD")
	if err != nil {
		return nil, err
	}
	if headCommit != currentCommit {
		return nil, fmt.Errorf("checked-out HEAD %s does not match release tag %s at %s", headCommit, opts.Tag, currentCommit)
	}
	if opts.SourceSHA != "" {
		if !fullSHA.MatchString(opts.SourceSHA) {
			return nil, fmt.Errorf("source SHA must be a full 40-character hexadecimal commit")
		}
		if !strings.EqualFold(opts.SourceSHA, currentCommit) {
			return nil, fmt.Errorf("source SHA %s does not match release tag %s at %s", opts.SourceSHA, opts.Tag, currentCommit)
		}
	}
	history, err := repository.FirstParentHistory(ctx, currentCommit)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 || !strings.EqualFold(history[len(history)-1].SHA, currentCommit) {
		return nil, fmt.Errorf("release commit %s is missing from its first-parent history", currentCommit)
	}
	tags, err := repository.VersionTags(ctx)
	if err != nil {
		return nil, err
	}
	previousTag, previousIndex, err := selectPrevious(ctx, repository, opts.PreviousTag, currentVersion, history, tags)
	if err != nil {
		return nil, err
	}
	start := previousIndex + 1
	entries := make([]entry, 0, len(history)-start)
	for _, commit := range history[start:] {
		entries = append(entries, parseEntry(commit))
	}
	supplementPath := ".github/release-notes/" + opts.Tag + ".md"
	supplementBytes, found, err := repository.BlobAt(ctx, currentCommit, supplementPath)
	if err != nil {
		return nil, err
	}
	supplement := ""
	if found {
		if len(supplementBytes) > maxSupplementBytes {
			return nil, fmt.Errorf("release-note supplement %s exceeds %d bytes", supplementPath, maxSupplementBytes)
		}
		if !utf8.Valid(supplementBytes) || strings.IndexByte(string(supplementBytes), 0) >= 0 {
			return nil, fmt.Errorf("release-note supplement %s is not valid UTF-8 text", supplementPath)
		}
		supplement = strings.TrimSpace(normalizeNewlines(string(supplementBytes)))
	}
	return render(document{
		Tag: opts.Tag, SourceSHA: currentCommit, PreviousTag: previousTag,
		Supplement: supplement, Entries: entries,
	}), nil
}

func selectPrevious(
	ctx context.Context,
	repository Repository,
	explicit string,
	current releaseversion.Version,
	history []Commit,
	tags []VersionTag,
) (string, int, error) {
	positions := make(map[string]int, len(history))
	for index, commit := range history {
		positions[strings.ToLower(commit.SHA)] = index
	}
	if explicit != "" {
		version, err := releaseversion.ParseTag(explicit)
		if err != nil {
			return "", -1, fmt.Errorf("invalid previous release tag: %w", err)
		}
		if version.Compare(current) >= 0 {
			return "", -1, fmt.Errorf("previous release tag %s is not older than the current release", explicit)
		}
		commit, err := repository.ResolveCommit(ctx, explicit)
		if err != nil {
			return "", -1, err
		}
		index, ok := positions[strings.ToLower(commit)]
		if !ok || index == len(history)-1 {
			return "", -1, fmt.Errorf("previous release tag %s is not a strict first-parent ancestor", explicit)
		}
		return explicit, index, nil
	}
	tagsByCommit := make(map[string][]VersionTag)
	for _, tag := range tags {
		tagsByCommit[strings.ToLower(tag.Commit)] = append(tagsByCommit[strings.ToLower(tag.Commit)], tag)
	}
	for index := 0; index < len(history)-1; index++ {
		for _, tag := range tagsByCommit[strings.ToLower(history[index].SHA)] {
			version, err := releaseversion.ParseTag(tag.Name)
			if err == nil && version.Compare(current) >= 0 {
				return "", -1, fmt.Errorf("release lineage contains non-older tag %s before the current release", tag.Name)
			}
		}
	}
	for index := len(history) - 2; index >= 0; index-- {
		var selected string
		var selectedVersion releaseversion.Version
		for _, tag := range tagsByCommit[strings.ToLower(history[index].SHA)] {
			version, err := releaseversion.ParseTag(tag.Name)
			if err != nil {
				continue
			}
			if selected == "" || version.Compare(selectedVersion) > 0 {
				selected, selectedVersion = tag.Name, version
			}
		}
		if selected != "" {
			return selected, index, nil
		}
	}
	return "", -1, nil
}

func normalizeNewlines(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	return strings.ReplaceAll(value, "\r", "\n")
}
