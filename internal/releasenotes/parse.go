package releasenotes

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	fullSHA       = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	modernMerge   = regexp.MustCompile(`^Merge MR !([1-9][0-9]*):[[:space:]]*(.+)$`)
	legacyMerge   = regexp.MustCompile(`^!([1-9][0-9]*)[[:space:]]+merge[[:space:]]+(.+)$`)
	issueNumber   = regexp.MustCompile(`(?:^|[^[:alnum:]_/])#([1-9][0-9]*)`)
	markdownLink  = regexp.MustCompile(`\[[^\]\n]*\]\(([^)\n]+)\)`)
	relationLine  = regexp.MustCompile(`(?i)\b(close[sd]?|fix(?:e[sd])?|resolve[sd]?|implement(?:s|ed)?|relate[sd]?(?:[[:space:]]+to)?|continue[sd]?|track(?:s|ed)?(?:[[:space:]]+by)?|issues?|epic|architecture|follow-up(?:[[:space:]]+for)?|depends?[[:space:]]+on)\b[^\n]*`)
	featurePrefix = regexp.MustCompile(`(?i)^(?:\[[^]]+\][[:space:]]*)?(?:[A-Z]+-[0-9]+[: ]+)?(?:feat(?:ure)?(?:\([^)]*\))?[: ]+|add\b|build\b|create\b|define\b|deliver\b|deploy\b|establish\b|implement\b|introduce\b|persist\b|project\b|provision\b|support\b)`)
	fixPrefix     = regexp.MustCompile(`(?i)^(?:\[[^]]+\][[:space:]]*)?(?:[A-Z]+-[0-9]+[: ]+)?(?:fix(?:\([^)]*\))?[: ]+|address\b|bugfix\b|correct\b|harden\b|hotfix\b|repair\b|resolve\b|revert\b)`)
	infraPrefix   = regexp.MustCompile(`(?i)^(?:\[[^]]+\][[:space:]]*)?(?:ci|build|chore|docs?|infra|refactor|release|test)(?:\([^)]*\))?[: ]+`)
	productCode   = regexp.MustCompile(`(?i)^(?:\[[^]]+\][[:space:]]*)?(?:MVP|RUNTIME|SESSION|TELEGRAM|WEB)-[0-9]+(?:[A-Z])?[: ]+`)
)

func parseEntry(commit Commit) entry {
	title, mergeRequest := commitTitle(commit)
	return entry{
		Title:        title,
		SHA:          strings.ToLower(commit.SHA),
		MergeRequest: mergeRequest,
		Issues:       extractIssues(commit.Subject + "\n" + commit.Body),
		Category:     classify(title, commit.Paths),
	}
}

func commitTitle(commit Commit) (string, int) {
	subject := normalizeTitle(commit.Subject)
	if len(commit.Parents) > 1 {
		if matches := modernMerge.FindStringSubmatch(subject); matches != nil {
			number, _ := strconv.Atoi(matches[1])
			return normalizeTitle(matches[2]), number
		}
		if matches := legacyMerge.FindStringSubmatch(subject); matches != nil {
			number, _ := strconv.Atoi(matches[1])
			suffix := normalizeTitle(matches[2])
			if strings.HasSuffix(strings.ToLower(suffix), " into main") || strings.Contains(suffix, "/") {
				if bodyTitle := firstBodyTitle(commit.Body); bodyTitle != "" {
					return bodyTitle, number
				}
			}
			return suffix, number
		}
	}
	return subject, 0
}

func firstBodyTitle(body string) string {
	for _, line := range strings.Split(normalizeNewlines(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "-") || strings.HasPrefix(line, "*") {
			continue
		}
		return normalizeTitle(line)
	}
	return ""
}

func normalizeTitle(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func extractIssues(message string) []int {
	message = stripMarkdownCode(normalizeNewlines(message))
	seen := make(map[int]struct{})
	issueURL := regexp.MustCompile(regexp.QuoteMeta(gitCodeRepositoryURL) + `/issues/([1-9][0-9]*)(?:[^0-9]|$)`)
	for _, match := range issueURL.FindAllStringSubmatch(message, -1) {
		addIssue(seen, match[1])
	}
	message = markdownLink.ReplaceAllStringFunc(message, func(link string) string {
		matches := markdownLink.FindStringSubmatch(link)
		if len(matches) == 2 && strings.HasPrefix(matches[1], gitCodeRepositoryURL+"/issues/") {
			return link
		}
		return ""
	})
	for _, line := range relationLine.FindAllString(message, -1) {
		for _, match := range issueNumber.FindAllStringSubmatch(line, -1) {
			addIssue(seen, match[1])
		}
	}
	numbers := make([]int, 0, len(seen))
	for number := range seen {
		numbers = append(numbers, number)
	}
	sort.Ints(numbers)
	return numbers
}

func addIssue(seen map[int]struct{}, text string) {
	number, err := strconv.Atoi(text)
	if err == nil && number > 0 {
		seen[number] = struct{}{}
	}
}

func stripMarkdownCode(value string) string {
	var result strings.Builder
	inFence := false
	for _, line := range strings.Split(value, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		inInline := false
		for _, character := range line {
			if character == '`' {
				inInline = !inInline
				continue
			}
			if !inInline {
				result.WriteRune(character)
			}
		}
		result.WriteByte('\n')
	}
	return result.String()
}

func classify(title string, paths []string) category {
	switch {
	case fixPrefix.MatchString(title):
		return categoryFixes
	case featurePrefix.MatchString(title), productCode.MatchString(title):
		return categoryFeatures
	case infraPrefix.MatchString(title), infrastructureTitle(title), infrastructureOnly(paths):
		return categoryInfra
	default:
		return categoryOther
	}
}

func infrastructureTitle(title string) bool {
	lower := strings.ToLower(title)
	for _, term := range []string{
		"release note", "release workflow", "container registry", "deployment image", "runtime image",
		"garbage collection", "terraform",
	} {
		if strings.Contains(lower, term) {
			return true
		}
	}
	return false
}

func infrastructureOnly(paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		if path == "Makefile" || path == "README.md" || path == "CONTRIBUTING.md" || path == "LICENSE" {
			continue
		}
		matched := false
		for _, prefix := range []string{".github/", "build/", "docs/", "infra/", "scripts/", "tools/"} {
			if strings.HasPrefix(path, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
