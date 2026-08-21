package releasenotes

import (
	"bytes"
	"fmt"
	"strings"
)

func render(document document) []byte {
	var output bytes.Buffer
	fmt.Fprintf(&output, "# %s\n\n", escapeMarkdown(document.Tag))
	fmt.Fprintf(&output, "Source commit: [`%s`](%s/commit/%s)\n\n", document.SourceSHA, gitCodeRepositoryURL, document.SourceSHA)
	if document.PreviousTag == "" {
		fmt.Fprintf(&output, "Compared range: `repository root..%s`\n", document.Tag)
	} else {
		fmt.Fprintf(&output, "Compared range: `%s..%s`\n", document.PreviousTag, document.Tag)
	}
	if document.Supplement != "" {
		output.WriteString("\n## Maintainer notes\n\n")
		output.WriteString(document.Supplement)
		output.WriteByte('\n')
	}
	sections := []struct {
		title    string
		category category
	}{
		{"Features", categoryFeatures},
		{"Fixes", categoryFixes},
		{"Infrastructure and documentation", categoryInfra},
		{"Other", categoryOther},
	}
	for _, section := range sections {
		fmt.Fprintf(&output, "\n## %s\n\n", section.title)
		count := 0
		for _, item := range document.Entries {
			if item.Category != section.category {
				continue
			}
			writeEntry(&output, item)
			count++
		}
		if count == 0 {
			output.WriteString("_None._\n")
		}
	}
	return output.Bytes()
}

func writeEntry(output *bytes.Buffer, item entry) {
	fmt.Fprintf(output, "- %s (", escapeMarkdown(item.Title))
	separator := ""
	if item.MergeRequest > 0 {
		fmt.Fprintf(output, "[!%d](%s/merge_requests/%d)", item.MergeRequest, gitCodeRepositoryURL, item.MergeRequest)
		separator = ", "
	}
	shortSHA := item.SHA
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	fmt.Fprintf(output, "%s[`%s`](%s/commit/%s)", separator, shortSHA, gitCodeRepositoryURL, item.SHA)
	for _, issue := range item.Issues {
		fmt.Fprintf(output, ", [#%d](%s/issues/%d)", issue, gitCodeRepositoryURL, issue)
	}
	output.WriteString(")\n")
}

func escapeMarkdown(value string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"`", "\\`",
		"*", "\\*",
		"_", "\\_",
		"[", "\\[",
		"]", "\\]",
		"<", "\\<",
		">", "\\>",
	)
	return replacer.Replace(value)
}
