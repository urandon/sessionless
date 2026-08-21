package registrygc

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func WriteJSON(writer io.Writer, report Report) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func WriteMarkdown(writer io.Writer, report Report) error {
	if _, err := fmt.Fprintf(writer,
		"# Container Registry cleanup report\n\n"+
			"- Environment: `%s`\n- Registry: `%s`\n- Mode: `%s`\n"+
			"- Terraform: `%s` serial `%d` (`%s`)\n"+
			"- Images: %d; retained: %d; delete candidates: %d\n"+
			"- Deleted: %d; already absent: %d; complete: %t\n"+
			"- Estimated reclaimable bytes: %d\n\n"+
			"| Repository | Digest | Decision | Reason | Created at | Age seconds | Estimated bytes | Execution |\n"+
			"|---|---|---|---|---|---:|---:|---|\n",
		report.Environment, report.RegistryID, report.Mode,
		report.TerraformLineage, report.TerraformSerial, report.TerraformDigest,
		report.Summary.Images, report.Summary.Retained, report.Summary.DeleteCandidates,
		report.Summary.Deleted, report.Summary.AlreadyAbsent, report.Complete,
		report.Summary.EstimatedReclaimBytes,
	); err != nil {
		return err
	}
	for _, decision := range report.Decisions {
		reasons := make([]string, 0, len(decision.Reasons))
		for _, reason := range decision.Reasons {
			value := reason.Kind
			if reason.Reference != "" {
				value += ":" + reason.Reference
			}
			reasons = append(reasons, value)
		}
		if _, err := fmt.Fprintf(writer, "| `%s` | `%s` | %s | %s | `%s` | %d | %d | %s |\n",
			escapeMarkdown(decision.Repository), escapeMarkdown(decision.Digest),
			escapeMarkdown(decision.Decision), escapeMarkdown(strings.Join(reasons, ", ")),
			decision.CreatedAt.Format("2006-01-02T15:04:05Z"), decision.AgeSeconds, decision.CompressedSize,
			escapeMarkdown(decision.Execution)); err != nil {
			return err
		}
	}
	return nil
}

func escapeMarkdown(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}
