// Package skeleton provides explicit placeholder processes for components whose
// production behavior belongs to later implementation issues.
package skeleton

import (
	"encoding/json"
	"fmt"
	"io"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
)

// Run writes one machine-readable readiness event and exits.
func Run(component string, output io.Writer) error {
	event := struct {
		Status string         `json:"status"`
		Build  buildinfo.Info `json:"build"`
	}{
		Status: "skeleton_ready",
		Build:  buildinfo.Current(component),
	}
	if err := json.NewEncoder(output).Encode(event); err != nil {
		return fmt.Errorf("encode readiness event: %w", err)
	}
	return nil
}
