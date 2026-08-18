package domain

import (
	"strings"
	"time"
)

const (
	MaxSessionTitleRunes   = 120
	MaxSessionPreviewRunes = 280
)

// SessionDisplay is a bounded materialization for list views. Canonical event
// payloads remain authoritative; this row contains no attachment content and
// can be rebuilt from the append-only stream.
type SessionDisplay struct {
	TenantID      TenantID   `json:"tenant_id"`
	SessionID     SessionID  `json:"session_id"`
	Title         string     `json:"title,omitempty"`
	Preview       string     `json:"preview,omitempty"`
	Origin        *Frontend  `json:"origin,omitempty"`
	CurrentRunID  *RunID     `json:"current_run_id,omitempty"`
	CurrentStatus *RunStatus `json:"current_run_status,omitempty"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (display SessionDisplay) Validate() error {
	if err := display.TenantID.Validate(); err != nil {
		return err
	}
	if err := display.SessionID.Validate(); err != nil {
		return err
	}
	if display.Origin != nil {
		if err := display.Origin.Validate(); err != nil {
			return err
		}
	}
	if display.CurrentRunID != nil {
		if err := display.CurrentRunID.Validate(); err != nil {
			return err
		}
		if display.CurrentStatus == nil || !display.CurrentStatus.Valid() {
			return ValidationError{Field: "session_display.current_run_status", Reason: "is required with a current run"}
		}
	} else if display.CurrentStatus != nil {
		return ValidationError{Field: "session_display.current_run", Reason: "status requires a run ID"}
	}
	if display.UpdatedAt.IsZero() {
		return ValidationError{Field: "session_display.updated_at", Reason: "must not be zero"}
	}
	if len([]rune(display.Title)) > MaxSessionTitleRunes || len([]rune(display.Preview)) > MaxSessionPreviewRunes {
		return ValidationError{Field: "session_display", Reason: "title or preview exceeds its bounded length"}
	}
	return nil
}

func BoundedSessionText(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return strings.TrimSpace(string(runes[:limit]))
}
