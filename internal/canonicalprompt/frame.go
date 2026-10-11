// Package canonicalprompt shares the exact initial Codex text renderer.
// Rendering proves neither canonical source identity nor execution permission.
package canonicalprompt

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	ContractV1      = "codex-text-v1"
	HeaderV1        = "Sessionless canonical transcript v1\n"
	MaxFrameBytesV1 = 1 << 20
	// This helper ceiling permits testing framing independently of the
	// driver's stricter 1 MiB FULL JSONL input limit. It does not enlarge that
	// limit or the future prepared-material constructor's payload ceiling.
	MaxPayloadBytesV1 = 2 << 20
)

var ErrInvalidFrame = errors.New("canonical prompt frame invalid")

// RenderFrameV1 returns caller-owned private bytes, without retaining or
// modifying payload. It preserves the original encoding/json decoder's field,
// duplicate-key, null and string-replacement semantics. Callers must separately
// verify immutable source/trigger coverage, cumulative header/frame bounds and
// all execution authority. The frame contains no token/cost estimate.
func RenderFrameV1(kind domain.SessionEventKind, payload []byte) ([]byte, error) {
	if len(payload) == 0 || len(payload) > MaxPayloadBytesV1 {
		return nil, ErrInvalidFrame
	}
	var role, text string
	switch kind {
	case domain.SessionEventUserMessage:
		var value struct {
			Version     uint32            `json:"version"`
			Origin      json.RawMessage   `json:"origin"`
			Text        string            `json:"text"`
			Metadata    map[string]string `json:"metadata"`
			Attachments []json.RawMessage `json:"attachments"`
		}
		if decodeExact(payload, &value) != nil || value.Version != 1 || len(value.Attachments) != 0 {
			return nil, ErrInvalidFrame
		}
		role, text = "user", value.Text
	case domain.SessionEventAssistantMessage:
		var value struct {
			Schema             string `json:"schema"`
			Summary            string `json:"summary"`
			ArtifactManifestID string `json:"artifact_manifest_id"`
		}
		if decodeExact(payload, &value) != nil || value.Schema != "sessionless.assistant-message.v1" {
			return nil, ErrInvalidFrame
		}
		role, text = "assistant", value.Summary
	default:
		return nil, ErrInvalidFrame
	}
	if text == "" || !utf8.ValidString(text) || strings.ContainsRune(text, 0) ||
		len(text) > MaxFrameBytesV1-len(role)-4 {
		return nil, ErrInvalidFrame
	}
	frame := make([]byte, 0, len(role)+4+len(text))
	frame = append(frame, '[')
	frame = append(frame, role...)
	frame = append(frame, ']', '\n')
	frame = append(frame, text...)
	return append(frame, '\n'), nil
}

func decodeExact(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return ErrInvalidFrame
	}
	return nil
}
