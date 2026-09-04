package codexexec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const (
	maxInstructionBytes = 1 << 20
	maxJSONLLineBytes   = 1 << 20
	maxJSONLOutputBytes = 16 << 20
	maxJSONLEvents      = 4096
	maxFinalBytes       = 256 << 10
	maxJSONDepth        = 32
)

type parseResult struct {
	accepted      bool
	terminal      bool
	protocolDrift bool
	terminalDrift bool
	eventCount    int
	stdoutBytes   int
	failureCode   string
	final         []byte
}

type eventEnvelope struct {
	Type string          `json:"type"`
	Item json.RawMessage `json:"item"`
}

type eventItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func parseJSONL(value []byte) parseResult {
	result := parseResult{}
	if len(value) > maxJSONLOutputBytes {
		return result.fail("stdout_limit_exceeded", false)
	}
	reader := bufio.NewReaderSize(bytes.NewReader(value), maxJSONLLineBytes)
	threadStarted := false
	for {
		line, err := reader.ReadSlice('\n')
		result.stdoutBytes += len(line)
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > maxJSONLLineBytes {
			return result.fail("jsonl_line_limit_exceeded", result.terminal)
		}
		if len(line) != 0 {
			if err != nil {
				return result.fail("jsonl_unterminated_frame", result.terminal)
			}
			line = bytes.TrimSuffix(line, []byte{'\n'})
			line = bytes.TrimSuffix(line, []byte{'\r'})
			if len(line) == 0 {
				return result.fail("jsonl_empty_frame", result.terminal)
			}
			result.eventCount++
			if result.eventCount > maxJSONLEvents {
				return result.fail("event_count_exceeded", result.terminal)
			}
			if result.terminal {
				return result.fail("event_after_terminal", true)
			}
			if !strictJSONObject(line) {
				return result.fail("invalid_jsonl_event", false)
			}
			var envelope eventEnvelope
			if json.Unmarshal(line, &envelope) != nil || envelope.Type == "" {
				return result.fail("invalid_jsonl_event", false)
			}
			switch envelope.Type {
			case "thread.started":
				if threadStarted || result.accepted {
					return result.fail("thread_started_out_of_order", false)
				}
				threadStarted = true
			case "turn.started":
				if !threadStarted {
					return result.fail("turn_started_before_thread", false)
				}
				if result.accepted {
					return result.fail("duplicate_turn_started", false)
				}
				result.accepted = true
			case "item.started":
				if !result.accepted {
					return result.fail("item_before_acceptance", false)
				}
				var item eventItem
				if len(envelope.Item) == 0 || !strictJSONObject(envelope.Item) ||
					json.Unmarshal(envelope.Item, &item) != nil ||
					(item.Type != "agent_message" && item.Type != "reasoning") {
					return result.fail("unexpected_effect_item", false)
				}
			case "item.completed":
				if !result.accepted {
					return result.fail("item_before_acceptance", false)
				}
				var item eventItem
				if len(envelope.Item) == 0 || !strictJSONObject(envelope.Item) ||
					json.Unmarshal(envelope.Item, &item) != nil || item.Type == "" {
					return result.fail("invalid_completed_item", false)
				}
				switch item.Type {
				case "agent_message":
					if len(result.final) != 0 || item.Text == "" || len(item.Text) > maxFinalBytes {
						return result.fail("invalid_final_agent_item", false)
					}
					result.final = append([]byte(nil), item.Text...)
				case "reasoning":
				default:
					return result.fail("unexpected_effect_item", false)
				}
			case "turn.completed":
				if !result.accepted || len(result.final) == 0 {
					return result.fail("terminal_without_final_agent_item", true)
				}
				if result.failureCode != "" {
					return result.fail("terminal_after_error", true)
				}
				result.terminal = true
			case "error":
				if result.failureCode == "" {
					result.failureCode = "codex_error_event"
				}
			default:
				return result.fail("unknown_jsonl_event", false)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return result
			}
			return result.fail("jsonl_read_failed", result.terminal)
		}
	}
}

func (result parseResult) fail(code string, terminalDrift bool) parseResult {
	if result.failureCode == "" {
		result.failureCode = code
	}
	result.protocolDrift = true
	result.terminalDrift = result.terminalDrift || terminalDrift || result.terminal
	return result
}

func strictJSONObject(value []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	if !scanJSONObject(decoder, 1) {
		return false
	}
	_, err = decoder.Token()
	return errors.Is(err, io.EOF)
}

func scanJSONObject(decoder *json.Decoder, depth int) bool {
	if depth > maxJSONDepth {
		return false
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		folded := strings.ToLower(name)
		if err != nil || !ok {
			return false
		}
		if _, exists := seen[folded]; exists {
			return false
		}
		seen[folded] = struct{}{}
		if !scanJSONValue(decoder, depth+1) {
			return false
		}
	}
	token, err := decoder.Token()
	return err == nil && token == json.Delim('}')
}

func scanJSONValue(decoder *json.Decoder, depth int) bool {
	if depth > maxJSONDepth {
		return false
	}
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return true
	}
	switch delim {
	case '{':
		return scanJSONObject(decoder, depth)
	case '[':
		for decoder.More() {
			if !scanJSONValue(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim(']')
	default:
		return false
	}
}
