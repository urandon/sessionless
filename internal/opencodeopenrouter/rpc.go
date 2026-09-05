package opencodeopenrouter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

type rpcResult struct {
	accepted       bool
	terminal       bool
	providerFailed bool
	protocolDrift  bool
	eventCount     int
	failureCode    string
	final          []byte
	inputTokens    *uint64
	outputTokens   *uint64
}

type rpcEnvelope struct {
	Type      string          `json:"type"`
	Timestamp uint64          `json:"timestamp"`
	SessionID string          `json:"sessionID"`
	Part      json.RawMessage `json:"part,omitempty"`
	Error     json.RawMessage `json:"error,omitempty"`
}

type partKind struct {
	Type string `json:"type"`
}

type stepStartPart struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionID"`
	MessageID string `json:"messageID"`
	Type      string `json:"type"`
	Snapshot  string `json:"snapshot,omitempty"`
}

type textPart struct {
	ID        string          `json:"id"`
	SessionID string          `json:"sessionID"`
	MessageID string          `json:"messageID"`
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Synthetic bool            `json:"synthetic,omitempty"`
	Ignored   bool            `json:"ignored,omitempty"`
	Time      *partTime       `json:"time,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type partTime struct {
	Start uint64  `json:"start"`
	End   *uint64 `json:"end,omitempty"`
}

type stepFinishPart struct {
	ID        string      `json:"id"`
	SessionID string      `json:"sessionID"`
	MessageID string      `json:"messageID"`
	Type      string      `json:"type"`
	Reason    string      `json:"reason"`
	Snapshot  string      `json:"snapshot,omitempty"`
	Cost      json.Number `json:"cost"`
	Tokens    *tokenUsage `json:"tokens"`
}

type tokenUsage struct {
	Total     *uint64     `json:"total,omitempty"`
	Input     *uint64     `json:"input"`
	Output    *uint64     `json:"output"`
	Reasoning *uint64     `json:"reasoning"`
	Cache     *cacheUsage `json:"cache"`
}

type cacheUsage struct {
	Read  uint64 `json:"read"`
	Write uint64 `json:"write"`
}

type rpcLimits struct {
	outputBytes int
	lineBytes   int
	events      int
	finalBytes  int
	jsonDepth   int
}

var productionRPCLimits = rpcLimits{
	outputBytes: maxOutputBytes,
	lineBytes:   maxLineBytes,
	events:      maxEvents,
	finalBytes:  maxFinalBytes,
	jsonDepth:   maxJSONDepth,
}

func parseOpenCodeJSONL(value []byte) rpcResult {
	return parseOpenCodeJSONLWithLimits(value, productionRPCLimits)
}

func parseOpenCodeJSONLWithLimits(value []byte, limits rpcLimits) rpcResult {
	result := rpcResult{}
	if limits.outputBytes <= 0 || limits.lineBytes <= 0 || limits.events <= 0 || limits.finalBytes <= 0 || limits.jsonDepth <= 0 ||
		len(value) == 0 || len(value) > limits.outputBytes {
		return result.fail("stdout_limit")
	}
	reader := bufio.NewReaderSize(bytes.NewReader(value), limits.lineBytes)
	seenParts := map[string]struct{}{}
	sessionID, messageID := "", ""
	stepStarted, stepFinished := false, false
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > limits.lineBytes {
			return result.fail("line_limit")
		}
		if len(line) != 0 {
			if err != nil {
				return result.fail("unterminated_frame")
			}
			line = bytes.TrimSuffix(line, []byte{'\n'})
			line = bytes.TrimSuffix(line, []byte{'\r'})
			if len(line) == 0 || !strictJSONObjectWithDepth(line, limits.jsonDepth) {
				return result.fail("invalid_frame")
			}
			result.eventCount++
			if result.eventCount > limits.events || result.terminal {
				return result.fail("event_boundary")
			}
			var envelope rpcEnvelope
			if decodeExact(line, &envelope) != nil || envelope.Timestamp == 0 || envelope.Timestamp > 1<<53 ||
				!validProtocolID(envelope.SessionID) || (sessionID != "" && envelope.SessionID != sessionID) {
				return result.fail("invalid_event")
			}
			if sessionID == "" {
				sessionID = envelope.SessionID
			}
			switch envelope.Type {
			case "step_start":
				if stepStarted || stepFinished || len(envelope.Error) != 0 {
					return result.fail("step_start_order")
				}
				var part stepStartPart
				if decodePart(envelope.Part, "step-start", &part, limits.jsonDepth) != nil ||
					part.SessionID != sessionID || !validProtocolID(part.MessageID) || !recordPartID(seenParts, part.ID) ||
					len(part.Snapshot) > 256 {
					return result.fail("step_start_invalid")
				}
				messageID, stepStarted, result.accepted = part.MessageID, true, true
			case "text":
				if !stepStarted || stepFinished || len(envelope.Error) != 0 {
					return result.fail("text_order")
				}
				var part textPart
				if decodePart(envelope.Part, "text", &part, limits.jsonDepth) != nil ||
					part.SessionID != sessionID || part.MessageID != messageID || !recordPartID(seenParts, part.ID) ||
					part.Synthetic || part.Ignored || part.Time == nil || part.Time.End == nil || *part.Time.End < part.Time.Start ||
					(len(part.Metadata) != 0 && string(part.Metadata) != "{}") || !validText(part.Text, limits.finalBytes) {
					return result.fail("text_invalid")
				}
				separator := 0
				if len(result.final) != 0 {
					separator = 1
				}
				if len(result.final)+separator+len(part.Text) > limits.finalBytes {
					return result.fail("final_limit")
				}
				if separator != 0 {
					result.final = append(result.final, '\n')
				}
				result.final = append(result.final, part.Text...)
			case "step_finish":
				if !stepStarted || stepFinished || len(envelope.Error) != 0 || len(result.final) == 0 {
					return result.fail("step_finish_order")
				}
				var part stepFinishPart
				if decodePart(envelope.Part, "step-finish", &part, limits.jsonDepth) != nil ||
					part.SessionID != sessionID || part.MessageID != messageID || !recordPartID(seenParts, part.ID) ||
					len(part.Snapshot) > 256 || !validCost(part.Cost) || !validUsage(part.Tokens) {
					return result.fail("step_finish_invalid")
				}
				stepFinished = true
				input, output := *part.Tokens.Input, *part.Tokens.Output
				result.inputTokens, result.outputTokens = &input, &output
				switch part.Reason {
				case "stop", "length":
					result.terminal = true
				case "error", "content-filter", "unknown":
					result.providerFailed, result.terminal, result.failureCode = true, true, "provider_failed"
				default:
					return result.fail("finish_reason")
				}
			case "error":
				if len(envelope.Part) != 0 || len(envelope.Error) == 0 || !strictJSONObjectWithDepth(envelope.Error, limits.jsonDepth) {
					return result.fail("error_invalid")
				}
				result.providerFailed, result.terminal, result.failureCode = stepStarted, true, "provider_failed"
			default:
				return result.fail("unknown_event")
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return result
			}
			return result.fail("read_failed")
		}
	}
}

func decodePart(value json.RawMessage, expected string, target any, depth int) error {
	if len(value) == 0 || !strictJSONObjectWithDepth(value, depth) {
		return ErrContract
	}
	var kind partKind
	if json.Unmarshal(value, &kind) != nil || kind.Type != expected {
		return ErrContract
	}
	return decodeExact(value, target)
}

func recordPartID(seen map[string]struct{}, value string) bool {
	if !validProtocolID(value) {
		return false
	}
	if _, exists := seen[value]; exists {
		return false
	}
	seen[value] = struct{}{}
	return true
}

func validProtocolID(value string) bool {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func validCost(value json.Number) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	cost, err := strconv.ParseFloat(string(value), 64)
	return err == nil && cost >= 0 && cost <= 1e12
}

func validUsage(value *tokenUsage) bool {
	const maximum = uint64(1 << 52)
	if value == nil || value.Input == nil || value.Output == nil || value.Reasoning == nil || value.Cache == nil ||
		*value.Input > maximum || *value.Output > maximum || *value.Reasoning > maximum ||
		value.Cache.Read > maximum || value.Cache.Write > maximum {
		return false
	}
	return value.Total == nil || *value.Total <= maximum
}

func (result rpcResult) fail(code string) rpcResult {
	result.protocolDrift = true
	if result.failureCode == "" {
		result.failureCode = code
	}
	return result
}

func strictJSONObject(value []byte) bool {
	return strictJSONObjectWithDepth(value, maxJSONDepth)
}

func strictJSONObjectWithDepth(value []byte, jsonDepthLimit int) bool {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') || !scanJSONObject(decoder, 1, jsonDepthLimit) {
		return false
	}
	_, err = decoder.Token()
	return errors.Is(err, io.EOF)
}

func scanJSONObject(decoder *json.Decoder, depth int, jsonDepthLimit int) bool {
	if depth > jsonDepthLimit {
		return false
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok {
			return false
		}
		folded := strings.ToLower(name)
		if _, exists := seen[folded]; exists {
			return false
		}
		seen[folded] = struct{}{}
		if !scanJSONValue(decoder, depth+1, jsonDepthLimit) {
			return false
		}
	}
	end, err := decoder.Token()
	return err == nil && end == json.Delim('}')
}

func scanJSONValue(decoder *json.Decoder, depth int, jsonDepthLimit int) bool {
	if depth > jsonDepthLimit {
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
		return scanJSONObject(decoder, depth, jsonDepthLimit)
	case '[':
		for decoder.More() {
			if !scanJSONValue(decoder, depth+1, jsonDepthLimit) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim(']')
	default:
		return false
	}
}
