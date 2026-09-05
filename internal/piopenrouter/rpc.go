package piopenrouter

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
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
	ID                    string          `json:"id"`
	Type                  string          `json:"type"`
	Command               string          `json:"command"`
	Success               *bool           `json:"success"`
	Error                 string          `json:"error"`
	Message               json.RawMessage `json:"message"`
	WillRetry             *bool           `json:"willRetry"`
	AssistantMessageEvent json.RawMessage `json:"assistantMessageEvent"`
}

type rpcMessage struct {
	Role          string       `json:"role"`
	Content       []rpcContent `json:"content"`
	Provider      string       `json:"provider"`
	Model         string       `json:"model"`
	ResponseModel string       `json:"responseModel"`
	Usage         rpcUsage     `json:"usage"`
	StopReason    string       `json:"stopReason"`
}

type rpcContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type rpcUsage struct {
	Input  *uint64 `json:"input"`
	Output *uint64 `json:"output"`
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

func parseRPCJSONL(value []byte, requestID string) rpcResult {
	return parseRPCJSONLWithLimits(value, requestID, productionRPCLimits)
}

func parseRPCJSONLWithLimits(value []byte, requestID string, limits rpcLimits) rpcResult {
	result := rpcResult{}
	if limits.outputBytes <= 0 || limits.lineBytes <= 0 || limits.events <= 0 || limits.finalBytes <= 0 || limits.jsonDepth <= 0 ||
		len(value) == 0 || len(value) > limits.outputBytes {
		return result.fail("stdout_limit")
	}
	reader := bufio.NewReaderSize(bytes.NewReader(value), limits.lineBytes)
	responseSeen, agentStarted, turnStarted, userStarted, userEnded := false, false, false, false, false
	assistantStarted, assistantEnded, turnEnded, agentEnded := false, false, false, false
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
			if json.Unmarshal(line, &envelope) != nil || envelope.Type == "" {
				return result.fail("invalid_event")
			}
			switch envelope.Type {
			case "response":
				if responseSeen || agentStarted || envelope.ID != requestID || envelope.Command != "prompt" || envelope.Success == nil {
					return result.fail("invalid_prompt_response")
				}
				responseSeen = true
				if !*envelope.Success {
					result.providerFailed = true
					result.failureCode = "prompt_rejected"
				} else if envelope.Error != "" {
					return result.fail("invalid_prompt_response")
				} else {
					result.accepted = true
				}
			case "agent_start":
				if !result.accepted || agentStarted {
					return result.fail("agent_start_order")
				}
				agentStarted = true
			case "turn_start":
				if !agentStarted || turnStarted {
					return result.fail("turn_start_order")
				}
				turnStarted = true
			case "message_start":
				role, valid := messageRole(envelope.Message, limits.jsonDepth)
				if !turnStarted || turnEnded || !valid {
					return result.fail("message_start_order")
				}
				switch role {
				case "user":
					if userStarted || userEnded || assistantStarted || assistantEnded {
						return result.fail("message_start_order")
					}
					userStarted = true
				case "assistant":
					if !userEnded || assistantStarted || assistantEnded {
						return result.fail("message_start_order")
					}
					assistantStarted = true
				}
			case "message_update":
				if !assistantStarted || assistantEnded || forbiddenAssistantUpdate(envelope.AssistantMessageEvent, limits.jsonDepth) {
					return result.fail("message_update_invalid")
				}
			case "message_end":
				if !turnStarted || turnEnded || len(envelope.Message) == 0 || !strictJSONObjectWithDepth(envelope.Message, limits.jsonDepth) {
					return result.fail("message_end_order")
				}
				var role struct {
					Role string `json:"role"`
				}
				if json.Unmarshal(envelope.Message, &role) != nil {
					return result.fail("message_end_invalid")
				}
				if role.Role == "user" {
					if !userStarted || userEnded || assistantStarted || assistantEnded {
						return result.fail("user_after_assistant")
					}
					userEnded = true
					break
				}
				if role.Role != "assistant" || !assistantStarted || assistantEnded {
					return result.fail("unexpected_message_role")
				}
				assistantEnded = true
				if !result.consumeAssistant(envelope.Message, limits.finalBytes) {
					return result.fail("assistant_message_invalid")
				}
			case "turn_end":
				if !assistantEnded || turnEnded {
					return result.fail("turn_end_order")
				}
				turnEnded = true
			case "agent_end":
				if !turnEnded || agentEnded || envelope.WillRetry == nil || *envelope.WillRetry {
					return result.fail("agent_end_invalid")
				}
				agentEnded = true
			case "agent_settled":
				if !agentEnded {
					return result.fail("agent_settled_order")
				}
				result.terminal = true
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

func (result *rpcResult) consumeAssistant(value []byte, finalByteLimit int) bool {
	var message rpcMessage
	if json.Unmarshal(value, &message) != nil || message.Role != "assistant" || message.Provider != ProviderIDV1 ||
		message.Model != ModelIDV1 || (message.ResponseModel != "" && message.ResponseModel != ModelIDV1) ||
		message.Usage.Input == nil || message.Usage.Output == nil || *message.Usage.Input > 1<<52 || *message.Usage.Output > 1<<52 {
		return false
	}
	var final strings.Builder
	for _, content := range message.Content {
		switch content.Type {
		case "thinking":
		case "text":
			if content.Text == "" || !utf8.ValidString(content.Text) || strings.ContainsRune(content.Text, 0) || final.Len()+len(content.Text) > finalByteLimit {
				return false
			}
			final.WriteString(content.Text)
		default:
			return false
		}
	}
	result.inputTokens, result.outputTokens = message.Usage.Input, message.Usage.Output
	switch message.StopReason {
	case "stop", "length":
		if final.Len() == 0 {
			return false
		}
		result.final = []byte(final.String())
	case "error", "aborted":
		result.providerFailed = true
		result.failureCode = "provider_failed"
	case "toolUse", "deferred", "pending", "":
		return false
	default:
		return false
	}
	return true
}

func (result rpcResult) fail(code string) rpcResult {
	result.protocolDrift = true
	if result.failureCode == "" {
		result.failureCode = code
	}
	return result
}

func messageRole(value json.RawMessage, jsonDepthLimit int) (string, bool) {
	if len(value) == 0 || !strictJSONObjectWithDepth(value, jsonDepthLimit) {
		return "", false
	}
	var role struct {
		Role string `json:"role"`
	}
	if json.Unmarshal(value, &role) != nil || (role.Role != "user" && role.Role != "assistant") {
		return "", false
	}
	return role.Role, true
}

func forbiddenAssistantUpdate(value json.RawMessage, jsonDepthLimit int) bool {
	if len(value) == 0 || !strictJSONObjectWithDepth(value, jsonDepthLimit) {
		return true
	}
	var event struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(value, &event) != nil {
		return true
	}
	switch event.Type {
	case "start", "text_start", "text_delta", "text_end", "thinking_start", "thinking_delta", "thinking_end", "done", "error":
		return false
	default:
		return true
	}
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
