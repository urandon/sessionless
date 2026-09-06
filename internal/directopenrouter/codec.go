package directopenrouter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"unicode/utf8"
)

const metadataProviderNameV1 = "Stealth"

type requestV1 struct {
	Model               string           `json:"model"`
	Messages            []requestMessage `json:"messages"`
	MaxCompletionTokens uint64           `json:"max_completion_tokens"`
	Stream              bool             `json:"stream"`
	Provider            providerRequest  `json:"provider"`
}

type requestMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type providerRequest struct {
	AllowFallbacks    bool     `json:"allow_fallbacks"`
	RequireParameters bool     `json:"require_parameters"`
	Only              []string `json:"only"`
}

func encodeRequest(prompt []byte, maxCompletionTokens uint64) ([]byte, error) {
	if len(prompt) == 0 || len(prompt) > maxPromptBytes || !utf8.Valid(prompt) || bytes.IndexByte(prompt, 0) >= 0 ||
		maxCompletionTokens < 16 || maxCompletionTokens > 65_536 {
		return nil, ErrContract
	}
	encoded, err := json.Marshal(requestV1{
		Model:               WireModelIDV1,
		Messages:            []requestMessage{{Role: "user", Content: string(prompt)}},
		MaxCompletionTokens: maxCompletionTokens,
		Stream:              false,
		Provider:            providerRequest{AllowFallbacks: false, RequireParameters: true, Only: []string{ProviderIDV1}},
	})
	if err != nil || len(encoded) > maxRequestBytes {
		clear(encoded)
		return nil, ErrContract
	}
	return encoded, nil
}

type responseV1 struct {
	ID                string           `json:"id"`
	Object            string           `json:"object"`
	Created           uint64           `json:"created"`
	Model             string           `json:"model"`
	Choices           []responseChoice `json:"choices"`
	Usage             responseUsage    `json:"usage"`
	SystemFingerprint json.RawMessage  `json:"system_fingerprint"`
	ServiceTier       *string          `json:"service_tier,omitempty"`
	Metadata          responseMetadata `json:"openrouter_metadata"`
}

type responseChoice struct {
	Index        uint64          `json:"index"`
	Message      responseMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
	Logprobs     json.RawMessage `json:"logprobs,omitempty"`
}

type responseMessage struct {
	Role      string          `json:"role"`
	Content   string          `json:"content"`
	Refusal   json.RawMessage `json:"refusal,omitempty"`
	Reasoning json.RawMessage `json:"reasoning,omitempty"`
}

type responseUsage struct {
	PromptTokens            uint64          `json:"prompt_tokens"`
	CompletionTokens        uint64          `json:"completion_tokens"`
	TotalTokens             uint64          `json:"total_tokens"`
	Cost                    json.RawMessage `json:"cost,omitempty"`
	IsBYOK                  *bool           `json:"is_byok,omitempty"`
	PromptTokensDetails     json.RawMessage `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails json.RawMessage `json:"completion_tokens_details,omitempty"`
	CostDetails             json.RawMessage `json:"cost_details,omitempty"`
	ServerToolUseDetails    json.RawMessage `json:"server_tool_use_details,omitempty"`
}

type responseMetadata struct {
	Attempt        uint64            `json:"attempt"`
	Endpoints      metadataEndpoints `json:"endpoints"`
	GenerationTime json.RawMessage   `json:"generation_time"`
	IsBYOK         json.RawMessage   `json:"is_byok"`
	Region         string            `json:"region"`
	Requested      string            `json:"requested"`
	Strategy       string            `json:"strategy"`
	Summary        string            `json:"summary"`
}

type metadataEndpoints struct {
	Available []metadataEndpoint `json:"available"`
	Total     uint64             `json:"total"`
}

type metadataEndpoint struct {
	Model    string `json:"model"`
	Provider string `json:"provider"`
	Selected bool   `json:"selected"`
}

type responseObservation struct {
	Final        []byte
	InputTokens  uint64
	OutputTokens uint64
}

func parseResponse(value []byte) (responseObservation, error) {
	if len(value) == 0 || len(value) > maxResponseBytes || !utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0 {
		return responseObservation{}, ErrContract
	}
	if err := validateJSONShape(value, maxJSONDepth, maxJSONMembers); err != nil {
		return responseObservation{}, ErrContract
	}
	var response responseV1
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return responseObservation{}, ErrContract
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return responseObservation{}, ErrContract
	}
	if response.ID == "" || len(response.ID) > 256 || response.Object != "chat.completion" || response.Created == 0 ||
		response.Model != WireModelIDV1 || len(response.Choices) != 1 || len(response.SystemFingerprint) == 0 ||
		response.Metadata.Attempt != 1 || response.Metadata.Requested != WireModelIDV1 || response.Metadata.Strategy != "direct" ||
		response.Metadata.Endpoints.Total != 1 || len(response.Metadata.Endpoints.Available) != 1 ||
		!validNullOrString(response.SystemFingerprint, 256) || !validOptionalString(response.ServiceTier, 128) ||
		!validUnsignedNumber(response.Metadata.GenerationTime) || !bytes.Equal(bytes.TrimSpace(response.Metadata.IsBYOK), []byte("false")) ||
		!validText(response.Metadata.Region, 64) || !validText(response.Metadata.Summary, 512) {
		return responseObservation{}, ErrContract
	}
	choice := response.Choices[0]
	if choice.Index != 0 || choice.Message.Role != "assistant" || choice.FinishReason != "stop" ||
		!validText(choice.Message.Content, maxFinalBytes) || !isAbsentOrJSONNull(choice.Message.Refusal) ||
		!isAbsentOrJSONNull(choice.Message.Reasoning) || !isAbsentOrJSONNull(choice.Logprobs) {
		return responseObservation{}, ErrContract
	}
	endpoint := response.Metadata.Endpoints.Available[0]
	if !endpoint.Selected || endpoint.Model != WireModelIDV1 || endpoint.Provider != metadataProviderNameV1 ||
		response.Usage.PromptTokens > math.MaxInt64 || response.Usage.CompletionTokens > math.MaxInt64 ||
		response.Usage.PromptTokens > math.MaxUint64-response.Usage.CompletionTokens ||
		response.Usage.TotalTokens == 0 || response.Usage.TotalTokens != response.Usage.PromptTokens+response.Usage.CompletionTokens {
		return responseObservation{}, ErrContract
	}
	return responseObservation{
		Final: []byte(choice.Message.Content), InputTokens: response.Usage.PromptTokens,
		OutputTokens: response.Usage.CompletionTokens,
	}, nil
}

func isAbsentOrJSONNull(value json.RawMessage) bool {
	return len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null"))
}

func validNullOrString(value json.RawMessage, max int) bool {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return true
	}
	var text string
	return json.Unmarshal(value, &text) == nil && len(text) <= max && utf8.ValidString(text) && !strings.ContainsRune(text, 0)
}

func validOptionalString(value *string, max int) bool {
	return value == nil || (len(*value) > 0 && len(*value) <= max && utf8.ValidString(*value) && !strings.ContainsRune(*value, 0))
}

func validUnsignedNumber(value json.RawMessage) bool {
	if len(value) == 0 {
		return false
	}
	var number uint64
	return json.Unmarshal(value, &number) == nil
}

func validateJSONShape(value []byte, maxDepth, maxMembers int) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	if err := consumeJSONValue(decoder, first, 1, maxDepth, maxMembers); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrContract
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, token json.Token, depth, maxDepth, maxMembers int) error {
	if depth > maxDepth {
		return ErrContract
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		members := 0
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrContract
			}
			folded := strings.ToLower(key)
			if _, exists := seen[folded]; exists {
				return ErrContract
			}
			seen[folded] = struct{}{}
			members++
			if members > maxMembers {
				return ErrContract
			}
			child, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := consumeJSONValue(decoder, child, depth+1, maxDepth, maxMembers); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrContract
		}
	case '[':
		members := 0
		for decoder.More() {
			members++
			if members > maxMembers {
				return ErrContract
			}
			child, err := decoder.Token()
			if err != nil {
				return err
			}
			if err := consumeJSONValue(decoder, child, depth+1, maxDepth, maxMembers); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrContract
		}
	default:
		return ErrContract
	}
	return nil
}
