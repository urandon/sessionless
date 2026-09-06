package codexopenrouter

import (
	"encoding/json"
	"fmt"
)

type modelCatalogV1 struct {
	Models []modelInfoV1 `json:"models"`
}

type modelInfoV1 struct {
	Slug                     string             `json:"slug"`
	DisplayName              string             `json:"display_name"`
	Description              string             `json:"description"`
	DefaultReasoningLevel    string             `json:"default_reasoning_level"`
	SupportedReasoningLevels []reasoningLevelV1 `json:"supported_reasoning_levels"`
	ShellType                string             `json:"shell_type"`
	Visibility               string             `json:"visibility"`
	SupportedInAPI           bool               `json:"supported_in_api"`
	Priority                 uint64             `json:"priority"`
	ContextWindow            uint64             `json:"context_window"`
	MaxContextWindow         uint64             `json:"max_context_window"`
	TruncationPolicy         truncationPolicyV1 `json:"truncation_policy"`
	InputModalities          []string           `json:"input_modalities"`
	SupportsParallelTools    bool               `json:"supports_parallel_tool_calls"`
	SupportsReasoningSummary bool               `json:"supports_reasoning_summaries"`
	SupportVerbosity         bool               `json:"support_verbosity"`
	ExperimentalTools        []string           `json:"experimental_supported_tools"`
	BaseInstructions         string             `json:"base_instructions"`
}

type reasoningLevelV1 struct {
	Effort      string `json:"effort"`
	Description string `json:"description"`
}

type truncationPolicyV1 struct {
	Mode  string `json:"mode"`
	Limit uint64 `json:"limit"`
}

func generatedFiles(profile ProfileV1) ([]GeneratedFileV1, error) {
	if err := profile.validate(); err != nil {
		return nil, err
	}
	config := fmt.Sprintf(`model = %q
model_provider = %q
model_catalog_json = %q
approval_policy = "never"
sandbox_mode = "read-only"
web_search = "disabled"
disable_response_storage = true
model_reasoning_effort = "none"
model_context_window = %d
model_auto_compact_token_limit = %d

[history]
persistence = "none"

[model_providers.openrouter]
name = "Sessionless OpenRouter"
base_url = %q
env_key = %q
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
stream_idle_timeout_ms = %d
requires_openai_auth = false
supports_websockets = false
supports_standalone_web_search = false

[features]
apps = false
browser_use = false
collab = false
connectors = false
memories = false
multi_agent = false
plugins = false
search_tool = false
standalone_web_search = false
tool_search = false
web_search = false
`, ModelIDV1, ProviderIDV1, ModelCatalogPathV1, profile.ContextWindow,
		profile.ContextWindow-profile.MaxOutputTokens, OpenRouterBaseURLV1,
		CredentialEnvironmentV1, profile.ProviderTimeoutMS)
	catalog, err := json.Marshal(modelCatalogV1{Models: []modelInfoV1{{
		Slug: ModelIDV1, DisplayName: "Sessionless Ox Alpha canary",
		Description:              "Pinned externally-shareable Sessionless Codex/OpenRouter profile",
		DefaultReasoningLevel:    "none",
		SupportedReasoningLevels: []reasoningLevelV1{{Effort: "none", Description: "No reasoning controls"}},
		ShellType:                "disabled", Visibility: "list", SupportedInAPI: true, Priority: 1,
		ContextWindow: profile.ContextWindow, MaxContextWindow: profile.ContextWindow,
		TruncationPolicy: truncationPolicyV1{Mode: "tokens", Limit: profile.ContextWindow - profile.MaxOutputTokens},
		InputModalities:  []string{"text"}, SupportsParallelTools: false,
		SupportsReasoningSummary: false, SupportVerbosity: false,
		ExperimentalTools: []string{},
		BaseInstructions:  "Return one bounded text answer. Do not call tools or access external state.",
	}}})
	if err != nil {
		return nil, ErrContract
	}
	return []GeneratedFileV1{
		{Name: "config.toml", Content: []byte(config)},
		{Name: "models.json", Content: catalog},
	}, nil
}

func processArguments() []string {
	return []string{
		"exec", "--json", "--ephemeral", "--ignore-rules",
		"--strict-config", "--sandbox", "read-only", "--skip-git-repo-check",
		"--color", "never", "--model", ModelIDV1, "-",
	}
}

func processEnvironment() []EnvironmentV1 {
	return []EnvironmentV1{
		{Name: "DO_NOT_TRACK", Value: "1"},
		{Name: "NO_COLOR", Value: "1"},
	}
}
