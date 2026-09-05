package opencodeopenrouter

import "encoding/json"

const (
	configDirectoryEnvironmentV1 = "XDG_CONFIG_HOME"
	configRelativeDirectoryV1    = "opencode"
	modelSelectorV1              = "openrouter/stealth/ox-alpha"
)

type configFileV1 struct {
	Schema           string                      `json:"$schema"`
	Autoupdate       bool                        `json:"autoupdate"`
	Share            string                      `json:"share"`
	Snapshot         bool                        `json:"snapshot"`
	EnabledProviders []string                    `json:"enabled_providers"`
	Model            string                      `json:"model"`
	SmallModel       string                      `json:"small_model"`
	DefaultAgent     string                      `json:"default_agent"`
	SubagentDepth    uint64                      `json:"subagent_depth"`
	Username         string                      `json:"username"`
	Plugin           []string                    `json:"plugin"`
	Command          map[string]json.RawMessage  `json:"command"`
	Skills           skillsConfigV1              `json:"skills"`
	Provider         map[string]providerConfigV1 `json:"provider"`
	MCP              map[string]json.RawMessage  `json:"mcp"`
	Formatter        bool                        `json:"formatter"`
	LSP              bool                        `json:"lsp"`
	Instructions     []string                    `json:"instructions"`
	Permission       string                      `json:"permission"`
	Tools            map[string]bool             `json:"tools"`
	Compaction       compactionConfigV1          `json:"compaction"`
	Experimental     experimentalConfigV1        `json:"experimental"`
	Agent            map[string]agentConfigV1    `json:"agent"`
}

type skillsConfigV1 struct {
	Paths []string `json:"paths"`
	URLs  []string `json:"urls"`
}

type providerConfigV1 struct {
	Environment []string                 `json:"env"`
	Whitelist   []string                 `json:"whitelist"`
	Options     providerOptionsV1        `json:"options"`
	Models      map[string]modelConfigV1 `json:"models"`
}

type providerOptionsV1 struct {
	BaseURL       string `json:"baseURL"`
	Timeout       uint64 `json:"timeout"`
	HeaderTimeout uint64 `json:"headerTimeout"`
	ChunkTimeout  uint64 `json:"chunkTimeout"`
}

type modelConfigV1 struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Reasoning  bool           `json:"reasoning"`
	ToolCall   bool           `json:"tool_call"`
	Modalities modalitiesV1   `json:"modalities"`
	Options    modelOptionsV1 `json:"options"`
}

type modalitiesV1 struct {
	Input  []string `json:"input"`
	Output []string `json:"output"`
}

type modelOptionsV1 struct {
	Provider routeConfigV1 `json:"provider"`
}

type routeConfigV1 struct {
	Only              []string `json:"only"`
	AllowFallbacks    bool     `json:"allow_fallbacks"`
	RequireParameters bool     `json:"require_parameters"`
}

type compactionConfigV1 struct {
	Auto  bool `json:"auto"`
	Prune bool `json:"prune"`
}

type experimentalConfigV1 struct {
	OpenTelemetry      bool     `json:"openTelemetry"`
	PrimaryTools       []string `json:"primary_tools"`
	ContinueLoopOnDeny bool     `json:"continue_loop_on_deny"`
}

type agentConfigV1 struct {
	Description string          `json:"description"`
	Mode        string          `json:"mode"`
	Model       string          `json:"model"`
	Steps       uint64          `json:"steps"`
	Permission  string          `json:"permission"`
	Tools       map[string]bool `json:"tools"`
}

func generatedFiles(profile ProfileV1) ([]GeneratedFileV1, error) {
	config := configFileV1{
		Schema: "https://opencode.ai/config.json", Autoupdate: false, Share: "disabled", Snapshot: false,
		EnabledProviders: []string{ProviderIDV1}, Model: modelSelectorV1, SmallModel: modelSelectorV1,
		DefaultAgent: "sessionless", SubagentDepth: 0, Username: "sessionless",
		Plugin: []string{}, Command: map[string]json.RawMessage{}, Skills: skillsConfigV1{Paths: []string{}, URLs: []string{}},
		Provider: map[string]providerConfigV1{
			ProviderIDV1: {
				Environment: []string{CredentialEnvironmentV1}, Whitelist: []string{ModelIDV1},
				Options: providerOptionsV1{
					BaseURL: OpenRouterBaseURLV1, Timeout: profile.ProviderTimeoutMS,
					HeaderTimeout: profile.ProviderTimeoutMS, ChunkTimeout: profile.ProviderTimeoutMS,
				},
				Models: map[string]modelConfigV1{
					ModelIDV1: {
						ID: ModelIDV1, Name: "Sessionless Ox Alpha canary", Reasoning: false, ToolCall: false,
						Modalities: modalitiesV1{Input: []string{"text"}, Output: []string{"text"}},
						Options: modelOptionsV1{Provider: routeConfigV1{
							Only: []string{ModelVendorIDV1}, AllowFallbacks: false, RequireParameters: true,
						}},
					},
				},
			},
		},
		MCP: map[string]json.RawMessage{}, Formatter: false, LSP: false, Instructions: []string{},
		Permission: "deny", Tools: map[string]bool{"*": false},
		Compaction:   compactionConfigV1{Auto: false, Prune: false},
		Experimental: experimentalConfigV1{OpenTelemetry: false, PrimaryTools: []string{}, ContinueLoopOnDeny: false},
		Agent: map[string]agentConfigV1{
			"sessionless": {
				Description: "Sessionless one-shot provider adapter", Mode: "primary", Model: modelSelectorV1,
				Steps: 1, Permission: "deny", Tools: map[string]bool{"*": false},
			},
		},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, ErrContract
	}
	return []GeneratedFileV1{{
		DirectoryEnvironmentName: configDirectoryEnvironmentV1,
		RelativeDirectory:        configRelativeDirectoryV1,
		Name:                     "opencode.json",
		Content:                  encoded,
	}}, nil
}

func processArguments() []string {
	return []string{
		"--pure", "run", "--format", "json", "--model", modelSelectorV1,
		"--agent", "sessionless", "--title", "sessionless-invocation", "--no-thinking",
	}
}

func processEnvironment() []EnvironmentV1 {
	return []EnvironmentV1{
		{Name: "OPENCODE_DISABLE_AUTOUPDATE", Value: "1"},
		{Name: "OPENCODE_DISABLE_AUTOCOMPACT", Value: "1"},
		{Name: "OPENCODE_DISABLE_DEFAULT_PLUGINS", Value: "1"},
		{Name: "OPENCODE_DISABLE_MODELS_FETCH", Value: "1"},
		{Name: "OPENCODE_DISABLE_PROJECT_CONFIG", Value: "1"},
		{Name: "OPENCODE_DISABLE_PRUNE", Value: "1"},
		{Name: "OPENCODE_PURE", Value: "1"},
		{Name: "OPENCODE_CLIENT", Value: "sessionless"},
		{Name: "DO_NOT_TRACK", Value: "1"},
		{Name: "NO_COLOR", Value: "1"},
	}
}

func privateDirectories() []PrivateDirectoryV1 {
	return []PrivateDirectoryV1{
		{EnvironmentName: "HOME", Purpose: "home"},
		{EnvironmentName: "XDG_DATA_HOME", Purpose: "data"},
		{EnvironmentName: "XDG_CACHE_HOME", Purpose: "cache"},
		// OpenCode's config loader otherwise starts a background npm install even
		// under --pure. The boundary writes opencode/opencode.json first and then
		// makes this invocation-private root read-only before process start.
		{EnvironmentName: "XDG_CONFIG_HOME", Purpose: "config", ReadOnlyAfterMaterialization: true},
		{EnvironmentName: "XDG_STATE_HOME", Purpose: "state"},
		{EnvironmentName: "TMPDIR", Purpose: "temporary"},
	}
}
