package piopenrouter

import "encoding/json"

type modelsFileV1 struct {
	Providers map[string]providerConfigV1 `json:"providers"`
}

type providerConfigV1 struct {
	BaseURL string          `json:"baseUrl"`
	APIKey  string          `json:"apiKey"`
	API     string          `json:"api"`
	Models  []modelConfigV1 `json:"models"`
}

type modelConfigV1 struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Reasoning bool          `json:"reasoning"`
	Input     []string      `json:"input"`
	Compat    modelCompatV1 `json:"compat"`
}

type modelCompatV1 struct {
	OpenRouterRouting routeConfigV1 `json:"openRouterRouting"`
}

type routeConfigV1 struct {
	AllowFallbacks    bool     `json:"allow_fallbacks"`
	RequireParameters bool     `json:"require_parameters"`
	Only              []string `json:"only"`
}

type settingsFileV1 struct {
	DefaultProjectTrust    string             `json:"defaultProjectTrust"`
	EnableInstallTelemetry bool               `json:"enableInstallTelemetry"`
	EnableAnalytics        bool               `json:"enableAnalytics"`
	DefaultTools           []string           `json:"defaultTools"`
	Compaction             compactionConfigV1 `json:"compaction"`
	Retry                  retryConfigV1      `json:"retry"`
}

type compactionConfigV1 struct {
	Enabled bool `json:"enabled"`
}

type retryConfigV1 struct {
	Enabled    bool                  `json:"enabled"`
	MaxRetries uint64                `json:"maxRetries"`
	Provider   providerRetryConfigV1 `json:"provider"`
}

type providerRetryConfigV1 struct {
	TimeoutMS  uint64 `json:"timeoutMs"`
	MaxRetries uint64 `json:"maxRetries"`
}

func generatedFiles(profile ProfileV1) ([]GeneratedFileV1, error) {
	models, err := json.Marshal(modelsFileV1{Providers: map[string]providerConfigV1{
		ProviderIDV1: {
			BaseURL: OpenRouterBaseURLV1, APIKey: "$" + CredentialEnvironmentV1, API: "openai-completions",
			Models: []modelConfigV1{{
				ID: ModelIDV1, Name: "Sessionless Ox Alpha canary", Reasoning: true, Input: []string{"text"},
				Compat: modelCompatV1{OpenRouterRouting: routeConfigV1{
					AllowFallbacks: false, RequireParameters: true, Only: []string{"stealth"},
				}},
			}},
		},
	}})
	if err != nil {
		return nil, ErrContract
	}
	settings, err := json.Marshal(settingsFileV1{
		DefaultProjectTrust: "never", EnableInstallTelemetry: false, EnableAnalytics: false,
		DefaultTools: []string{}, Compaction: compactionConfigV1{Enabled: false},
		Retry: retryConfigV1{Enabled: false, MaxRetries: 0, Provider: providerRetryConfigV1{
			TimeoutMS: profile.ProviderTimeoutMS, MaxRetries: 0,
		}},
	})
	if err != nil {
		return nil, ErrContract
	}
	return []GeneratedFileV1{{Name: "models.json", Content: models}, {Name: "settings.json", Content: settings}}, nil
}

func processArguments() []string {
	return []string{
		"--mode", "rpc", "--provider", ProviderIDV1, "--model", ModelIDV1,
		"--thinking", "off", "--no-session", "--no-tools", "--no-extensions",
		"--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-approve",
	}
}

func processEnvironment() []EnvironmentV1 {
	return []EnvironmentV1{
		{Name: "PI_OFFLINE", Value: "1"},
		{Name: "PI_SKIP_VERSION_CHECK", Value: "1"},
		{Name: "PI_TELEMETRY", Value: "0"},
	}
}
