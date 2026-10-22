package codexexec

import (
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

const nativeProviderIDV2 = "sessionless_native"

// These are supported keys from the pinned 0.162.1 feature inventory and H7
// profile, not proof of complete effect exclusion or actual platform/OAuth
// qualification. Do not replace explicit exclusions with native defaults.
const nativeFeaturesV2 = "features={" +
	"shell_tool=false,view_image=false,multi_agent=false,multi_agent_v2=false," +
	"apps=false,plugins=false,code_mode=false,code_mode_only=false,sleep_tool=false," +
	"browser_use=false,computer_use=false,image_generation=false,goals=false," +
	"unbounded_connection_retries=false,hooks=false,unified_exec=false,unified_exec_tty=false," +
	"shell_snapshot=false,code_mode_host=false,browser_use_external=false,browser_use_full_cdp_access=false," +
	"in_app_browser=false,realtime_conversation=false,skill_search=false,skill_mcp_dependency_install=false," +
	"workspace_dependencies=false,daemon_auto_start=false,remote_plugin=false,plugin_sharing=false," +
	"auth_elicitation=false,tool_call_mcp_elicitation=false,tool_suggest=false,api_key_model_discovery=false," +
	"system_proxy_fallback=false,respect_system_proxy=false,standalone_web_search=false," +
	"default_mode_request_user_input=false,request_permissions_tool=false,unified_image_budget=false," +
	"skip_host_skill_discovery=true}"

// Stock native exec requests strict structured output: every declared property
// is required. An empty artifacts array represents an answer-only result.
// Actual schema acceptance remains a separate qualification; host validation
// additionally enforces byte/depth/name limits not expressed by this schema.
const nativeOutputSchemaV2 = `{"type":"object","properties":{"answer":{"type":"string"},"artifacts":{"type":"array","maxItems":2,"items":{"type":"object","properties":{"name":{"type":"string"},"media_type":{"type":"string","enum":["text/plain","application/json"]},"content":{"type":"string"}},"required":["name","media_type","content"],"additionalProperties":false}}},"required":["answer","artifacts"],"additionalProperties":false}`

type generatedNativeModelV2 struct {
	Slug                     string   `json:"slug"`
	DisplayName              string   `json:"display_name"`
	Description              *string  `json:"description"`
	SupportedReasoningLevels []string `json:"supported_reasoning_levels"`
	ShellType                string   `json:"shell_type"`
	Visibility               string   `json:"visibility"`
	SupportedInAPI           bool     `json:"supported_in_api"`
	Priority                 int      `json:"priority"`
	SupportVerbosity         bool     `json:"support_verbosity"`
	DefaultVerbosity         *string  `json:"default_verbosity"`
	ApplyPatchToolType       *string  `json:"apply_patch_tool_type"`
	TruncationPolicy         struct {
		Mode  string `json:"mode"`
		Limit int    `json:"limit"`
	} `json:"truncation_policy"`
	ExperimentalSupportedTools []string `json:"experimental_supported_tools"`
	InputModalities            []string `json:"input_modalities"`
	ToolMode                   string   `json:"tool_mode"`
	NodeREPLDisabled           bool     `json:"node_repl_disabled"`
	ContextWindow              int64    `json:"context_window"`
	ModelMessages              struct {
		InstructionsTemplate string `json:"instructions_template"`
	} `json:"model_messages"`
}

// generateNativeProfileV2 emits only deterministic owned profile bytes/argv.
// It does not read files, ambient configuration, credentials or model catalogs.
// Configuration is the exact ordered -c content, joined as a TOML document;
// it is not another configuration file passed to the native process.
func generateNativeProfileV2(p NativeProfileV2) (nativeGeneratedV2, error) {
	if !validModelID(p.Backend.Model) || p.Backend.ExecutableVersion != "0.162.1" || p.DeclaredContextWindow == 0 || p.DeclaredContextWindow > math.MaxInt64 ||
		p.OutputReserveTokens == 0 || p.OutputReserveTokens >= p.DeclaredContextWindow || p.StreamIdleTimeoutMS == 0 || p.StreamIdleTimeoutMS > 60_000 ||
		p.Launch.Version != 3 || p.Launch.Workload.Executable != p.Backend.Executable || p.Launch.Workload.Digest != p.Backend.ExecutableDigest {
		return nativeGeneratedV2{}, ErrContract
	}
	model := generatedNativeModelV2{Slug: p.Backend.Model, DisplayName: p.Backend.Model, SupportedReasoningLevels: []string{}, ShellType: "disabled", Visibility: "list", SupportedInAPI: true,
		ExperimentalSupportedTools: []string{}, InputModalities: []string{"text", "image"}, ToolMode: "direct", NodeREPLDisabled: true, ContextWindow: int64(p.DeclaredContextWindow)}
	model.TruncationPolicy.Mode, model.TruncationPolicy.Limit = "bytes", 10000
	model.ModelMessages.InstructionsTemplate = "You are a Sessionless assistant. Return the requested answer and any requested provider-authored text or JSON artifacts using the supplied output schema. Tools are disabled."
	catalog, err := json.Marshal(struct {
		Models []generatedNativeModelV2 `json:"models"`
	}{[]generatedNativeModelV2{model}})
	if err != nil || len(catalog) == 0 || uint64(len(catalog)) > nativeharnessinput.MaxFileBytes {
		return nativeGeneratedV2{}, ErrContract
	}
	settings := []string{
		`model_provider="` + nativeProviderIDV2 + `"`,
		`model_providers={` + nativeProviderIDV2 + `={name="OpenAI",wire_api="responses",requires_openai_auth=true,supports_websockets=false,supports_standalone_web_search=false,request_max_retries=0,stream_max_retries=0,stream_idle_timeout_ms=` + strconv.FormatUint(p.StreamIdleTimeoutMS, 10) + `}}`,
		`model_catalog_json="/sessionless/profile/models.json"`,
		`model_context_window=` + strconv.FormatUint(p.DeclaredContextWindow, 10),
		nativeFeaturesV2,
		`tools={experimental_request_user_input={enabled=false},update_plan={enabled=false}}`,
		`skills={include_instructions=false}`,
		`web_search="disabled"`,
		`check_for_update_on_startup=false`,
		`history={persistence="none"}`,
		`forced_login_method="chatgpt"`,
		`cli_auth_credentials_store="file"`,
	}
	base := []string{"exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--strict-config", "--sandbox", "read-only", "--skip-git-repo-check", "--color", "never", "--model", p.Backend.Model, "--output-schema", "/sessionless/profile/output.schema.json"}
	for _, setting := range settings {
		if len(setting) == 0 || len(setting) > 4096 || strings.ContainsAny(setting, "\x00\r\n") {
			return nativeGeneratedV2{}, ErrContract
		}
		base = append(base, "-c", setting)
	}
	g := nativeGeneratedV2{configuration: []byte(strings.Join(settings, "\n") + "\n"), catalog: catalog, schema: []byte(nativeOutputSchemaV2), environment: []attachedworkerdaemon.EnvironmentVariable{}}
	if len(g.configuration) > 128<<10 || uint64(len(g.schema)) > nativeharnessinput.MaxFileBytes {
		return nativeGeneratedV2{}, ErrContract
	}
	for i := range g.arguments {
		paths, err := nativeharnessinput.ImagePaths(uint32(i))
		if err != nil {
			return nativeGeneratedV2{}, ErrContract
		}
		args := slices.Clone(base)
		for _, path := range paths {
			args = append(args, "--image", path)
		}
		g.arguments[i] = append(args, "-")
	}
	// The shared builder checks complete bootstrap/relay vectors, not just
	// the native child, and reserves the two runtime carrier arguments.
	check := p.Launch.Clone()
	check.NativeArguments = g.arguments
	if _, _, err := nativeharnessbootstrap.InstallationArgumentsV3(check); err != nil {
		return nativeGeneratedV2{}, ErrContract
	}
	// NO_COLOR=1 is already owned by the supervisor baseline. It is reserved
	// and must not appear again in extras. DO_NOT_TRACK is not forwarded by the
	// pinned bootstrap ABI; do not seal or promise an ineffective setting.
	return g, nil
}
