package codexexec

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
)

func nativeGeneratedFixtureV2() NativeProfileV2 {
	native := attachedworkerdaemon.NativeArtifactV2{Executable: "/owned/codex", Digest: attachedworkerdaemon.ExecutableDigest{1}}
	return NativeProfileV2{Backend: Config{Executable: native.Executable, ExecutableVersion: "0.162.1", ExecutableDigest: native.Digest, Model: "model-native-test"},
		Launch: attachedworkerdaemon.NativeLaunchV3{Version: 3, Bootstrap: attachedworkerdaemon.NativeArtifactV2{Executable: "/owned/bootstrap", Digest: attachedworkerdaemon.ExecutableDigest{2}}, Relay: attachedworkerdaemon.NativeArtifactV2{Executable: "/owned/relay", Digest: attachedworkerdaemon.ExecutableDigest{3}}, Companion: attachedworkerdaemon.NativeArtifactV2{Executable: "/owned/companion", Digest: attachedworkerdaemon.ExecutableDigest{4}}, Workload: native,
			SocketDestination: "/conduit/native.sock", ProviderHosts: []string{"auth.openai.com", "chatgpt.com"}, Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 1 << 20, OutboundBytes: 1 << 20, Active: time.Minute, Cleanup: 10 * time.Second, CompanionMemoryBytes: 64 << 20, CompanionGoroutines: 16}},
		OperatingSystem: "linux", Architecture: "amd64", DeclaredContextWindow: 128000, OutputReserveTokens: 8192, StreamIdleTimeoutMS: 60000}
}

func TestNativeGeneratedV2ExactOverridesAndSevenVectors(t *testing.T) {
	p := nativeGeneratedFixtureV2()
	g, err := generateNativeProfileV2(p)
	if err != nil {
		t.Fatal(err)
	}
	falseFeatures := []string{"shell_tool", "view_image", "multi_agent", "multi_agent_v2", "apps", "plugins", "code_mode", "code_mode_only", "sleep_tool", "browser_use", "computer_use", "image_generation", "goals", "unbounded_connection_retries", "hooks", "unified_exec", "unified_exec_tty", "shell_snapshot", "code_mode_host", "browser_use_external", "browser_use_full_cdp_access", "in_app_browser", "realtime_conversation", "skill_search", "skill_mcp_dependency_install", "workspace_dependencies", "daemon_auto_start", "remote_plugin", "plugin_sharing", "auth_elicitation", "tool_call_mcp_elicitation", "tool_suggest", "api_key_model_discovery", "system_proxy_fallback", "respect_system_proxy", "standalone_web_search", "default_mode_request_user_input", "request_permissions_tool", "unified_image_budget"}
	wantFeaturePairs := make([]string, 0, len(falseFeatures)+1)
	for _, key := range falseFeatures {
		wantFeaturePairs = append(wantFeaturePairs, key+"=false")
	}
	wantFeaturePairs = append(wantFeaturePairs, "skip_host_skill_discovery=true")
	wantSettings := []string{
		`model_provider="sessionless_native"`,
		`model_providers={sessionless_native={name="OpenAI",wire_api="responses",requires_openai_auth=true,supports_websockets=false,supports_standalone_web_search=false,request_max_retries=0,stream_max_retries=0,stream_idle_timeout_ms=60000}}`,
		`model_catalog_json="/sessionless/profile/models.json"`,
		`model_context_window=128000`,
		"features={" + strings.Join(wantFeaturePairs, ",") + "}",
		`tools={experimental_request_user_input={enabled=false},update_plan={enabled=false}}`,
		`skills={include_instructions=false}`, `web_search="disabled"`, `check_for_update_on_startup=false`, `history={persistence="none"}`, `forced_login_method="chatgpt"`, `cli_auth_credentials_store="file"`,
	}
	if string(g.configuration) != strings.Join(wantSettings, "\n")+"\n" {
		t.Fatalf("actual ordered override document differs:\n%s", g.configuration)
	}
	wantBase := []string{"exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--strict-config", "--sandbox", "read-only", "--skip-git-repo-check", "--color", "never", "--model", "model-native-test", "--output-schema", "/sessionless/profile/output.schema.json"}
	for _, setting := range wantSettings {
		wantBase = append(wantBase, "-c", setting)
	}
	imageNames := [7][]string{{}, {"image-01.png"}, {"image-01.jpg"}, {"image-01.png", "image-02.png"}, {"image-01.png", "image-02.jpg"}, {"image-01.jpg", "image-02.png"}, {"image-01.jpg", "image-02.jpg"}}
	for variant, names := range imageNames {
		want := slices.Clone(wantBase)
		for _, name := range names {
			want = append(want, "--image", "/sessionless/input/"+name)
		}
		want = append(want, "-")
		if !slices.Equal(g.arguments[variant], want) {
			t.Fatalf("variant%d actual argv differs\ngot%q\nwant%q", variant, g.arguments[variant], want)
		}
		for _, arg := range g.arguments[variant] {
			if len(arg) > 4096 || strings.ContainsRune(arg, 0) {
				t.Fatalf("variant%d invalid arg bound", variant)
			}
		}
	}
	for _, forbidden := range []string{"base_url", "env_key", "experimental_bearer_token", "max_output_tokens", "output_reserve_tokens", "requires_openai_auth=false", "--oss", "--local-provider", "--worktree", "--dangerously-bypass"} {
		if bytes.Contains(g.configuration, []byte(forbidden)) {
			t.Errorf("unexpected generated override %q", forbidden)
		}
		for _, args := range g.arguments {
			for _, arg := range args {
				if strings.Contains(arg, forbidden) {
					t.Errorf("unexpected generated argument %q", arg)
				}
			}
		}
	}
	if len(g.environment) != 0 {
		t.Fatalf("reserved baseline env must not be duplicated in extras: %+v", g.environment)
	}
	check := p.Launch.Clone()
	check.NativeArguments = g.arguments
	boot, relay, err := nativeharnessbootstrap.InstallationArgumentsV3(check)
	if err != nil || len(relay) == 0 {
		t.Fatalf("shared complete helper lowering: %v", err)
	}
	for i, args := range boot {
		if len(args) > 126 || args[0] != "bootstrap-v3" {
			t.Fatalf("variant%d complete helper lacks descriptor capacity: argc%d", i, len(args))
		}
	}
}

func TestNativeGeneratedV2LiteralCatalogAndStrictArtifactSchema(t *testing.T) {
	g, err := generateNativeProfileV2(nativeGeneratedFixtureV2())
	if err != nil {
		t.Fatal(err)
	}
	wantCatalog := `{"models":[{"slug":"model-native-test","display_name":"model-native-test","description":null,"supported_reasoning_levels":[],"shell_type":"disabled","visibility":"list","supported_in_api":true,"priority":0,"support_verbosity":false,"default_verbosity":null,"apply_patch_tool_type":null,"truncation_policy":{"mode":"bytes","limit":10000},"experimental_supported_tools":[],"input_modalities":["text","image"],"tool_mode":"direct","node_repl_disabled":true,"context_window":128000,"model_messages":{"instructions_template":"You are a Sessionless assistant. Return the requested answer and any requested provider-authored text or JSON artifacts using the supplied output schema. Tools are disabled."}}]}`
	if string(g.catalog) != wantCatalog {
		t.Fatalf("literal owned catalog differs:\ngot%s\nwant%s", g.catalog, wantCatalog)
	}
	wantSchema := `{"type":"object","properties":{"answer":{"type":"string"},"artifacts":{"type":"array","maxItems":2,"items":{"type":"object","properties":{"name":{"type":"string"},"media_type":{"type":"string","enum":["text/plain","application/json"]},"content":{"type":"string"}},"required":["name","media_type","content"],"additionalProperties":false}}},"required":["answer","artifacts"],"additionalProperties":false}`
	if string(g.schema) != wantSchema {
		t.Fatalf("literal structured output schema differs: %s", g.schema)
	}
	var schema map[string]any
	if err := json.Unmarshal(g.schema, &schema); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema["required"], []any{"answer", "artifacts"}) || schema["additionalProperties"] != false {
		t.Fatal("native strict output no longer requires its exact declared fields")
	}
	properties := schema["properties"].(map[string]any)
	artifact := properties["artifacts"].(map[string]any)
	if artifact["maxItems"] != float64(2) || artifact["items"].(map[string]any)["additionalProperties"] != false || artifact["minItems"] != nil {
		t.Fatal("artifact count/object closure changed")
	}
}

func TestNativeGeneratedV2IsIndependentFrozenDataNotAppliedReserve(t *testing.T) {
	p := nativeGeneratedFixtureV2()
	original, err := generateNativeProfileV2(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*NativeProfileV2){func(p *NativeProfileV2) { p.Backend.Enabled = true }, func(p *NativeProfileV2) { p.OutputReserveTokens++ }} {
		other := p.Clone()
		change(&other)
		got, err := generateNativeProfileV2(other)
		if err != nil || !reflect.DeepEqual(got, original) {
			t.Fatalf("activation/reserve became runtime config: %v", err)
		}
	}
	mutated, err := generateNativeProfileV2(p)
	if err != nil {
		t.Fatal(err)
	}
	mutated.configuration[0] = 'X'
	mutated.catalog[0] = 'X'
	mutated.schema[0] = 'X'
	for i := range mutated.arguments {
		mutated.arguments[i][0] = "mutated"
	}
	fresh, err := generateNativeProfileV2(p)
	if err != nil || !reflect.DeepEqual(fresh, original) {
		t.Fatalf("returned generated values alias later calls: %v", err)
	}
	p.DeclaredContextWindow++
	got, err := generateNativeProfileV2(p)
	if err != nil || bytes.Equal(got.catalog, original.catalog) || bytes.Equal(got.configuration, original.configuration) {
		t.Fatalf("declared context not frozen: %v", err)
	}
	p = nativeGeneratedFixtureV2()
	p.StreamIdleTimeoutMS--
	got, err = generateNativeProfileV2(p)
	if err != nil || bytes.Equal(got.configuration, original.configuration) {
		t.Fatalf("stream deadline not frozen: %v", err)
	}
	p = nativeGeneratedFixtureV2()
	p.Backend.Model = "other-model"
	got, err = generateNativeProfileV2(p)
	if err != nil || bytes.Equal(got.catalog, original.catalog) || slices.Equal(got.arguments[0], original.arguments[0]) {
		t.Fatalf("model not frozen: %v", err)
	}
}

func TestNativeGeneratedV2RejectsInvalidGeneratorInputs(t *testing.T) {
	cases := []struct {
		name   string
		change func(*NativeProfileV2)
	}{
		{"unknown-version", func(p *NativeProfileV2) { p.Backend.ExecutableVersion = "0.162.2" }},
		{"empty-model", func(p *NativeProfileV2) { p.Backend.Model = "" }},
		{"model-injection", func(p *NativeProfileV2) { p.Backend.Model = "model\"\napi_key=true" }},
		{"model-overlong", func(p *NativeProfileV2) { p.Backend.Model = strings.Repeat("a", 257) }},
		{"context-zero", func(p *NativeProfileV2) { p.DeclaredContextWindow = 0 }},
		{"context-overflow", func(p *NativeProfileV2) { p.DeclaredContextWindow = uint64(math.MaxInt64) + 1 }},
		{"reserve-zero", func(p *NativeProfileV2) { p.OutputReserveTokens = 0 }},
		{"reserve-exceeds-context", func(p *NativeProfileV2) { p.OutputReserveTokens = p.DeclaredContextWindow }},
		{"idle-zero", func(p *NativeProfileV2) { p.StreamIdleTimeoutMS = 0 }},
		{"idle-over", func(p *NativeProfileV2) { p.StreamIdleTimeoutMS = 60001 }},
		{"launch-version", func(p *NativeProfileV2) { p.Launch.Version = 2 }},
		{"workload-binding", func(p *NativeProfileV2) { p.Launch.Workload.Executable = "/different/native" }},
		{"workload-digest", func(p *NativeProfileV2) { p.Launch.Workload.Digest[0]++ }},
		{"missing-helper", func(p *NativeProfileV2) { p.Launch.Bootstrap.Digest = attachedworkerdaemon.ExecutableDigest{} }},
		{"bad-helper-path", func(p *NativeProfileV2) { p.Launch.Relay.Executable = "relative" }},
		{"bad-native-path", func(p *NativeProfileV2) { p.Backend.Executable = "relative"; p.Launch.Workload.Executable = "relative" }},
		{"bad-origin", func(p *NativeProfileV2) { p.Launch.ProviderHosts = []string{"127.0.0.1"} }},
		{"bad-budget", func(p *NativeProfileV2) { p.Launch.Limits.Connections = 9 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := nativeGeneratedFixtureV2()
			tc.change(&p)
			if _, err := generateNativeProfileV2(p); !errors.Is(err, ErrContract) {
				t.Fatalf("invalid generated input accepted: %v", err)
			}
		})
	}
	p := nativeGeneratedFixtureV2()
	p.DeclaredContextWindow = math.MaxInt64
	p.StreamIdleTimeoutMS = 1
	if _, err := generateNativeProfileV2(p); err != nil {
		t.Fatalf("valid numeric boundary denied: %v", err)
	}
}
