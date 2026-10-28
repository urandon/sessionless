package attachedresourcegeneration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sys/unix"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func configMutation(t *testing.T, raw []byte, change func(map[string]any)) []byte {
	t.Helper()
	var value map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		t.Fatal(err)
	}
	change(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func configBaseline(value map[string]any) map[string]any {
	return value["baselines"].([]any)[0].(map[string]any)
}

func TestRegistryConfigExactClosedNestedShape(t *testing.T) {
	t.Parallel()
	b := registryBaseline(t, 1, 0)
	raw := registryConfig(t, b)
	registry, err := attachedresourcegeneration.DecodeRegistryConfigV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, found := registry.Lookup(b.TenantID, b.OwnerUserID, b.SubscriptionConnectionID)
	if !found || !reflect.DeepEqual(got, b) {
		t.Fatal("closed config changed complete baseline")
	}
	// JSON object order and whitespace are not installation identities.
	reordered := configMutation(t, raw, func(map[string]any) {})
	reordered = append([]byte(" \n"), reordered...)
	if _, err := attachedresourcegeneration.DecodeRegistryConfigV1(reordered); err != nil {
		t.Fatalf("ordinary JSON formatting rejected: %v", err)
	}
	exactBound := append(append([]byte(nil), raw...), bytes.Repeat([]byte(" "), attachedresourcegeneration.MaxConfigBytesV1-len(raw))...)
	if _, err := attachedresourcegeneration.DecodeRegistryConfigV1(exactBound); err != nil {
		t.Fatalf("inclusive config byte bound: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"unknown top", func(v map[string]any) { v["unexpected"] = true }},
		{"missing version", func(v map[string]any) { delete(v, "version") }},
		{"null version", func(v map[string]any) { v["version"] = nil }},
		{"unsupported version", func(v map[string]any) { v["version"] = 2 }},
		{"case top", func(v map[string]any) { v["Version"] = v["version"]; delete(v, "version") }},
		{"empty entries", func(v map[string]any) { v["baselines"] = []any{} }},
		{"null entries", func(v map[string]any) { v["baselines"] = nil }},
		{"null baseline", func(v map[string]any) { v["baselines"].([]any)[0] = nil }},
		{"null delegation", func(v map[string]any) { configBaseline(v)["generation_only_delegation"] = nil }},
		{"missing delegation", func(v map[string]any) { delete(configBaseline(v), "generation_only_delegation") }},
		{"nested baseline alias", func(v map[string]any) {
			m := configBaseline(v)
			m["Owner_User_ID"] = m["owner_user_id"]
			delete(m, "owner_user_id")
		}},
		{"nested binding alias", func(v map[string]any) {
			m := configBaseline(v)["binding"].(map[string]any)
			m["Model_ID"] = m["model_id"]
			delete(m, "model_id")
		}},
		{"nested backend alias", func(v map[string]any) {
			m := configBaseline(v)["binding"].(map[string]any)["backend"].(map[string]any)
			m["Backend_Kind"] = m["backend_kind"]
			delete(m, "backend_kind")
		}},
		{"missing required zero scope", func(v map[string]any) { delete(configBaseline(v)["binding"].(map[string]any), "run_id") }},
		{"null required zero scope", func(v map[string]any) { configBaseline(v)["binding"].(map[string]any)["run_id"] = nil }},
		{"optional pointer null", func(v map[string]any) { configBaseline(v)["binding"].(map[string]any)["agent_loop_policy"] = nil }},
		{"null evidence object", func(v map[string]any) { configBaseline(v)["evidence"] = nil }},
		{"nested evidence unknown", func(v map[string]any) {
			configBaseline(v)["evidence"].(map[string]any)["capability"].(map[string]any)["authorization"] = "not-a-grant"
		}},
		{"nested state alias", func(v map[string]any) {
			m := configBaseline(v)["evidence"].(map[string]any)["capability"].(map[string]any)
			m["Tool_Calling"] = m["tool_calling"]
			delete(m, "tool_calling")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := attachedresourcegeneration.DecodeRegistryConfigV1(configMutation(t, raw, tc.change))
			if registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
				t.Fatalf("invalid config returned registry=%v err=%v", registry, err)
			}
		})
	}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"duplicate", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{"nested duplicate", bytes.Replace(raw, []byte(`"tool_calling":"unknown"`), []byte(`"tool_calling":"unknown","tool_calling":"supported"`), 1)},
		{"fraction version", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1.0`), 1)},
		{"invalid Unicode", bytes.Replace(raw, []byte(`"owner"`), []byte(`"\ud800"`), 1)},
		{"trailing", append(append([]byte(nil), raw...), []byte(`{}`)...)},
		{"oversize", append(exactBound, ' ')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if registry, err := attachedresourcegeneration.DecodeRegistryConfigV1(tc.raw); registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
				t.Fatalf("invalid lexical config: registry=%v err=%v", registry, err)
			}
		})
	}
}

func TestRegistryConfigPreservesCanonicalNullAndEmptyUnknownCollections(t *testing.T) {
	t.Parallel()
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "null", true: "empty"}[empty], func(t *testing.T) {
			b := registryBaseline(t, 1, 0)
			b.Evidence.Route.State = domain.ProviderEvidenceUnknownV1
			b.Evidence.Route.PolicyID, b.Evidence.Route.Revision, b.Evidence.Route.Routes = "", 0, nil
			b.Evidence.Policy.Verdict, b.Evidence.Policy.AllowedDataClasses = domain.ProviderPolicyUnknownV1, nil
			if empty {
				b.Evidence.Route.Routes = []domain.ProviderRouteV1{}
				b.Evidence.Policy.AllowedDataClasses = []domain.ProviderDataClassV1{}
			}
			route, err := b.Evidence.Route.Digest()
			if err != nil {
				t.Fatal(err)
			}
			b.Binding.ProviderRouteDigest, b.Evidence.Policy.RoutePolicyDigest = string(route), string(route)
			policy, err := b.Evidence.Policy.Digest()
			if err != nil {
				t.Fatal(err)
			}
			b.Binding.EffectivePolicyDigest = string(policy)
			raw := registryConfig(t, b)
			registry, err := attachedresourcegeneration.DecodeRegistryConfigV1(raw)
			if err != nil {
				t.Fatal(err)
			}
			got, found := registry.Lookup(b.TenantID, b.OwnerUserID, b.SubscriptionConnectionID)
			if !found || !reflect.DeepEqual(got, b) {
				t.Fatal("canonical unknown collection changed on installation")
			}
			binding, evidence, err := domain.ProjectAttachedResourceGenerationV1(got, b.Binding.Resource.CredentialGeneration)
			if err != nil || !reflect.DeepEqual(binding, b.Binding) || !reflect.DeepEqual(evidence, b.Evidence) {
				t.Fatalf("same-generation unknown shape/digest changed: %v", err)
			}
			missing := configMutation(t, raw, func(v map[string]any) {
				delete(configBaseline(v)["evidence"].(map[string]any)["route"].(map[string]any), "routes")
			})
			if _, err := attachedresourcegeneration.DecodeRegistryConfigV1(missing); !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
				t.Fatalf("missing required collection treated as canonical null: %v", err)
			}
			missing = configMutation(t, raw, func(v map[string]any) {
				delete(configBaseline(v)["evidence"].(map[string]any)["policy"].(map[string]any), "allowed_data_classes")
			})
			if _, err := attachedresourcegeneration.DecodeRegistryConfigV1(missing); !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
				t.Fatalf("missing required classes treated as canonical null: %v", err)
			}
		})
	}
}

func configEnvironment(flag, path string) func(string) string {
	return func(name string) string {
		switch name {
		case "ATTACHED_RESOURCE_GENERATION_ENABLED":
			return flag
		case "ATTACHED_RESOURCE_GENERATION_CONFIG_FILE":
			return path
		}
		return ""
	}
}

func writeRegistryFile(t *testing.T, raw []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "baselines.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRegistryEnvironmentRequiresExactExplicitFileConfiguration(t *testing.T) {
	t.Parallel()
	for _, flag := range []string{"", "false"} {
		if registry, err := attachedresourcegeneration.LoadRegistryFromEnv(configEnvironment(flag, "")); err != nil || registry != nil {
			t.Fatalf("default off flag=%q registry=%v err=%v", flag, registry, err)
		}
	}
	if registry, err := attachedresourcegeneration.LoadRegistryFromEnv(nil); registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
		t.Fatal("nil environment accessor accepted")
	}
	for _, tc := range []struct{ flag, path string }{{"", "/does-not-exist"}, {"false", "/does-not-exist"}, {"TRUE", ""}, {"true ", ""}, {"true", ""}, {"true", "relative.json"}, {"true", "/absolute/../config.json"}, {"true", " /config.json"}, {"true", "/" + strings.Repeat("x", 4096)}} {
		registry, err := attachedresourcegeneration.LoadRegistryFromEnv(configEnvironment(tc.flag, tc.path))
		if registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) || err.Error() != attachedresourcegeneration.ErrInvalidConfig.Error() {
			t.Fatalf("invalid flag/path yielded non-sanitized result: flag=%q registry=%v err=%v", tc.flag, registry, err)
		}
	}
	b := registryBaseline(t, 1, 0)
	path := writeRegistryFile(t, registryConfig(t, b))
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	registry, err := attachedresourcegeneration.LoadRegistryFromEnv(configEnvironment("true", path))
	if err != nil || registry.Len() != 1 {
		t.Fatalf("read-only metadata configuration rejected: %v", err)
	}
	// Only this test-owned file changes. Installed values never reread it.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("invalid replacement"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, found := registry.Lookup(b.TenantID, b.OwnerUserID, b.SubscriptionConnectionID)
	if !found || !reflect.DeepEqual(got, b) {
		t.Fatal("registry reloaded a changed operator file")
	}
}

func TestRegistryEnvironmentRejectsUnsafeAndOversizeFiles(t *testing.T) {
	t.Parallel()
	valid := registryConfig(t, registryBaseline(t, 1, 0))
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) string
	}{
		{"missing", func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing-private-name.json") }},
		{"empty", func(t *testing.T) string { return writeRegistryFile(t, nil) }},
		{"bad JSON", func(t *testing.T) string { return writeRegistryFile(t, []byte("sensitive-config-marker")) }},
		{"oversize", func(t *testing.T) string {
			return writeRegistryFile(t, bytes.Repeat([]byte(" "), attachedresourcegeneration.MaxConfigBytesV1+1))
		}},
		{"directory", func(t *testing.T) string { return t.TempDir() }},
		{"writable by others", func(t *testing.T) string {
			path := writeRegistryFile(t, valid)
			if err := os.Chmod(path, 0o666); err != nil {
				t.Fatal(err)
			}
			return path
		}},
		{"symlink", func(t *testing.T) string {
			original := writeRegistryFile(t, valid)
			link := filepath.Join(t.TempDir(), "link.json")
			if err := os.Symlink(original, link); err != nil {
				t.Fatal(err)
			}
			return link
		}},
		{"FIFO without writer", func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "fifo")
			if err := unix.Mkfifo(path, 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.setup(t)
			registry, err := attachedresourcegeneration.LoadRegistryFromEnv(configEnvironment("true", path))
			if registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) || err.Error() != attachedresourcegeneration.ErrInvalidConfig.Error() || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "sensitive-config-marker") {
				t.Fatalf("unsafe config returned unbounded/contentful failure: registry=%v err=%v", registry, err)
			}
		})
	}
}
