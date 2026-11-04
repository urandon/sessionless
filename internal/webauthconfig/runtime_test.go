package webauthconfig

import "testing"

func TestRuntimeConfigurationClosedVersionAndSensitiveGate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		env   map[string]string
		want  Runtime
		valid bool
	}{
		{"v1", map[string]string{"WEB_IDENTITY_VERSION": "1"}, Runtime{Version: "1"}, true},
		{"v1off", map[string]string{"WEB_IDENTITY_VERSION": "1", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "false"}, Runtime{Version: "1"}, true},
		{"v2off", map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "false"}, Runtime{Version: "2"}, true},
		{"v2on", map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "true"}, Runtime{Version: "2", SensitiveOperationsEnabled: true}, true},
		{"missing", map[string]string{}, Runtime{}, false},
		{"empty", map[string]string{"WEB_IDENTITY_VERSION": ""}, Runtime{}, false},
		{"padded", map[string]string{"WEB_IDENTITY_VERSION": " 2"}, Runtime{}, false},
		{"unknown", map[string]string{"WEB_IDENTITY_VERSION": "3"}, Runtime{}, false},
		{"v2missinggate", map[string]string{"WEB_IDENTITY_VERSION": "2"}, Runtime{}, false},
		{"v2emptygate", map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": ""}, Runtime{}, false},
		{"v2aliasgate", map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "1"}, Runtime{}, false},
		{"v2casegate", map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "TRUE"}, Runtime{}, false},
		{"v1on", map[string]string{"WEB_IDENTITY_VERSION": "1", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "true"}, Runtime{}, false},
		{"v1emptygate", map[string]string{"WEB_IDENTITY_VERSION": "1", "WEB_IDENTITY_SENSITIVE_OPERATIONS": ""}, Runtime{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RuntimeFromEnvironment(func(key string) (string, bool) { v, ok := tc.env[key]; return v, ok })
			if (err == nil) != tc.valid || got != tc.want {
				t.Fatalf("mode %s: got=%+v err=%v want=%+v valid=%v", tc.name, got, err, tc.want, tc.valid)
			}
		})
	}
	if got, err := RuntimeFromEnvironment(nil); err == nil || got != (Runtime{}) {
		t.Fatal("nil lookup yielded authority")
	}
}
