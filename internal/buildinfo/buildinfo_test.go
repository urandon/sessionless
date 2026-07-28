package buildinfo

import "testing"

func TestCurrent(t *testing.T) {
	t.Parallel()

	info := Current("control-api")
	if info.Component != "control-api" {
		t.Fatalf("Component = %q, want control-api", info.Component)
	}
	if info.Version == "" || info.Commit == "" || info.BuiltAt == "" || info.GoVersion == "" {
		t.Fatalf("Current returned an empty field: %+v", info)
	}
}
