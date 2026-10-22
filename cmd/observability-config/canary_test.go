package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanaryOfflineCLIAndSchema(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	input := "../../observability/canary.yaml"
	if err := run([]string{"canary-validate", "--input", input}, &stdout); err != nil {
		t.Fatalf("canary validate: %v", err)
	}
	if !strings.Contains(stdout.String(), "PLAN_ONLY") {
		t.Errorf("missing plan-only status: %q", stdout.String())
	}
	out := filepath.Join(t.TempDir(), "canary-preview")
	stdout.Reset()
	if err := run([]string{"canary-render", "--input", input, "--out", out}, &stdout); err != nil {
		t.Fatalf("canary render: %v", err)
	}
	if strings.Contains(stdout.String(), out) || strings.Contains(stdout.String(), "selector") {
		t.Error("stdout leaked output path or selector")
	}
	for _, name := range []string{"canary-normalized.json", "canary-console.json", "canary-plan.json"} {
		info, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("artifact %s mode=%v want=0600", name, info.Mode().Perm())
		}
		data, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte("PLAN_ONLY_NO_LIVE_AUTHORITY")) {
			t.Errorf("artifact %s lacks authority limit", name)
		}
		if !bytes.Contains(data, []byte("PENDING_RULE_READBACK")) {
			t.Errorf("artifact %s pretends actual notification controls were read", name)
		}
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("canary artifact count=%d want=3", len(entries))
	}
	for _, name := range []string{"dashboard.tf.json", "writer.json", "normalized.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); !os.IsNotExist(err) {
			t.Errorf("unexpected executable/production artifact %s: %v", name, err)
		}
	}
	if err := run([]string{"canary-render", "--input", input, "--out", out}, &stdout); err == nil {
		t.Error("existing output overwritten")
	}
	if err := run([]string{"compare", "--expected", filepath.Join(out, "canary-normalized.json"), "--actual", filepath.Join(out, "canary-normalized.json"), "--out", filepath.Join(out, "compare.json")}, &stdout); err == nil {
		t.Error("production compare accepted canary artifact")
	}
	schemaOut := filepath.Join(t.TempDir(), "schema")
	if err := run([]string{"canary-schema", "--out", schemaOut}, &stdout); err != nil {
		t.Fatalf("schema: %v", err)
	}
	for _, name := range []string{"canary.schema.json", "canary-bindings.schema.json"} {
		data, err := os.ReadFile(filepath.Join(schemaOut, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte(`"additionalProperties": false`)) || !bytes.Contains(data, []byte(`"required"`)) {
			t.Errorf("schema %s not closed/required", name)
		}
	}
}

func TestCanaryCLIRejectsMixedFlags(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"canary-validate", "--root", "observability"},
		{"canary-validate", "--profile", "cloud-dev"},
		{"canary-validate", "--out", ""},
		{"canary-validate", "--bindings", ""},
		{"canary-validate", "--now", ""},
		{"canary-validate", "--ownership", ""},
		{"canary-validate", "--expected", ""},
		{"canary-validate", "--actual", ""},
		{"canary-validate", "positional"},
		{"canary-schema", "--out", "unused", "--input", ""},
		{"canary-schema", "--out", "unused", "--bindings", ""},
		{"canary-schema", "--out", "unused", "--now", ""},
		{"canary-schema"},
		{"canary-render", "--input", "../../observability/canary.yaml"},
		{"canary-render", "--out", "unused", "--bindings", "private"},
		{"canary-render", "--out", "unused", "--now", "2026-01-01T00:00:00Z"},
		{"canary-render", "--out", "unused", "--bindings", "", "--now", ""},
		{"render", "--input", "../../observability/canary.yaml"},
		{"validate", "--input", "../../observability/canary.yaml"},
	}
	for i, args := range cases {
		var stdout bytes.Buffer
		if err := run(args, &stdout); err == nil {
			t.Errorf("mixed flag case %d accepted: %v", i, args)
		}
		if stdout.Len() != 0 {
			t.Errorf("rejected flags emitted output in case %d", i)
		}
	}
}

func TestCanaryBoundCLIPrivateOutput(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../observability/canary-bindings.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.NewReplacer(
		`revision: example-not-admitted`, `revision: private-sentinel`,
		`folder_id: ""`, `folder_id: fixturefolder`,
		`project_id: ""`, `project_id: folder__fixturefolder`,
		`channel_id: ""`, `channel_id: folder__fixturefolder_fixture-push`,
		`instance: ""`, `instance: "0123456789abcdef0123456789abcdef"`,
		`"2000-01-01T00:00:00Z"`, `"2026-01-01T12:00:00Z"`,
		`: false`, `: true`,
		`rule_collision: true`, `rule_collision: false`,
		`series_collision: true`, `series_collision: false`,
	).Replace(string(data)))
	parent := t.TempDir()
	private := filepath.Join(parent, "binding.yaml")
	if err := os.WriteFile(private, data, 0600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(parent, "bound")
	args := []string{"canary-render", "--input", "../../observability/canary.yaml", "--out", out, "--bindings", private, "--now", "2026-01-01T12:00:00Z"}
	var stdout bytes.Buffer
	if err := run(args, &stdout); err != nil {
		t.Fatalf("bound synthetic canary export: %v", err)
	}
	for _, sentinel := range []string{"private-sentinel", "fixturefolder", "fixture-push", "0123456789abcdef0123456789abcdef", private, out} {
		if strings.Contains(stdout.String(), sentinel) {
			t.Errorf("stdout leaked private sentinel %q", sentinel)
		}
	}
	console, err := os.ReadFile(filepath.Join(out, "canary-console.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(console, []byte("fixture-push")) || !bytes.Contains(console, []byte("PLAN_ONLY")) || !bytes.Contains(console, []byte("PENDING_RULE_READBACK")) {
		t.Error("private output lost target or plan-only limit")
	}
	if err := os.Chmod(private, 0644); err != nil {
		t.Fatal(err)
	}
	args[4] = filepath.Join(parent, "rejected")
	stdout.Reset()
	if err := run(args, &stdout); err == nil {
		t.Error("permissive private CLI binding accepted")
	} else if strings.Contains(err.Error(), "private-sentinel") || strings.Contains(err.Error(), private) {
		t.Error("private error leaked input")
	}
}

func TestCanaryExclusiveOutput(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	original := filepath.Join(parent, "original")
	if err := os.Mkdir(original, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(original, "sentinel")
	if err := os.WriteFile(file, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(original, link); err != nil {
		t.Fatal(err)
	}
	if err := writeCanaryBundle(link, map[string]any{"canary-plan.json": "private-sentinel"}); err == nil {
		t.Error("existing symlink output accepted")
	}
	if err := writeCanaryBundle(original, map[string]any{"canary-plan.json": "private-sentinel"}); err == nil {
		t.Error("existing output accepted")
	}
	data, err := os.ReadFile(file)
	if err != nil || string(data) != "keep" {
		t.Fatalf("existing evidence changed: got=%q err=%v", data, err)
	}
}
