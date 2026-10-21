package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineValidateAndPreview(t *testing.T) {
	t.Parallel()
	var stdout bytes.Buffer
	if err := run([]string{"validate", "--root", "../../observability"}, &stdout); err != nil {
		t.Fatalf("validate repository YAML: %v", err)
	}
	if !strings.Contains(stdout.String(), "VALID:") {
		t.Errorf("validate status: got=%q", stdout.String())
	}
	parent := t.TempDir()
	out := filepath.Join(parent, "private-preview")
	stdout.Reset()
	if err := run([]string{"render", "--root", "../../observability", "--out", out}, &stdout); err != nil {
		t.Fatalf("render preview: %v", err)
	}
	if strings.Contains(stdout.String(), out) || strings.Contains(stdout.String(), "folderId") {
		t.Errorf("stdout leaked private path or selector: %q", stdout.String())
	}
	for _, name := range []string{"normalized.json", "preview.json", "alerts-console.json"} {
		info, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("%s mode: got=%v want=0600", name, info.Mode().Perm())
		}
	}
	if _, err := os.Stat(filepath.Join(out, "dashboard.tf.json")); !os.IsNotExist(err) {
		t.Errorf("preview produced deployable Terraform or unexpected stat error: %v", err)
	}
	if err := run([]string{"render", "--root", "../../observability", "--out", out}, &stdout); err == nil {
		t.Error("existing output directory silently overwritten")
	}
	if err := run([]string{"compare", "--expected", filepath.Join(out, "normalized.json"), "--actual", filepath.Join(out, "normalized.json"), "--out", filepath.Join(out, "comparison.json")}, &stdout); err != nil {
		t.Fatalf("compare normalized local preview: %v", err)
	}
}
func TestPrivateOutputSafety(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	out := filepath.Join(parent, "output")
	if err := os.Mkdir(out, 0755); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(filepath.Join(out, "private.json"), []byte("private-sentinel")); err == nil {
		t.Error("public output parent accepted")
	}
	privateDir := filepath.Join(parent, "private")
	if err := os.Mkdir(privateDir, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(privateDir, "existing.json")
	if err := os.WriteFile(file, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(file, []byte("replacement")); err == nil {
		t.Error("existing artifact overwritten")
	}
	got, err := os.ReadFile(file)
	if err != nil || string(got) != "original" {
		t.Fatalf("existing artifact changed: got=%q err=%v", got, err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(privateDir, link); err != nil {
		t.Fatal(err)
	}
	if err := writeExclusive(filepath.Join(link, "private.json"), []byte("private-sentinel")); err == nil {
		t.Error("symlink output parent accepted")
	}
	if err := writeBundle(link, map[string]any{"preview.json": "private-sentinel"}); err == nil {
		t.Error("existing symlink output directory accepted")
	}
}
func TestOfflineSchemaExport(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "schema")
	var stdout bytes.Buffer
	if err := run([]string{"schema", "--out", out}, &stdout); err != nil {
		t.Fatalf("schema export: %v", err)
	}
	for _, name := range []string{"queries", "dashboards", "alerts", "profile"} {
		data, err := os.ReadFile(filepath.Join(out, name+".schema.json"))
		if err != nil || !bytes.Contains(data, []byte(`"additionalProperties": false`)) {
			t.Errorf("closed %s schema: err=%v", name, err)
		}
	}
}
