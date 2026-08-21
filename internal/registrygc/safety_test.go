package registrygc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type discoverSpy struct{ calls int }

func (spy *discoverSpy) Discover(context.Context, Inventory) (LiveState, error) {
	spy.calls++
	return LiveState{}, nil
}
func (*discoverSpy) GetImage(context.Context, string) (CloudImage, error) { return CloudImage{}, nil }
func (*discoverSpy) DeleteImage(context.Context, string) error            { return nil }

func TestRunValidatesStaticEvidenceBeforeCloudCalls(t *testing.T) {
	fixture := newGCFixture(t, ModeDryRun)
	fixture.inventory.RegistryID = "INVALID"
	spy := &discoverSpy{}
	_, err := Run(context.Background(), fixture.config, fixture.inventory, fixture.manifests, fixture.protected, spy)
	if err == nil || spy.calls != 0 {
		t.Fatalf("Run() error=%v Discover calls=%d, want static rejection before cloud", err, spy.calls)
	}
}

func TestRunNormalizesMissingProtectedDigestFile(t *testing.T) {
	fixture := newGCFixture(t, ModeDryRun)
	cloud := &fakeCloud{live: fixture.live, get: func(string) (CloudImage, error) { return CloudImage{}, nil }}
	if _, err := Run(context.Background(), fixture.config, fixture.inventory, fixture.manifests, ProtectedDigests{}, cloud); err != nil {
		t.Fatalf("Run() with no protected file: %v", err)
	}
}

func TestReportJSONIsByteStableAtFixedClock(t *testing.T) {
	fixture := newGCFixture(t, ModeDryRun)
	first, err := BuildPlan(fixture.config, fixture.inventory, fixture.live, fixture.manifests, fixture.protected)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildPlan(fixture.config, fixture.inventory, fixture.live, fixture.manifests, fixture.protected)
	if err != nil {
		t.Fatal(err)
	}
	var firstJSON, secondJSON bytes.Buffer
	if err := WriteJSON(&firstJSON, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSON(&secondJSON, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON.Bytes(), secondJSON.Bytes()) {
		t.Fatal("fixed evidence produced non-deterministic report JSON")
	}
}

func TestDecodeStrictRejectsUnknownInventoryFields(t *testing.T) {
	var inventory Inventory
	payload, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.Replace(payload, []byte("{"), []byte(`{"unknown":true,`), 1)
	if _, err := DecodeStrict[Inventory](strings.NewReader(string(payload))); err == nil {
		t.Fatal("unknown inventory field was accepted")
	}
}

func TestLoadInventoryRejectsPayloadChangedAfterTerraformExport(t *testing.T) {
	fixture := newGCFixture(t, ModeDryRun)
	inventory := fixture.inventory
	inventory.Terraform.OutputsDigest = ""
	raw, err := json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	delete(payload, "terraform")
	canonical, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	canonical = append(canonical, '\n')
	inventory.Terraform.OutputsDigest = fmt.Sprintf("sha256:%x", sha256.Sum256(canonical))
	raw, err = json.Marshal(inventory)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "inventory.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadInventory(path); err != nil {
		t.Fatalf("valid inventory export rejected: %v", err)
	}
	tampered := bytes.Replace(raw, []byte(`"folder_id":"folder123"`), []byte(`"folder_id":"folder999"`), 1)
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadInventory(path); err == nil || !strings.Contains(err.Error(), "outputs digest mismatch") {
		t.Fatalf("tampered inventory error = %v, want outputs digest mismatch", err)
	}
}
