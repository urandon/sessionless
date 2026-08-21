package registrygc

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func DecodeStrict[T any](reader io.Reader) (T, error) {
	var value T
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, errors.New("JSON contains more than one value")
		}
		return value, fmt.Errorf("decode trailing JSON: %w", err)
	}
	return value, nil
}

func LoadInventory(path string) (Inventory, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return Inventory{}, err
	}
	var envelope map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		return Inventory{}, fmt.Errorf("decode %s: %w", path, err)
	}
	terraformValue, exists := envelope["terraform"]
	if !exists {
		return Inventory{}, errors.New("inventory is missing terraform evidence")
	}
	terraformObject, ok := terraformValue.(map[string]any)
	if !ok {
		return Inventory{}, errors.New("inventory terraform evidence must be an object")
	}
	expectedDigest, ok := terraformObject["outputs_digest"].(string)
	if !ok || expectedDigest == "" {
		return Inventory{}, errors.New("inventory terraform outputs_digest is missing")
	}
	delete(envelope, "terraform")
	canonical, err := json.Marshal(envelope)
	if err != nil {
		return Inventory{}, fmt.Errorf("canonicalize inventory: %w", err)
	}
	canonical = append(canonical, '\n')
	actualDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(canonical))
	if actualDigest != expectedDigest {
		return Inventory{}, fmt.Errorf("inventory outputs digest mismatch: expected %s, got %s", expectedDigest, actualDigest)
	}
	inventory, err := DecodeStrict[Inventory](bytes.NewReader(payload))
	if err != nil {
		return Inventory{}, fmt.Errorf("decode %s: %w", path, err)
	}
	return inventory, nil
}

func LoadProtectedDigests(path string) (ProtectedDigests, error) {
	return loadStrict[ProtectedDigests](path)
}

func loadStrict[T any](path string) (T, error) {
	var zero T
	file, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	defer file.Close()
	value, err := DecodeStrict[T](file)
	if err != nil {
		return zero, fmt.Errorf("decode %s: %w", path, err)
	}
	return value, nil
}

func LoadManifestDir(path string) ([]DeploymentManifest, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && strings.HasSuffix(name, ".json") {
			paths = append(paths, filepath.Join(path, name))
		}
	}
	sort.Strings(paths)
	if len(paths) != 3 {
		return nil, fmt.Errorf("manifest directory must contain exactly three JSON manifests, got %d", len(paths))
	}
	manifests := make([]DeploymentManifest, 0, len(paths))
	for _, path := range paths {
		manifest, loadErr := loadStrict[DeploymentManifest](path)
		if loadErr != nil {
			return nil, loadErr
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}
