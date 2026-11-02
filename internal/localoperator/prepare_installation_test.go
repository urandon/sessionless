//go:build darwin || linux

package localoperator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"golang.org/x/sys/unix"
)

// Only synthetic complete declaration DATA is reused. The explicit executable
// is never executed; these test-owned key bytes have no external authority.
func freshPreparationInput(t *testing.T) (string, string, localoperator.ManifestV1) {
	t.Helper()
	_, exe, m := preparedTemplateFiles(t)
	m.KeyFingerprint = ""
	m.Configuration.WorkerExecutableDigest = ""
	m.Configuration.LocalHostIdentityDigest = ""
	m.Configuration.LocalNetworkNamespaceIdentityDigest = ""
	s := &m.Profile.Substrate
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest = "", ""
	s.OuterHarnessArtifactDigest, s.EgressProxyArtifactDigest, s.EgressProxyIdentityDigest = "", "", ""
	s.EgressPolicyDigest, s.AdmissionCostCeilingDigest = "", ""
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("canonical synthetic output parent", err)
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal("private synthetic output parent", err)
	}
	return filepath.Join(parent, "installation"), exe, m
}

func assertNoPreparedOutput(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied preparation created or retained unexpected output", err)
	}
}

func readPreparationFile(t *testing.T, path string, mode os.FileMode) []byte {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != mode {
		t.Fatal("prepared regular private file predicate failed", err)
	}
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || uint32(stat.Mode)&0o7777 != uint32(mode) {
		t.Fatal("prepared file owner/nlink/exact mode failed", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("read synthetic prepared output", err)
	}
	return raw
}

func TestPrepareInstallationFreshPrivateFilesAndOriginalIntake(t *testing.T) {
	root, exe, input := freshPreparationInput(t)
	raw := preparedRaw(t, input)
	before := bytes.Clone(raw)
	if err := localoperator.PrepareInstallation(t.Context(), root, exe, raw); err != nil {
		t.Fatal("complete synthetic preparation denied", err)
	}
	if !bytes.Equal(raw, before) {
		t.Fatal("preparation changed caller-owned canonical input bytes")
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		t.Fatal("prepared root is not an exact private directory", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 || entries[0].Name() != "manifest.json" || entries[1].Name() != "source.key" {
		t.Fatal("preparation created anything other than the two fixed files", err)
	}
	manifest := readPreparationFile(t, filepath.Join(root, "manifest.json"), 0o600)
	key := readPreparationFile(t, filepath.Join(root, "source.key"), 0o600)
	defer clear(key)
	if len(key) != 32 {
		t.Fatal("prepared synthetic key is not 32 bytes")
	}
	var got localoperator.ManifestV1
	if err := json.Unmarshal(manifest, &got); err != nil {
		t.Fatal("prepared canonical manifest decode", err)
	}
	if !bytes.Equal(manifest, preparedRaw(t, got)) || got.KeyFingerprint != fitDigest(key) {
		t.Fatal("prepared canonical bytes/key fingerprint correlation failed")
	}
	identity, err := localoperator.InspectRuntime(t.Context(), exe)
	if err != nil {
		t.Fatal("independent runtime inspection", err)
	}
	want := input
	want.KeyFingerprint = fitDigest(key)
	c, s := &want.Configuration, &want.Profile.Substrate
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest = identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	s.OuterHarnessArtifactDigest, s.EgressProxyArtifactDigest = identity.WorkerExecutableDigest, identity.WorkerExecutableDigest
	s.EgressProxyIdentityDigest, err = localoperator.LinkedConnectorIdentity(identity.WorkerExecutableDigest)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(*c)
	if err != nil {
		t.Fatal(err)
	}
	s.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	s.AdmissionCostCeilingDigest, err = want.Profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(manifest, preparedRaw(t, want)) || !reflect.DeepEqual(input.Evidence, got.Evidence) {
		t.Fatal("preparation changed an operator-authored declaration/body/ref or derived the wrong runtime DATA")
	}
	if err := localoperator.ValidateManifestTemplate(t.Context(), manifest, exe); err != nil {
		t.Fatal("prepared output failed independent read-only validation", err)
	}
	// Genuine original file/key intake, TEST ONLY. This produces an unpublished
	// grant; no Store/receipt/currentness or actual model qualification is claimed.
	source := openFitSource(t, t.Context(), root, exe)
	configuration, err := source.Configuration()
	if err != nil || configuration.Resource.ResourceID == "" || configuration.LocalInstallationID == "" || configuration.Resource.Revision != 1 || configuration.ProfileRevision != 1 ||
		configuration.OriginalSource.Digest != fitDigest(manifest) || configuration.OriginalSource.Bytes != uint64(len(manifest)) {
		t.Fatal("original intake did not own fresh IDs and exact prepared manifest bytes", err)
	}
	publication, err := source.Publication()
	if err != nil || publication.Profile.ValidateInitial() != nil || publication.Validate() != nil {
		t.Fatal("original intake publication validation failed", err)
	}
	if !bytes.Equal(manifest, readPreparationFile(t, filepath.Join(root, "manifest.json"), 0o600)) || !bytes.Equal(key, readPreparationFile(t, filepath.Join(root, "source.key"), 0o600)) {
		t.Fatal("validation/intake changed the prepared private files")
	}
}

func TestPrepareInstallationRejectsNonemptyDerivedFieldsBeforeOutput(t *testing.T) {
	for _, check := range []struct {
		name   string
		change func(*localoperator.ManifestV1)
	}{
		{"key-fingerprint", func(m *localoperator.ManifestV1) { m.KeyFingerprint = strings.Repeat("f", 64) }},
		{"worker", func(m *localoperator.ManifestV1) { m.Configuration.WorkerExecutableDigest = strings.Repeat("f", 64) }},
		{"host", func(m *localoperator.ManifestV1) { m.Configuration.LocalHostIdentityDigest = strings.Repeat("f", 64) }},
		{"namespace", func(m *localoperator.ManifestV1) {
			m.Configuration.LocalNetworkNamespaceIdentityDigest = strings.Repeat("f", 64)
		}},
		{"substrate-host", func(m *localoperator.ManifestV1) {
			m.Profile.Substrate.LocalHostIdentityDigest = strings.Repeat("f", 64)
		}},
		{"substrate-namespace", func(m *localoperator.ManifestV1) {
			m.Profile.Substrate.LocalNetworkNamespaceIdentityDigest = strings.Repeat("f", 64)
		}},
		{"outer-artifact", func(m *localoperator.ManifestV1) {
			m.Profile.Substrate.OuterHarnessArtifactDigest = strings.Repeat("f", 64)
		}},
		{"egress-artifact", func(m *localoperator.ManifestV1) {
			m.Profile.Substrate.EgressProxyArtifactDigest = strings.Repeat("f", 64)
		}},
		{"connector", func(m *localoperator.ManifestV1) {
			m.Profile.Substrate.EgressProxyIdentityDigest = strings.Repeat("f", 64)
		}},
		{"egress-policy", func(m *localoperator.ManifestV1) { m.Profile.Substrate.EgressPolicyDigest = strings.Repeat("f", 64) }},
		{"cost-digest", func(m *localoperator.ManifestV1) {
			m.Profile.Substrate.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(strings.Repeat("f", 64))
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			root, exe, m := freshPreparationInput(t)
			check.change(&m)
			if err := localoperator.PrepareInstallation(t.Context(), root, exe, preparedRaw(t, m)); !errors.Is(err, localoperator.ErrDenied) {
				t.Fatal("nonempty derived field was overwritten or accepted", err)
			}
			assertNoPreparedOutput(t, root)
		})
	}
}

func TestPrepareInstallationInvalidDataCreatesNoOutput(t *testing.T) {
	for _, check := range []struct {
		name   string
		change func(*testing.T, *localoperator.ManifestV1)
	}{
		{"installed-id", func(_ *testing.T, m *localoperator.ManifestV1) { m.Profile.InstallationID = "already-installed" }},
		{"installed-reference", func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Configuration.OriginalSource = m.Configuration.OperatorTerms
		}},
		{"wrong-version", func(_ *testing.T, m *localoperator.ManifestV1) { m.Version = 2 }},
		{"invalid-endpoint", func(_ *testing.T, m *localoperator.ManifestV1) { m.Configuration.BaseURL = "http://localhost:11434/v1" }},
		{"missing-calibration", func(t *testing.T, m *localoperator.ManifestV1) {
			removePreparedBody(t, m, m.Configuration.CalibrationDigest)
		}},
		{"unsupported-mcp-schema", func(t *testing.T, m *localoperator.ManifestV1) {
			rebindPreparedMCPSchema(t, m, json.RawMessage(`{"additionalProperties":false,"allOf":[],"properties":{"query":{"maxLength":30,"minLength":1,"type":"string"}},"required":["query"],"type":"object"}`))
			m.Profile.Substrate.AdmissionCostCeilingDigest = ""
		}},
		{"missing-search", func(_ *testing.T, m *localoperator.ManifestV1) { m.SearchExpenditure = nil }},
		{"changed-body", func(_ *testing.T, m *localoperator.ManifestV1) {
			m.Evidence[0].Body = []byte("not the original evidence")
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			root, exe, m := freshPreparationInput(t)
			check.change(t, &m)
			if err := localoperator.PrepareInstallation(t.Context(), root, exe, preparedRaw(t, m)); !errors.Is(err, localoperator.ErrDenied) {
				t.Fatal("invalid full preparation DATA accepted", err)
			}
			assertNoPreparedOutput(t, root)
		})
	}
	root, exe, m := freshPreparationInput(t)
	raw := preparedRaw(t, m)
	for _, invalid := range [][]byte{nil, bytes.Repeat([]byte("x"), (256<<10)+1), raw[:len(raw)-1], append(bytes.Clone(raw), []byte(`{}`)...), append([]byte(" "), raw...), bytes.Replace(raw, []byte(`{"version":1,`), []byte(`{"version":1,"version":1,`), 1)} {
		if err := localoperator.PrepareInstallation(t.Context(), root, exe, invalid); !errors.Is(err, localoperator.ErrDenied) {
			t.Fatal("invalid/oversized canonical encoding accepted", err)
		}
		assertNoPreparedOutput(t, root)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, invalid := range []context.Context{nil, ctx} {
		if err := localoperator.PrepareInstallation(invalid, root, exe, raw); !errors.Is(err, localoperator.ErrDenied) {
			t.Fatal("invalid/cancelled context accepted", err)
		}
		assertNoPreparedOutput(t, root)
	}
	if err := localoperator.PrepareInstallation(t.Context(), root, filepath.Join(filepath.Dir(exe), "missing-private-marker"), raw); !errors.Is(err, localoperator.ErrDenied) || strings.Contains(err.Error(), "missing-private-marker") {
		t.Fatal("missing executable did not safely deny", err)
	}
	assertNoPreparedOutput(t, root)
}

func TestPrepareInstallationRejectsExistingAndPartialRootsWithoutChanges(t *testing.T) {
	for _, name := range []string{"complete", "empty", "manifest-only", "key-only", "symlink-root", "regular-root"} {
		t.Run(name, func(t *testing.T) {
			root, exe, m := freshPreparationInput(t)
			raw := preparedRaw(t, m)
			if name == "complete" {
				if err := localoperator.PrepareInstallation(t.Context(), root, exe, raw); err != nil {
					t.Fatal("initial preparation", err)
				}
			} else if name == "regular-root" {
				if err := os.WriteFile(root, []byte("original root file"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				target := root
				if name == "symlink-root" {
					target = filepath.Join(filepath.Dir(root), "original-target")
				}
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				if name == "manifest-only" || name == "symlink-root" {
					if err := os.WriteFile(filepath.Join(target, "manifest.json"), []byte("retained incomplete original manifest"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if name == "key-only" {
					if err := os.WriteFile(filepath.Join(target, "source.key"), []byte("incomplete original key"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if name == "symlink-root" {
					if err := os.Symlink(target, root); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := snapshotPreparationTree(t, filepath.Dir(root))
			for range 2 {
				if err := localoperator.PrepareInstallation(t.Context(), root, exe, raw); !errors.Is(err, localoperator.ErrDenied) {
					t.Fatal("existing/partial output reused or overwritten", err)
				}
			}
			if !reflect.DeepEqual(before, snapshotPreparationTree(t, filepath.Dir(root))) {
				t.Fatal("denial changed existing path identities, modes or bytes")
			}
		})
	}
}

type preparationTreeEntry struct {
	Mode     os.FileMode
	Dev, Ino uint64
	Body     string
}

func snapshotPreparationTree(t *testing.T, root string) map[string]preparationTreeEntry {
	t.Helper()
	result := make(map[string]preparationTreeEntry)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		var stat unix.Stat_t
		if err := unix.Lstat(path, &stat); err != nil {
			return err
		}
		value := preparationTreeEntry{Mode: info.Mode(), Dev: uint64(stat.Dev), Ino: stat.Ino}
		if info.Mode().IsRegular() {
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// Record a digest, never retain a test key in diagnostic map output.
			value.Body = fitDigest(raw)
			clear(raw)
		} else if info.Mode()&os.ModeSymlink != 0 {
			value.Body, err = os.Readlink(path)
			if err != nil {
				return err
			}
		}
		result[path] = value
		return nil
	})
	if err != nil {
		t.Fatal("snapshot owned synthetic paths", err)
	}
	return result
}

func TestPrepareInstallationRequiresCanonicalPrivateParent(t *testing.T) {
	for _, name := range []string{"relative", "unclean", "root", "missing-parent", "nonprivate-parent", "symlink-parent", "symlink-ancestor"} {
		t.Run(name, func(t *testing.T) {
			root, exe, m := freshPreparationInput(t)
			path := root
			switch name {
			case "relative":
				path = "relative-private-root"
			case "unclean":
				path = filepath.Dir(root) + "/./installation"
			case "root":
				path = "/"
			case "missing-parent":
				path = filepath.Join(filepath.Dir(root), "absent", "installation")
			case "nonprivate-parent":
				if err := os.Chmod(filepath.Dir(root), 0o750); err != nil {
					t.Fatal(err)
				}
			case "symlink-parent", "symlink-ancestor":
				link := filepath.Join(filepath.Dir(root), "alias")
				if err := os.Symlink(filepath.Dir(root), link); err != nil {
					t.Fatal(err)
				}
				if name == "symlink-parent" {
					path = filepath.Join(link, "installation")
				} else {
					if err := os.Mkdir(filepath.Join(filepath.Dir(root), "private-child"), 0o700); err != nil {
						t.Fatal(err)
					}
					path = filepath.Join(link, "private-child", "installation")
				}
			}
			before := snapshotPreparationTree(t, filepath.Dir(root))
			if err := localoperator.PrepareInstallation(t.Context(), path, exe, preparedRaw(t, m)); !errors.Is(err, localoperator.ErrDenied) {
				t.Fatal("noncanonical/nonprivate output path accepted", err)
			}
			assertNoPreparedOutput(t, root)
			if !reflect.DeepEqual(before, snapshotPreparationTree(t, filepath.Dir(root))) {
				t.Fatal("denial repaired/chmodded/changed parent or existing entries")
			}
		})
	}
}

func TestPrepareInstallationConcurrentExclusiveReservation(t *testing.T) {
	root, exe, m := freshPreparationInput(t)
	raw := preparedRaw(t, m)
	start := make(chan struct{})
	results := make(chan error, 2)
	var ready, done sync.WaitGroup
	ready.Add(2)
	done.Add(2)
	for range 2 {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results <- localoperator.PrepareInstallation(t.Context(), root, exe, raw)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(results)
	succeeded, denied := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, localoperator.ErrDenied) {
			denied++
		} else {
			t.Fatal("unexpected concurrent result", err)
		}
	}
	if succeeded != 1 || denied != 1 {
		t.Fatalf("exclusive fresh reservation successes=%d denials=%d, want 1/1", succeeded, denied)
	}
	manifest := readPreparationFile(t, filepath.Join(root, "manifest.json"), 0o600)
	key := readPreparationFile(t, filepath.Join(root, "source.key"), 0o600)
	defer clear(key)
	var prepared localoperator.ManifestV1
	if json.Unmarshal(manifest, &prepared) != nil || len(key) != 32 || fitDigest(key) != prepared.KeyFingerprint || localoperator.ValidateManifestTemplate(t.Context(), manifest, exe) != nil {
		t.Fatal("concurrent reservation left a mixed or incomplete winner")
	}
}
