//go:build darwin || linux

package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

func executionV3Fixture(t *testing.T, bundle NativeLaunchV3, variant uint32) (*NativeExecutionV3, []NativeInputFileV3) {
	t.Helper()
	policy, err := bundle.InstallPolicyDigestV3()
	if err != nil {
		t.Fatal(err)
	}
	bundle.PolicyDigest = policy
	d := nativeharnessinput.NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: hex.EncodeToString(bundle.ScopeDigest[:]), InstallDigest: hex.EncodeToString(policy[:]), Variant: variant,
		Images: []nativeharnessinput.ImageV3{}, Catalog: bundle.Catalog, Schema: bundle.Schema}
	paths, err := nativeharnessinput.ImagePaths(variant)
	if err != nil {
		t.Fatal(err)
	}
	for i := range paths {
		body := "image" + strconv.Itoa(i+1)
		hash := sha256.Sum256([]byte(body))
		d.Images = append(d.Images, nativeharnessinput.ImageV3{Ordinal: uint32(i + 1), Size: uint64(len(body)), SHA256: hex.EncodeToString(hash[:])})
	}
	slots, err := nativeharnessinput.Slots(d)
	if err != nil {
		t.Fatal(err)
	}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var files []NativeInputFileV3
	for i, slot := range slots {
		body := "catalog"
		if i == 1 {
			body = "schema"
		}
		if i >= 2 {
			body = "image" + strconv.Itoa(i-1)
		}
		path := filepath.Join(directory, filepath.Base(slot.Path))
		if err := os.WriteFile(path, []byte(body), 0400); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Errorf("source close: %v", err)
			}
		})
		files = append(files, NativeInputFileV3{Source: path, File: file})
	}
	v, err := NewNativeExecutionV3(t.Context(), bundle, d, files)
	if err != nil {
		t.Fatal(err)
	}
	return v, files
}

func TestNativeExecutionV3KeepsInstallStableAndOriginalReadsetDistinct(t *testing.T) {
	bundle := installV3Fixture(t)
	policy, err := bundle.InstallPolicyDigestV3()
	if err != nil {
		t.Fatal(err)
	}
	bundle.PolicyDigest = policy
	for variant := uint32(0); variant < 7; variant++ {
		t.Run(strconv.Itoa(int(variant)), func(t *testing.T) {
			a, _ := executionV3Fixture(t, bundle, variant)
			b, _ := executionV3Fixture(t, bundle, variant)
			if err := a.ValidateV3(t.Context(), bundle); err != nil {
				t.Fatal(err)
			}
			if a.DigestV3() == (ExecutableDigest{}) || a.DigestV3() == b.DigestV3() {
				t.Fatal("fresh original readsets have same execution identity")
			}
			got, err := bundle.InstallPolicyDigestV3()
			if err != nil || got != policy {
				t.Fatalf("execution changed installation: %x %v", got, err)
			}
			args := a.ArgumentsV3()
			if len(args) < 3 || args[0] != "bootstrap-v3" || args[len(args)-2] != "--execution-descriptor-json" {
				t.Fatalf("wrong actual bootstrap: %q", args)
			}
			decoded, err := nativeharnessinput.Parse([]byte(args[len(args)-1]))
			if err != nil || decoded.Variant != variant {
				t.Fatalf("carrier variant=%d err=%v", decoded.Variant, err)
			}
			copied := *a
			if copied.ValidateV3(t.Context(), bundle) == nil || copied.DigestV3() != (ExecutableDigest{}) {
				t.Fatal("copied readset accepted")
			}
			maps := a.MappingsV3()
			maps[0].Source = "/foreign"
			args[0] = "caller mutation"
			d := a.DescriptorV3()
			d.Catalog.SHA256 = "bad"
			if len(d.Images) > 0 {
				d.Images[0].SHA256 = "bad"
			}
			if a.ArgumentsV3()[0] != "bootstrap-v3" || a.MappingsV3()[0].Source == "/foreign" || a.ValidateV3(t.Context(), bundle) != nil {
				t.Fatal("public observation mutated original")
			}
			foreign := bundle.Clone()
			foreign.ScopeDigest = ExecutableDigest{9}
			if a.ValidateV3(t.Context(), foreign) == nil {
				t.Fatal("foreign scope accepted")
			}
		})
	}
}

func TestNativeExecutionV3RejectsSourceReplacementAndClosedOriginal(t *testing.T) {
	for _, mode := range []string{"same-bytes-new-inode", "changed-bytes", "symlink", "closed", "writable"} {
		t.Run(mode, func(t *testing.T) {
			bundle := installV3Fixture(t)
			bundle.PolicyDigest, _ = bundle.InstallPolicyDigestV3()
			v, files := executionV3Fixture(t, bundle, 1)
			source := files[2].Source
			switch mode {
			case "same-bytes-new-inode", "symlink":
				if err := os.Rename(source, source+".original"); err != nil {
					t.Fatal(err)
				}
				if mode == "symlink" {
					if err := os.Symlink(source+".original", source); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(source, []byte("image1"), 0400); err != nil {
					t.Fatal(err)
				}
			case "changed-bytes":
				if err := os.Chmod(source, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(source, []byte("image2"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(source, 0400); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := files[2].File.Close(); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(source, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := v.ValidateV3(t.Context(), bundle); !errors.Is(err, ErrSupervisorConfig) {
				t.Fatalf("%s accepted: %v", mode, err)
			}
		})
	}
}

func TestNativeExecutionV3RejectsUnboundDescriptorFilesAndCancellation(t *testing.T) {
	bundle := installV3Fixture(t)
	bundle.PolicyDigest, _ = bundle.InstallPolicyDigestV3()
	v, files := executionV3Fixture(t, bundle, 4)
	for _, field := range []string{"scope", "install", "catalog", "image-order", "files-missing", "file-alias", "path-alias"} {
		t.Run(field, func(t *testing.T) {
			d := v.DescriptorV3()
			inputs := append([]NativeInputFileV3(nil), files...)
			switch field {
			case "scope":
				d.ScopeManifestDigest = hex.EncodeToString(bundle.PolicyDigest[:])
			case "install":
				d.InstallDigest = hex.EncodeToString(bundle.ScopeDigest[:])
			case "catalog":
				d.Catalog = d.Schema
			case "image-order":
				d.Images[0], d.Images[1] = d.Images[1], d.Images[0]
			case "files-missing":
				inputs = inputs[:len(inputs)-1]
			case "file-alias":
				inputs[1] = inputs[0]
			case "path-alias":
				inputs[0].Source = filepath.Join(filepath.Dir(inputs[0].Source), ".", filepath.Base(inputs[0].Source)) + "/../models.json"
			}
			if _, err := NewNativeExecutionV3(t.Context(), bundle, d, inputs); !errors.Is(err, ErrSupervisorConfig) {
				t.Fatalf("%s accepted: %v", field, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if v.ValidateV3(ctx, bundle) == nil {
		t.Fatal("cancelled readset validation accepted")
	}
	if _, err := NewNativeExecutionV3(ctx, bundle, v.DescriptorV3(), files); err == nil {
		t.Fatal("cancelled derivation accepted")
	}
	if _, err := NewNativeExecutionV3(nil, bundle, v.DescriptorV3(), files); err == nil {
		t.Fatal("nil context accepted")
	}
}
