//go:build darwin || linux

package credentiallifecycle

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestImmutableFilePathBoundBeforeCredentialMatchesActualMaterialization(t *testing.T) {
	// Unicode/JSON-special path bytes must be counted, not rune length or an
	// arbitrary PATH_MAX. Test-owned public literal is NOT a real credential.
	root := filepath.Join(providerScratchRoot(t), "unicode-é-quote\"-backslash\\")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	fs, err := NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Error("owned namespace close:", err)
		}
	})
	before, err := os.ReadDir(fs.filesystem.rootPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"api-key", "public-fixture.json"} {
		bound, err := fs.ImmutableFilePathBound(t.Context(), name)
		if err != nil || bound == 0 {
			t.Fatalf("filename=%q bound=%d err=%v", name, bound, err)
		}
		after, err := os.ReadDir(fs.filesystem.rootPath)
		if err != nil || !reflect.DeepEqual(before, after) || len(fs.owned) != 0 {
			t.Fatalf("sizing created credential/materialization: before=%v after=%v owned=%d err=%v", before, after, len(fs.owned), err)
		}
		literal := []byte("PUBLIC-NONCREDENTIAL-FILE-BOUND-CONTROL")
		file, err := fs.CreateImmutable(name, literal, 1024)
		if err != nil {
			t.Fatal(err)
		}
		if uint64(len(file.FilePath)) != bound {
			t.Errorf("actual materialization bytes=%d original pre-Issue bound=%d", len(file.FilePath), bound)
		}
		if err := fs.VerifyImmutable(t.Context(), file, domain.FingerprintCredential(literal), 1024); err != nil {
			t.Error("actual immutable verification:", err)
		}
		if err := fs.Cleanup(file); err != nil {
			t.Fatal("owned file cleanup:", err)
		}
	}
}

func TestImmutableFilePathBoundMissingClosedCancelledAndForeignNamespaceDeny(t *testing.T) {
	for _, mode := range []string{"nil", "nil-context", "cancelled", "closed", "renamed", "wrong-mode", "filename"} {
		t.Run(mode, func(t *testing.T) {
			fs, err := NewPinnedSecretFileSystem(providerScratchRoot(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = fs.Close() })
			var target = fs
			ctx := t.Context()
			name := "api-key"
			switch mode {
			case "nil":
				target = nil
			case "nil-context":
				ctx = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "closed":
				if err := fs.Close(); err != nil {
					t.Fatal(err)
				}
			case "renamed":
				original := fs.filesystem.rootPath
				moved := original + "-owned-test-renamed"
				if err := os.Rename(original, moved); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Rename(moved, original) })
			case "wrong-mode":
				root := fs.filesystem.rootPath
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(root, 0700) })
			case "filename":
				name = "../foreign"
			}
			if bound, err := target.ImmutableFilePathBound(ctx, name); err == nil || bound != 0 {
				t.Fatalf("mode=%s bound=%d err=%v, want zero/error", mode, bound, err)
			}
		})
	}
}
