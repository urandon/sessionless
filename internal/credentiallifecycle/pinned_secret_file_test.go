//go:build darwin || linux

package credentiallifecycle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func providerScratchRoot(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/private/tmp", "sessionless-provider-scratch-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestPinnedSecretFileSystemRecoversCrashResidueBeforeReuse(t *testing.T) {
	root := providerScratchRoot(t)
	first, err := NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	file, err := first.Create("provider.json", []byte("provider-secret-marker"), 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(file.FilePath); err != nil {
		t.Fatal(err)
	}
	// A process crash closes all descriptors without running Cleanup.
	if err := unix.Close(file.pinned.dirFD); err != nil {
		t.Fatal(err)
	}
	file.pinned.dirFD = -1
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatalf("restart recovery failed: %v", err)
	}
	defer second.Close()
	if _, err := os.Lstat(file.FilePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("crash-retained credential survived restart: %v", err)
	}
}

func TestPinnedSecretFileSystemRequiresExclusiveRoot(t *testing.T) {
	root := providerScratchRoot(t)
	first, err := NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := NewPinnedSecretFileSystem(root); err == nil {
		t.Fatal("second service acquired an active provider credential scratch root")
	}
}

func TestPinnedSecretFileSystemRecoveryFailsClosedOnUntrustedEntries(t *testing.T) {
	for name, prepare := range map[string]func(*testing.T, string){
		"foreign entry": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "unrelated"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"symlink root": func(t *testing.T, root string) {
			external := t.TempDir()
			if err := os.Symlink(external, filepath.Join(root, providerRootPrefix+"poison")); err != nil {
				t.Fatal(err)
			}
		},
		"wrong mode": func(t *testing.T, root string) {
			path := filepath.Join(root, providerRootPrefix+"mode")
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o755); err != nil {
				t.Fatal(err)
			}
		},
		"over bound": func(t *testing.T, root string) {
			for index := 0; index < maxRecoveredProviderRoots+1; index++ {
				if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("%s%03d", providerRootPrefix, index)), 0o700); err != nil {
					t.Fatal(err)
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := providerScratchRoot(t)
			prepare(t, root)
			if _, err := NewPinnedSecretFileSystem(root); err == nil {
				t.Fatal("untrusted crash residue was accepted")
			}
		})
	}
}
