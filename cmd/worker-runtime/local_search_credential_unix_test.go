//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func localSearchTestKey(t *testing.T) (string, []byte) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{0x31}, 32) // synthetic, never a provider key
	key[0] = 0                            // zero bytes are legal; only the all-zero key is forbidden
	path := filepath.Join(root, "private-test-key")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(key) })
	return path, key
}

func TestLocalSearchKeyReadsOnlyExactPrivateFile(t *testing.T) {
	path, expected := localSearchTestKey(t)
	got, err := readLocalSearchKey(t.Context(), path)
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatalf("synthetic private key intake: bytes=%d err=%v", len(got), err)
	}
	clear(got)
	again, err := readLocalSearchKey(t.Context(), path)
	defer clear(again)
	if err != nil || !bytes.Equal(again, expected) {
		t.Fatal("caller-owned key buffer aliases file/intake state", err)
	}
}

func TestLocalSearchKeyRejectsInvalidFileWithoutLeakingPath(t *testing.T) {
	for _, fault := range []string{"missing", "empty", "short", "long", "oversized", "zero", "permissions", "directory", "symlink", "hardlink", "fifo", "relative", "unclean", "root", "nil-context", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			path, _ := localSearchTestKey(t)
			ctx := t.Context()
			switch fault {
			case "missing":
				path += ".missing-canary"
			case "empty", "short", "long", "oversized", "zero":
				size := map[string]int{"empty": 0, "short": 31, "long": 33, "oversized": 64 << 10, "zero": 32}[fault]
				data := bytes.Repeat([]byte{7}, size)
				if fault == "zero" {
					clear(data)
				}
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
				clear(data)
			case "permissions":
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
			case "directory":
				path += ".dir"
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(path, path+".alias"); err != nil {
					t.Fatal(err)
				}
				path += ".alias"
			case "hardlink":
				if err := os.Link(path, path+".link"); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				path += ".fifo"
				if err := unix.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
			case "relative":
				path = "private-canary-relative"
			case "unclean":
				path = filepath.Dir(path) + "/../" + filepath.Base(filepath.Dir(path)) + "/" + filepath.Base(path)
			case "root":
				path = "/"
			case "nil-context":
				ctx = nil
			case "cancelled":
				call, cancel := context.WithCancel(ctx)
				cancel()
				ctx = call
			}
			got, err := readLocalSearchKey(ctx, path)
			defer clear(got)
			if got != nil || err != errWorkerLocalExecution || strings.Contains(err.Error(), path) && path != "/" {
				t.Fatalf("invalid key intake exposed data/path: bytes=%d err=%v", len(got), err)
			}
		})
	}
}

func TestLocalSearchKeyRejectsNamedReplacementWithLiveOriginalDescriptor(t *testing.T) {
	for _, replacement := range []string{"file", "parent", "contents"} {
		t.Run(replacement, func(t *testing.T) {
			base, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			parent := filepath.Join(base, "selected")
			if err := os.Mkdir(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(parent, "key")
			key := bytes.Repeat([]byte{0x32}, 32)
			defer clear(key)
			if err := os.WriteFile(path, key, 0o600); err != nil {
				t.Fatal(err)
			}
			owner, err := openLocalSearchKeyFile(t.Context(), path)
			if err != nil {
				t.Fatal("valid original descriptor prerequisite", err)
			}
			t.Cleanup(func() { _ = owner.close() })
			original, err := owner.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			switch replacement {
			case "file":
				if err := os.Rename(path, filepath.Join(parent, "original")); err != nil {
					t.Fatal(err)
				}
			case "parent":
				if err := os.Rename(parent, filepath.Join(base, "original-parent")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(parent, 0o700); err != nil {
					t.Fatal(err)
				}
			case "contents":
				key[0]++
			}
			if err := os.WriteFile(path, key, 0o600); err != nil {
				t.Fatal(err)
			}
			if replacement == "contents" {
				// Force a distinct file-time value instead of depending on the
				// host filesystem's timestamp granularity or a real-time sleep.
				at := original.ModTime().Add(time.Hour)
				if err := os.Chtimes(path, at, at); err != nil {
					t.Fatal(err)
				}
			}
			if stat, err := owner.file.Stat(); err != nil || stat.Size() != 32 {
				t.Fatal("replacement accidentally invalidated the old open descriptor", err)
			}
			got, err := owner.read(t.Context())
			defer clear(got)
			if got != nil || err != errWorkerLocalExecution {
				t.Fatal("live old descriptor authorized a replaced selected key", err)
			}
		})
	}
}

func TestLocalSearchKeyMetadataOwnerAndModePredicate(t *testing.T) {
	path, _ := localSearchTestKey(t)
	owner, err := openLocalSearchKeyFile(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close() })
	if !validLocalSearchKeyStat(owner.identity) {
		t.Fatal("actual private owner metadata prerequisite")
	}
	// Pure metadata negatives; no chown, administrator permission or claim
	// that a synthetic stat record is an actual kernel ownership observation.
	for _, field := range []string{"owner", "mode", "type", "links", "size"} {
		t.Run(field, func(t *testing.T) {
			bad := owner.identity
			switch field {
			case "owner":
				bad.Uid ^= 1
			case "mode":
				bad.Mode |= 0o040
			case "type":
				bad.Mode = unix.S_IFIFO | 0o600
			case "links":
				bad.Nlink = 2
			case "size":
				bad.Size = 33
			}
			if validLocalSearchKeyStat(bad) {
				t.Fatal("invalid private-key metadata admitted")
			}
		})
	}
}

func TestLocalSearchKeyRejectsSymlinkParentAndClosesOriginalFile(t *testing.T) {
	path, _ := localSearchTestKey(t)
	alias := filepath.Join(filepath.Dir(filepath.Dir(path)), filepath.Base(filepath.Dir(path))+"-alias")
	if err := os.Symlink(filepath.Dir(path), alias); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(alias); err != nil {
			t.Error(err)
		}
	})
	if key, err := readLocalSearchKey(t.Context(), filepath.Join(alias, filepath.Base(path))); err == nil || key != nil {
		clear(key)
		t.Fatal("symlink in canonical parent was followed", err)
	}
	owner, err := openLocalSearchKeyFile(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	file := owner.file
	if err := owner.close(); err != nil || owner.close() != nil {
		t.Fatal("key descriptor close is not successful/idempotent", err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) || owner.parent != -1 {
		t.Fatal("private key descriptor remained open", err)
	}
}
