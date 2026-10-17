//go:build darwin || linux

package localoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPrivateOperatorRootNeedsExplicitPrivateMode(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte(`{"synthetic":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.key"), bytes.Repeat([]byte{1}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Stat(root, &stat); err != nil {
		t.Fatal(err)
	}
	t.Logf("synthetic TempDir root predicate: mode=%04o private700=%t", stat.Mode&0o7777, privateOperatorRoot(stat))
	if !privateOperatorRoot(stat) {
		manifest, key, err := readOperatorFiles(t.Context(), root)
		if !errors.Is(err, ErrDenied) || manifest != nil || key != ([32]byte{}) {
			t.Fatal("non-private root returned usable private material")
		}
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	_, key, err := readOperatorFiles(t.Context(), root)
	defer clear(key[:])
	if err != nil {
		t.Fatalf("explicit private root rejected: %v", err)
	}
}

func privateOperatorFixture(t *testing.T) (string, []byte, [32]byte) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"version":1,"source":"synthetic-test-only"}`)
	var key [32]byte
	for index := range key {
		key[index] = byte(index + 1)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.key"), key[:], 0o600); err != nil {
		t.Fatal(err)
	}
	return root, manifest, key
}

func diagnosePrivateOperatorFile(t *testing.T, ctx context.Context, root int, name string, maximum, exact int64) {
	t.Helper()
	fd, err := unix.Openat(root, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("synthetic %s openat predicate: %v", name, err)
	}
	defer unix.Close(fd)
	var before, entry, after unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		t.Fatal(err)
	}
	if !privateOperatorFile(before, maximum, exact) {
		t.Fatalf("synthetic %s private predicate: regular=%t mode600=%t owner=%t nlink1=%t positive_size=%t bounded_size=%t exact_size=%t", name, before.Mode&unix.S_IFMT == unix.S_IFREG, before.Mode&0o7777 == 0o600, before.Uid == uint32(os.Geteuid()), before.Nlink == 1, before.Size > 0, before.Size <= maximum, exact == 0 || before.Size == exact)
	}
	if err := unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	if !stableOperatorStat(before, entry) {
		t.Fatalf("synthetic %s initial FD/path stamp mismatch", name)
	}
	if _, err := readPrivateOperatorFile(ctx, root, name, maximum, exact); err != nil {
		if err := unix.Fstat(fd, &after); err != nil {
			t.Fatal(err)
		}
		if err := unix.Fstatat(root, name, &entry, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("synthetic %s bounded read rejected: error=%v fd_stable=%t path_stable=%t mode_stable=%t owner_stable=%t size_stable=%t mtime_stable=%t ctime_stable=%t", name, err, stableOperatorStat(before, after), stableOperatorStat(before, entry), before.Mode == after.Mode, before.Uid == after.Uid, before.Size == after.Size, before.Mtim == after.Mtim, before.Ctim == after.Ctim)
	}
}

func TestPrivateOperatorFilesActualPrivateFixture(t *testing.T) {
	root, wantManifest, wantKey := privateOperatorFixture(t)
	fd, err := openCanonicalDir(root)
	if err != nil {
		t.Fatalf("canonical synthetic root open failed: %v", err)
	}
	defer unix.Close(fd)
	var before, after unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		t.Fatal(err)
	}
	if !privateOperatorRoot(before) {
		t.Fatalf("synthetic root private predicate: directory=%t mode700=%t owner=%t", before.Mode&unix.S_IFMT == unix.S_IFDIR, before.Mode&0o7777 == 0o700, before.Uid == uint32(os.Geteuid()))
	}
	diagnosePrivateOperatorFile(t, t.Context(), fd, "manifest.json", int64(maxOperatorManifestBytes), 0)
	diagnosePrivateOperatorFile(t, t.Context(), fd, "source.key", 32, 32)
	if err := unix.Fstat(fd, &after); err != nil {
		t.Fatal(err)
	}
	if !stableOperatorStat(before, after) {
		t.Fatal("synthetic root stamp changed across read-only private file intake")
	}
	manifest, key, err := readOperatorFiles(t.Context(), root)
	if err != nil {
		t.Fatalf("valid actual private fixture rejected after individual predicates passed: %v", err)
	}
	defer clear(key[:])
	if !bytes.Equal(manifest, wantManifest) || key != wantKey {
		t.Fatal("private intake returned wrong synthetic bytes")
	}
}

func TestPrivateOperatorRuntimeActualOwnedExecutable(t *testing.T) {
	root, _, _ := privateOperatorFixture(t)
	path := filepath.Join(root, "worker")
	content := []byte("synthetic-owned-executable-bytes-not-launched")
	if err := os.WriteFile(path, content, 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := InspectRuntime(t.Context(), path)
	if err != nil {
		t.Fatalf("actual finite owned executable inspection failed: %v", err)
	}
	digest := sha256.Sum256(content)
	if got.WorkerExecutableDigest != hex.EncodeToString(digest[:]) || len(got.LocalHostIdentityDigest) != 64 || len(got.LocalNetworkNamespaceIdentityDigest) != 64 {
		t.Fatal("runtime inspection did not bind actual executable and host/network identities")
	}
	again, err := InspectRuntime(t.Context(), path)
	if err != nil || again != got {
		t.Fatal("same actual host boot/executable cannot reproduce original identity")
	}
}
