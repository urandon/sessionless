//go:build darwin || linux

package credentiallifecycle

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"golang.org/x/sys/unix"
)

const immutableMarker = "synthetic-immutable-provider-marker"

func immutableFixture(t *testing.T) (*PinnedSecretFileSystem, *PinnedSecretFile) {
	t.Helper()
	filesystem, err := NewPinnedSecretFileSystem(providerScratchRoot(t))
	if err != nil {
		t.Fatalf("acquire owned filesystem: %v", err)
	}
	t.Cleanup(func() {
		if err := filesystem.Close(); err != nil {
			t.Errorf("close owned filesystem: %v", err)
		}
	})
	file, err := filesystem.CreateImmutable("provider.key", []byte(immutableMarker), 1024)
	if err != nil {
		t.Fatalf("create immutable file: %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Cleanup(file) })
	return filesystem, file
}

func checkImmutable(t *testing.T, filesystem *PinnedSecretFileSystem, file *PinnedSecretFile, wantError bool) {
	t.Helper()
	err := filesystem.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte(immutableMarker)), 1024)
	if wantError && !errors.Is(err, ErrCredentialMaterialization) || !wantError && err != nil {
		t.Errorf("immutable verification: err=%v, want rejection=%v", err, wantError)
	}
}

func onceRestore(t *testing.T, restore func() error) func() {
	t.Helper()
	var once sync.Once
	f := func() {
		once.Do(func() {
			if err := restore(); err != nil {
				t.Errorf("restore original owned fixture: %v", err)
			}
		})
	}
	t.Cleanup(f)
	return f
}

func mustFS(t *testing.T, operation string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

func TestPinnedSecretFileImmutableRetainsCreatorAndRepeatsFullVerification(t *testing.T) {
	filesystem, file := immutableFixture(t)
	flags, err := unix.FcntlInt(uintptr(file.pinned.fileFD), unix.F_GETFL, 0)
	mustFS(t, "creator access flags", err)
	if flags&unix.O_ACCMODE != unix.O_RDWR {
		t.Errorf("creator access mode=%d, want O_RDWR", flags&unix.O_ACCMODE)
	}
	flags, err = unix.FcntlInt(uintptr(file.pinned.fileFD), unix.F_GETFD, 0)
	mustFS(t, "creator descriptor flags", err)
	if flags&unix.FD_CLOEXEC == 0 {
		t.Error("creator descriptor is missing CLOEXEC")
	}
	for range 3 {
		checkImmutable(t, filesystem, file, false)
	}
	fileFD, dirFD := file.pinned.fileFD, file.pinned.dirFD
	mustFS(t, "cleanup original", filesystem.Cleanup(file))
	mustFS(t, "idempotent cleanup original", filesystem.Cleanup(file))
	assertClosedFD(t, "creator", fileFD)
	assertClosedFD(t, "invocation directory", dirFD)
	if _, err := os.Lstat(file.FilePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("released owned path remains: %v", err)
	}
	checkImmutable(t, filesystem, file, true)
}

func TestPinnedSecretFileImmutableDeniesMutationsAndAcceptsRestoredOriginal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *PinnedSecretFileSystem, *PinnedSecretFile) func()
	}{
		{"same bytes replacement inode", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			original := filepath.Join(t.TempDir(), "original")
			mustFS(t, "move original file", os.Rename(file.FilePath, original))
			restore := onceRestore(t, func() error {
				if err := os.Remove(file.FilePath); err != nil {
					return err
				}
				return os.Rename(original, file.FilePath)
			})
			mustFS(t, "create byte-identical replacement", os.WriteFile(file.FilePath, []byte(immutableMarker), 0o600))
			return restore
		}},
		{"same inode changed bytes", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			mustFS(t, "rewrite same inode", os.WriteFile(file.FilePath, []byte("X"+immutableMarker[1:]), 0o600))
			return onceRestore(t, func() error { return os.WriteFile(file.FilePath, []byte(immutableMarker), 0o600) })
		}},
		{"truncated", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			mustFS(t, "truncate original", os.Truncate(file.FilePath, 1))
			return onceRestore(t, func() error { return os.WriteFile(file.FilePath, []byte(immutableMarker), 0o600) })
		}},
		{"zero size", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			mustFS(t, "truncate original to zero", os.Truncate(file.FilePath, 0))
			return onceRestore(t, func() error { return os.WriteFile(file.FilePath, []byte(immutableMarker), 0o600) })
		}},
		{"growth", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			mustFS(t, "grow original", os.WriteFile(file.FilePath, []byte(immutableMarker+"x"), 0o600))
			return onceRestore(t, func() error { return os.WriteFile(file.FilePath, []byte(immutableMarker), 0o600) })
		}},
		{"file permissions", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			mustFS(t, "change file mode", os.Chmod(file.FilePath, 0o644))
			return onceRestore(t, func() error { return os.Chmod(file.FilePath, 0o600) })
		}},
		{"file special bits", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			mustFS(t, "set special file mode", unix.Chmod(file.FilePath, 0o4600))
			return onceRestore(t, func() error { return unix.Chmod(file.FilePath, 0o600) })
		}},
		{"hardlink", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			link := filepath.Join(t.TempDir(), "link")
			mustFS(t, "add external hardlink", os.Link(file.FilePath, link))
			return onceRestore(t, func() error { return os.Remove(link) })
		}},
		{"file symlink", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			original := filepath.Join(t.TempDir(), "original")
			mustFS(t, "move original before symlink", os.Rename(file.FilePath, original))
			restore := onceRestore(t, func() error {
				if err := os.Remove(file.FilePath); err != nil {
					return err
				}
				return os.Rename(original, file.FilePath)
			})
			mustFS(t, "add file symlink", os.Symlink(original, file.FilePath))
			return restore
		}},
		{"file nonregular", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			original := filepath.Join(t.TempDir(), "original")
			mustFS(t, "move original before directory", os.Rename(file.FilePath, original))
			restore := onceRestore(t, func() error {
				if err := os.Remove(file.FilePath); err != nil {
					return err
				}
				return os.Rename(original, file.FilePath)
			})
			mustFS(t, "add directory at file path", os.Mkdir(file.FilePath, 0o600))
			return restore
		}},
		{"extra invocation entry", func(t *testing.T, _ *PinnedSecretFileSystem, file *PinnedSecretFile) func() {
			path := filepath.Join(file.RootDir, "unexpected")
			mustFS(t, "add invocation entry", os.WriteFile(path, []byte("x"), 0o600))
			return onceRestore(t, func() error { return os.Remove(path) })
		}},
		{"extra root entry", func(t *testing.T, filesystem *PinnedSecretFileSystem, _ *PinnedSecretFile) func() {
			path := filepath.Join(filesystem.filesystem.rootPath, "unexpected")
			mustFS(t, "add service root entry", os.Mkdir(path, 0o700))
			return onceRestore(t, func() error { return os.Remove(path) })
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filesystem, file := immutableFixture(t)
			checkImmutable(t, filesystem, file, false)
			restore := test.mutate(t, filesystem, file)
			checkImmutable(t, filesystem, file, true)
			restore()
			checkImmutable(t, filesystem, file, false)
		})
	}
}

func TestPinnedSecretFileImmutableDeniesNamespaceChanges(t *testing.T) {
	for _, level := range []string{"scratch", "service", "invocation"} {
		for _, mutation := range []string{"rename", "replacement", "symlink", "file", "mode", "special bits"} {
			t.Run(level+"/"+mutation, func(t *testing.T) {
				filesystem, file := immutableFixture(t)
				path := file.RootDir
				if level == "service" {
					path = filesystem.filesystem.rootPath
				}
				if level == "scratch" {
					path = filesystem.identity.scratchPath
				}
				if mutation == "mode" || mutation == "special bits" {
					mode := uint32(0o755)
					if mutation == "special bits" {
						mode = 0o1700
					}
					mustFS(t, "change namespace permissions", unix.Chmod(path, mode))
					restore := onceRestore(t, func() error { return unix.Chmod(path, 0o700) })
					checkImmutable(t, filesystem, file, true)
					restore()
					checkImmutable(t, filesystem, file, false)
					return
				}
				moved := filepath.Join(t.TempDir(), "original")
				mustFS(t, "rename original namespace", os.Rename(path, moved))
				restore := onceRestore(t, func() error {
					if mutation != "rename" {
						if err := os.Remove(path); err != nil {
							return err
						}
					}
					return os.Rename(moved, path)
				})
				switch mutation {
				case "replacement":
					mustFS(t, "create replacement directory", os.Mkdir(path, 0o700))
				case "symlink":
					mustFS(t, "substitute directory symlink", os.Symlink(moved, path))
				case "file":
					mustFS(t, "substitute non-directory", os.WriteFile(path, []byte("x"), 0o700))
				}
				checkImmutable(t, filesystem, file, true)
				restore()
				checkImmutable(t, filesystem, file, false)
			})
		}
	}
}

func assertClosedFD(t *testing.T, name string, fd int) {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); !errors.Is(err, unix.EBADF) {
		t.Errorf("%s descriptor=%d closure: err=%v, want EBADF", name, fd, err)
	}
}

func TestPinnedSecretFileImmutableBoundsContextAndOwnership(t *testing.T) {
	filesystem, file := immutableFixture(t)
	other, otherFile := immutableFixture(t)
	if err := other.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte(immutableMarker)), 1024); err == nil {
		t.Error("foreign wrapper accepted original file")
	}
	if err := filesystem.Cleanup(otherFile); err == nil {
		t.Error("foreign wrapper cleaned another file")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{nil, ctx} {
		if err := filesystem.VerifyImmutable(ctx, file, domain.FingerprintCredential([]byte(immutableMarker)), 1024); err == nil {
			t.Error("missing/cancelled context accepted")
		}
	}
	for _, maxBytes := range []int64{-1, 0, int64(len(immutableMarker) - 1)} {
		if err := filesystem.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte(immutableMarker)), maxBytes); err == nil {
			t.Errorf("invalid byte bound=%d accepted", maxBytes)
		}
	}
	if err := filesystem.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte(immutableMarker)), math.MaxInt64); err != nil {
		t.Errorf("small original under large bound overflowed: %v", err)
	}
	originalSize := file.pinned.immutable.file.Size
	file.pinned.immutable.file.Size = math.MaxInt64
	if err := hashImmutable(context.Background(), file.pinned, file.pinned.immutable.fingerprint, math.MaxInt64); err == nil {
		t.Error("original-size-plus-sentinel overflow accepted")
	}
	file.pinned.immutable.file.Size = originalSize
	if err := filesystem.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte("different")), 1024); err == nil {
		t.Error("foreign expected fingerprint accepted")
	}
	checkImmutable(t, filesystem, file, false)
}

func TestPinnedSecretFileImmutableRegistryIncludesMutableAndFailedCleanupResidue(t *testing.T) {
	filesystem, file := immutableFixture(t)
	mutable, err := filesystem.Create("router.key", []byte("mutable-marker"), 1024)
	mustFS(t, "create legitimate mutable invocation", err)
	t.Cleanup(func() { _ = filesystem.Cleanup(mutable) })
	immutable, err := filesystem.CreateImmutable("second.key", []byte(immutableMarker), 1024)
	mustFS(t, "create legitimate immutable invocation", err)
	t.Cleanup(func() { _ = filesystem.Cleanup(immutable) })
	checkImmutable(t, filesystem, file, false)
	extra := filepath.Join(mutable.RootDir, "residue")
	mustFS(t, "add test-owned cleanup residue", os.WriteFile(extra, []byte("x"), 0o600))
	originalDirFD := mutable.pinned.dirFD
	if err := filesystem.Cleanup(mutable); !errors.Is(err, ErrCredentialMaterialization) {
		t.Errorf("extra-entry cleanup outcome=%v, want rejection", err)
	}
	// Failed cleanup retains its known namespace, not a foreign-root false alarm.
	checkImmutable(t, filesystem, file, false)
	if err := filesystem.Cleanup(mutable); !errors.Is(err, ErrCredentialMaterialization) {
		t.Errorf("repeated failed cleanup=%v, want stored rejection", err)
	}
	assertClosedFD(t, "failed cleanup original directory", originalDirFD)
}

func TestPinnedSecretFileImmutableCleanupPreservesReplacementAndClosesOwnedFDs(t *testing.T) {
	filesystem, file := immutableFixture(t)
	fileFD, dirFD := file.pinned.fileFD, file.pinned.dirFD
	original := filepath.Join(t.TempDir(), "original")
	mustFS(t, "retain test-owned original", os.Rename(file.FilePath, original))
	mustFS(t, "put foreign replacement at public name", os.WriteFile(file.FilePath, []byte("foreign-marker"), 0o600))
	for range 2 {
		if err := filesystem.Cleanup(file); !errors.Is(err, ErrCredentialMaterialization) {
			t.Errorf("replacement cleanup=%v, want stored rejection", err)
		}
	}
	assertClosedFD(t, "replacement creator", fileFD)
	assertClosedFD(t, "replacement directory", dirFD)
	content, err := os.ReadFile(file.FilePath)
	mustFS(t, "read preserved test-owned replacement", err)
	if string(content) != "foreign-marker" {
		t.Error("cleanup deleted or changed replacement")
	}
}

func TestPinnedSecretFileImmutableCloseDrainsWithoutNamespaceCleanup(t *testing.T) {
	filesystem, file := immutableFixture(t)
	fileFD, dirFD, rootFD, scratchFD := file.pinned.fileFD, file.pinned.dirFD, filesystem.filesystem.rootFD, filesystem.scratchFD
	mustFS(t, "close unreleased filesystem", filesystem.Close())
	mustFS(t, "repeat close", filesystem.Close())
	for name, fd := range map[string]int{"creator": fileFD, "directory": dirFD, "root": rootFD, "scratch": scratchFD} {
		assertClosedFD(t, name, fd)
	}
	if _, err := os.Stat(file.FilePath); err != nil {
		t.Errorf("Close unexpectedly cleaned namespace: %v", err)
	}
	if err := filesystem.Cleanup(file); !errors.Is(err, ErrCredentialMaterialization) {
		t.Errorf("Close manufactured cleanup success: %v", err)
	}
	checkImmutable(t, filesystem, file, true)
}

func TestPinnedSecretFileImmutableConcurrentCleanupAndVerification(t *testing.T) {
	filesystem, file := immutableFixture(t)
	fileFD, dirFD := file.pinned.fileFD, file.pinned.dirFD
	start := make(chan struct{})
	results := make(chan error, 8)
	var workers sync.WaitGroup
	for index := range 8 {
		workers.Go(func() {
			<-start
			if index%2 == 0 {
				results <- filesystem.Cleanup(file)
				return
			}
			results <- filesystem.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte(immutableMarker)), 1024)
		})
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, ErrCredentialMaterialization) {
			t.Errorf("concurrent operation unexpected error: %v", err)
		}
	}
	mustFS(t, "final idempotent cleanup", filesystem.Cleanup(file))
	assertClosedFD(t, "concurrent creator", fileFD)
	assertClosedFD(t, "concurrent directory", dirFD)
}

func TestPinnedSecretFileImmutableCrashClosesAllDescriptorsBeforeRecovery(t *testing.T) {
	filesystem, file := immutableFixture(t)
	root := filesystem.identity.scratchPath
	fileFD, dirFD := file.pinned.fileFD, file.pinned.dirFD
	// Simulate the OS crash descriptor teardown, not successful Cleanup.
	mustFS(t, "crash close creator", file.pinned.creator.Close())
	file.pinned.creator = nil
	file.pinned.fileFD = -1
	mustFS(t, "crash close directory", unix.Close(file.pinned.dirFD))
	file.pinned.dirFD = -1
	mustFS(t, "crash close remaining wrapper descriptors", filesystem.Close())
	assertClosedFD(t, "crashed creator", fileFD)
	assertClosedFD(t, "crashed directory", dirFD)
	restarted, err := NewPinnedSecretFileSystem(root)
	mustFS(t, "restart bounded recovery", err)
	t.Cleanup(func() { _ = restarted.Close() })
	if _, err := os.Lstat(file.FilePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("immutable crash residue survived recovery: %v", err)
	}
}

func TestPinnedSecretFileImmutableCreateFailureDoesNotExposeOrLeak(t *testing.T) {
	filesystem, file := immutableFixture(t)
	for _, test := range []struct {
		name     string
		secret   []byte
		maxBytes int64
	}{
		{"empty", nil, 1024}, {"zero bound", []byte("x"), 0}, {"oversize", []byte("xx"), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			created, err := filesystem.CreateImmutable("failure.key", test.secret, test.maxBytes)
			if created != nil || !errors.Is(err, ErrCredentialMaterialization) {
				t.Errorf("invalid create: file present=%v, err=%v", created != nil, err)
			}
			checkImmutable(t, filesystem, file, false)
		})
	}
	if created, err := filesystem.CreateImmutable("../foreign", []byte("x"), 1024); created != nil || err == nil {
		t.Error("invalid name exposed a file")
	}
	mutable, err := filesystem.Create("mutable.key", []byte(immutableMarker), 1024)
	mustFS(t, "create unchanged mutable route", err)
	t.Cleanup(func() { _ = filesystem.Cleanup(mutable) })
	checkImmutable(t, filesystem, mutable, true)
}

func TestPinnedSecretFileImmutableLiveRegistryHasNoRecoveryAdmissionCap(t *testing.T) {
	filesystem, file := immutableFixture(t)
	for range maxRecoveredProviderRoots + 1 {
		mutable, err := filesystem.Create("router.key", []byte("synthetic-mutable-marker"), 1024)
		mustFS(t, "create legitimate invocation beyond recovery ceiling", err)
		t.Cleanup(func() { _ = filesystem.Cleanup(mutable) })
	}
	checkImmutable(t, filesystem, file, false)
	checkImmutable(t, filesystem, file, false)
}

func TestPinnedSecretFileImmutableClosedDescriptorFailuresStayFailClosed(t *testing.T) {
	for _, descriptor := range []string{"creator", "invocation", "root", "scratch"} {
		t.Run(descriptor, func(t *testing.T) {
			filesystem, file := immutableFixture(t)
			fileFD, dirFD := file.pinned.fileFD, file.pinned.dirFD
			switch descriptor {
			case "creator":
				mustFS(t, "close test-owned creator", file.pinned.creator.Close())
				file.pinned.creator = nil
				file.pinned.fileFD = -1
			case "invocation":
				mustFS(t, "close test-owned invocation", unix.Close(dirFD))
				file.pinned.dirFD = -1
			case "root":
				mustFS(t, "close test-owned root", unix.Close(filesystem.filesystem.rootFD))
				filesystem.filesystem.rootFD = -1
			case "scratch":
				mustFS(t, "close test-owned scratch", unix.Close(filesystem.scratchFD))
				filesystem.scratchFD = -1
			}
			checkImmutable(t, filesystem, file, true)
			if descriptor == "root" || descriptor == "scratch" {
				created, err := filesystem.CreateImmutable("after-failure.key", []byte(immutableMarker), 1024)
				if created != nil || !errors.Is(err, ErrCredentialMaterialization) {
					t.Errorf("invalid ancestry create: exposed=%v, err=%v", created != nil, err)
				}
			}
			_ = filesystem.Cleanup(file)
			// No live resources are acquired between teardown and closure checks,
			// so numeric-descriptor reuse cannot mask a double-close or leak.
			assertClosedFD(t, "failed creator", fileFD)
			assertClosedFD(t, "failed invocation", dirFD)
		})
	}
}

func TestPinnedSecretFileImmutableConcurrentCloseDrainsAllDescriptors(t *testing.T) {
	filesystem, file := immutableFixture(t)
	fileFD, dirFD := file.pinned.fileFD, file.pinned.dirFD
	start := make(chan struct{})
	results := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Go(func() { <-start; results <- filesystem.Close() })
	workers.Go(func() { <-start; results <- filesystem.Cleanup(file) })
	workers.Go(func() {
		<-start
		results <- filesystem.VerifyImmutable(context.Background(), file, domain.FingerprintCredential([]byte(immutableMarker)), 1024)
	})
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, ErrCredentialMaterialization) {
			t.Errorf("concurrent close/cleanup/verify unexpected error: %v", err)
		}
	}
	assertClosedFD(t, "close-raced creator", fileFD)
	assertClosedFD(t, "close-raced invocation", dirFD)
	mustFS(t, "idempotent raced close", filesystem.Close())
}

func TestPinnedSecretFileImmutableHashesCompleteBoundedOriginalAndGrowthSentinel(t *testing.T) {
	filesystem, _ := immutableFixture(t)
	original := bytes.Repeat([]byte("synthetic-marker-"), 1024)
	t.Cleanup(func() { zero(original) })
	file, err := filesystem.CreateImmutable("complete.key", original, int64(len(original)))
	mustFS(t, "create multi-buffer original", err)
	t.Cleanup(func() { _ = filesystem.Cleanup(file) })
	expected := domain.FingerprintCredential(original)
	mustFS(t, "verify complete multi-buffer original", filesystem.VerifyImmutable(context.Background(), file, expected, int64(len(original))))
	written, err := unix.Pwrite(file.pinned.fileFD, []byte("X"), int64(len(original)-1))
	mustFS(t, "change last original byte", err)
	if written != 1 {
		t.Fatalf("last-byte write=%d, want1", written)
	}
	if err := filesystem.VerifyImmutable(context.Background(), file, expected, int64(len(original))); !errors.Is(err, ErrCredentialMaterialization) {
		t.Errorf("unchanged prefix with changed last byte accepted: %v", err)
	}
	written, err = unix.Pwrite(file.pinned.fileFD, original[len(original)-1:], int64(len(original)-1))
	mustFS(t, "restore original last byte", err)
	if written != 1 {
		t.Fatalf("last-byte restore=%d, want1", written)
	}
	mustFS(t, "verify restored complete original", filesystem.VerifyImmutable(context.Background(), file, expected, int64(len(original))))
	written, err = unix.Pwrite(file.pinned.fileFD, []byte("X"), int64(len(original)))
	mustFS(t, "append sentinel byte", err)
	if written != 1 {
		t.Fatalf("sentinel write=%d, want1", written)
	}
	// The retained-reader itself detects growth, independently of fstat checks.
	if err := hashImmutable(context.Background(), file.pinned, expected, int64(len(original))); !errors.Is(err, ErrCredentialMaterialization) {
		t.Errorf("original-size-plus-one reader accepted appended byte: %v", err)
	}
	mustFS(t, "truncate to original length", unix.Ftruncate(file.pinned.fileFD, int64(len(original))))
	mustFS(t, "verify original after sentinel restoration", filesystem.VerifyImmutable(context.Background(), file, expected, int64(len(original))))
}

func TestPinnedSecretFileImmutableOptInDoesNotTightenMutableConstructor(t *testing.T) {
	root := providerScratchRoot(t)
	mustFS(t, "set original mutable-compatible scratch mode", unix.Chmod(root, 0o1700))
	t.Cleanup(func() { _ = unix.Chmod(root, 0o700) })
	filesystem, err := NewPinnedSecretFileSystem(root)
	mustFS(t, "construct unchanged mutable filesystem", err)
	t.Cleanup(func() { _ = filesystem.Close() })
	mutable, err := filesystem.Create("router.key", []byte("synthetic-mutable-marker"), 1024)
	mustFS(t, "create unchanged mutable file", err)
	t.Cleanup(func() { _ = filesystem.Cleanup(mutable) })
	immutable, err := filesystem.CreateImmutable("immutable.key", []byte(immutableMarker), 1024)
	if immutable != nil || !errors.Is(err, ErrCredentialMaterialization) {
		t.Errorf("opt-in immutable special-mode rejection: exposed=%v, err=%v", immutable != nil, err)
	}
	mustFS(t, "restore scratch to exact immutable mode", unix.Chmod(root, 0o700))
	immutable, err = filesystem.CreateImmutable("immutable.key", []byte(immutableMarker), 1024)
	mustFS(t, "create restored exact-mode immutable file", err)
	t.Cleanup(func() { _ = filesystem.Cleanup(immutable) })
	checkImmutable(t, filesystem, immutable, false)
}
