package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePrivateAtomicReplacesSymlinkWithoutFollowingIt(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "unrelated")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "report.json")
	if err := os.Symlink(target, output); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateAtomic(output, []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	gotTarget, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotTarget) != "keep" {
		t.Fatalf("symlink target = %q", gotTarget)
	}
	info, err := os.Lstat(output)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode = %v", info.Mode())
	}
}
