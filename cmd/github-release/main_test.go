package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadRegularRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	link := filepath.Join(directory, "release-notes.md")
	if err := os.WriteFile(target, []byte("notes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegular(link); err == nil {
		t.Fatal("symlink release input was accepted")
	}
}
