//go:build darwin || linux

package nativeharnessinput

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func mountedFixture(t *testing.T, variant uint32) (string, *os.Root, NativeExecutionDescriptorV3) {
	t.Helper()
	dir := t.TempDir()
	d := testDescriptor(variant)
	for _, sub := range []string{"sessionless/profile", "sessionless/input"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	slots, err := Slots(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range slots {
		body := "image"
		if slot.Ordinal == 0 {
			body = "catalog"
			if slot.RelativePath == "sessionless/profile/output.schema.json" {
				body = "schema"
			}
		}
		if err := os.WriteFile(filepath.Join(dir, slot.RelativePath), []byte(body), 0o400); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("close fixture root: %v", err)
		}
	})
	return dir, root, d
}

func TestVerifyMountedSevenVariants(t *testing.T) {
	for v := uint32(0); v < 7; v++ {
		t.Run(string(rune('0'+v)), func(t *testing.T) {
			_, root, d := mountedFixture(t, v)
			if err := VerifyMounted(t.Context(), root, d); err != nil {
				t.Fatalf("variant%d: %v", v, err)
			}
		})
	}
	t.Run("no-input-directory", func(t *testing.T) {
		dir, root, d := mountedFixture(t, 0)
		if err := os.Remove(filepath.Join(dir, "sessionless/input")); err != nil {
			t.Fatal(err)
		}
		if err := VerifyMounted(t.Context(), root, d); err != nil {
			t.Fatal(err)
		}
	})
}

func TestVerifyMountedRejectsCausalChanges(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, string, *NativeExecutionDescriptorV3)
	}{
		{"content", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			replaceFile(t, filepath.Join(dir, "sessionless/input/image-01.png"), "other")
		}},
		{"size", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) { d.Images[0].Size++ }},
		{"catalog-content", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			replaceFile(t, filepath.Join(dir, "sessionless/profile/models.json"), "CATALOG")
		}},
		{"schema-content", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			replaceFile(t, filepath.Join(dir, "sessionless/profile/output.schema.json"), "SCHEMA")
		}},
		{"writable", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.Chmod(filepath.Join(dir, "sessionless/input/image-01.png"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.Remove(filepath.Join(dir, "sessionless/input/image-01.png")); err != nil {
				t.Fatal(err)
			}
		}},
		{"suffix", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.Rename(filepath.Join(dir, "sessionless/input/image-01.png"), filepath.Join(dir, "sessionless/input/image-01.jpg")); err != nil {
				t.Fatal(err)
			}
		}},
		{"extra-image", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.WriteFile(filepath.Join(dir, "sessionless/input/image-01.jpg"), []byte("image"), 0o400); err != nil {
				t.Fatal(err)
			}
		}},
		{"extra-profile", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.WriteFile(filepath.Join(dir, "sessionless/profile/extra"), []byte("image"), 0o400); err != nil {
				t.Fatal(err)
			}
		}},
		{"extra-sessionless", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.WriteFile(filepath.Join(dir, "sessionless/extra"), []byte("image"), 0o400); err != nil {
				t.Fatal(err)
			}
		}},
		{"file-symlink", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			path := filepath.Join(dir, "sessionless/input/image-01.png")
			if err := os.Rename(path, path+".original"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("image-01.png.original", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"parent-symlink", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			path := filepath.Join(dir, "sessionless/input")
			if err := os.Rename(path, path+"-original"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("input-original", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"session-symlink", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			path := filepath.Join(dir, "sessionless")
			if err := os.Rename(path, path+"-original"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("sessionless-original", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"directory-not-file", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			path := filepath.Join(dir, "sessionless/input/image-01.png")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o500); err != nil {
				t.Fatal(err)
			}
		}},
		{"hardlink", func(t *testing.T, dir string, d *NativeExecutionDescriptorV3) {
			if err := os.Link(filepath.Join(dir, "sessionless/input/image-01.png"), filepath.Join(dir, "alias")); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir, root, d := mountedFixture(t, 1)
			if err := VerifyMounted(t.Context(), root, d); err != nil {
				t.Fatalf("unchanged fixture denied: %v", err)
			}
			tc.mutate(t, dir, &d)
			if err := VerifyMounted(t.Context(), root, d); !errors.Is(err, ErrMounted) {
				t.Fatalf("mutation accepted: %v", err)
			}
		})
	}
}

func replaceFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o400); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyMountedContextAndExtraNoImage(t *testing.T) {
	dir, root, d := mountedFixture(t, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := VerifyMounted(ctx, root, d); !errors.Is(err, ErrMounted) {
		t.Fatalf("cancelled context: %v", err)
	}
	if err := VerifyMounted(nil, root, d); !errors.Is(err, ErrMounted) {
		t.Fatalf("nil context: %v", err)
	}
	if err := VerifyMounted(t.Context(), nil, d); !errors.Is(err, ErrMounted) {
		t.Fatalf("nil root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sessionless/input/unselected"), []byte("image"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := VerifyMounted(t.Context(), root, d); !errors.Is(err, ErrMounted) {
		t.Fatalf("unselected image accepted: %v", err)
	}
}

type readContext struct {
	context.Context
	checks int
	change func()
}

func (c *readContext) Err() error {
	c.checks++
	if c.checks == 2 {
		c.change()
	}
	return c.Context.Err()
}

// This context synchronously replaces the pathname only after verifyFile has
// opened its held descriptor and before its first bounded read. There is no
// timing race or production mutation hook; same bytes must not hide new inode.
func TestVerifyHeldFileRejectsSameBytesPathReplacement(t *testing.T) {
	dir, root, d := mountedFixture(t, 1)
	slots, _ := Slots(d)
	parent, err := root.Open("sessionless/input")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	path := filepath.Join(dir, slots[2].RelativePath)
	ctx := &readContext{Context: t.Context(), change: func() {
		if err := os.Rename(path, filepath.Join(dir, "held-original")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("image"), 0o400); err != nil {
			t.Fatal(err)
		}
	}}
	if err := verifyFile(ctx, parent, slots[2]); !errors.Is(err, ErrMounted) {
		t.Fatalf("held file inode substitution accepted: %v", err)
	}
}
