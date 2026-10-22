//go:build darwin || linux

package nativeharnessbootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

func v3FixtureDescriptor(variant uint32) nativeharnessinput.NativeExecutionDescriptorV3 {
	digest := func(body string) string { hash := sha256.Sum256([]byte(body)); return hex.EncodeToString(hash[:]) }
	d := nativeharnessinput.NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: digest("original accepted scope"), InstallDigest: digest("sealed installation"), Variant: variant,
		Images:  []nativeharnessinput.ImageV3{},
		Catalog: nativeharnessinput.FileV3{Size: uint64(len("catalog")), SHA256: digest("catalog")},
		Schema:  nativeharnessinput.FileV3{Size: uint64(len("schema")), SHA256: digest("schema")}}
	count := 0
	if variant > 0 {
		count = 1
	}
	if variant > 2 {
		count = 2
	}
	for i := range count {
		body := "image" + strconv.Itoa(i+1)
		d.Images = append(d.Images, nativeharnessinput.ImageV3{Ordinal: uint32(i + 1), Size: uint64(len(body)), SHA256: digest(body)})
	}
	return d
}

func v3NativeArgs(t *testing.T, descriptor nativeharnessinput.NativeExecutionDescriptorV3) []string {
	t.Helper()
	slots, err := nativeharnessinput.Slots(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"literal argument", "$not-a-shell"}
	for _, slot := range slots {
		if slot.Ordinal != 0 {
			args = append(args, "--image", slot.Path)
		}
	}
	return append(args, "-")
}

func TestBootstrapV3CarrierIsExactAndNotNativeArgv(t *testing.T) {
	for variant := uint32(0); variant < 7; variant++ {
		t.Run(strconv.Itoa(int(variant)), func(t *testing.T) {
			d := v3FixtureDescriptor(variant)
			w, helpers := contract()
			w.Arguments = v3NativeArgs(t, d)
			boot, relay, err := ArgumentsV3(w, helpers, d)
			if err != nil {
				t.Fatal(err)
			}
			p, decoded, err := parseBootstrapV3(boot)
			if err != nil || decoded.Variant != variant || !slices.Equal(p.native.args, w.Arguments) || !slices.Equal(p.relay.args, relay) {
				t.Fatalf("variant%d handoff: native=%q descriptor=%+v err=%v", variant, p.native.args, decoded, err)
			}
			if _, err := parseBootstrap(boot); !errors.Is(err, ErrConfig) {
				t.Fatalf("V3 accepted by V2: %v", err)
			}
			for _, mutate := range []struct {
				name  string
				apply func([]string) []string
			}{
				{"missing", func(a []string) []string { return a[:len(a)-2] }},
				{"extra", func(a []string) []string { return append(a, "--execution-descriptor-json", a[len(a)-1]) }},
				{"unknown", func(a []string) []string {
					a[len(a)-1] = strings.TrimSuffix(a[len(a)-1], "}") + `,"unexpected":true}`
					return a
				}},
				{"noncanonical", func(a []string) []string { a[len(a)-1] += " "; return a }},
				{"too-large", func(a []string) []string { a[len(a)-1] = strings.Repeat("x", 4097); return a }},
			} {
				t.Run(mutate.name, func(t *testing.T) {
					if _, _, err := parseBootstrapV3(mutate.apply(slices.Clone(boot))); !errors.Is(err, ErrConfig) {
						t.Fatalf("malformed carrier accepted: %v", err)
					}
				})
			}
			w.Arguments = append(w.Arguments, "--image", "/foreign/image.png")
			if _, _, err := ArgumentsV3(w, helpers, d); err == nil {
				t.Fatal("image argv outside selected variant accepted")
			}
			if variant > 2 {
				w.Arguments = v3NativeArgs(t, d)
				w.Arguments[3], w.Arguments[5] = w.Arguments[5], w.Arguments[3]
				if _, _, err := ArgumentsV3(w, helpers, d); err == nil {
					t.Fatal("native image order differs from original descriptor")
				}
			}
		})
	}
}

func v3MountedFixture(t *testing.T, descriptor nativeharnessinput.NativeExecutionDescriptorV3) *os.Root {
	t.Helper()
	directory := t.TempDir()
	slots, err := nativeharnessinput.Slots(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range slots {
		body := "image" + strconv.Itoa(int(slot.Ordinal))
		switch slot.Path {
		case "/sessionless/profile/models.json":
			body = "catalog"
		case "/sessionless/profile/output.schema.json":
			body = "schema"
		}
		path := filepath.Join(directory, slot.RelativePath)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0400); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("fixture root close: %v", err)
		}
	})
	return root
}

func TestBootstrapV3RejectsImageAliasesBeforeHandoff(t *testing.T) {
	d := v3FixtureDescriptor(1)
	for _, alias := range []string{"-i", "-i/foreign.png", "-i=/foreign.png", "--image=/foreign.png"} {
		t.Run(alias, func(t *testing.T) {
			w, helpers := contract()
			w.Arguments = append(v3NativeArgs(t, d), alias)
			if _, _, err := ArgumentsV3(w, helpers, d); !errors.Is(err, ErrConfig) {
				t.Fatalf("image alias %q accepted: %v", alias, err)
			}
		})
	}
}

func configureV3Fixture(t *testing.T, f *fixture, descriptor nativeharnessinput.NativeExecutionDescriptorV3) {
	t.Helper()
	p, err := parseBootstrap(f.args)
	if err != nil {
		t.Fatal(err)
	}
	countIndex := len(f.args) - len(p.native.args) - 1
	f.args = append(slices.Clone(f.args[:countIndex]), strconv.Itoa(len(v3NativeArgs(t, descriptor))))
	f.args = append(f.args, v3NativeArgs(t, descriptor)...)
	raw, err := nativeharnessinput.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	f.args[0] = "bootstrap-v3"
	f.args = append(f.args, "--execution-descriptor-json", string(raw))
}

func TestBootstrapV3MountedInputHandoffUsesExistingChildLifecycle(t *testing.T) {
	for variant := uint32(0); variant < 7; variant++ {
		t.Run(strconv.Itoa(int(variant)), func(t *testing.T) {
			d := v3FixtureDescriptor(variant)
			root := v3MountedFixture(t, d)
			f := newFixtureWithRun(t, "ready", "echo", func(f *fixture) { configureV3Fixture(t, f, d) }, func(ctx context.Context, f *fixture) error {
				return runBootstrapV3(ctx, f.args, f.env, f.in, f.out, f.errout, root)
			})
			if err := f.await(t); err != nil {
				t.Fatalf("variant%d actual fixture handoff: %v", variant, err)
			}
			raw, err := os.ReadFile(f.out.Name())
			if err != nil || string(raw) != "canonical stdin\n" {
				t.Fatalf("native stdio=%q err=%v", raw, err)
			}
			f.assertReaped(t)
		})
	}
}

func TestBootstrapV3WrongMountedBytesDenyBeforeAnyChild(t *testing.T) {
	d := v3FixtureDescriptor(6)
	root := v3MountedFixture(t, d)
	// A valid but foreign expected digest is still canonical. Only actual
	// mounted-byte checking can reject this substitution before relay/native.
	d.Images[0].SHA256 = d.Images[1].SHA256
	f := newFixtureWithRun(t, "ready", "echo", func(f *fixture) { configureV3Fixture(t, f, d) }, func(ctx context.Context, f *fixture) error {
		return runBootstrapV3(ctx, f.args, f.env, f.in, f.out, f.errout, root)
	})
	if err := f.await(t); !errors.Is(err, ErrConfig) {
		t.Fatalf("foreign bytes result=%v", err)
	}
	for _, path := range []string{f.relay, f.native} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("child created before integrity check: %s %v", path, err)
		}
	}
}

func TestBootstrapV3MountedMutationDuringRelayStartupDeniesNative(t *testing.T) {
	d := v3FixtureDescriptor(6)
	root := v3MountedFixture(t, d)
	f := newFixtureWithRun(t, "late-ready", "echo", func(f *fixture) { configureV3Fixture(t, f, d) }, func(ctx context.Context, f *fixture) error {
		return runBootstrapV3(ctx, f.args, f.env, f.in, f.out, f.errout, root)
	})
	// The actual owned relay marker proves the first input check completed.
	// Gate readiness, replace only this fixture's readonly original, then allow
	// startup to continue. No sleep decides which check saw the substitution.
	f.marker(t, f.relay)
	path := "sessionless/input/image-01.jpg"
	if err := root.Remove(path); err != nil {
		t.Fatal(err)
	}
	file, err := root.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0400)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteString("image2")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("owned input substitution: write=%v close=%v", writeErr, closeErr)
	}
	if err := os.WriteFile(f.gate, []byte("release readiness"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.await(t); !errors.Is(err, ErrConfig) {
		t.Fatalf("changed mounted image permitted native: %v", err)
	}
	if _, err := os.Stat(f.native); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native started after changed bytes: %v", err)
	}
	f.assertReaped(t)
}

func TestBootstrapV3CancellationDuringRelayStartupNeverStartsNative(t *testing.T) {
	d := v3FixtureDescriptor(1)
	root := v3MountedFixture(t, d)
	f := newFixtureWithRun(t, "late-ready", "echo", func(f *fixture) { configureV3Fixture(t, f, d) }, func(ctx context.Context, f *fixture) error {
		return runBootstrapV3(ctx, f.args, f.env, f.in, f.out, f.errout, root)
	})
	f.marker(t, f.relay)
	f.cancel()
	if err := os.WriteFile(f.gate, []byte("late readiness"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.await(t); err == nil {
		t.Fatal("cancelled V3 startup accepted")
	}
	if _, err := os.Stat(f.native); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native started after cancellation: %v", err)
	}
	f.assertReaped(t)
}
