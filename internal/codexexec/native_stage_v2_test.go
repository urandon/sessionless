//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
	"golang.org/x/sys/unix"
)

// Inert owned fake artifacts only: these filesystem tests never execute a helper,
// create a native process, or obtain credentials or dispatch authority.
func nativeStageFixtureV2(t *testing.T, kinds []string) (*preparedHistory, *mediaInput, *nativeFrozenProfileV2) {
	t.Helper()
	p := nativeGeneratedFixtureV2()
	root := privatePreparedScratch(t)
	artifacts := []*attachedworkerdaemon.NativeArtifactV2{&p.Launch.Bootstrap, &p.Launch.Relay, &p.Launch.Companion, &p.Launch.Workload}
	for i, artifact := range artifacts {
		body := []byte(fmt.Sprintf("inert stage fixture %d\n", i))
		artifact.Executable = filepath.Join(root, fmt.Sprintf("helper-%d", i))
		artifact.Digest = attachedworkerdaemon.ExecutableDigest(sha256.Sum256(body))
		if err := os.WriteFile(artifact.Executable, body, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	p.Backend.Executable, p.Backend.ExecutableDigest = p.Launch.Workload.Executable, p.Launch.Workload.Digest
	g, err := generateNativeProfileV2(p)
	if err != nil {
		t.Fatalf("generate inert profile: %v", err)
	}
	file := func(raw []byte) nativeharnessinput.FileV3 {
		h := sha256.Sum256(raw)
		return nativeharnessinput.FileV3{Size: uint64(len(raw)), SHA256: hex.EncodeToString(h[:])}
	}
	bundle := p.Launch.Clone()
	bundle.NativeArguments = g.arguments
	bundle.Catalog = file(g.catalog)
	bundle.Schema = file(g.schema)
	bundle.BootstrapArguments, bundle.RelayArguments, err = nativeharnessbootstrap.InstallationArgumentsV3(bundle)
	if err != nil {
		t.Fatalf("lower inert bootstrap: %v", err)
	}
	bundle.StaticProfileDigest = attachedworkerdaemon.ExecutableDigest{7}
	bundle.PolicyDigest, err = bundle.InstallPolicyDigestV3()
	if err != nil {
		t.Fatalf("seal inert install: %v", err)
	}
	profile := &nativeFrozenProfileV2{profile: p, generated: g, launch: bundle}
	media := &mediaInput{}
	for i, kind := range kinds {
		mediaType := "image/png"
		if kind == "jpeg" {
			mediaType = "image/jpeg"
		}
		body := mediaImageFixture(t, mediaType, 2, 2)
		h := sha256.Sum256(body)
		media.sources = append(media.sources, mediaSource{Sequence: 1, Ordinal: i + 1, ImageOrdinal: i + 1, Name: fmt.Sprintf("quoted-%d", i), MediaType: mediaType, Body: body, Size: int64(len(body)), Digest: hex.EncodeToString(h[:])})
	}
	history, err := stagePreparedHistory(privatePreparedScratch(t), []byte("bounded original history\n"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// No real dispatch exists in this fixture; reset only simulated marks.
		if history.nativeV2 != nil {
			history.nativeV2.dispatched = false
		}
		_ = history.remove()
	})
	if err = history.stageImages(media); err != nil {
		t.Fatal(err)
	}
	return history, media, profile
}

func TestNativeStageV2SevenOriginalReadsetsAndCleanup(t *testing.T) {
	variants := [][]string{{}, {"png"}, {"jpeg"}, {"png", "png"}, {"png", "jpeg"}, {"jpeg", "png"}, {"jpeg", "jpeg"}}
	for variant, kinds := range variants {
		t.Run(fmt.Sprint(variant), func(t *testing.T) {
			h, m, p := nativeStageFixtureV2(t, kinds)
			s, err := stageNativeInputV2(h, m, p)
			if err != nil {
				t.Fatal(err)
			}
			if h.nativeV2 != s || s.self != s || len(s.platform.files) != 2 {
				t.Fatal("original owner was not registered with exactly two held profiles")
			}
			for _, entry := range s.platform.files {
				info, err := entry.file.Stat()
				if err != nil || info.Mode().Perm() != 0o400 {
					t.Fatalf("held readonly profile: %v %v", info, err)
				}
				if _, err = entry.file.Write([]byte("x")); err == nil {
					t.Fatal("profile held FD is writable")
				}
			}
			bundle := p.launch.Clone()
			bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest{8}
			execution, err := s.execution(context.Background(), bundle)
			if err != nil {
				t.Fatal(err)
			}
			d := execution.DescriptorV3()
			if d.Variant != uint32(variant) || len(d.Images) != len(kinds) {
				t.Fatalf("variant descriptor=%+v", d)
			}
			maps := execution.MappingsV3()
			want := []string{"/sessionless/profile/models.json", "/sessionless/profile/output.schema.json"}
			paths, _ := nativeharnessinput.ImagePaths(uint32(variant))
			want = append(want, paths...)
			for i, mapping := range maps {
				if mapping.Destination != want[i] || mapping.Source != filepath.Join(h.path, "context", s.platform.files[i].name) || mapping.Inode == 0 {
					t.Fatalf("original fixed mapping%d=%+v want%q", i, mapping, want[i])
				}
			}
			again, err := s.execution(context.Background(), bundle)
			if err != nil || again != execution {
				t.Fatalf("rederivation replaced original readset: %v", err)
			}
			if execution.ValidateV3(context.Background(), bundle) != nil {
				t.Fatal("original readset not reusable for same execution")
			}
			path := h.path
			if err = h.remove(); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("owned cleanup path still present: %v", err)
			}
			if execution.ValidateV3(context.Background(), bundle) == nil {
				t.Fatal("closed/removed original files accepted")
			}
		})
	}
}

func TestNativeStageV2RejectsForeignCopiedAndChangedOwner(t *testing.T) {
	h, m, p := nativeStageFixtureV2(t, []string{"png"})
	copyHistory := *h
	if _, err := stageNativeInputV2(&copyHistory, m, p); err == nil {
		t.Fatal("preattachment copied history acquired native custody")
	}
	s, err := stageNativeInputV2(h, m, p)
	if err != nil {
		t.Fatal(err)
	}
	copyStage := *s
	copyProfile := *p
	copyMedia := *m
	for name, check := range map[string]func() error{
		"copied-stage":   func() error { return copyStage.verify(context.Background(), p, m) },
		"copied-profile": func() error { return s.verify(context.Background(), &copyProfile, m) },
		"copied-media":   func() error { return s.verify(context.Background(), p, &copyMedia) },
		"canceled": func() error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return s.verify(ctx, p, m)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if check() == nil {
				t.Fatal("foreign owner accepted")
			}
		})
	}
	p.generated.configuration[0] ^= 1
	if s.verify(context.Background(), p, m) == nil {
		t.Fatal("same-pointer profile mutation accepted")
	}
	p.generated.configuration[0] ^= 1
	bundle := p.launch.Clone()
	bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest{8}
	bundle.Workload.Digest[0] ^= 1
	if _, err = s.execution(context.Background(), bundle); err == nil {
		t.Fatal("caller changed installed workload")
	}
	h.nativeV2 = &copyStage
	if h.remove() == nil {
		t.Fatal("copied custody allowed cleanup")
	}
	h.nativeV2 = s
	if s.verify(context.Background(), p, m) != nil {
		t.Fatal("negative probes damaged original owner")
	}
}

func TestNativeStageV2RejectsTamperedMountedSources(t *testing.T) {
	for _, kind := range []string{"profile-replacement", "profile-mode", "profile-hardlink", "profile-bytes", "profile-symlink", "profile-missing", "image-replacement", "image-order", "context-replacement", "context-symlink"} {
		t.Run(kind, func(t *testing.T) {
			h, m, p := nativeStageFixtureV2(t, []string{"png", "jpeg"})
			s, err := stageNativeInputV2(h, m, p)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(h.path, "context", "models.json")
			switch kind {
			case "profile-symlink", "profile-missing":
				if err = os.Rename(path, path+"-original"); err != nil {
					t.Fatal(err)
				}
				if kind == "profile-symlink" {
					if err = os.Symlink(path+"-original", path); err != nil {
						t.Fatal(err)
					}
				}
			case "profile-replacement", "image-replacement":
				if kind == "image-replacement" {
					path = h.imagePaths()[0]
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Rename(path, path+"-original"); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, raw, 0o400); err != nil {
					t.Fatal(err)
				}
			case "profile-mode":
				if err = os.Chmod(path, 0o600); err != nil {
					t.Fatal(err)
				}
			case "profile-hardlink":
				if err = os.Link(path, path+"-alias"); err != nil {
					t.Fatal(err)
				}
			case "profile-bytes":
				raw := slices.Clone(p.generated.catalog)
				raw[0] ^= 1
				if err = os.Chmod(path, 0o600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, raw, 0o400); err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(path, 0o400); err != nil {
					t.Fatal(err)
				}
			case "image-order":
				m.sources[0], m.sources[1] = m.sources[1], m.sources[0]
			case "context-replacement", "context-symlink":
				path = filepath.Join(h.path, "context")
				if err = os.Rename(path, path+"-original"); err != nil {
					t.Fatal(err)
				}
				if kind == "context-replacement" {
					if err = os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
				} else if err = os.Symlink(path+"-original", path); err != nil {
					t.Fatal(err)
				}
			}
			if s.verify(context.Background(), p, m) == nil {
				t.Fatal("substituted/modified original source accepted")
			}
			bundle := p.launch.Clone()
			bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest{8}
			if _, err = s.execution(context.Background(), bundle); err == nil {
				t.Fatal("tampered source reached readset construction")
			}
			if kind == "profile-replacement" {
				if h.remove() == nil {
					t.Fatal("cleanup silently accepted substituted inode")
				}
				body, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(body, p.generated.catalog) {
					t.Fatal("cleanup removed foreign replacement")
				}
			}
		})
	}
}

func TestNativeStageV2UnknownDispatchRetainsEveryHandleAndPath(t *testing.T) {
	h, m, p := nativeStageFixtureV2(t, []string{"png"})
	s, err := stageNativeInputV2(h, m, p)
	if err != nil {
		t.Fatal(err)
	}
	bundle := p.launch.Clone()
	bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest{8}
	v, err := s.execution(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	s.dispatched = true // Simulated supervisor mark, not actual dispatch evidence.
	fds := []int{h.root, h.work, h.context}
	path := h.path
	if h.remove() == nil || s.remove() == nil {
		t.Fatal("unknown handback allowed cleanup")
	}
	if h.path != path || !slices.Equal(fds, []int{h.root, h.work, h.context}) {
		t.Fatal("unknown handback discarded original directory ownership")
	}
	for _, fd := range fds {
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil {
			t.Fatal("directory FD closed on unknown handback")
		}
	}
	if v.ValidateV3(context.Background(), bundle) != nil || s.verify(context.Background(), p, m) != nil {
		t.Fatal("unknown handback dropped original input handles")
	}
	s.quiesced = true // Controlled fake success; real integration must supply it.
	if err = h.remove(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeStageV2PartialAcquisitionRemainsHistoryOwned(t *testing.T) {
	h, m, p := nativeStageFixtureV2(t, nil)
	p.launch.Schema.SHA256 = "invalid"
	if _, err := stageNativeInputV2(h, m, p); err == nil {
		t.Fatal("invalid second profile accepted")
	}
	if h.nativeV2 == nil || len(h.nativeV2.platform.files) != 1 {
		t.Fatal("partial first file did not remain registered with history")
	}
	path := h.path
	if err := h.remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("partial owned acquisition leaked: %v", err)
	}
}

func TestNativeStageV2InjectedClockIsNotProfileAuthority(t *testing.T) {
	h, m, p := nativeStageFixtureV2(t, []string{"png"})
	clockCalls := 0
	p.profile.Backend.Now = func() time.Time {
		clockCalls++
		return time.Unix(1234, 0)
	}
	s, err := stageNativeInputV2(h, m, p)
	if err != nil {
		t.Fatalf("original profile with injected clock rejected: %v", err)
	}
	if p.profile.Backend.Now == nil || s.frozen.profile.Backend.Now == nil {
		t.Fatal("snapshot check mutated an original clock")
	}
	if err = s.verify(context.Background(), p, m); err != nil {
		t.Fatalf("same original clock profile rejected: %v", err)
	}
	// Replacing a clock cannot authorize another profile or weaken immutable
	// data checks; the stage never evaluates either callback.
	p.profile.Backend.Now = func() time.Time {
		clockCalls++
		return time.Unix(5678, 0)
	}
	if err = s.verify(context.Background(), p, m); err != nil {
		t.Fatalf("non-identity clock replacement rejected: %v", err)
	}
	p.profile.Backend.Model += "-changed"
	if s.verify(context.Background(), p, m) == nil {
		t.Fatal("clock exclusion erased actual frozen backend comparison")
	}
	p.profile.Backend.Model = s.frozen.profile.Backend.Model
	p.generated.catalog[0] ^= 1
	if s.verify(context.Background(), p, m) == nil {
		t.Fatal("clock exclusion erased frozen byte comparison")
	}
	p.generated.catalog[0] ^= 1
	bundle := p.launch.Clone()
	bundle.ScopeDigest = attachedworkerdaemon.ExecutableDigest{8}
	if _, err = s.execution(context.Background(), bundle); err != nil {
		t.Fatalf("restored original with injected clock cannot derive readset: %v", err)
	}
	if clockCalls != 0 {
		t.Fatalf("stage evaluated clock as authority: calls=%d", clockCalls)
	}
}
