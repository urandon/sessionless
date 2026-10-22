//go:build darwin || linux

package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
	"golang.org/x/sys/unix"
)

type nativeStageFileV2 struct {
	name     string
	file     *os.File
	identity unix.Stat_t
	spec     nativeharnessinput.FileV3
}

type nativeInputStagePlatformV2 struct {
	root, work, context int
	path, name          string
	directories         [3]unix.Stat_t
	files               []nativeStageFileV2 // profile files first, then original held images
}

func stageNativeInputV2(history *preparedHistory, media *mediaInput, profile *nativeFrozenProfileV2) (*nativeInputStageV2, error) {
	if history == nil || history.self != history || history.nativeV2 != nil || history.root < 0 || history.work < 0 || history.context < 0 || media == nil || profile == nil || history.verifyImages(media) != nil {
		return nil, ErrContract
	}
	s := &nativeInputStageV2{history: history, media: media, profile: profile, frozen: *profile,
		platform: nativeInputStagePlatformV2{root: history.root, work: history.work, context: history.context, path: history.path, name: history.name}}
	s.self = s
	s.frozen.profile = profile.profile.Clone()
	s.frozen.launch = profile.launch.Clone()
	s.frozen.generated.configuration = slices.Clone(profile.generated.configuration)
	s.frozen.generated.catalog = slices.Clone(profile.generated.catalog)
	s.frozen.generated.schema = slices.Clone(profile.generated.schema)
	s.frozen.generated.environment = slices.Clone(profile.generated.environment)
	for i := range s.frozen.generated.arguments {
		s.frozen.generated.arguments[i] = slices.Clone(profile.generated.arguments[i])
	}
	history.nativeV2 = s // cleanup ownership precedes any new filesystem object
	for i, fd := range []int{history.root, history.work, history.context} {
		if unix.Fstat(fd, &s.platform.directories[i]) != nil {
			return nil, ErrContract
		}
	}
	for i, body := range [][]byte{profile.generated.catalog, profile.generated.schema} {
		name, spec := "models.json", profile.launch.Catalog
		if i == 1 {
			name, spec = "output.schema.json", profile.launch.Schema
		}
		hash := sha256.Sum256(body)
		if len(body) == 0 || uint64(len(body)) > nativeharnessinput.MaxFileBytes || spec.Size != uint64(len(body)) || spec.SHA256 != hex.EncodeToString(hash[:]) {
			return nil, ErrContract
		}
		fd, err := unix.Openat(history.context, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if err != nil {
			return nil, ErrContract
		}
		entry := nativeStageFileV2{name: name, file: os.NewFile(uintptr(fd), name), spec: spec}
		s.platform.files = append(s.platform.files, entry)
		owned := &s.platform.files[len(s.platform.files)-1]
		if unix.Fstat(fd, &owned.identity) != nil {
			return nil, ErrContract
		}
		n, writeErr := owned.file.Write(body)
		modeErr := owned.file.Chmod(0o400)
		closeErr := owned.file.Close()
		owned.file = nil
		if n != len(body) || writeErr != nil || modeErr != nil || closeErr != nil {
			return nil, ErrContract
		}
		fd, err = unix.Openat(history.context, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, ErrContract
		}
		owned.file = os.NewFile(uintptr(fd), name)
	}
	if s.verify(context.Background(), profile, media) != nil {
		return nil, ErrContract
	}
	return s, nil
}

func nativeSameInodeV2(a, b *unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode&unix.S_IFMT == b.Mode&unix.S_IFMT
}

func (s *nativeInputStageV2) verifyDirectories() error {
	p, h := &s.platform, s.history
	if h.root != p.root || h.work != p.work || h.context != p.context || h.path != p.path || h.name != p.name || h.workIdentity == nil || h.contextIdentity == nil ||
		!nativeSameInodeV2(h.workIdentity, &p.directories[1]) || !nativeSameInodeV2(h.contextIdentity, &p.directories[2]) {
		return ErrContract
	}
	for i, fd := range []int{p.root, p.work, p.context} {
		var actual unix.Stat_t
		if unix.Fstat(fd, &actual) != nil || !nativeSameInodeV2(&actual, &p.directories[i]) || actual.Mode&unix.S_IFMT != unix.S_IFDIR || actual.Mode&0o777 != 0o700 || actual.Uid != uint32(os.Geteuid()) {
			return ErrContract
		}
	}
	var actual unix.Stat_t
	root := filepath.Dir(p.path)
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root || unix.Lstat(root, &actual) != nil || !nativeSameInodeV2(&actual, &p.directories[0]) ||
		unix.Fstatat(p.root, p.name, &actual, unix.AT_SYMLINK_NOFOLLOW) != nil || !nativeSameInodeV2(&actual, &p.directories[1]) ||
		unix.Fstatat(p.work, "context", &actual, unix.AT_SYMLINK_NOFOLLOW) != nil || !nativeSameInodeV2(&actual, &p.directories[2]) {
		return ErrContract
	}
	return nil
}

func (s *nativeInputStageV2) verifyFile(ctx context.Context, entry *nativeStageFileV2) error {
	if entry.file == nil || entry.spec.Size == 0 || entry.spec.Size > nativeharnessinput.MaxFileBytes {
		return ErrContract
	}
	check := func() error {
		var held, path unix.Stat_t
		flags, err := unix.FcntlInt(entry.file.Fd(), unix.F_GETFL, 0)
		if err != nil || flags&unix.O_ACCMODE != unix.O_RDONLY || unix.Fstat(int(entry.file.Fd()), &held) != nil || unix.Fstatat(s.platform.context, entry.name, &path, unix.AT_SYMLINK_NOFOLLOW) != nil {
			return ErrContract
		}
		for _, actual := range []unix.Stat_t{held, path} {
			if !nativeSameInodeV2(&actual, &entry.identity) || actual.Mode&unix.S_IFMT != unix.S_IFREG || actual.Mode&0o777 != 0o400 || actual.Uid != uint32(os.Geteuid()) || actual.Nlink != 1 || actual.Size < 0 || uint64(actual.Size) != entry.spec.Size {
				return ErrContract
			}
		}
		return nil
	}
	if check() != nil {
		return ErrContract
	}
	hash := sha256.New()
	reader := io.NewSectionReader(entry.file, 0, int64(entry.spec.Size)+1)
	var buf [8192]byte
	var used uint64
	for {
		if ctx.Err() != nil {
			return ErrContract
		}
		n, err := reader.Read(buf[:])
		used += uint64(n)
		if used > entry.spec.Size {
			return ErrContract
		}
		_, _ = hash.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil || n == 0 {
			return ErrContract
		}
	}
	if used != entry.spec.Size || hex.EncodeToString(hash.Sum(nil)) != entry.spec.SHA256 || check() != nil {
		return ErrContract
	}
	return nil
}

func (s *nativeInputStageV2) verifyPlatform(ctx context.Context) error {
	// An injected clock is a test/time dependency, deliberately excluded from
	// immutable profile identity. Go functions never DeepEqual when non-nil.
	// Clear only the copied clock fields: never invoke or mutate the originals,
	// and retain exact comparison of every other frozen profile member.
	actual, frozen := *s.profile, s.frozen
	actual.profile.Backend.Now, frozen.profile.Backend.Now = nil, nil
	if !reflect.DeepEqual(actual, frozen) || s.verifyDirectories() != nil || s.history.verifyImages(s.media) != nil || len(s.platform.files) < 2 || len(s.platform.files) > 4 {
		return ErrContract
	}
	for i := range s.platform.files {
		if s.verifyFile(ctx, &s.platform.files[i]) != nil {
			return ErrContract
		}
	}
	if ctx.Err() != nil || s.verifyDirectories() != nil {
		return ErrContract
	}
	return nil
}

func (s *nativeInputStageV2) executionPlatform(ctx context.Context, bundle attachedworkerdaemon.NativeLaunchV3) (*attachedworkerdaemon.NativeExecutionV3, error) {
	expected := s.frozen.launch.Clone()
	expected.ScopeDigest = bundle.ScopeDigest
	if !reflect.DeepEqual(expected, bundle) {
		return nil, ErrContract
	}
	if s.readset != nil {
		if s.readset.ValidateV3(ctx, bundle) != nil {
			return nil, ErrContract
		}
		return s.readset, nil
	}
	if len(s.platform.files) != 2 {
		return nil, ErrContract
	}
	d := nativeharnessinput.NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: hex.EncodeToString(bundle.ScopeDigest[:]), InstallDigest: hex.EncodeToString(bundle.PolicyDigest[:]), Images: []nativeharnessinput.ImageV3{}, Catalog: bundle.Catalog, Schema: bundle.Schema}
	var suffixes []string
	for _, source := range s.media.sources {
		if source.ImageOrdinal == 0 {
			continue
		}
		i := len(suffixes)
		if i >= len(s.history.images) || source.ImageOrdinal != i+1 || !mediaBodyMatches(source) {
			return nil, ErrContract
		}
		suffix := "png"
		if source.MediaType == "image/jpeg" {
			suffix = "jpg"
		} else if source.MediaType != "image/png" {
			return nil, ErrContract
		}
		name := "image-0" + string(rune('1'+i)) + "." + suffix
		original := s.history.images[i]
		if original.name != name || !reflect.DeepEqual(original.source, source) {
			return nil, ErrContract
		}
		fd, err := unix.Openat(s.platform.context, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, ErrContract
		}
		s.platform.files = append(s.platform.files, nativeStageFileV2{name: name, file: os.NewFile(uintptr(fd), name), identity: original.identity, spec: nativeharnessinput.FileV3{Size: uint64(source.Size), SHA256: source.Digest}})
		suffixes = append(suffixes, suffix)
		d.Images = append(d.Images, nativeharnessinput.ImageV3{Ordinal: uint32(i + 1), Size: uint64(source.Size), SHA256: source.Digest})
	}
	switch len(suffixes) {
	case 0:
		d.Variant = 0
	case 1:
		d.Variant = 1
		if suffixes[0] == "jpg" {
			d.Variant = 2
		}
	case 2:
		d.Variant = 3
		if suffixes[0] == "jpg" {
			d.Variant += 2
		}
		if suffixes[1] == "jpg" {
			d.Variant++
		}
	default:
		return nil, ErrContract
	}
	if s.verify(ctx, s.profile, s.media) != nil {
		return nil, ErrContract
	}
	files := make([]attachedworkerdaemon.NativeInputFileV3, 0, len(s.platform.files))
	for _, entry := range s.platform.files {
		files = append(files, attachedworkerdaemon.NativeInputFileV3{Source: filepath.Join(s.platform.path, "context", entry.name), File: entry.file})
	}
	v, err := attachedworkerdaemon.NewNativeExecutionV3(ctx, bundle, d, files)
	if err != nil {
		return nil, ErrContract
	}
	s.readset = v
	return v, nil
}

func (s *nativeInputStageV2) remove() error {
	if s == nil {
		return nil
	}
	if s.self != s || s.dispatched && !s.quiesced {
		return ErrContract
	}
	var result error
	for i := range s.platform.files {
		entry := &s.platform.files[i]
		if i < 2 {
			var actual unix.Stat_t
			if unix.Fstatat(s.platform.context, entry.name, &actual, unix.AT_SYMLINK_NOFOLLOW) != nil || !nativeSameInodeV2(&actual, &entry.identity) || actual.Mode&unix.S_IFMT != unix.S_IFREG {
				result = errors.Join(result, ErrContract)
			} else if unix.Unlinkat(s.platform.context, entry.name, 0) != nil {
				result = errors.Join(result, ErrContract)
			}
		}
		if entry.file != nil {
			result = errors.Join(result, entry.file.Close())
			entry.file = nil
		}
	}
	s.platform.files = nil
	return result
}
