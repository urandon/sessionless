//go:build darwin || linux

package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type preparedImage struct {
	name     string
	source   mediaSource
	identity unix.Stat_t
}

// Images share the original held history-directory lifetime. User filenames
// never enter a path, argument or cleanup operation. Original bytes are staged
// unchanged; native preparation is a distinct, not-yet-attested effect.
func (stage *preparedHistory) stageImages(input *mediaInput) error {
	if stage == nil || stage.context < 0 || input == nil || len(stage.images) != 0 {
		return ErrContract
	}
	var used int
	for _, source := range input.sources {
		if source.ImageOrdinal == 0 {
			continue
		}
		used += len(source.Body)
		if len(stage.images) >= 2 || used > mediaImageTotal || !mediaBodyMatches(source) {
			return ErrContract
		}
		ext := "png"
		if source.MediaType == "image/jpeg" {
			ext = "jpg"
		}
		name := fmt.Sprintf("image-%02d.%s", source.ImageOrdinal, ext)
		fd, err := unix.Openat(stage.context, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if err != nil {
			return ErrContract
		}
		entry := preparedImage{name: name, source: source}
		if unix.Fstat(fd, &entry.identity) != nil {
			unix.Close(fd)
			return ErrContract
		}
		stage.images = append(stage.images, entry) // acquire cleanup before writing
		file := os.NewFile(uintptr(fd), name)
		n, writeErr := file.Write(source.Body)
		modeErr := file.Chmod(0o400)
		closeErr := file.Close()
		if n != len(source.Body) || writeErr != nil || modeErr != nil || closeErr != nil {
			return ErrContract
		}
	}
	return stage.verifyImages(input)
}

func (stage *preparedHistory) verifyImages(input *mediaInput) error {
	if stage == nil || stage.context < 0 || input == nil {
		return ErrContract
	}
	ordinal := 0
	for _, source := range input.sources {
		if source.ImageOrdinal == 0 {
			continue
		}
		ordinal++
		if ordinal > len(stage.images) {
			return ErrContract
		}
		entry := stage.images[ordinal-1]
		if entry.source.EventID != source.EventID || entry.source.Sequence != source.Sequence || entry.source.Ordinal != source.Ordinal || entry.source.Blob != source.Blob || entry.source.ImageOrdinal != source.ImageOrdinal {
			return ErrContract
		}
		fd, err := unix.Openat(stage.context, entry.name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return ErrContract
		}
		file := os.NewFile(uintptr(fd), entry.name)
		var actual unix.Stat_t
		if unix.Fstat(fd, &actual) != nil || actual.Dev != entry.identity.Dev || actual.Ino != entry.identity.Ino || actual.Mode&unix.S_IFMT != unix.S_IFREG || actual.Mode&0o777 != 0o400 || actual.Uid != uint32(os.Geteuid()) || actual.Nlink != 1 || actual.Size != source.Size {
			file.Close()
			return ErrContract
		}
		h := sha256.New()
		n, readErr := io.Copy(h, io.LimitReader(file, source.Size+1))
		closeErr := file.Close()
		if n != source.Size || readErr != nil || closeErr != nil || hex.EncodeToString(h.Sum(nil)) != source.Digest {
			return ErrContract
		}
	}
	if ordinal != len(stage.images) {
		return ErrContract
	}
	return nil
}

func (stage *preparedHistory) imagePaths() []string {
	paths := make([]string, 0, len(stage.images))
	for _, entry := range stage.images {
		paths = append(paths, filepath.Join(stage.path, "context", entry.name))
	}
	return paths
}

func (stage *preparedHistory) removeImages() error {
	var result error
	for _, entry := range stage.images {
		var actual unix.Stat_t
		if unix.Fstatat(stage.context, entry.name, &actual, unix.AT_SYMLINK_NOFOLLOW) != nil || actual.Dev != entry.identity.Dev || actual.Ino != entry.identity.Ino || actual.Mode&unix.S_IFMT != unix.S_IFREG {
			result = errors.Join(result, ErrContract)
			continue
		}
		if unix.Unlinkat(stage.context, entry.name, 0) != nil {
			result = errors.Join(result, ErrContract)
		}
	}
	stage.images = nil
	return result
}
