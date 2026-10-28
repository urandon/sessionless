package attachedresourcegeneration

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"golang.org/x/sys/unix"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

type registryConfigV1 struct {
	Version   uint32                                        `json:"version"`
	Baselines []domain.AttachedResourceGenerationBaselineV1 `json:"baselines"`
}

// DecodeRegistryConfigV1 installs no runtime authority or mutable source. Exact
// nested keys are checked beyond encoding/json's case-insensitive matching.
func DecodeRegistryConfigV1(raw []byte) (*Registry, error) {
	if strictjson.ValidateDocument(raw, MaxConfigBytesV1, 16, 512) != nil {
		return nil, ErrInvalidConfig
	}
	var config registryConfigV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF || config.Version != 1 {
		return nil, ErrInvalidConfig
	}
	canonical, err := json.Marshal(config)
	if err != nil || len(canonical) > MaxConfigBytesV1 {
		return nil, ErrInvalidConfig
	}
	// Preserve JsonDocument-compatible whitespace/member order while rejecting
	// aliases, omitted required fields and null-as-default, including nested
	// existing provider/evidence records. Optional omitempty fields may be
	// absent; explicit null does not silently become absence.
	var originalShape, canonicalShape any
	for _, item := range []struct {
		raw    []byte
		target *any
	}{{raw, &originalShape}, {canonical, &canonicalShape}} {
		d := json.NewDecoder(bytes.NewReader(item.raw))
		d.UseNumber()
		if d.Decode(item.target) != nil {
			return nil, ErrInvalidConfig
		}
	}
	if !reflect.DeepEqual(originalShape, canonicalShape) {
		return nil, ErrInvalidConfig
	}
	return NewRegistry(config.Baselines)
}

// LoadRegistryFromEnv reads only the two exact operator startup settings.
// Disabled configuration performs no file I/O. There is no inline-JSON alias,
// live reload, remote baseline write, credential read or fallback.
func LoadRegistryFromEnv(getenv func(string) string) (*Registry, error) {
	if getenv == nil {
		return nil, ErrInvalidConfig
	}
	flag := getenv("ATTACHED_RESOURCE_GENERATION_ENABLED")
	path := getenv("ATTACHED_RESOURCE_GENERATION_CONFIG_FILE")
	switch flag {
	case "", "false":
		if path != "" {
			return nil, ErrInvalidConfig
		}
		return nil, nil
	case "true":
	default:
		return nil, ErrInvalidConfig
	}
	if len(path) == 0 || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.TrimSpace(path) != path {
		return nil, ErrInvalidConfig
	}
	// Nonblocking open lets Fstat reject a FIFO without waiting for a writer.
	// NOFOLLOW pins the final configured file, not a symlink target. Metadata
	// may be root-owned/read-only; it need not use private credential modes.
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || info.Size() <= 0 || info.Size() > MaxConfigBytesV1 {
		return nil, ErrInvalidConfig
	}
	raw, err := io.ReadAll(io.LimitReader(file, MaxConfigBytesV1+1))
	if err != nil || len(raw) == 0 || len(raw) > MaxConfigBytesV1 {
		return nil, ErrInvalidConfig
	}
	return DecodeRegistryConfigV1(raw)
}
