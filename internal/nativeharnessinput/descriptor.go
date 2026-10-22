// Package nativeharnessinput defines bounded native V3 input integrity data.
// A descriptor is a value, not admission, original-inode or mount authority.
package nativeharnessinput

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
)

const (
	MaxDescriptorBytes        = 4096
	MaxFileBytes       uint64 = 1 << 20
)

var ErrDescriptor = errors.New("invalid native execution descriptor v3")
var ErrMounted = errors.New("native mounted input verification failed")

type FileV3 struct {
	Size   uint64 `json:"size"`
	SHA256 string `json:"sha256"`
}

type ImageV3 struct {
	Ordinal uint32 `json:"ordinal"`
	Size    uint64 `json:"size"`
	SHA256  string `json:"sha256"`
}

// NativeExecutionDescriptorV3 contains no user-selected path. The variant fixes
// the exact ordered PNG/JPEG suffixes; scope and installation stay distinct.
type NativeExecutionDescriptorV3 struct {
	Version             uint32    `json:"version"`
	ScopeManifestDigest string    `json:"scope_manifest_sha256"`
	InstallDigest       string    `json:"install_sha256"`
	Variant             uint32    `json:"variant"`
	Images              []ImageV3 `json:"images"`
	Catalog             FileV3    `json:"catalog"`
	Schema              FileV3    `json:"schema"`
}

// SlotV3 is a derived copy. Profile slots have ordinal zero. RelativePath is
// relative to a root representing the workload filesystem, not /sessionless.
type SlotV3 struct {
	RelativePath, Path string
	Ordinal            uint32
	Size               uint64
	SHA256             string
}

var variants = [...][]string{
	{}, {"png"}, {"jpg"}, {"png", "png"}, {"png", "jpg"}, {"jpg", "png"}, {"jpg", "jpg"},
}

func validHash(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	raw, err := hex.DecodeString(s)
	return err == nil && hex.EncodeToString(raw) == s && !bytes.Equal(raw, make([]byte, sha256.Size))
}

func validFile(size uint64, hash string) bool {
	return size > 0 && size <= MaxFileBytes && validHash(hash)
}

func Validate(d NativeExecutionDescriptorV3) error {
	if d.Version != 3 || !validHash(d.ScopeManifestDigest) || !validHash(d.InstallDigest) || d.Variant >= uint32(len(variants)) ||
		d.Images == nil || len(d.Images) != len(variants[d.Variant]) || !validFile(d.Catalog.Size, d.Catalog.SHA256) || !validFile(d.Schema.Size, d.Schema.SHA256) {
		return ErrDescriptor
	}
	for i, image := range d.Images {
		if image.Ordinal != uint32(i+1) || !validFile(image.Size, image.SHA256) {
			return ErrDescriptor
		}
	}
	return nil
}

func Marshal(d NativeExecutionDescriptorV3) ([]byte, error) {
	if Validate(d) != nil {
		return nil, ErrDescriptor
	}
	raw, err := json.Marshal(d)
	if err != nil || len(raw) > MaxDescriptorBytes {
		return nil, ErrDescriptor
	}
	return raw, nil
}

// Parse accepts exactly Marshal's encoding: duplicate/unknown/missing fields,
// whitespace, reordered fields, escaped alternatives and noncanonical numbers
// are rejected, rather than normalized into an accepted carrier.
func Parse(raw []byte) (NativeExecutionDescriptorV3, error) {
	var d NativeExecutionDescriptorV3
	if len(raw) == 0 || len(raw) > MaxDescriptorBytes {
		return d, ErrDescriptor
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&d) != nil {
		return NativeExecutionDescriptorV3{}, ErrDescriptor
	}
	if decoder.Decode(new(any)) != io.EOF {
		return NativeExecutionDescriptorV3{}, ErrDescriptor
	}
	canonical, err := Marshal(d)
	if err != nil || !bytes.Equal(canonical, raw) {
		return NativeExecutionDescriptorV3{}, ErrDescriptor
	}
	return d, nil
}

func Slots(d NativeExecutionDescriptorV3) ([]SlotV3, error) {
	if Validate(d) != nil {
		return nil, ErrDescriptor
	}
	result := []SlotV3{
		{RelativePath: "sessionless/profile/models.json", Path: "/sessionless/profile/models.json", Size: d.Catalog.Size, SHA256: d.Catalog.SHA256},
		{RelativePath: "sessionless/profile/output.schema.json", Path: "/sessionless/profile/output.schema.json", Size: d.Schema.Size, SHA256: d.Schema.SHA256},
	}
	for i, suffix := range variants[d.Variant] {
		relative := "sessionless/input/image-0" + string(rune('1'+i)) + "." + suffix
		image := d.Images[i]
		result = append(result, SlotV3{RelativePath: relative, Path: "/" + relative, Ordinal: image.Ordinal, Size: image.Size, SHA256: image.SHA256})
	}
	return result, nil
}
