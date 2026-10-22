package nativeharnessinput

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func testHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func testDescriptor(variant uint32) NativeExecutionDescriptorV3 {
	d := NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: testHash("scope"), InstallDigest: testHash("install"), Variant: variant,
		Images: []ImageV3{}, Catalog: FileV3{Size: 7, SHA256: testHash("catalog")}, Schema: FileV3{Size: 6, SHA256: testHash("schema")}}
	if variant < uint32(len(variants)) {
		for i := range variants[variant] {
			d.Images = append(d.Images, ImageV3{Ordinal: uint32(i + 1), Size: 5, SHA256: testHash("image")})
		}
	}
	return d
}

func TestDescriptorSevenVariants(t *testing.T) {
	want := [][]string{{}, {"image-01.png"}, {"image-01.jpg"}, {"image-01.png", "image-02.png"}, {"image-01.png", "image-02.jpg"}, {"image-01.jpg", "image-02.png"}, {"image-01.jpg", "image-02.jpg"}}
	for variant, images := range want {
		t.Run(string(rune('0'+variant)), func(t *testing.T) {
			d := testDescriptor(uint32(variant))
			raw, err := Marshal(d)
			if err != nil {
				t.Fatalf("marshal variant %d: %v", variant, err)
			}
			parsed, err := Parse(raw)
			if err != nil {
				t.Fatalf("parse variant %d: %v", variant, err)
			}
			repeated, err := Marshal(parsed)
			if err != nil || !bytes.Equal(raw, repeated) {
				t.Fatalf("nonstable canonical roundtrip: %s: %v", raw, err)
			}
			slots, err := Slots(parsed)
			if err != nil || len(slots) != 2+len(images) {
				t.Fatalf("slots=%+v error=%v", slots, err)
			}
			if slots[0].Path != "/sessionless/profile/models.json" || slots[1].Path != "/sessionless/profile/output.schema.json" || slots[0].Ordinal != 0 || slots[1].Ordinal != 0 {
				t.Fatalf("wrong profile slots: %+v", slots)
			}
			for i, name := range images {
				if slots[i+2].Path != "/sessionless/input/"+name || slots[i+2].RelativePath != "sessionless/input/"+name || slots[i+2].Ordinal != uint32(i+1) {
					t.Errorf("image slot[%d]=%+v want=%q", i, slots[i+2], name)
				}
			}
			slots[0].Path = "caller mutation"
			fresh, _ := Slots(parsed)
			if fresh[0].Path != "/sessionless/profile/models.json" {
				t.Fatal("returned slots alias another derivation")
			}
		})
	}
}

func TestImagePathsAreFixedFreshAndBounded(t *testing.T) {
	for variant := uint32(0); variant < 7; variant++ {
		paths, err := ImagePaths(variant)
		if err != nil {
			t.Fatalf("variant%d: %v", variant, err)
		}
		slots, err := Slots(testDescriptor(variant))
		if err != nil || len(paths) != len(slots)-2 {
			t.Fatalf("paths=%q slots=%+v error=%v", paths, slots, err)
		}
		for i, path := range paths {
			if path != slots[i+2].Path {
				t.Fatalf("variant%d path%d=%q slot=%q", variant, i, path, slots[i+2].Path)
			}
		}
		if len(paths) != 0 {
			original := paths[0]
			paths[0] = "/caller-mutated"
			fresh, _ := ImagePaths(variant)
			if fresh[0] != original {
				t.Fatal("image path derivation aliases caller slice")
			}
		}
	}
	for _, variant := range []uint32{7, ^uint32(0)} {
		if _, err := ImagePaths(variant); !errors.Is(err, ErrDescriptor) {
			t.Fatalf("invalid variant%d: %v", variant, err)
		}
	}
}

func TestDescriptorRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*NativeExecutionDescriptorV3)
	}{
		{"version", func(d *NativeExecutionDescriptorV3) { d.Version = 2 }},
		{"scope-zero", func(d *NativeExecutionDescriptorV3) { d.ScopeManifestDigest = strings.Repeat("0", 64) }},
		{"install-upper", func(d *NativeExecutionDescriptorV3) { d.InstallDigest = strings.ToUpper(d.InstallDigest) }},
		{"install-short", func(d *NativeExecutionDescriptorV3) { d.InstallDigest = "a" }},
		{"scope-nonhex", func(d *NativeExecutionDescriptorV3) { d.ScopeManifestDigest = strings.Repeat("z", 64) }},
		{"variant", func(d *NativeExecutionDescriptorV3) { d.Variant = 7 }},
		{"images-nil", func(d *NativeExecutionDescriptorV3) { d.Images = nil }},
		{"images-missing", func(d *NativeExecutionDescriptorV3) { d.Images = d.Images[:1] }},
		{"images-extra", func(d *NativeExecutionDescriptorV3) { d.Images = append(d.Images, d.Images[1]) }},
		{"images-order", func(d *NativeExecutionDescriptorV3) { d.Images[0], d.Images[1] = d.Images[1], d.Images[0] }},
		{"images-ordinal-zero", func(d *NativeExecutionDescriptorV3) { d.Images[0].Ordinal = 0 }},
		{"image-size-zero", func(d *NativeExecutionDescriptorV3) { d.Images[0].Size = 0 }},
		{"image-size-over", func(d *NativeExecutionDescriptorV3) { d.Images[0].Size = MaxFileBytes + 1 }},
		{"image-size-overflow", func(d *NativeExecutionDescriptorV3) { d.Images[0].Size = ^uint64(0) }},
		{"image-zero-hash", func(d *NativeExecutionDescriptorV3) { d.Images[0].SHA256 = strings.Repeat("0", 64) }},
		{"catalog-size-zero", func(d *NativeExecutionDescriptorV3) { d.Catalog.Size = 0 }},
		{"catalog-over", func(d *NativeExecutionDescriptorV3) { d.Catalog.Size = MaxFileBytes + 1 }},
		{"schema-over", func(d *NativeExecutionDescriptorV3) { d.Schema.Size = MaxFileBytes + 1 }},
		{"schema-missing-hash", func(d *NativeExecutionDescriptorV3) { d.Schema.SHA256 = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := testDescriptor(4)
			tc.mutate(&d)
			if !errors.Is(Validate(d), ErrDescriptor) {
				t.Fatalf("validate accepted %+v", d)
			}
			if _, err := Marshal(d); !errors.Is(err, ErrDescriptor) {
				t.Fatalf("marshal accepted invalid descriptor: %v", err)
			}
			if _, err := Slots(d); !errors.Is(err, ErrDescriptor) {
				t.Fatalf("slots accepted invalid descriptor: %v", err)
			}
			raw, _ := json.Marshal(d)
			if _, err := Parse(raw); !errors.Is(err, ErrDescriptor) {
				t.Fatalf("parse accepted invalid descriptor %s: %v", raw, err)
			}
		})
	}
	for variant := uint32(0); variant < 7; variant++ {
		d := testDescriptor(variant)
		d.Catalog.Size = MaxFileBytes
		d.Schema.Size = MaxFileBytes
		for i := range d.Images {
			d.Images[i].Size = MaxFileBytes
		}
		if _, err := Marshal(d); err != nil {
			t.Fatalf("valid inclusive size boundary variant%d: %v", variant, err)
		}
	}
}

func TestDescriptorRejectsNoncanonicalCarrier(t *testing.T) {
	raw, err := Marshal(testDescriptor(4))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty": nil, "oversize": bytes.Repeat([]byte(" "), MaxDescriptorBytes+1),
		"leading-space": append([]byte(" "), raw...), "trailing-newline": append(append([]byte(nil), raw...), '\n'),
		"duplicate":        bytes.Replace(raw, []byte(`"version":3`), []byte(`"version":3,"version":3`), 1),
		"unknown":          bytes.Replace(raw, []byte(`"version":3`), []byte(`"version":3,"host_path":"/tmp/foreign"`), 1),
		"nested-duplicate": bytes.Replace(raw, []byte(`"ordinal":1`), []byte(`"ordinal":1,"ordinal":1`), 1),
		"nested-unknown":   bytes.Replace(raw, []byte(`"ordinal":1`), []byte(`"ordinal":1,"path":"/foreign"`), 1),
		"missing-version":  bytes.Replace(raw, []byte(`"version":3,`), nil, 1),
		"missing-variant":  bytes.Replace(raw, []byte(`"variant":4,`), nil, 1),
		"field-order": bytes.Replace(raw,
			[]byte(`"version":3,"scope_manifest_sha256":"`+testHash("scope")+`"`),
			[]byte(`"scope_manifest_sha256":"`+testHash("scope")+`","version":3`), 1),
		"number-exponent": bytes.Replace(raw, []byte(`"size":5`), []byte(`"size":5e0`), 1),
		"number-fraction": bytes.Replace(raw, []byte(`"size":5`), []byte(`"size":5.0`), 1),
		"number-negative": bytes.Replace(raw, []byte(`"size":5`), []byte(`"size":-1`), 1),
		"number-overflow": bytes.Replace(raw, []byte(`"size":5`), []byte(`"size":18446744073709551616`), 1),
		"null":            []byte(`null`), "extra-value": append(append([]byte(nil), raw...), []byte(`{}`)...),
		"escaped-name": bytes.Replace(raw, []byte(`"version"`), []byte(`"versi\u006fn"`), 1),
	}
	zero, _ := Marshal(testDescriptor(0))
	cases["missing-zero-variant"] = bytes.Replace(zero, []byte(`"variant":0,`), nil, 1)
	cases["null-images"] = bytes.Replace(zero, []byte(`"images":[]`), []byte(`"images":null`), 1)
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(body); !errors.Is(err, ErrDescriptor) {
				t.Fatalf("accepted %q: %v", body, err)
			}
		})
	}
}

func FuzzDescriptorCanonical(f *testing.F) {
	for v := uint32(0); v < 7; v++ {
		raw, _ := Marshal(testDescriptor(v))
		f.Add(raw)
	}
	f.Add([]byte(`{"version":3,"version":3}`))
	f.Add([]byte(`null`))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		d, err := Parse(raw)
		if err != nil {
			return
		}
		canonical, err := Marshal(d)
		if err != nil || !bytes.Equal(raw, canonical) {
			t.Fatalf("accepted noncanonical input: %q error=%v", raw, err)
		}
	})
}
