package codexexec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"reflect"
	"strings"
	"testing"
)

func mediaImageFixture(t *testing.T, kind string, width, height int) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetNRGBA(x, y, color.NRGBA{R: 240, G: 30, B: 10, A: 255})
		}
	}
	var output bytes.Buffer
	var err error
	if kind == "image/png" {
		err = png.Encode(&output, img)
	} else {
		err = jpeg.Encode(&output, img, &jpeg.Options{Quality: 85})
	}
	if err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestMediaFullImageValidation(t *testing.T) {
	for _, tc := range []struct {
		name, kind    string
		width, height int
	}{
		{"small-png", "image/png", 40, 20}, {"small-jpeg", "image/jpeg", 40, 20}, {"native-resize-required", "image/png", 3000, 1000}, {"maximum-pixels", "image/png", 2000, 2000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := mediaImageFixture(t, tc.kind, tc.width, tc.height)
			original := bytes.Clone(body)
			width, height, err := validateMediaImage(tc.kind, body)
			if err != nil || width != tc.width || height != tc.height || !bytes.Equal(body, original) {
				t.Fatalf("original validation width=%d height=%d err=%v original-mutated=%t", width, height, err, !bytes.Equal(body, original))
			}
		})
	}
}

func pngChunk(kind string, body []byte) []byte {
	result := make([]byte, len(body)+12)
	binary.BigEndian.PutUint32(result, uint32(len(body)))
	copy(result[4:], kind)
	copy(result[8:], body)
	binary.BigEndian.PutUint32(result[len(result)-4:], crc32.ChecksumIEEE(result[4:len(result)-4]))
	return result
}

func TestMediaImageInvalidInput(t *testing.T) {
	pngBody := mediaImageFixture(t, "image/png", 20, 10)
	jpegBody := mediaImageFixture(t, "image/jpeg", 20, 10)
	corrupt := bytes.Clone(pngBody)
	corrupt[20] ^= 1
	animated := append(bytes.Clone(pngBody[:33]), pngChunk("acTL", make([]byte, 8))...)
	animated = append(animated, pngBody[33:]...)
	// A CRC-correct declared image bomb is rejected before full Decode.
	bomb := bytes.Clone(pngBody)
	binary.BigEndian.PutUint32(bomb[16:20], 4_000_001)
	binary.BigEndian.PutUint32(bomb[29:33], crc32.ChecksumIEEE(bomb[12:29]))
	truncated := bytes.Clone(pngBody[:len(pngBody)-12])
	truncated = append(truncated, pngChunk("IEND", nil)...)
	truncated[45] ^= 1
	for _, tc := range []struct {
		name, kind string
		body       []byte
	}{
		{"signature-mismatch", "image/png", jpegBody}, {"signature-mismatch-jpeg", "image/jpeg", pngBody}, {"crc", "image/png", corrupt}, {"animated", "image/png", animated},
		{"dimension-bomb", "image/png", bomb}, {"full-codec-corrupt", "image/png", truncated}, {"png-trailing", "image/png", append(bytes.Clone(pngBody), 0)},
		{"jpeg-trailing", "image/jpeg", append(bytes.Clone(jpegBody), 0)}, {"jpeg-truncated", "image/jpeg", jpegBody[:len(jpegBody)-3]},
		{"unsupported", "image/svg+xml", pngBody}, {"one-byte-compressed-over", "image/png", make([]byte, mediaImageBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := validateMediaImage(tc.kind, tc.body); err == nil {
				t.Fatal("invalid input reached full accepted image")
			}
		})
	}
}

func TestMediaProspectiveJSONFraming(t *testing.T) {
	for _, text := range []string{"ascii", `"quoted"`, "<tag>&\n\t\x00", "кириллица\u2028\u2029"} {
		value := struct {
			Text    string
			Ordinal int
			Empty   string `json:"empty,omitempty"`
		}{Text: text, Ordinal: 2}
		size, ok := mediaJSONSize(reflect.ValueOf(value), 0)
		encoded, err := json.Marshal(value)
		if !ok || err != nil || size != len(encoded) {
			t.Fatalf("prospective size=%d ok=%t actual=%d err=%v", size, ok, len(encoded), err)
		}
	}
	if _, ok := mediaJSONSize(reflect.ValueOf(map[string]string{"unknown": "shape"}), 0); ok {
		t.Fatal("unknown shape estimated")
	}
}

func TestMediaTextValidationPreservesOriginalJSONTokens(t *testing.T) {
	original := []byte(` { "huge": 90071992547409931234567890, "exponent": 1.0000000000000000001e+99 } `)
	if !validMediaText(original) || !json.Valid(original) {
		t.Fatal("valid numeric JSON rejected")
	}
	encoded, err := json.Marshal(struct {
		Content string `json:"content"`
	}{string(original)})
	var decoded struct {
		Content string `json:"content"`
	}
	if err != nil || json.Unmarshal(encoded, &decoded) != nil || decoded.Content != string(original) {
		t.Fatal("original numeric tokens changed")
	}
	for _, text := range [][]byte{{0xff}, []byte("nul\x00"), []byte(strings.Repeat("x", mediaTextBytes+1))} {
		if len(text) <= mediaTextBytes && validMediaText(text) {
			t.Fatalf("invalid text accepted %x", text)
		}
	}
}
