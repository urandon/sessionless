package codexexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// These output ceilings are distinct from the larger input-file and raw
// protocol ceilings. Keep them sealed in the immutable native profile.
const (
	nativeFinalBytesV2    = 128 << 10
	nativeAnswerBytesV2   = 32 << 10
	nativeArtifactBytesV2 = 32 << 10
	nativeArtifactTotalV2 = 64 << 10
	nativeArtifactCountV2 = 2
)

// Only the original completed boundary mints this private semantic carrier.
// Public output DTOs and paths are not provider-producer evidence.
type nativeStructuredFinalV2 struct {
	self          *nativeStructuredFinalV2
	original      *preparedObservation
	profileDigest string
	schema        nativeharnessinput.FileV3
	rawDigest     [32]byte
	answer        string
	artifacts     []nativeStructuredArtifactV2
}

type nativeStructuredArtifactV2 struct {
	name, mediaType string
	content         []byte
}

func (f *nativeStructuredFinalV2) clear() {
	if f != nil {
		for i := range f.artifacts {
			clear(f.artifacts[i].content)
		}
		f.artifacts, f.answer = nil, ""
	}
}

func (boundary *PreparedProcessBoundaryV1) structuredNativeFinalV2(original *preparedObservation, raw []byte) (*nativeStructuredFinalV2, error) {
	if boundary == nil || boundary.nativeIdentity != boundary || boundary.native == nil || boundary.native.profileV2 == nil ||
		original == nil || original.self != original || original.boundary != boundary || original.native != boundary.native || original.authority != boundary.authority ||
		original.binding.Backend != boundary.native.profileV2.descriptor || original.runErr != nil || original.process.ExitCode != 0 || original.process.FailureCode != "" ||
		original.process.Cancelled || original.process.Deadline || !original.process.DescendantsReaped || !original.process.BoundaryReleased || !original.process.CleanupSucceeded ||
		!original.protocol.terminal || original.protocol.protocolDrift || original.protocol.terminalDrift || original.protocol.failedTerminal ||
		len(raw) == 0 || len(raw) > nativeFinalBytesV2 || !utf8.Valid(raw) || original.rawFinalDigestV2 != sha256.Sum256(raw) ||
		strictjson.ValidateDocument(raw, nativeFinalBytesV2, 32, nativeFinalBytesV2) != nil {
		return nil, ErrContract
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 {
		return nil, ErrContract
	}
	for key := range fields {
		if key != "answer" && key != "artifacts" {
			return nil, ErrContract
		}
	}
	var answer string
	if json.Unmarshal(fields["answer"], &answer) != nil || len(answer) > nativeAnswerBytesV2 || !validMediaText([]byte(answer)) || strings.TrimSpace(answer) == "" || utf8.RuneCountInString(answer) > 32_000 {
		return nil, ErrContract
	}
	f := &nativeStructuredFinalV2{original: original, profileDigest: boundary.native.profileV2.descriptor.BackendProfileDigest,
		schema: boundary.native.profileV2.launch.Schema, rawDigest: original.rawFinalDigestV2, answer: answer}
	f.self = f
	if artifacts, exists := fields["artifacts"]; exists {
		total := 0
		decoder := json.NewDecoder(bytes.NewReader(artifacts))
		first, err := decoder.Token()
		if err != nil || first != json.Delim('[') {
			return nil, ErrContract
		}
		for decoder.More() {
			if len(f.artifacts) >= nativeArtifactCountV2 {
				f.clear()
				return nil, ErrContract
			}
			var item map[string]json.RawMessage
			if decoder.Decode(&item) != nil || len(item) != 3 {
				f.clear()
				return nil, ErrContract
			}
			for key := range item {
				if key != "name" && key != "media_type" && key != "content" {
					f.clear()
					return nil, ErrContract
				}
			}
			var name, mediaType, content string
			if json.Unmarshal(item["name"], &name) != nil || json.Unmarshal(item["media_type"], &mediaType) != nil || json.Unmarshal(item["content"], &content) != nil ||
				!nativeArtifactNameV2(name) || (mediaType != "text/plain" && mediaType != "application/json") || !validMediaText([]byte(content)) ||
				len(content) == 0 || len(content) > nativeArtifactBytesV2 || total > nativeArtifactTotalV2-len(content) || mediaType == "application/json" && !nativeArtifactJSONV2([]byte(content)) {
				f.clear()
				return nil, ErrContract
			}
			for _, prior := range f.artifacts {
				if strings.EqualFold(name, prior.name) {
					f.clear()
					return nil, ErrContract
				}
			}
			f.artifacts = append(f.artifacts, nativeStructuredArtifactV2{name: name, mediaType: mediaType, content: []byte(content)})
			total += len(content)
		}
		last, err := decoder.Token()
		if err != nil || last != json.Delim(']') {
			f.clear()
			return nil, ErrContract
		}
		if _, err := decoder.Token(); err != io.EOF {
			f.clear()
			return nil, ErrContract
		}
	}
	return f, nil
}

func nativeArtifactJSONV2(body []byte) bool {
	return strictjson.ValidateDocument(body, nativeArtifactBytesV2, 32, nativeArtifactBytesV2) == nil
}

func nativeArtifactNameV2(name string) bool {
	if name == "" || len(name) > 255 || name == "." || name == ".." || !utf8.ValidString(name) || strings.TrimSpace(name) != name || strings.ContainsAny(name, "/\\\x00") {
		return false
	}
	for _, r := range name {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
