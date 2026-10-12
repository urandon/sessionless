//go:build ydbintegration

package codexexec

import "encoding/hex"

// NewSyntheticMediaFixtureConfigV1 exists only in the repository's explicit
// test-provider build. It selects a distinct synthetic descriptor, not an
// eligible CODEX-EXEC-MEDIA released artifact. Fake executable observation
// proves staging/argv/lifecycle plumbing, never vision or native send policing.
func NewSyntheticMediaFixtureConfigV1(backend Config) (MediaConfigV1, error) {
	if backend.Model != "sessionless-media-fixture" {
		return MediaConfigV1{}, ErrContract
	}
	if _, err := prepareConfig(backend); err != nil {
		return MediaConfigV1{}, err
	}
	return MediaConfigV1{Backend: backend, fixture: true, Native: NativeMediaPolicyV1{
		GuardProtocol: "sessionless.synthetic-native-media.v1", GuardArtifactDigest: hex.EncodeToString(backend.ExecutableDigest[:]),
		Configuration: []byte(`{"fixture":"staging-and-lifecycle-only"}`), ModelMetadata: []byte(`{"model":"sessionless-media-fixture","vision_evidence":"none"}`),
	}}, nil
}
