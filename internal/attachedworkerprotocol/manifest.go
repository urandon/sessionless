package attachedworkerprotocol

import (
	"crypto/sha256"
	"encoding/binary"
)

const manifestDigestDomainV1 = "sessionless.attached-worker.capability-manifest.v1"

type IsolationEvidenceV1 string

const (
	IsolationProcessBoundary    IsolationEvidenceV1 = "process_boundary"
	IsolationFilesystemBoundary IsolationEvidenceV1 = "filesystem_boundary"
	IsolationNetworkBoundary    IsolationEvidenceV1 = "network_boundary"
)

type ProtocolFeatureV1 string

const (
	FeatureCancellation ProtocolFeatureV1 = "cancellation_v1"
	FeatureProgress     ProtocolFeatureV1 = "progress_v1"
	FeatureReconnect    ProtocolFeatureV1 = "reconnect_v1"
)

type HarnessSurfaceV1 string

const HarnessSurfaceSessionTurn HarnessSurfaceV1 = "session_turn_v1"

type CapabilityManifestV1 struct {
	WorkerID                string                `json:"worker_id"`
	EnrollmentGeneration    uint64                `json:"enrollment_generation"`
	Revision                uint64                `json:"revision"`
	ProtocolOffer           VersionOfferV1        `json:"protocol_offer"`
	OperatingSystem         string                `json:"operating_system"`
	Architecture            string                `json:"architecture"`
	BuildID                 string                `json:"build_id"`
	HarnessName             string                `json:"harness_name"`
	HarnessVersion          string                `json:"harness_version"`
	HarnessSurface          HarnessSurfaceV1      `json:"harness_surface"`
	HarnessExecutableDigest []byte                `json:"harness_executable_digest"`
	IsolationEvidence       []IsolationEvidenceV1 `json:"isolation_evidence"`
	Features                []ProtocolFeatureV1   `json:"features"`
	MaxConcurrentAttempts   uint32                `json:"max_concurrent_attempts"`
}

func (manifest CapabilityManifestV1) Validate() error {
	if !validOpaque(manifest.WorkerID) || manifest.EnrollmentGeneration == 0 || manifest.Revision == 0 ||
		manifest.ProtocolOffer.Validate() != nil || !validOpaque(manifest.OperatingSystem) ||
		!validOpaque(manifest.Architecture) || !validOpaque(manifest.BuildID) ||
		!validOpaque(manifest.HarnessName) || !validOpaque(manifest.HarnessVersion) ||
		manifest.HarnessSurface != HarnessSurfaceSessionTurn ||
		!validBytes(manifest.HarnessExecutableDigest, sha256.Size) || manifest.MaxConcurrentAttempts != 1 ||
		len(manifest.IsolationEvidence) == 0 || len(manifest.IsolationEvidence) > 8 ||
		len(manifest.Features) == 0 || len(manifest.Features) > 8 {
		return protocolError(ErrorInvalidFrame)
	}
	previousIsolation := IsolationEvidenceV1("")
	for _, evidence := range manifest.IsolationEvidence {
		if evidence != IsolationFilesystemBoundary && evidence != IsolationNetworkBoundary && evidence != IsolationProcessBoundary {
			return protocolError(ErrorInvalidFrame)
		}
		if previousIsolation != "" && evidence <= previousIsolation {
			return protocolError(ErrorInvalidFrame)
		}
		previousIsolation = evidence
	}
	previousFeature := ProtocolFeatureV1("")
	for _, feature := range manifest.Features {
		if feature != FeatureCancellation && feature != FeatureProgress && feature != FeatureReconnect {
			return protocolError(ErrorInvalidFrame)
		}
		if previousFeature != "" && feature <= previousFeature {
			return protocolError(ErrorInvalidFrame)
		}
		previousFeature = feature
	}
	return nil
}

// CanonicalManifestBytesV1 uses explicit length prefixes and fixed-width
// integers. JSON representation is never part of the digest contract.
func CanonicalManifestBytesV1(manifest CapabilityManifestV1) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	result := make([]byte, 0, 512)
	result = appendCanonicalField(result, []byte(manifestDigestDomainV1))
	result = appendCanonicalField(result, []byte(manifest.WorkerID))
	result = appendCanonicalUint64(result, manifest.EnrollmentGeneration)
	result = appendCanonicalUint64(result, manifest.Revision)
	result = appendVersionOffer(result, manifest.ProtocolOffer)
	result = appendCanonicalField(result, []byte(manifest.OperatingSystem))
	result = appendCanonicalField(result, []byte(manifest.Architecture))
	result = appendCanonicalField(result, []byte(manifest.BuildID))
	result = appendCanonicalField(result, []byte(manifest.HarnessName))
	result = appendCanonicalField(result, []byte(manifest.HarnessVersion))
	result = appendCanonicalField(result, []byte(manifest.HarnessSurface))
	result = appendCanonicalField(result, manifest.HarnessExecutableDigest)
	result = appendCanonicalUint32(result, manifest.MaxConcurrentAttempts)
	result = appendCanonicalUint32(result, uint32(len(manifest.IsolationEvidence)))
	for _, evidence := range manifest.IsolationEvidence {
		result = appendCanonicalField(result, []byte(evidence))
	}
	result = appendCanonicalUint32(result, uint32(len(manifest.Features)))
	for _, feature := range manifest.Features {
		result = appendCanonicalField(result, []byte(feature))
	}
	return result, nil
}

func ManifestDigestV1(manifest CapabilityManifestV1) ([]byte, error) {
	canonical, err := CanonicalManifestBytesV1(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(canonical)
	return append([]byte(nil), digest[:]...), nil
}

func appendCanonicalField(destination, value []byte) []byte {
	destination = appendCanonicalUint32(destination, uint32(len(value)))
	return append(destination, value...)
}

func appendCanonicalUint32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendCanonicalUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}
