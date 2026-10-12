package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	AttachedPiSDKProfileIDV1        = "PI-SDK-MEDIA-2026-10-V1"
	AttachedPiSDKNativeProtocolV1   = "pi-sdk-standard-responses.v1"
	attachedPiSDKSourceRevisionV1   = "6c4f360264397c59801f6da2bdac13e3b1fcbe91"
	attachedPiSDKOpenAIIntegrityV1  = "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA=="
	attachedPiSDKEndpointMaxBytesV1 = 2048
	// Fixed producer: 88 keys (each <=64 ASCII bytes), 19 SHA256 leaves,
	// variable token bytes: Node/vendor/OS/arch 4*128, model256, endpoint2048.
	// Six bytes per UTF-8 byte covers JSON escaping; fixed source/behavior
	// strings and numbers fit the per-key 128-byte value/framing allowance.
	// This is metadata, never a native input/request/process resource ceiling.
	attachedPiSDKCanonicalMaxBytesV1 = 88*(64+128) + 19*64 + 6*(4*128+256+attachedPiSDKEndpointMaxBytesV1)
)

// AttachedPiSDKNativeProfileV1 is the sole canonical native P preimage.
// Validation/identity equality are structural, not executable conformance,
// current owner permission, spending authority or an enabled registration.
// No E/T/R/M, principal, resource or final binding/job backreference belongs here.
type AttachedPiSDKNativeProfileV1 struct {
	Version               uint32                        `json:"version"`
	ProfileID             string                        `json:"profile_id"`
	NativeProtocolVersion string                        `json:"native_protocol_version"`
	Sources               AttachedPiSDKNativeSourcesV1  `json:"sources"`
	Model                 AttachedPiSDKNativeModelV1    `json:"model"`
	Platform              AttachedPiSDKNativePlatformV1 `json:"platform"`
	Limits                AttachedPiSDKNativeLimitsV1   `json:"limits"`
	Behavior              AttachedPiSDKNativeBehaviorV1 `json:"behavior"`
}

type AttachedPiSDKNativeSourcesV1 struct {
	PiSourceRevision         string `json:"pi_source_revision"`
	PiPackageDigest          string `json:"pi_package_digest"`
	PiBuildDigest            string `json:"pi_build_digest"`
	PiDependencyLockDigest   string `json:"pi_dependency_lock_digest"`
	PiGeneratedCatalogDigest string `json:"pi_generated_catalog_digest"`
	OpenAISDKVersion         string `json:"open_ai_sdk_version"`
	OpenAISDKIntegrity       string `json:"open_ai_sdk_integrity"`
	OpenAISDKPackageDigest   string `json:"open_ai_sdk_package_digest"`
	NodeVersion              string `json:"node_version"`
	NodeSourceDigest         string `json:"node_source_digest"`
	NodeExecutableDigest     string `json:"node_executable_digest"`
	NativeArtifactDigest     string `json:"native_artifact_digest"`
	BootstrapDigest          string `json:"bootstrap_digest"`
	StreamWrapperDigest      string `json:"stream_wrapper_digest"`
	PhysicalTransportDigest  string `json:"physical_transport_digest"`
	RequestSchemaDigest      string `json:"request_schema_digest"`
	FinalSchemaDigest        string `json:"final_schema_digest"`
}

type AttachedPiSDKNativeModelV1 struct {
	ModelVendorID           string `json:"model_vendor_id"`
	ModelID                 string `json:"model_id"`
	Endpoint                string `json:"endpoint"`
	VisionMetadataDigest    string `json:"vision_metadata_digest"`
	ContextAccountingDigest string `json:"context_accounting_digest"`
	ModelContextTokens      uint64 `json:"model_context_tokens"`
	MaxBilledInputTokens    uint64 `json:"max_billed_input_tokens"`
	MaxOutputTokens         uint64 `json:"max_output_tokens"`
}

type AttachedPiSDKNativePlatformV1 struct {
	OS                    string `json:"os"`
	Architecture          string `json:"architecture"`
	RuntimePolicyDigest   string `json:"runtime_policy_digest"`
	IsolationPolicyDigest string `json:"isolation_policy_digest"`
	EgressPolicyDigest    string `json:"egress_policy_digest"`
	CleanupPolicyDigest   string `json:"cleanup_policy_digest"`
}

type AttachedPiSDKNativeLimitsV1 struct {
	CanonicalInputBytes     uint64 `json:"canonical_input_bytes"`
	NativeRequestBytes      uint64 `json:"native_request_bytes"`
	PreSendAllocationBytes  uint64 `json:"pre_send_allocation_bytes"`
	RequestHeaderBytes      uint64 `json:"request_header_bytes"`
	ResponseHeaderBytes     uint64 `json:"response_header_bytes"`
	ResponseBodyBytes       uint64 `json:"response_body_bytes"`
	ResponseChunkBytes      uint64 `json:"response_chunk_bytes"`
	SSELineBytes            uint64 `json:"sse_line_bytes"`
	SSEEventBytes           uint64 `json:"sse_event_bytes"`
	SSEEvents               uint64 `json:"sse_events"`
	JSONNodes               uint64 `json:"json_nodes"`
	ResponseCopyWorkBytes   uint64 `json:"response_copy_work_bytes"`
	ResponseAllocationBytes uint64 `json:"response_allocation_bytes"`
	ProcessMemoryBytes      uint64 `json:"process_memory_bytes"`
	ProcessDiskBytes        uint64 `json:"process_disk_bytes"`
	ProcessFrameBytes       uint64 `json:"process_frame_bytes"`
	ProcessStdoutBytes      uint64 `json:"process_stdout_bytes"`
	ProcessEvents           uint64 `json:"process_events"`
	JSONDepth               uint64 `json:"json_depth"`
	RawFinalBytes           uint64 `json:"raw_final_bytes"`
	StructuredFinalBytes    uint64 `json:"structured_final_bytes"`
	AnswerBytes             uint64 `json:"answer_bytes"`
	ArtifactBytes           uint64 `json:"artifact_bytes"`
	ArtifactTotalBytes      uint64 `json:"artifact_total_bytes"`
}

type AttachedPiSDKNativeBehaviorV1 struct {
	ProviderAPI       string `json:"provider_api"`
	ImageDerivation   string `json:"image_derivation"`
	ImageDetail       string `json:"image_detail"`
	Transport         string `json:"transport"`
	LocalToolRegistry string `json:"local_tool_registry"`
	MaxPhysicalSends  uint64 `json:"max_physical_sends"`
	SDKRetries        uint64 `json:"sdk_retries"`
	ProviderRetries   uint64 `json:"provider_retries"`
	SessionRetries    uint64 `json:"session_retries"`
	FallbackTransport bool   `json:"fallback_transport"`
	NativeCompaction  bool   `json:"native_compaction"`
	Discovery         bool   `json:"discovery"`
}

// This fixed inherited-media commitment is part of the SAME canonical P wire
// representation. It is not an optional second profile or runtime calibration.
type attachedPiSDKInheritedMediaV1 struct {
	TextCount               uint64 `json:"text_count"`
	TextBytes               uint64 `json:"text_bytes"`
	TextTotalBytes          uint64 `json:"text_total_bytes"`
	ImageCount              uint64 `json:"image_count"`
	ImageBytes              uint64 `json:"image_bytes"`
	ImageTotalBytes         uint64 `json:"image_total_bytes"`
	ImagePixels             uint64 `json:"image_pixels"`
	DecodedRGBABytes        uint64 `json:"decoded_rgba_bytes"`
	GoValidationMemoryBytes uint64 `json:"go_validation_memory_bytes"`
	ArtifactCount           uint64 `json:"artifact_count"`
	AnswerRunes             uint64 `json:"answer_runes"`
}

type attachedPiSDKCanonicalV1 struct {
	Profile        AttachedPiSDKNativeProfileV1  `json:"profile"`
	InheritedMedia attachedPiSDKInheritedMediaV1 `json:"inherited_media"`
}

func attachedPiSDKInheritedMedia() attachedPiSDKInheritedMediaV1 {
	return attachedPiSDKInheritedMediaV1{2, 128 << 10, 256 << 10, 2, 1 << 20, 2 << 20, 4_000_000, 32 << 20, 96 << 20, 2, 32000}
}

func (p AttachedPiSDKNativeProfileV1) Validate() error {
	if p.Version != 1 || p.ProfileID != AttachedPiSDKProfileIDV1 || p.NativeProtocolVersion != AttachedPiSDKNativeProtocolV1 {
		return attachedPiSDKProfileError("identity", "requires the closed version1 SDK media identity")
	}
	s := p.Sources
	if s.PiSourceRevision != attachedPiSDKSourceRevisionV1 || s.OpenAISDKVersion != "6.40.0" || s.OpenAISDKIntegrity != attachedPiSDKOpenAIIntegrityV1 {
		return attachedPiSDKProfileError("sources", "requires the exact accepted Pi/SDK source pins")
	}
	for _, item := range []struct{ name, digest string }{
		{"pi_package", s.PiPackageDigest}, {"pi_build", s.PiBuildDigest}, {"pi_lock", s.PiDependencyLockDigest}, {"pi_catalog", s.PiGeneratedCatalogDigest},
		{"sdk_package", s.OpenAISDKPackageDigest}, {"node_source", s.NodeSourceDigest}, {"node_executable", s.NodeExecutableDigest}, {"native_artifact", s.NativeArtifactDigest},
		{"bootstrap", s.BootstrapDigest}, {"stream_wrapper", s.StreamWrapperDigest}, {"physical_transport", s.PhysicalTransportDigest}, {"request_schema", s.RequestSchemaDigest}, {"final_schema", s.FinalSchemaDigest},
		{"vision_metadata", p.Model.VisionMetadataDigest}, {"context_accounting", p.Model.ContextAccountingDigest}, {"runtime_policy", p.Platform.RuntimePolicyDigest},
		{"isolation_policy", p.Platform.IsolationPolicyDigest}, {"egress_policy", p.Platform.EgressPolicyDigest}, {"cleanup_policy", p.Platform.CleanupPolicyDigest},
	} {
		if err := validateSHA256("attached_pi_sdk_profile."+item.name, item.digest); err != nil {
			return err
		}
		if item.digest == strings.Repeat("0", 64) {
			return attachedPiSDKProfileError(item.name, "must not be zero")
		}
	}
	for _, item := range []struct{ name, value string }{{"node_version", s.NodeVersion}, {"os", p.Platform.OS}, {"architecture", p.Platform.Architecture}} {
		if err := validateProviderToken("attached_pi_sdk_profile."+item.name, item.value, 128); err != nil {
			return err
		}
	}
	if err := validateProviderModel(p.Model.ModelVendorID, p.Model.ModelID); err != nil {
		return err
	}
	if err := validateProviderToken("attached_pi_sdk_profile.endpoint", p.Model.Endpoint, attachedPiSDKEndpointMaxBytesV1); err != nil {
		return err
	}
	u, err := url.Parse(p.Model.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(p.Model.Endpoint, "#") || u.Opaque != "" || u.String() != p.Model.Endpoint {
		return attachedPiSDKProfileError("endpoint", "requires exact absolute HTTPS without credentials, query or fragment")
	}
	m := p.Model
	if m.ModelContextTokens == 0 || m.MaxBilledInputTokens == 0 || m.MaxOutputTokens == 0 || m.MaxBilledInputTokens > math.MaxUint64-m.MaxOutputTokens || m.MaxBilledInputTokens+m.MaxOutputTokens > m.ModelContextTokens {
		return attachedPiSDKProfileError("context", "requires positive finite input/output inside declared model context")
	}
	b := p.Behavior
	if b.ProviderAPI != "openai-responses" || b.ImageDerivation != "NoTransformOriginal" || b.ImageDetail != "auto" || b.Transport != "http-sse" || b.LocalToolRegistry != "empty" || b.MaxPhysicalSends != 1 || b.SDKRetries != 0 || b.ProviderRetries != 0 || b.SessionRetries != 0 || b.FallbackTransport || b.NativeCompaction || b.Discovery {
		return attachedPiSDKProfileError("behavior", "requires the one-send original-image closed SDK lane")
	}
	l := p.Limits
	for _, value := range []uint64{l.CanonicalInputBytes, l.NativeRequestBytes, l.PreSendAllocationBytes, l.RequestHeaderBytes, l.ResponseHeaderBytes, l.ResponseBodyBytes, l.ResponseChunkBytes, l.SSELineBytes, l.SSEEventBytes, l.SSEEvents, l.JSONNodes, l.ResponseCopyWorkBytes, l.ResponseAllocationBytes, l.ProcessMemoryBytes, l.ProcessDiskBytes, l.ProcessFrameBytes, l.ProcessStdoutBytes, l.ProcessEvents, l.JSONDepth, l.RawFinalBytes, l.StructuredFinalBytes, l.AnswerBytes, l.ArtifactBytes, l.ArtifactTotalBytes} {
		if value == 0 {
			return attachedPiSDKProfileError("limits", "every declared envelope must be positive")
		}
	}
	if l.CanonicalInputBytes > 1<<20 || l.ProcessFrameBytes > 1<<20 || l.ProcessStdoutBytes > 16<<20 || l.ProcessEvents > 4096 || l.JSONDepth > 32 || l.RawFinalBytes > 256<<10 || l.StructuredFinalBytes > 128<<10 || l.StructuredFinalBytes > l.RawFinalBytes || l.AnswerBytes > 32<<10 || l.AnswerBytes > l.StructuredFinalBytes || l.ArtifactBytes > 32<<10 || l.ArtifactTotalBytes > 64<<10 || l.ArtifactBytes > math.MaxUint64/2 || 2*l.ArtifactBytes > l.ArtifactTotalBytes || l.SSELineBytes > l.SSEEventBytes || l.SSEEventBytes > l.ResponseBodyBytes || l.ResponseChunkBytes > l.ResponseBodyBytes || l.NativeRequestBytes > l.PreSendAllocationBytes || l.PreSendAllocationBytes > l.ProcessMemoryBytes || l.ResponseBodyBytes > l.ResponseAllocationBytes || l.ResponseAllocationBytes > l.ProcessMemoryBytes || l.ResponseBodyBytes > l.ResponseCopyWorkBytes {
		return attachedPiSDKProfileError("limits", "must preserve inherited MIN ceilings and consistent declared envelopes")
	}
	return nil
}

func EncodeAttachedPiSDKNativeProfileV1(p AttachedPiSDKNativeProfileV1) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(attachedPiSDKCanonicalV1{p, attachedPiSDKInheritedMedia()})
	if err != nil || len(raw) > attachedPiSDKCanonicalMaxBytesV1 {
		return nil, attachedPiSDKProfileError("codec", "cannot encode bounded canonical P")
	}
	return raw, nil
}

// Decode accepts ONLY exact producer bytes. This also rejects duplicate keys,
// case aliases, omitted/null fields and changed inherited media commitments.
// Every failure returns a zero profile, never a partially validated value.
func DecodeAttachedPiSDKNativeProfileV1(raw []byte) (AttachedPiSDKNativeProfileV1, error) {
	zero := AttachedPiSDKNativeProfileV1{}
	if len(raw) == 0 || len(raw) > attachedPiSDKCanonicalMaxBytesV1 || !utf8.Valid(raw) {
		return zero, attachedPiSDKProfileError("codec", "requires bounded canonical UTF-8 P")
	}
	var v attachedPiSDKCanonicalV1
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil {
		return zero, attachedPiSDKProfileError("codec", "invalid canonical P")
	}
	var trailing any
	if !errors.Is(d.Decode(&trailing), io.EOF) {
		return zero, attachedPiSDKProfileError("codec", "trailing canonical P")
	}
	expected, err := EncodeAttachedPiSDKNativeProfileV1(v.Profile)
	if err != nil || v.InheritedMedia != attachedPiSDKInheritedMedia() || !bytes.Equal(raw, expected) {
		return zero, attachedPiSDKProfileError("codec", "requires exact canonical producer bytes")
	}
	return v.Profile, nil
}

func (p AttachedPiSDKNativeProfileV1) CanonicalBytes() ([]byte, error) {
	return EncodeAttachedPiSDKNativeProfileV1(p)
}

func (p AttachedPiSDKNativeProfileV1) Digest() (string, error) {
	raw, err := p.CanonicalBytes()
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte("sessionless.attached-pi-sdk-native-profile.v1\x00"))
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (p AttachedPiSDKNativeProfileV1) Descriptor() (HarnessBackendDescriptorV1, error) {
	digest, err := p.Digest()
	if err != nil {
		return HarnessBackendDescriptorV1{}, err
	}
	d := HarnessBackendDescriptorV1{HarnessKind: HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: HarnessBackendPiV1, ArtifactKind: HarnessArtifactExecutableV1, ArtifactDigest: p.Sources.NativeArtifactDigest, NativeProtocolVersion: AttachedPiSDKNativeProtocolV1, BackendProfileDigest: digest, ProviderContractKind: ProviderContractInvocationV1, CredentialDeliveryKind: ProviderCredentialDeliveryFileV1}
	if err := d.Validate(); err != nil {
		return HarnessBackendDescriptorV1{}, err
	}
	return d, nil
}

// IsAttachedPiSDKNativeDescriptorV1 classifies closed STRUCTURAL discriminants.
// It does not authenticate P, match a source-owned expected profile, or grant
// execution. In particular an opaque valid-looking hash is never reviewed P.
func IsAttachedPiSDKNativeDescriptorV1(d HarnessBackendDescriptorV1) bool {
	return d.Validate() == nil && d.ArtifactDigest != strings.Repeat("0", 64) && d.BackendProfileDigest != strings.Repeat("0", 64) && d.HarnessKind == HarnessKindSessionlessV1 && d.HarnessVersion == "1" && d.BackendKind == HarnessBackendPiV1 && d.ArtifactKind == HarnessArtifactExecutableV1 && d.NativeProtocolVersion == AttachedPiSDKNativeProtocolV1 && d.ProviderContractKind == ProviderContractInvocationV1 && d.CredentialDeliveryKind == ProviderCredentialDeliveryFileV1
}

// ValidateBindingIdentity must be called on the privately retained SOURCE-OWNED
// EXPECTED P, never a P supplied by a job/browser/child. Equality is structural:
// serving, E/T/R/M/current owner/resource/spending and runtime proof are separate.
// This prerequisite does not require the future original-binding M carrier.
func (p AttachedPiSDKNativeProfileV1) ValidateBindingIdentity(binding HarnessBindingV2, placement ExecutionPlacementV2) error {
	d, err := p.Descriptor()
	if err != nil {
		return err
	}
	if err := binding.ValidateForScope(binding.TenantID, binding.OwnerUserID, binding.RunID, binding.AttemptID, placement); err != nil {
		return err
	}
	if binding.Profile != HarnessExecutionSingleTurnV2 || placement.Kind != ExecutionPlacementAttachedWorker || placement.OwnerUserID != binding.OwnerUserID || binding.AgentLoopPolicy != nil || binding.Resource.Kind != ProviderResourceAPIAccountV1 || binding.Resource.CredentialMode != ProviderCredentialInvocationV1 || binding.Backend != d || binding.ModelVendorID != p.Model.ModelVendorID || binding.ModelID != p.Model.ModelID {
		return attachedPiSDKProfileError("binding_identity", "must match the exact expected P/model and original attached API single-turn scope")
	}
	return nil
}

func attachedPiSDKProfileError(field, reason string) error {
	return ValidationError{Field: "attached_pi_sdk_profile." + field, Reason: reason}
}
