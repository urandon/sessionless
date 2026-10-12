package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
)

// MediaConformanceManifestV1 is a finite plan, not registration evidence or
// permission to execute it. Unknown released/model/platform authority means
// NO-GO. No fixture observation can fill Missing or grant provider authority.
type MediaConformanceManifestV1 struct {
	Status                                                                                           string
	ProfileVersion                                                                                   string
	ProfileDigest                                                                                    string
	ExecutableVersion                                                                                string
	ExecutableDigest                                                                                 string
	GuardProtocol                                                                                    string
	GuardArtifactDigest                                                                              string
	ConfigurationDigest                                                                              string
	ModelMetadataDigest                                                                              string
	ModelContextTokens, OutputReserveTokens, ProviderWireBytes, ProcessMemoryBytes, ProcessDiskBytes uint64
	SourcePin                                                                                        string
	RequiredNativeHook                                                                               []string
	Missing                                                                                          []string
	Tasks                                                                                            []string
	Limits                                                                                           MediaConformanceLimitsV1
	Policy                                                                                           []string
}

type MediaConformanceLimitsV1 struct {
	ProviderTurns                     int
	TotalWallSeconds                  int
	TotalInputTokens                  int
	TotalOutputTokens                 int
	MaximumCostUSDMicros              int
	MaximumEncodedImageBytes          int
	MaximumAggregateEncodedImageBytes int
	MaximumOriginalRGBABytes          int
	MaximumResampledRGBABytes         int
	MaximumDimension                  int
	MaximumPatches                    int
	PatchWidth, PatchHeight           int
}

func (config MediaConfigV1) ConformanceManifestV1() (MediaConformanceManifestV1, error) {
	descriptor, err := config.DescriptorV1()
	if err != nil {
		return MediaConformanceManifestV1{}, err
	}
	digest := func(body []byte) string {
		if len(body) == 0 {
			return ""
		}
		sum := sha256.Sum256(body)
		return hex.EncodeToString(sum[:])
	}
	return MediaConformanceManifestV1{
		Status:         "NO-GO: exact native before-send guard and activation authority pending",
		ProfileVersion: MediaProfileVersionV1, ProfileDigest: descriptor.BackendProfileDigest,
		ExecutableVersion: config.Backend.ExecutableVersion, ExecutableDigest: descriptor.ArtifactDigest,
		GuardProtocol: config.Native.GuardProtocol, GuardArtifactDigest: config.Native.GuardArtifactDigest,
		ConfigurationDigest: digest(config.Native.Configuration), ModelMetadataDigest: digest(config.Native.ModelMetadata),
		ModelContextTokens: config.Native.ModelContextTokens, OutputReserveTokens: config.Native.OutputReserveTokens,
		ProviderWireBytes: config.Native.ProviderWireBytes, ProcessMemoryBytes: config.Native.ProcessMemoryBytes, ProcessDiskBytes: config.Native.ProcessDiskBytes,
		SourcePin: "openai/codex@f5420174dafba153913a3e697f89002c338dfd7e",
		RequiredNativeHook: []string{
			"core/src/session/mod.rs:3078,3147: pin exact model metadata/features to DetailBased and Defer; UnifiedImageBudget disabled explicitly",
			"core/src/image_preparation.rs:35,276 and protocol/src/models.rs:1965: LocalImage conversion must verify original digest/order, reject placeholders, record actual prepared dimensions/encoding/digest and unchanged-versus-resized association before send",
			"utils/image/src/lib.rs:29,32: reserve original/resampled/copy/codec scratch/cache within exact process memory BEFORE allocation; existing 1GiB sanity/64MiB cache are not this profile",
			"native provider-client send: serialize complete request under bounded allocation, measure every dataURL/base64/JSON/notice/tool/config frame and calibrated text+prepared-image+output reserve BEFORE physical send; growing Vec and eventual stdout are not prevention",
			"same concrete supervised executable only: no proxy, second process runner, credential lifecycle, claim, native resume, egress or NetworkDenied change",
		},
		Missing: []string{"exact released artifact provenance/SBOM and guard implementation/hash", "actual platform architecture/memory/disk and read-only confinement/effect-sink conformance", "complete exact artifact tool inventory and closed feature/config/model-catalog digests", "calibrated exact model text/image context fit and output reserve", "trusted original-to-prepared model-visible request evidence", "owner/resource/private-data/provider-egress eligibility tuple with expiry", "separately approved finite credential/data/time/token/cost authority and Sessionless exact reviewed revision"},
		Tasks:   []string{"unchanged PNG: orientation/color/source association", "unchanged JPEG: orientation/color/quality/source association", "3000x1000 PNG: native 2048/2500x32 resize, exact re-encoding and task-relevant detail-loss inspection", "mixed snapshot+tail, historical text trigger, repeated original filenames and two images in exact model-visible order", "corrupt/unreadable/placeholder/source-swap cases: zero physical send", "maximum prepared encoding/base64/full-wire/context/copy/cache/process bounds and one-byte excess: zero send", "full tool inventory and unexpected effect-sink denial, actual read-only platform and authorized egress only", "cancel/finish/writeback/release/cleanup/receipt barriers and zero replay; no output upload under #199"},
		Limits:  MediaConformanceLimitsV1{ProviderTurns: 8, TotalWallSeconds: 300, TotalInputTokens: 256000, TotalOutputTokens: 32768, MaximumCostUSDMicros: 5_000_000, MaximumEncodedImageBytes: 2 << 20, MaximumAggregateEncodedImageBytes: 4 << 20, MaximumOriginalRGBABytes: 32 << 20, MaximumResampledRGBABytes: 32 << 20, MaximumDimension: 2048, MaximumPatches: 2500, PatchWidth: 32, PatchHeight: 32},
		Policy:  []string{"all limits MIN(profile, admitted job/product, transport, exact installation and earlier caller/run/lease/resource authority); inherited 1MiB unchanged", "public generated fixtures only unless separately approved exact private-data policy; unknowns deny", "no trial may start until every Missing item has independently reviewed exact evidence and finite authority", "private request evidence stays private; retain only content-free association digests in public observations", "quiesce original supervisor/descendants, finish original WriteBack/Release and inode-bound input cleanup; ambiguous outcome never reruns", "defaults off; rollback revokes exact resource registration without changing canonical history"},
	}, nil
}
