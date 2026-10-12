package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var ErrMediaNativeGuardUnavailable = errors.New("codex media exact native preparation/request guard is unavailable")

// MediaConfigV1 is an explicit immutable install-policy selection. It does not
// extend Config, change text defaults, or infer selection from attachments.
// Native is a declaration to compare with independently attested exact-artifact
// enforcement, NOT a caller attestation or permission to send provider input.
type MediaConfigV1 struct {
	Backend Config
	Native  NativeMediaPolicyV1
	fixture bool
}

type NativeMediaPolicyV1 struct {
	GuardProtocol       string `json:"guard_protocol"`
	GuardArtifactDigest string `json:"guard_artifact_digest"`
	Configuration       []byte `json:"configuration"`
	ModelMetadata       []byte `json:"model_metadata"`
	ModelContextTokens  uint64 `json:"model_context_tokens"`
	OutputReserveTokens uint64 `json:"output_reserve_tokens"`
	ProviderWireBytes   uint64 `json:"provider_wire_bytes"`
	ProcessMemoryBytes  uint64 `json:"process_memory_bytes"`
	ProcessDiskBytes    uint64 `json:"process_disk_bytes"`
}

func (config MediaConfigV1) Clone() MediaConfigV1 {
	// Malformed unbounded install policy must not trigger a sensitive copy.
	// Keep the invalid original shape for DescriptorV1 to reject, never truncate
	// it into an accepted declaration. Valid selected policies are deep frozen.
	if len(config.Native.Configuration) > 128<<10 || len(config.Native.ModelMetadata) > 128<<10 || len(config.Native.GuardProtocol) > 256 || len(config.Native.GuardArtifactDigest) > 64 {
		return config
	}
	config.Native.Configuration = append([]byte(nil), config.Native.Configuration...)
	config.Native.ModelMetadata = append([]byte(nil), config.Native.ModelMetadata...)
	return config
}

func (config MediaConfigV1) DescriptorV1() (domain.HarnessBackendDescriptorV1, error) {
	descriptor, err := prepareConfig(config.Backend)
	if err != nil || len(config.Native.Configuration) > 128<<10 || len(config.Native.ModelMetadata) > 128<<10 || len(config.Native.GuardProtocol) > 256 || len(config.Native.GuardArtifactDigest) > 64 {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	// Fixed numerical/source/preparation policy is part of the descriptor,
	// including unknown install fields. Missing attestation never inherits the
	// text profile's eligibility or an ambient native feature default.
	version := MediaProfileVersionV1
	if config.fixture {
		version = "sessionless.synthetic-native-media.v1"
	}
	policy := struct {
		Version                                                                           string
		TextCount, TextBytes, TextTotal, ImageCount, ImageBytes, ImageTotal, Pixels, RGBA int
		Preparation, Detail                                                               string
		Dimension, Patches, PatchWidth, PatchHeight                                       int
		PreparedBytes, PreparedTotal                                                      int
		UnifiedImageBudget                                                                bool
		Native                                                                            NativeMediaPolicyV1
	}{version, 2, mediaTextBytes, mediaTextTotal, 2, mediaImageBytes, mediaImageTotal, mediaPixels, mediaRGBATotal,
		"DetailBased", "High", 2048, 2500, 32, 32, 2 << 20, 4 << 20, false, config.Native}
	encoded, err := json.Marshal(policy)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	defer clear(encoded)
	h := sha256.New()
	h.Write([]byte(descriptor.BackendProfileDigest))
	h.Write(encoded)
	descriptor.BackendProfileDigest = hex.EncodeToString(h.Sum(nil))
	return descriptor, nil
}

// eligibility requires an implemented exact-artifact guard, not merely valid
// configuration or matching digests. No stock/released artifact in this tree
// currently provides a trusted before-allocation/before-send hook. There is no
// public boolean or injected callback that can manufacture that missing fact.
// This is an unfinished mandatory #199 gate, not completed media delivery.
func (config MediaConfigV1) eligibility() error {
	if _, err := config.DescriptorV1(); err != nil {
		return err
	}
	if config.fixture && config.Backend.Model == "sessionless-media-fixture" && config.Native.GuardProtocol == "sessionless.synthetic-native-media.v1" && config.Native.GuardArtifactDigest == hex.EncodeToString(config.Backend.ExecutableDigest[:]) {
		return nil
	}
	return ErrMediaNativeGuardUnavailable
}

func NewPreparedMediaAttachedWorkerRunnerV1(config MediaConfigV1, scratch string, supervisor *attachedworkerdaemon.Supervisor,
	credentials ports.CredentialLifecycle, outerConfig attachedworkerdaemon.InvocationRunnerConfig) (*PreparedAttachedWorkerRunnerV1, error) {
	if supervisor == nil || credentials == nil {
		return nil, ErrContract
	}
	config = config.Clone()
	if _, err := config.DescriptorV1(); err != nil {
		return nil, err
	}
	owner := &PreparedAttachedWorkerRunnerV1{config: config.Backend, mediaConfig: &config, scratch: scratch, supervisor: supervisor}
	outer, err := attachedworkerdaemon.NewPreparedInvocationRunner(outerConfig, &preparedExecutor{owner: owner}, credentials)
	if err != nil {
		return nil, err
	}
	owner.outer = outer
	return owner, nil
}

// NewDisabledMediaDriverV1 permits exact closed-registry identity/cancellation
// wiring while refusing enabled registration without implemented native guard
// authority. It cannot consume media without the private prepared owner stage.
func NewDisabledMediaDriverV1(config MediaConfigV1, authority AuthorityResolverV1, boundary ProcessBoundaryV1) (*Driver, error) {
	if config.Backend.Enabled {
		return nil, ErrMediaNativeGuardUnavailable
	}
	config = config.Clone()
	driver, err := NewDriver(config.Backend, authority, boundary)
	if err != nil {
		return nil, err
	}
	driver.descriptor, err = config.DescriptorV1()
	if err != nil {
		return nil, err
	}
	driver.mediaConfig = &config
	return driver, nil
}
