package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// These are hashes of named LOCAL TEST DATA, not installed executable/package
// pins, runtime calibration, owner grants or an enabled production profile.
func piSDKFixtureDigest(label string) string {
	sum := sha256.Sum256([]byte("sessionless-pi-sdk-local-test-fixture:" + label))
	return hex.EncodeToString(sum[:])
}

func piSDKFixtureProfile(t testing.TB) AttachedPiSDKNativeProfileV1 {
	t.Helper()
	p := AttachedPiSDKNativeProfileV1{
		Version: 1, ProfileID: AttachedPiSDKProfileIDV1, NativeProtocolVersion: AttachedPiSDKNativeProtocolV1,
		Sources:  AttachedPiSDKNativeSourcesV1{PiSourceRevision: attachedPiSDKSourceRevisionV1, OpenAISDKVersion: "6.40.0", OpenAISDKIntegrity: attachedPiSDKOpenAIIntegrityV1, NodeVersion: "local-fixture-runtime"},
		Model:    AttachedPiSDKNativeModelV1{ModelVendorID: "local-fixture-vendor", ModelID: "local-fixture-model", Endpoint: "https://fixture.invalid/v1/responses", ModelContextTokens: 8192, MaxBilledInputTokens: 2048, MaxOutputTokens: 1024},
		Platform: AttachedPiSDKNativePlatformV1{OS: "local-fixture-os", Architecture: "local-fixture-architecture"},
		Limits:   AttachedPiSDKNativeLimitsV1{CanonicalInputBytes: 512 << 10, NativeRequestBytes: 1 << 20, PreSendAllocationBytes: 8 << 20, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBodyBytes: 1 << 20, ResponseChunkBytes: 32 << 10, SSELineBytes: 32 << 10, SSEEventBytes: 128 << 10, SSEEvents: 512, JSONNodes: 4096, ResponseCopyWorkBytes: 16 << 20, ResponseAllocationBytes: 8 << 20, ProcessMemoryBytes: 256 << 20, ProcessDiskBytes: 1 << 20, ProcessFrameBytes: 512 << 10, ProcessStdoutBytes: 8 << 20, ProcessEvents: 2048, JSONDepth: 16, RawFinalBytes: 192 << 10, StructuredFinalBytes: 96 << 10, AnswerBytes: 16 << 10, ArtifactBytes: 16 << 10, ArtifactTotalBytes: 48 << 10},
		Behavior: AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1},
	}
	for _, target := range []any{&p.Sources, &p.Model, &p.Platform} {
		value := reflect.ValueOf(target).Elem()
		for i := 0; i < value.NumField(); i++ {
			if strings.HasSuffix(value.Type().Field(i).Name, "Digest") {
				value.Field(i).SetString(piSDKFixtureDigest(value.Type().Name() + "." + value.Type().Field(i).Name))
			}
		}
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("local pure profile setup: %v", err)
	}
	return p
}

func piSDKFixtureBinding(t testing.TB, p AttachedPiSDKNativeProfileV1) (HarnessBindingV2, ExecutionPlacementV2) {
	t.Helper()
	d, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	pl := ExecutionPlacementV2{Version: ExecutionPlacementVersionV2, Kind: ExecutionPlacementAttachedWorker, FallbackPolicy: ExecutionFallbackDenied, OwnerUserID: "local-fixture-owner", WorkerID: "local-fixture-worker", CapabilityDigest: AttachedWorkerCapabilityDigest(piSDKFixtureDigest("capability")), PolicyDigest: AttachedWorkerPolicyDigest(piSDKFixtureDigest("placement-policy"))}
	pd, err := ExecutionPlacementDigest(pl)
	if err != nil {
		t.Fatal(err)
	}
	expiry := time.Unix(2000000000, 0).UTC()
	b := HarnessBindingV2{Version: HarnessBindingVersionV2, Profile: HarnessExecutionSingleTurnV2, TenantID: "local-fixture-tenant", OwnerUserID: pl.OwnerUserID, RunID: "local-fixture-run", AttemptID: "local-fixture-attempt", Backend: d,
		Resource: ProviderResourceBindingV1{Kind: ProviderResourceAPIAccountV1, ResourceID: "local-fixture-resource", OwnerUserID: pl.OwnerUserID, Revision: 1, CredentialMode: ProviderCredentialInvocationV1, CredentialGeneration: 1}, ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, InputDataClass: ProviderDataPublicV1,
		ProviderCatalogDigest: piSDKFixtureDigest("catalog"), ProviderRouteDigest: piSDKFixtureDigest("route"), PrivacyPolicyDigest: piSDKFixtureDigest("privacy"), CapabilityEvidenceDigest: piSDKFixtureDigest("capability-evidence"), EffectivePolicyDigest: piSDKFixtureDigest("effective-policy"), ExecutionPlacementDigest: string(pd), EvidenceExpiresAt: &expiry}
	b.AttachedPiAPIInvocationPolicyDigest = AttachedPiAPIInvocationPolicyMappingDigestV1(piSDKFixtureDigest("structural-mapping-not-owner-permission"))
	if err := p.ValidateBindingIdentity(b, pl); err != nil {
		t.Fatalf("local structural binding setup: %v", err)
	}
	return b, pl
}

func TestAttachedPiSDKProfileCanonicalIdentity(t *testing.T) {
	p := piSDKFixtureProfile(t)
	raw, err := p.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAttachedPiSDKNativeProfileV1(raw)
	if err != nil || decoded != p {
		t.Fatalf("round trip got=%+v err=%v", decoded, err)
	}
	var wire attachedPiSDKCanonicalV1
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.InheritedMedia != attachedPiSDKInheritedMedia() || wire.InheritedMedia.ImageCount != 2 || wire.InheritedMedia.ArtifactCount != 2 {
		t.Fatalf("complete inherited cohorts lost: %+v", wire.InheritedMedia)
	}
	sum := sha256.Sum256(append([]byte("sessionless.attached-pi-sdk-native-profile.v1\x00"), raw...))
	digest, err := p.Digest()
	if err != nil || digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest got=%q err=%v", digest, err)
	}
	d, err := p.Descriptor()
	if err != nil || !IsAttachedPiSDKNativeDescriptorV1(d) || d.ArtifactDigest != p.Sources.NativeArtifactDigest || d.BackendProfileDigest != digest {
		t.Fatalf("descriptor got=%+v err=%v", d, err)
	}
	if d.ArtifactDigest == p.Sources.NodeExecutableDigest || d.ArtifactDigest == p.Sources.BootstrapDigest || d.ArtifactDigest == digest {
		t.Fatal("supervised artifact conflated with Node/bootstrap/P")
	}
	b, pl := piSDKFixtureBinding(t, p)
	before, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	bd, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ValidateBindingIdentity(b, pl); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(b)
	ad, _ := b.Digest()
	if !bytes.Equal(before, after) || bd != ad || b.Clone().Backend != b.Backend {
		t.Fatal("pure identity comparison mutated original absent binding bytes/digest")
	}
}

// This complete literal oracle is specified independently of CanonicalBytes,
// production constants, reflection and fixture helpers. Its local-test pin
// hashes were computed independently with shasum from the named fixture bytes;
// the full digest covers the literal domain, NUL and EXACT literal vector.
// Changing wrapper/key order/fixed policy or any of the eleven inherited
// commitments requires an intentional reviewed golden change.
func TestAttachedPiSDKProfileLiteralCanonicalGolden(t *testing.T) {
	const vector = `{"profile":{"version":1,"profile_id":"PI-SDK-MEDIA-2026-10-V1","native_protocol_version":"pi-sdk-standard-responses.v1","sources":{"pi_source_revision":"6c4f360264397c59801f6da2bdac13e3b1fcbe91","pi_package_digest":"6006f3761dab44a004de22c87b4bf4e823a182af5e93244a13d856b4fdf4f7aa","pi_build_digest":"be58ef3f6ef9b3a944c4d31c3bbad87be9145d15c303d7926fc4c17107541f43","pi_dependency_lock_digest":"0de2570a9d093f2bd53304b0376016b756ed25645b6f27d69217467d38915905","pi_generated_catalog_digest":"77f13557066e34c839f7fbdb4422f5964d256eba1753b5b4674003f94ce5cad5","open_ai_sdk_version":"6.40.0","open_ai_sdk_integrity":"sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==","open_ai_sdk_package_digest":"1bab07ddc63c24f5a42849f3f48b7ccdb55b5253696154188dd192745e576d73","node_version":"local-fixture-runtime","node_source_digest":"312b23f09a5eefe8c5533a852d4a84f1fef200a9ae84d507da5b7c0b2f3ba85a","node_executable_digest":"8851b2a0663f83b4117cbaf35b72c62e65cd07abaf7a25ba8d18650877e8c2bf","native_artifact_digest":"9bc237d7dc93ae9bf42ffab0e2947edad20012a2b6f4512aa3f38860af32756f","bootstrap_digest":"400addb5fc1e0691b0f19bead7b95d9ed22b1cc67be886b2a9f5c9a78975a31d","stream_wrapper_digest":"96ade0f056513b2fad80ad69353d0d1b237351eeb6ba5027ad449dd9a43f9d14","physical_transport_digest":"2d26b5cbbcac8f7497d25a0a024c7fb618aca06a1daa453b14f22bb53b40b4bf","request_schema_digest":"3a9d94c632c0cc5a006580abe871fe65472c4d91fd6a0b3cf2ce2d166e107d09","final_schema_digest":"b451a108fad261b6664563ff10126aa50e596f822504a8a3ceb9d4478b83955a"},"model":{"model_vendor_id":"local-fixture-vendor","model_id":"local-fixture-model","endpoint":"https://fixture.invalid/v1/responses","vision_metadata_digest":"d266749f9efd7a7516a7c8880b6062d6887d3531a4222cc7bcdbff56f50e6aff","context_accounting_digest":"db43276b2b3e814394929a55d5bcdad8ad92ccdd2c19ebd4d72f2a5862394a90","model_context_tokens":8192,"max_billed_input_tokens":2048,"max_output_tokens":1024},"platform":{"os":"local-fixture-os","architecture":"local-fixture-architecture","runtime_policy_digest":"8142ca22603aaccda108095d45eeb2b8aee533b75c3353c35cc230379f547d42","isolation_policy_digest":"33e1301cf71c7fc6e313a8791a32dfc00a3bba8447990ec2fdb45cd19b416629","egress_policy_digest":"42bcf9f472427cae397098993ad27fdb33b32fe8d1dee7d1eb6837b917c1f290","cleanup_policy_digest":"15a1d8fbfd623d8d5dadcd65775323babf7eada4a501a416f7e8b11c5c50100b"},"limits":{"canonical_input_bytes":524288,"native_request_bytes":1048576,"pre_send_allocation_bytes":8388608,"request_header_bytes":4096,"response_header_bytes":4096,"response_body_bytes":1048576,"response_chunk_bytes":32768,"sse_line_bytes":32768,"sse_event_bytes":131072,"sse_events":512,"json_nodes":4096,"response_copy_work_bytes":16777216,"response_allocation_bytes":8388608,"process_memory_bytes":268435456,"process_disk_bytes":1048576,"process_frame_bytes":524288,"process_stdout_bytes":8388608,"process_events":2048,"json_depth":16,"raw_final_bytes":196608,"structured_final_bytes":98304,"answer_bytes":16384,"artifact_bytes":16384,"artifact_total_bytes":49152},"behavior":{"provider_api":"openai-responses","image_derivation":"NoTransformOriginal","image_detail":"auto","transport":"http-sse","local_tool_registry":"empty","max_physical_sends":1,"sdk_retries":0,"provider_retries":0,"session_retries":0,"fallback_transport":false,"native_compaction":false,"discovery":false}},"inherited_media":{"text_count":2,"text_bytes":131072,"text_total_bytes":262144,"image_count":2,"image_bytes":1048576,"image_total_bytes":2097152,"image_pixels":4000000,"decoded_rgba_bytes":33554432,"go_validation_memory_bytes":100663296,"artifact_count":2,"answer_runes":32000}}`
	const wantDigest = "934b0691c63fc6b1f40ae12a4d9b820190ecb9458107ed8010db34c85ba1b98a"
	p := piSDKFixtureProfile(t)
	got, err := p.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte(vector)) {
		first := 0
		for first < len(got) && first < len(vector) && got[first] == vector[first] {
			first++
		}
		t.Errorf("canonical golden drift: got bytes=%d want=%d first differing offset=%d", len(got), len(vector), first)
	}
	digest, err := p.Digest()
	if err != nil || digest != wantDigest {
		t.Errorf("fixed canonical digest got=%q want=%q err=%v", digest, wantDigest, err)
	}
	decoded, err := DecodeAttachedPiSDKNativeProfileV1([]byte(vector))
	if err != nil || decoded != p {
		t.Errorf("literal golden decode got=%+v want local fixture=%+v err=%v", decoded, p, err)
	}
}

func TestAttachedPiSDKProfileEveryVariableFieldSealed(t *testing.T) {
	p := piSDKFixtureProfile(t)
	original, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	b, pl := piSDKFixtureBinding(t, p)
	for _, section := range []string{"Sources", "Model", "Platform", "Limits"} {
		value := reflect.ValueOf(p).FieldByName(section)
		for i := 0; i < value.NumField(); i++ {
			name := value.Type().Field(i).Name
			if name == "PiSourceRevision" || name == "OpenAISDKVersion" || name == "OpenAISDKIntegrity" {
				continue
			}
			t.Run(section+"/"+name, func(t *testing.T) {
				changed := p
				field := reflect.ValueOf(&changed).Elem().FieldByName(section).FieldByName(name)
				switch field.Kind() {
				case reflect.String:
					if strings.HasSuffix(name, "Digest") {
						field.SetString(piSDKFixtureDigest("mutant-" + section + name))
					} else {
						field.SetString(field.String() + "x")
					}
				case reflect.Uint64:
					field.SetUint(field.Uint() + 1)
				default:
					t.Fatalf("unhandled field kind %s", field.Kind())
				}
				if err := changed.Validate(); err != nil {
					t.Fatalf("valid independent mutation %s.%s: %v", section, name, err)
				}
				digest, err := changed.Digest()
				if err != nil || digest == original {
					t.Fatalf("field not sealed digest=%q err=%v", digest, err)
				}
				// The old binding cannot substitute P2 for its expected P1.
				if err := changed.ValidateBindingIdentity(b, pl); err == nil {
					t.Fatal("changed expected P accepted old descriptor")
				}
				changedBinding, changedPlacement := piSDKFixtureBinding(t, changed)
				if err := p.ValidateBindingIdentity(changedBinding, changedPlacement); err == nil {
					t.Fatal("original source-owned P accepted substituted valid-looking P2")
				}
				if err := p.ValidateBindingIdentity(b, pl); err != nil {
					t.Fatalf("restored original control: %v", err)
				}
			})
		}
	}
}

func TestAttachedPiSDKProfileFixedAndInvalidFieldsDeny(t *testing.T) {
	p := piSDKFixtureProfile(t)
	cases := map[string]func(*AttachedPiSDKNativeProfileV1){
		"version": func(p *AttachedPiSDKNativeProfileV1) { p.Version = 2 }, "profile": func(p *AttachedPiSDKNativeProfileV1) { p.ProfileID = "unreviewed" }, "protocol": func(p *AttachedPiSDKNativeProfileV1) { p.NativeProtocolVersion = "pi-rpc-jsonl.v1" },
		"pi_pin": func(p *AttachedPiSDKNativeProfileV1) { p.Sources.PiSourceRevision = "other" }, "sdk_pin": func(p *AttachedPiSDKNativeProfileV1) { p.Sources.OpenAISDKVersion = "latest" }, "sdk_integrity": func(p *AttachedPiSDKNativeProfileV1) { p.Sources.OpenAISDKIntegrity = "other" },
		"zero_pin": func(p *AttachedPiSDKNativeProfileV1) { p.Sources.NativeArtifactDigest = strings.Repeat("0", 64) }, "uppercase_pin": func(p *AttachedPiSDKNativeProfileV1) {
			p.Sources.PiBuildDigest = strings.ToUpper(p.Sources.PiBuildDigest)
		}, "short_pin": func(p *AttachedPiSDKNativeProfileV1) { p.Sources.NodeSourceDigest = "a" },
		"zero_context": func(p *AttachedPiSDKNativeProfileV1) { p.Model.ModelContextTokens = 0 }, "context_overflow": func(p *AttachedPiSDKNativeProfileV1) { p.Model.MaxBilledInputTokens = math.MaxUint64 }, "insufficient_context": func(p *AttachedPiSDKNativeProfileV1) { p.Model.ModelContextTokens = 1024 },
		"endpoint_http": func(p *AttachedPiSDKNativeProfileV1) { p.Model.Endpoint = "http://fixture.invalid/v1/responses" }, "endpoint_auth": func(p *AttachedPiSDKNativeProfileV1) { p.Model.Endpoint = "https://key@fixture.invalid/v1/responses" }, "endpoint_query": func(p *AttachedPiSDKNativeProfileV1) { p.Model.Endpoint = "https://fixture.invalid/v1/responses?key=x" }, "endpoint_fragment": func(p *AttachedPiSDKNativeProfileV1) { p.Model.Endpoint = "https://fixture.invalid/v1/responses#" }, "endpoint_relative": func(p *AttachedPiSDKNativeProfileV1) { p.Model.Endpoint = "/v1/responses" },
		"native_request_allocation": func(p *AttachedPiSDKNativeProfileV1) {
			p.Limits.NativeRequestBytes = p.Limits.PreSendAllocationBytes + 1
		}, "response_allocation": func(p *AttachedPiSDKNativeProfileV1) {
			p.Limits.ResponseBodyBytes = p.Limits.ResponseAllocationBytes + 1
		}, "copy_work": func(p *AttachedPiSDKNativeProfileV1) { p.Limits.ResponseCopyWorkBytes = p.Limits.ResponseBodyBytes - 1 },
		"body_event": func(p *AttachedPiSDKNativeProfileV1) { p.Limits.SSEEventBytes = p.Limits.ResponseBodyBytes + 1 }, "event_line": func(p *AttachedPiSDKNativeProfileV1) { p.Limits.SSELineBytes = p.Limits.SSEEventBytes + 1 }, "body_chunk": func(p *AttachedPiSDKNativeProfileV1) { p.Limits.ResponseChunkBytes = p.Limits.ResponseBodyBytes + 1 }, "process_allocation": func(p *AttachedPiSDKNativeProfileV1) {
			p.Limits.ResponseAllocationBytes = p.Limits.ProcessMemoryBytes + 1
		},
		"artifact_overflow": func(p *AttachedPiSDKNativeProfileV1) { p.Limits.ArtifactBytes = math.MaxUint64 }, "artifact_aggregate": func(p *AttachedPiSDKNativeProfileV1) { p.Limits.ArtifactTotalBytes = 2*p.Limits.ArtifactBytes - 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			q := p
			mutate(&q)
			if err := q.Validate(); err == nil {
				t.Fatal("invalid source profile accepted")
			}
			if _, err := q.Descriptor(); err == nil {
				t.Fatal("invalid profile produced descriptor")
			}
			if err := p.Validate(); err != nil {
				t.Fatalf("restored control: %v", err)
			}
		})
	}
	for i := 0; i < reflect.TypeOf(p.Behavior).NumField(); i++ {
		name := reflect.TypeOf(p.Behavior).Field(i).Name
		t.Run("behavior/"+name, func(t *testing.T) {
			q := p
			f := reflect.ValueOf(&q.Behavior).Elem().FieldByName(name)
			switch f.Kind() {
			case reflect.String:
				f.SetString("unreviewed")
			case reflect.Uint64:
				f.SetUint(f.Uint() + 1)
			case reflect.Bool:
				f.SetBool(true)
			}
			if q.Validate() == nil {
				t.Fatal("changed fixed operational policy accepted")
			}
		})
	}
	for i := 0; i < reflect.TypeOf(p.Limits).NumField(); i++ {
		name := reflect.TypeOf(p.Limits).Field(i).Name
		t.Run("zero_limit/"+name, func(t *testing.T) {
			q := p
			reflect.ValueOf(&q.Limits).Elem().FieldByName(name).SetUint(0)
			if q.Validate() == nil {
				t.Fatal("omitted finite native envelope accepted")
			}
		})
	}
	for name, cap := range map[string]uint64{"CanonicalInputBytes": 1 << 20, "ProcessFrameBytes": 1 << 20, "ProcessStdoutBytes": 16 << 20, "ProcessEvents": 4096, "JSONDepth": 32, "RawFinalBytes": 256 << 10, "StructuredFinalBytes": 128 << 10, "AnswerBytes": 32 << 10, "ArtifactBytes": 32 << 10, "ArtifactTotalBytes": 64 << 10} {
		t.Run("inherited_cap/"+name, func(t *testing.T) {
			q := p
			reflect.ValueOf(&q.Limits).Elem().FieldByName(name).SetUint(cap + 1)
			if q.Validate() == nil {
				t.Fatal("inherited ceiling increased")
			}
		})
	}
}

func TestAttachedPiSDKProfileStrictCodecAndZeroFailure(t *testing.T) {
	p := piSDKFixtureProfile(t)
	raw, err := p.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty": nil, "over_cap": bytes.Repeat([]byte{'x'}, attachedPiSDKCanonicalMaxBytesV1+1), "invalid_utf8": {0xff}, "trailing": append(append([]byte{}, raw...), []byte("{}")...), "whitespace": append([]byte(" "), raw...),
		"profile_null": []byte(`{"profile":null,"inherited_media":null}`), "partial": []byte(`{"profile":{"version":1}}`), "duplicate": bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), "nested_duplicate": bytes.Replace(raw, []byte(`"sdk_retries":0`), []byte(`"sdk_retries":0,"sdk_retries":0`), 1),
		"alias": bytes.Replace(raw, []byte(`"native_protocol_version"`), []byte(`"Native_Protocol_Version"`), 1), "null_zero": bytes.Replace(raw, []byte(`"sdk_retries":0`), []byte(`"sdk_retries":null`), 1), "omitted_zero": bytes.Replace(raw, []byte(`"sdk_retries":0,`), nil, 1), "null_false": bytes.Replace(raw, []byte(`"discovery":false`), []byte(`"discovery":null`), 1), "omitted_false": bytes.Replace(raw, []byte(`,"discovery":false`), nil, 1), "wrong_media": bytes.Replace(raw, []byte(`"image_count":2`), []byte(`"image_count":1`), 1), "raw_profile": mustPiSDKProfileJSON(t, p),
	}
	for _, key := range []string{"reviewed", "principal", "resource", "privacy_policy_digest", "price", "terms_digest", "receipt_digest", "mapping_digest", "binding_digest", "job_digest"} {
		cases["backreference/"+key] = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"`+key+`":"forbidden"`), 1)
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := DecodeAttachedPiSDKNativeProfileV1(input)
			if err == nil || got != (AttachedPiSDKNativeProfileV1{}) {
				t.Fatalf("invalid codec got=%+v err=%v", got, err)
			}
			if control, err := DecodeAttachedPiSDKNativeProfileV1(raw); err != nil || control != p {
				t.Fatalf("restored control got=%+v err=%v", control, err)
			}
		})
	}
}

func mustPiSDKProfileJSON(t testing.TB, p AttachedPiSDKNativeProfileV1) []byte {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestAttachedPiSDKProfileClassifierAndExpectedIdentityDeny(t *testing.T) {
	p := piSDKFixtureProfile(t)
	b, pl := piSDKFixtureBinding(t, p)
	for name, mutate := range map[string]func(*HarnessBackendDescriptorV1){"rpc": func(d *HarnessBackendDescriptorV1) { d.NativeProtocolVersion = "pi-rpc-jsonl.v1" }, "unknown_sdk": func(d *HarnessBackendDescriptorV1) { d.NativeProtocolVersion = "pi-sdk-unknown.v1" }, "codex": func(d *HarnessBackendDescriptorV1) { d.BackendKind = HarnessBackendCodexExecV1 }, "fixture": func(d *HarnessBackendDescriptorV1) { d.BackendKind = HarnessBackendDeterministicFixtureV1 }, "container": func(d *HarnessBackendDescriptorV1) { d.ArtifactKind = HarnessArtifactContainerImageV1 }, "environment": func(d *HarnessBackendDescriptorV1) {
		d.CredentialDeliveryKind = ProviderCredentialDeliveryEnvironmentV1
	}, "direct": func(d *HarnessBackendDescriptorV1) { d.CredentialDeliveryKind = ProviderCredentialDeliveryDirectV1 }, "version": func(d *HarnessBackendDescriptorV1) { d.HarnessVersion = "2" }, "zero_artifact": func(d *HarnessBackendDescriptorV1) { d.ArtifactDigest = strings.Repeat("0", 64) }, "zero_profile": func(d *HarnessBackendDescriptorV1) { d.BackendProfileDigest = strings.Repeat("0", 64) }} {
		t.Run("classifier/"+name, func(t *testing.T) {
			d := b.Backend
			mutate(&d)
			if IsAttachedPiSDKNativeDescriptorV1(d) {
				t.Fatal("forbidden descriptor classified SDK")
			}
		})
	}
	for name, mutate := range map[string]func(*HarnessBindingV2, *ExecutionPlacementV2){"router": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) { b.Resource.Kind = ProviderResourceRouterAccountV1 }, "subscription": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) { b.Resource.Kind = ProviderResourceSubscriptionV1 }, "model": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) { b.ModelID = "other" }, "vendor": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) { b.ModelVendorID = "other" }, "foreign_p": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) {
		b.Backend.BackendProfileDigest = piSDKFixtureDigest("foreign-valid-looking-P")
	}, "foreign_artifact": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) {
		b.Backend.ArtifactDigest = piSDKFixtureDigest("foreign-artifact")
	}, "placement_digest": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) {
		b.ExecutionPlacementDigest = piSDKFixtureDigest("foreign-placement")
	}, "foreign_placement_owner": func(b *HarnessBindingV2, pl *ExecutionPlacementV2) {
		pl.OwnerUserID = "foreign-local-fixture-owner"
		digest, err := ExecutionPlacementDigest(*pl)
		if err != nil {
			t.Fatal(err)
		}
		b.ExecutionPlacementDigest = string(digest)
	}, "managed": func(b *HarnessBindingV2, pl *ExecutionPlacementV2) {
		*pl = ExecutionPlacementV2{Version: ExecutionPlacementVersionV2, Kind: ExecutionPlacementManaged, FallbackPolicy: ExecutionFallbackDenied, SubstrateBindingDigest: piSDKFixtureDigest("substrate")}
		d, err := ExecutionPlacementDigest(*pl)
		if err != nil {
			t.Fatal(err)
		}
		b.ExecutionPlacementDigest = string(d)
	}, "owned_agent": func(b *HarnessBindingV2, _ *ExecutionPlacementV2) { b.Profile = HarnessExecutionOwnedAgentV2 }} {
		t.Run("expected_identity/"+name, func(t *testing.T) {
			q, placement := b, pl
			mutate(&q, &placement)
			if p.ValidateBindingIdentity(q, placement) == nil {
				t.Fatal("foreign profile/resource/placement accepted")
			}
			if err := p.ValidateBindingIdentity(b, pl); err != nil {
				t.Fatalf("restored pure identity: %v", err)
			}
		})
	}
	// The discriminator alone deliberately cannot authenticate an opaque P hash.
	d := b.Backend
	d.BackendProfileDigest = piSDKFixtureDigest("unknown-but-shaped-P")
	if !IsAttachedPiSDKNativeDescriptorV1(d) {
		t.Fatal("classifier incorrectly claims provenance knowledge")
	}
	q := b
	q.Backend = d
	if p.ValidateBindingIdentity(q, pl) == nil {
		t.Fatal("expected-P equality failed to deny unknown shaped P")
	}
}

func TestAttachedPiSDKProfileMaxMetadataBound(t *testing.T) {
	p := piSDKFixtureProfile(t)
	p.Sources.NodeVersion = strings.Repeat("<", 128)
	p.Model.ModelVendorID = strings.Repeat("<", 128)
	p.Model.ModelID = strings.Repeat("<", 256)
	p.Platform.OS = strings.Repeat("<", 128)
	p.Platform.Architecture = strings.Repeat("<", 128)
	p.Model.Endpoint = "https://fixture.invalid/" + strings.Repeat("a", attachedPiSDKEndpointMaxBytesV1-len("https://fixture.invalid/"))
	p.Model.ModelContextTokens = math.MaxUint64
	p.Model.MaxBilledInputTokens = math.MaxUint64 - 1
	p.Model.MaxOutputTokens = 1
	p.Limits.ProcessMemoryBytes = math.MaxUint64
	p.Limits.ProcessDiskBytes = math.MaxUint64
	p.Limits.SSEEvents = math.MaxUint64
	p.Limits.JSONNodes = math.MaxUint64
	p.Limits.ResponseCopyWorkBytes = math.MaxUint64
	p.Limits.NativeRequestBytes = math.MaxUint64
	p.Limits.PreSendAllocationBytes = math.MaxUint64
	p.Limits.RequestHeaderBytes = math.MaxUint64
	p.Limits.ResponseHeaderBytes = math.MaxUint64
	p.Limits.ResponseBodyBytes = math.MaxUint64
	p.Limits.ResponseChunkBytes = math.MaxUint64
	p.Limits.SSELineBytes = math.MaxUint64
	p.Limits.SSEEventBytes = math.MaxUint64
	p.Limits.ResponseAllocationBytes = math.MaxUint64
	p.Limits.CanonicalInputBytes = 1 << 20
	p.Limits.ProcessFrameBytes = 1 << 20
	p.Limits.ProcessStdoutBytes = 16 << 20
	p.Limits.ProcessEvents = 4096
	p.Limits.JSONDepth = 32
	p.Limits.RawFinalBytes = 256 << 10
	p.Limits.StructuredFinalBytes = 128 << 10
	p.Limits.AnswerBytes = 32 << 10
	p.Limits.ArtifactBytes = 32 << 10
	p.Limits.ArtifactTotalBytes = 64 << 10
	raw, err := p.CanonicalBytes()
	if err != nil {
		t.Fatalf("maximum legal structural metadata: %v", err)
	}
	if len(raw) > attachedPiSDKCanonicalMaxBytesV1 {
		t.Fatalf("metadata bytes=%d bound=%d", len(raw), attachedPiSDKCanonicalMaxBytesV1)
	}
	// Independently inventory the wire shape used by the bound derivation. A
	// future field addition must update the auditable schema budget, not silently
	// borrow unused bytes from today's conservative allowance.
	var wire any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	keys := 0
	var countKeys func(any)
	countKeys = func(v any) {
		if object, ok := v.(map[string]any); ok {
			for key, child := range object {
				keys++
				if len(key) > 64 {
					t.Fatalf("key %q exceeds derived fixed-key budget", key)
				}
				countKeys(child)
			}
		}
	}
	countKeys(wire)
	if keys != 88 {
		t.Fatalf("canonical fixed keys got=%d want=88", keys)
	}
	got, err := DecodeAttachedPiSDKNativeProfileV1(raw)
	if err != nil || got != p {
		t.Fatalf("max metadata roundtrip err=%v", err)
	}
	// No JS/native execution: such declared uint64 values need exact safe-integer
	// conversion and runtime calibration before any future native registration.
}

func FuzzAttachedPiSDKProfileCanonical(f *testing.F) {
	p := piSDKFixtureProfile(f)
	raw, err := p.CanonicalBytes()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"profile":null}`))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		p, err := DecodeAttachedPiSDKNativeProfileV1(raw)
		if err != nil {
			if p != (AttachedPiSDKNativeProfileV1{}) {
				t.Fatal("partial profile escaped error")
			}
			return
		}
		encoded, err := p.CanonicalBytes()
		if err != nil || !bytes.Equal(raw, encoded) {
			t.Fatalf("canonical roundtrip err=%v", err)
		}
	})
}
