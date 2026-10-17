package domain_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRegisteredProxyV2ExclusiveTagsAndClosedLocalShape(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	local := f.proxy.Clone()
	local.TransportKind, local.PinnedHTTPS, local.ServiceKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2, nil, domain.RegisteredProxyProviderV1
	hash := strings.Repeat("a", 64)
	local.LocalLoopback = &domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: hash, EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, ConnectorArtifactDigest: hash, InstallationID: "local-installation", InstallationRevision: 1, HostIdentityDigest: hash, NetworkNamespaceIdentityDigest: hash, LiteralPeerDigest: hash}
	if err := local.Validate(); err != nil {
		t.Fatalf("pure local observation data: %v", err)
	}
	if err := local.ValidateForAllocation(f.allocation, f.source); err == nil {
		t.Fatal("local shape became permission through old image-backed allocation")
	}
	raw, err := domain.EncodeRegisteredProxyAttestationV2(local)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeRegisteredProxyAttestationV2(raw)
	if err != nil || !reflect.DeepEqual(decoded, local) {
		t.Fatalf("tagged local data codec: %v", err)
	}
	for _, forbidden := range []string{"resolution_set_digest", "proxy_artifact_digest", "proxy_identity_digest", "verified_tls"} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("local data fabricated %s", forbidden)
		}
	}
	for name, mutate := range map[string]func(*domain.RegisteredProxyAttestationV2){
		"unknown_tag":       func(v *domain.RegisteredProxyAttestationV2) { v.TransportKind = "unknown" },
		"missing_tag":       func(v *domain.RegisteredProxyAttestationV2) { v.TransportKind = "" },
		"mixed_https":       func(v *domain.RegisteredProxyAttestationV2) { h := *f.proxy.PinnedHTTPS; v.PinnedHTTPS = &h },
		"missing_local":     func(v *domain.RegisteredProxyAttestationV2) { v.LocalLoopback = nil },
		"search_service":    func(v *domain.RegisteredProxyAttestationV2) { v.ServiceKind = domain.RegisteredProxyWebSearchV1 },
		"tool_service":      func(v *domain.RegisteredProxyAttestationV2) { v.ServiceKind = domain.RegisteredProxyMCPToolV1 },
		"wrong_policy":      func(v *domain.RegisteredProxyAttestationV2) { v.LocalLoopback.EndpointPolicy = "https" },
		"zero_installation": func(v *domain.RegisteredProxyAttestationV2) { v.LocalLoopback.InstallationRevision = 0 },
		"zero_sequence":     func(v *domain.RegisteredProxyAttestationV2) { v.OperationSequence = 0 },
		"overflow_sequence": func(v *domain.RegisteredProxyAttestationV2) { v.OperationSequence = 13 },
		"no_operation":      func(v *domain.RegisteredProxyAttestationV2) { v.OperationDigest = "" },
		"old_version":       func(v *domain.RegisteredProxyAttestationV2) { v.Version = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			v := local.Clone()
			mutate(&v)
			if v.Validate() == nil {
				t.Fatal("invalid tagged observation accepted")
			}
		})
	}
	spoofed := bytes.Replace(raw, []byte(`"local_loopback":{`), []byte(`"local_loopback":{"verified_tls":true,`), 1)
	if _, err := domain.DecodeRegisteredProxyAttestationV2(spoofed); err == nil {
		t.Fatal("local codec accepted invented TLS fact")
	}
	cloned := local.Clone()
	cloned.LocalLoopback.LiteralPeerDigest = strings.Repeat("b", 64)
	if local.LocalLoopback.LiteralPeerDigest != hash {
		t.Fatal("local clone retained mutable variant alias")
	}
}

func TestRegisteredProxyV2SealsOperationAndHTTPSFacts(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	base, err := f.proxy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.RegisteredProxyAttestationV2){
		"operation": func(v *domain.RegisteredProxyAttestationV2) {
			v.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("c", 64))
		},
		"sequence": func(v *domain.RegisteredProxyAttestationV2) { v.OperationSequence++ },
		"resolution": func(v *domain.RegisteredProxyAttestationV2) {
			v.PinnedHTTPS.ResolutionSetDigest = strings.Repeat("c", 64)
		},
		"artifact": func(v *domain.RegisteredProxyAttestationV2) {
			v.PinnedHTTPS.ProxyArtifactDigest = strings.Repeat("c", 64)
		},
		"identity": func(v *domain.RegisteredProxyAttestationV2) {
			v.PinnedHTTPS.ProxyIdentityDigest = strings.Repeat("c", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := f.proxy.Clone()
			mutate(&v)
			digest, err := v.Digest()
			if err != nil || digest == base {
				t.Fatalf("valid %s fact not sealed: %v", name, err)
			}
			if f.proxy.PinnedHTTPS.ProxyIdentityDigest != f.allocation.ObservedProxyIdentityDigest {
				t.Fatal("variant clone changed original")
			}
		})
	}
	missing := f.proxy.Clone()
	missing.PinnedHTTPS = nil
	if missing.Validate() == nil {
		t.Fatal("HTTPS observation inferred missing HTTPS variant")
	}
	// The operation hash seals the entire immutable operation, not just its
	// request body. This test does not authenticate a producer or source.
	op := operationFixture()
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	changed := op.Clone()
	changed.CallID = "another-call-with-same-request"
	changedDigest, err := changed.Digest()
	if err != nil || od == changedDigest || op.RequestDigest != changed.RequestDigest {
		t.Fatalf("whole operation hash did not distinguish equal-body calls: %v", err)
	}
	left, right := f.proxy.Clone(), f.proxy.Clone()
	left.OperationDigest, right.OperationDigest = od, changedDigest
	ld, err := left.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rd, err := right.Digest()
	if err != nil || ld == rd {
		t.Fatalf("observation sealed only request bytes rather than operation identity: %v", err)
	}
}

func TestRegisteredProxyV2RejectsOriginalUntaggedV1Bytes(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	raw, err := domain.EncodeRegisteredProxyAttestationV2(f.proxy)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "transport_kind")
	delete(fields, "operation_sequence")
	delete(fields, "operation_digest")
	delete(fields, "pinned_https")
	fields["version"] = json.RawMessage(`1`)
	for name, value := range map[string]string{"resolution_set_digest": f.proxy.PinnedHTTPS.ResolutionSetDigest, "proxy_artifact_digest": f.proxy.PinnedHTTPS.ProxyArtifactDigest, "proxy_identity_digest": f.proxy.PinnedHTTPS.ProxyIdentityDigest} {
		fields[name], err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	legacy, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeRegisteredProxyAttestationV2(legacy)
	if err == nil || !reflect.DeepEqual(decoded, domain.RegisteredProxyAttestationV2{}) {
		t.Fatal("V1 serving fallback decoded untagged historical record")
	}
}
