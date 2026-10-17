package ports_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Pure canonical DATA fixtures: no installer, network, real peer, signature,
// Store/current authority or actual producer permission is established here.
func loopbackPolicyConfig() domain.ConfiguredInferenceProfileV1 {
	hash := strings.Repeat("1", 64)
	return domain.ConfiguredInferenceProfileV1{BaseURL: "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1,
		WorkerExecutableDigest: hash, LocalHostIdentityDigest: hash, LocalNetworkNamespaceIdentityDigest: hash,
		Limits: domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 1024, MaxOutputTokens: 1024, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second}}
}
func localProxyPublication(t *testing.T) ports.OwnedProxyPublicationV2 {
	t.Helper()
	p := taggedProxyPublicationFixture(t)
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(loopbackPolicyConfig())
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := policy.Digest()
	peer, _ := policy.LiteralPeerDigest()
	att := ports.LocalLoopbackEgressAttestationV1{Version: 1, PolicyDigest: digest, ObservedAt: p.ObservedAt, ExpiresAt: p.ObservedAt.Add(20 * time.Second), Observation: domain.RegisteredLocalLoopbackProxyV2{
		ConfiguredProfileDigest: strings.Repeat("2", 64), EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: policy.ConnectorArtifactDigest, InstallationID: "synthetic-installation", InstallationRevision: 1, HostIdentityDigest: policy.HostIdentityDigest, NetworkNamespaceIdentityDigest: policy.NetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}}
	p.TransportKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2
	p.OperationDeadline = att.ExpiresAt
	p.PolicyBody, err = json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	p.AttestationBody, err = json.Marshal(att)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("pure local publication data: %v", err)
	}
	return p
}

func TestLocalLoopbackPolicyAcyclicPreMintAndExactIndependentDigests(t *testing.T) {
	c := loopbackPolicyConfig()
	p, err := ports.BuildLocalLoopbackEgressPolicyV1(c)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := p.Digest()
	transport, _ := p.TransportDigest()
	peer, _ := p.LiteralPeerDigest()
	// Independent literal oracle: no scheme, /v1 suffix, normalization or DNS.
	wantPeer := sha256.Sum256([]byte("sessionless.local-loopback-literal-peer.v1\x00" + `"127.0.0.1:11434"`))
	if peer != hex.EncodeToString(wantPeer[:]) {
		t.Fatal("literal peer digest did not seal exact canonical host:port")
	}
	c.Resource.ResourceID = "fresh-id"
	c.Resource.Revision = 8
	c.ProfileRevision = 8
	c.LocalInstallationID = "fresh-installation"
	c.LocalInstallationRevision = 8
	c.ModelID = "another-model"
	c.ExpiresAt = time.Date(2026, 10, 17, 1, 0, 0, 0, time.UTC)
	again, err := ports.BuildLocalLoopbackEgressPolicyV1(c)
	if err != nil || again != p {
		t.Fatalf("static policy became dependent on post-mint configuration: %v", err)
	}
	if digest == transport || digest == peer || peer == transport {
		t.Fatal("independent digest namespaces collapsed")
	}
	raw, _ := json.Marshal(p)
	for _, denied := range []string{"resource", "installation", "profile_digest", "expires_at", "tls", "dns"} {
		if bytes.Contains(raw, []byte(denied)) {
			t.Errorf("acyclic policy acquired dynamic/fabricated field %q", denied)
		}
	}
	for name, change := range map[string]func(*ports.LocalLoopbackEgressPolicyV1){
		"endpoint":  func(v *ports.LocalLoopbackEgressPolicyV1) { v.BaseURL = "http://[::1]:11434/v1" },
		"artifact":  func(v *ports.LocalLoopbackEgressPolicyV1) { v.ConnectorArtifactDigest = strings.Repeat("3", 64) },
		"host":      func(v *ports.LocalLoopbackEgressPolicyV1) { v.HostIdentityDigest = strings.Repeat("3", 64) },
		"namespace": func(v *ports.LocalLoopbackEgressPolicyV1) { v.NetworkNamespaceIdentityDigest = strings.Repeat("3", 64) },
		"auth_data": func(v *ports.LocalLoopbackEgressPolicyV1) { v.Auth = domain.ConfiguredInferenceAuthBearerV1 },
		"limit":     func(v *ports.LocalLoopbackEgressPolicyV1) { v.Limits.JSONMembers-- },
	} {
		t.Run(name, func(t *testing.T) {
			v := p
			change(&v)
			d, err := v.Digest()
			if err != nil || d == digest {
				t.Fatalf("independent %s fact not sealed: %v", name, err)
			}
		})
	}
	for _, endpoint := range []string{"http://localhost:11434/v1", "http://127.0.0.1:011434/v1", "http://127.0.0.1:11434/v1/", "https://127.0.0.1:11434/v1", "http://169.254.169.254:80/v1"} {
		v := p
		v.BaseURL = endpoint
		if v.Validate() == nil {
			t.Errorf("normalized/unsafe endpoint accepted: %q", endpoint)
		}
	}
	for _, change := range []func(*ports.LocalLoopbackEgressPolicyV1){func(v *ports.LocalLoopbackEgressPolicyV1) { v.Version = 2 }, func(v *ports.LocalLoopbackEgressPolicyV1) { v.Auth = "ambient" }, func(v *ports.LocalLoopbackEgressPolicyV1) { v.EndpointPolicy = "https" }, func(v *ports.LocalLoopbackEgressPolicyV1) { v.NativeProtocolVersion = "responses" }, func(v *ports.LocalLoopbackEgressPolicyV1) { v.Limits.RequestBytes = 0 }, func(v *ports.LocalLoopbackEgressPolicyV1) { v.HostIdentityDigest = "bad" }} {
		v := p
		change(&v)
		if v.Validate() == nil {
			t.Error("malformed static policy accepted")
		}
	}
}

func TestLocalLoopbackPublicationTaggedBodiesAndIntent(t *testing.T) {
	p := localProxyPublication(t)
	b, err := p.Bodies()
	if err != nil || b.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 || b.LocalLoopback == nil || b.PinnedHTTPS != nil {
		t.Fatalf("closed local union: %v", err)
	}
	d, err := b.PolicyDigest()
	if err != nil || d != b.LocalLoopback.Attestation.PolicyDigest || !b.ExpiresAt().Equal(p.OperationDeadline) {
		t.Fatal("typed bodies lost policy/expiry")
	}
	intent, err := p.IntentBytes()
	if err != nil {
		t.Fatal(err)
	}
	cloned := p.Clone()
	cloned.PolicyBody[0] = 'x'
	cloned.AttestationBody[0] = 'x'
	cloned.Authenticator[0]++
	b.LocalLoopback.Policy.BaseURL = "http://[::1]:11434/v1"
	b.LocalLoopback.Attestation.Observation.InstallationRevision++
	again, err := p.IntentBytes()
	if err != nil || !bytes.Equal(intent, again) {
		t.Fatal("cloned bytes/decoded union alias original publication")
	}
	for _, change := range []func(*ports.OwnedProxyPublicationV2){func(v *ports.OwnedProxyPublicationV2) { v.OperationSequence++ }, func(v *ports.OwnedProxyPublicationV2) {
		v.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("3", 64))
	}, func(v *ports.OwnedProxyPublicationV2) { v.SourceDigest = strings.Repeat("3", 64) }, func(v *ports.OwnedProxyPublicationV2) { v.Scope.OwnerUserID = "different-owner" }} {
		v := p.Clone()
		change(&v)
		raw, err := v.IntentBytes()
		if err != nil || bytes.Equal(raw, intent) {
			t.Fatalf("outer original operation/source/scope not sealed: %v", err)
		}
	}
	https := taggedProxyPublicationFixture(t)
	hb, err := https.Bodies()
	if err != nil || hb.PinnedHTTPS == nil || hb.LocalLoopback != nil || hb.Validate() != nil {
		t.Fatalf("existing HTTPS body semantics regressed: %v", err)
	}
	for _, u := range []ports.OwnedProxyBodiesV2{{}, {TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2}, {TransportKind: domain.RegisteredProxyTransportPinnedHTTPSV2, PinnedHTTPS: hb.PinnedHTTPS, LocalLoopback: &ports.OwnedLocalLoopbackBodiesV2{}}} {
		if u.Validate() == nil || !u.ExpiresAt().IsZero() {
			t.Error("missing/mixed union interpreted as a valid observation")
		}
		if _, err := u.PolicyDigest(); err == nil {
			t.Error("missing/mixed union yielded policy digest")
		}
	}
}

func TestLocalLoopbackAttestationAndCanonicalBodiesDenyMismatch(t *testing.T) {
	p := localProxyPublication(t)
	b, _ := p.Bodies()
	local := *b.LocalLoopback
	for name, change := range map[string]func(*ports.LocalLoopbackEgressAttestationV1){
		"version": func(v *ports.LocalLoopbackEgressAttestationV1) { v.Version = 2 }, "policy": func(v *ports.LocalLoopbackEgressAttestationV1) { v.PolicyDigest = strings.Repeat("3", 64) },
		"profile": func(v *ports.LocalLoopbackEgressAttestationV1) { v.Observation.ConfiguredProfileDigest = "bad" }, "installation": func(v *ports.LocalLoopbackEgressAttestationV1) { v.Observation.InstallationID = "" }, "revision": func(v *ports.LocalLoopbackEgressAttestationV1) { v.Observation.InstallationRevision = 0 },
		"artifact": func(v *ports.LocalLoopbackEgressAttestationV1) {
			v.Observation.ConnectorArtifactDigest = strings.Repeat("3", 64)
		}, "host": func(v *ports.LocalLoopbackEgressAttestationV1) {
			v.Observation.HostIdentityDigest = strings.Repeat("3", 64)
		}, "net": func(v *ports.LocalLoopbackEgressAttestationV1) {
			v.Observation.NetworkNamespaceIdentityDigest = strings.Repeat("3", 64)
		}, "peer": func(v *ports.LocalLoopbackEgressAttestationV1) {
			v.Observation.LiteralPeerDigest = strings.Repeat("3", 64)
		}, "endpoint": func(v *ports.LocalLoopbackEgressAttestationV1) { v.Observation.EndpointPolicy = "https" },
		"nano": func(v *ports.LocalLoopbackEgressAttestationV1) { v.ObservedAt = v.ObservedAt.Add(time.Nanosecond) }, "zero": func(v *ports.LocalLoopbackEgressAttestationV1) { v.ObservedAt = time.Time{} }, "exclusive": func(v *ports.LocalLoopbackEgressAttestationV1) { v.ExpiresAt = v.ObservedAt }, "unbounded": func(v *ports.LocalLoopbackEgressAttestationV1) { v.ExpiresAt = v.ObservedAt.Add(31 * time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			v := local.Attestation
			change(&v)
			if v.ValidateForPolicy(local.Policy) == nil {
				t.Fatalf("intrinsic %s mismatch accepted", name)
			}
		})
	}
	for name, change := range map[string]func(*ports.OwnedProxyPublicationV2){
		"unknown_policy": func(v *ports.OwnedProxyPublicationV2) {
			v.PolicyBody = bytes.Replace(v.PolicyBody, []byte(`"version":1`), []byte(`"version":1,"verified_tls":true`), 1)
		},
		"duplicate": func(v *ports.OwnedProxyPublicationV2) {
			v.PolicyBody = bytes.Replace(v.PolicyBody, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
		},
		"noncanonical": func(v *ports.OwnedProxyPublicationV2) { v.PolicyBody = append([]byte(" "), v.PolicyBody...) },
		"unknown_attestation": func(v *ports.OwnedProxyPublicationV2) {
			v.AttestationBody = bytes.Replace(v.AttestationBody, []byte(`"version":1`), []byte(`"version":1,"resolved_dns":"fiction"`), 1)
		},
		"oversized":     func(v *ports.OwnedProxyPublicationV2) { v.PolicyBody = bytes.Repeat([]byte{' '}, (16<<10)+1) },
		"mixed_tag":     func(v *ports.OwnedProxyPublicationV2) { v.TransportKind = domain.RegisteredProxyTransportPinnedHTTPSV2 },
		"search":        func(v *ports.OwnedProxyPublicationV2) { v.ServiceKind = domain.RegisteredProxyWebSearchV1 },
		"tool":          func(v *ports.OwnedProxyPublicationV2) { v.ServiceKind = domain.RegisteredProxyMCPToolV1 },
		"observed_time": func(v *ports.OwnedProxyPublicationV2) { v.ObservedAt = v.ObservedAt.Add(time.Microsecond) },
		"deadline": func(v *ports.OwnedProxyPublicationV2) {
			v.OperationDeadline = v.OperationDeadline.Add(time.Microsecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := p.Clone()
			change(&v)
			if v.Validate() == nil {
				t.Fatalf("%s publication accepted", name)
			}
			if got, err := v.Bodies(); name != "deadline" && (err == nil || !reflect.DeepEqual(got, ports.OwnedProxyBodiesV2{})) {
				t.Fatal("invalid body returned a partial union")
			}
		})
	}
}
