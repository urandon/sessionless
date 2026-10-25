package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Genuine original MODEL/MCP preparation, Store reserve/before-send/issuer,
// normalized body adapter and actual retained Source. Proxy observations,
// external responses and SQL are scripted DATA, not physical DNS/TLS/MCP proof.
// Returned holder has NOT primed a conservative witness; tests exercise that
// original producer separately, outside any SQL callback.
func completedOriginalMCPFixture(t *testing.T, prebuild ...localRuntimeActualPrebuild) (*compactionSourceFixture, *localPreparedMCPV1) {
	t.Helper()
	if len(prebuild) > 1 {
		t.Fatal("at most one pre-intake original MCP fixture declaration")
	}
	p := localRuntimeActualPrebuild{compiledCompaction: true, networkOperation: 5 * time.Second}
	if len(prebuild) == 1 {
		p = prebuild[0]
	}
	f, _ := localPreparedModelFixtureWithPrebuild(t, p)
	model, err := f.entry.PrepareModel(t.Context())
	if err != nil {
		t.Fatal("original MODEL preparation", err)
	}
	recordPreparedLocalToolCall(t, f, model.operation, model.frozen)
	m, err := f.entry.PrepareMCP(t.Context())
	if err != nil || m == nil {
		t.Fatal("original MCP preparation", err)
	}
	op := m.operation
	at := f.parent.graph.at
	policy, err := ports.BuildLocalPublicMCPHTTPSProxyPolicyV1(f.actual.config, m.grant)
	if err != nil {
		t.Fatal(err)
	}
	pd, _ := policy.Digest()
	runtime := f.entry.allocation.LocalRuntime
	att := ports.LocalPublicMCPHTTPSProxyAttestationV1{Version: 1, PolicyDigest: pd, Observation: domain.RegisteredLocalPublicMCPHTTPSProxyV1{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, ResolutionSetDigest: strings.Repeat("9", 64), MCPProfileDigest: policy.MCPProfileDigest, GrantDigest: policy.GrantDigest, DescriptorDigest: policy.DescriptorDigest}, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "developers.openai.com", ResolvedHost: "developers.openai.com", ResolvedPort: 443, ObservedAt: at, ExpiresAt: op.Deadline}
	pb, _ := json.Marshal(policy)
	ab, _ := json.Marshal(att)
	bodies, err := (ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1, ServiceKind: domain.RegisteredProxyMCPToolV1, ObservedAt: at, PolicyBody: pb, AttestationBody: ab}).Bodies()
	if err != nil {
		t.Fatal(err)
	}
	od, _ := op.Digest()
	proxy, err := buildOwnedProxyRecord(bodies, f.entry.allocation, f.entry.source, domain.RegisteredProxyMCPToolV1, pb, ab, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: op.Deadline}, op.Sequence, od)
	if err != nil {
		t.Fatal("scripted proxy DATA", err)
	}
	raw, _ := domain.EncodeRegisteredProxyAttestationV2(proxy)
	digest, _ := proxy.Digest()
	proxyRow := []driver.Value{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), string(raw), string(digest), string(raw), string(digest), string(pb), string(ab)}
	f.queries.proxies = append(f.queries.proxies, append(append([]driver.Value{op.Sequence}, proxyRow...), string(domain.RegisteredProxyMCPToolV1)))
	r, err := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || r.Capability == nil {
		t.Fatal("actual original MCP reserve", err)
	}
	if err = f.store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); err != nil {
		t.Fatal("actual original MCP BeforeSend", err)
	}
	wire, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": m.call.ID(), "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": "bounded original result"}}, "isError": false}})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := m.request.DecodeToolResultV1(wire)
	if err != nil {
		t.Fatal(err)
	}
	usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
	normalized, err := mcpadmission.EncodeToolResultV1(m.call, op, parsed, usage)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(normalized)
	ref, err := f.entry.bodies.Write(t.Context(), normalized)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Reservation.Digest()
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: at, Usage: usage}
	auth, err := f.bootstrap.operate.AuthenticateAttemptOperationObservation(r.ObservationGrant, o)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: r.ObservationGrant, Observation: auth}); err != nil {
		t.Fatal("actual original MCP durable normalized observation", err)
	}
	readback, err := f.store.ReadAttemptOperationObservation(t.Context(), r.ObservationGrant)
	if err != nil || !reflect.DeepEqual(readback, auth) {
		t.Fatal("actual original MCP observation readback", err)
	}
	if !reflect.DeepEqual(m.observation, auth) {
		t.Fatal("original holder did not remember exact authenticated durable observation")
	}
	return f, m
}

func primeOriginalMCPConservative(t *testing.T, f *compactionSourceFixture) {
	t.Helper()
	ctx, finish := ownedAuthorityCallContext(t.Context())
	defer finish()
	f.entry.preparationMu.Lock()
	defer f.entry.preparationMu.Unlock()
	if err := f.entry.prepareMCPConservativeLocked(ctx); err != nil {
		t.Fatal("original conservative witness", err)
	}
}

func TestLocalMCPConservativeOriginalWitnessAndHistoricalData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, m := completedOriginalMCPFixture(t)
		r, o := m.reservation, m.observation.Observation
		if ownedObservationAllowsSuccessor(r, o) || f.entry.originalObservationAllowsSuccessor(r, o) {
			t.Fatal("unknown money was generically permitted or witness manufactured before original body verification")
		}
		primeOriginalMCPConservative(t, f)
		if !f.entry.originalObservationAllowsSuccessor(r, o) || ownedObservationAllowsSuccessor(r, o) {
			t.Fatal("narrow original witness failed or generic gate changed")
		}
		if err := f.entry.revalidateMCPConservativeFunding(t.Context(), f.parent.graph.at); err != nil {
			t.Fatal("same actual Source funding", err)
		}
		if err := f.entry.revalidateMCPConservativeFunding(t.Context(), f.entry.source.Spending[2].Window.ExpiresAt); err == nil {
			t.Fatal("original funding expiry was renewed")
		}
		w := m.conservative.Load()
		if _, err := json.Marshal(w); err == nil {
			t.Fatal("private witness serialized")
		}
		source, err := f.entry.CaptureCompactionSource(t.Context())
		if err != nil {
			t.Fatal("original current snapshot and durable MODEL/MCP bodies", err)
		}
		defer source.Clear()
		if len(source.state.operations) != 2 || !reflect.DeepEqual(source.state.operations[1].Reservation, r) || !reflect.DeepEqual(source.state.operations[1].Observation, o) {
			t.Fatal("original snapshot did not retain the exact MODEL/MCP pair")
		}
		data, err := f.entry.mcpConservativeDataFor(source.state.operations)
		if err != nil || len(data) != 1 || data[0].Reserved != r.Operation.Debit || data[0].MCPSpendingDigest == "" || o.Usage.AmountMicrounits != nil {
			t.Fatal("truthful exact context DATA", err)
		}
		data[0].MCPSpendingDigest = strings.Repeat("f", 64)
		if m.conservative.Load().data.MCPSpendingDigest == data[0].MCPSpendingDigest {
			t.Fatal("projection changed original witness")
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		if !f.entry.originalObservationAllowsSuccessor(r, o) {
			t.Fatal("exact private historical cleanup facts were lost with active Source")
		}
		if err := f.entry.revalidateMCPConservativeFunding(t.Context(), f.parent.graph.at); err == nil {
			t.Fatal("historical witness authorized a current effect after Source close")
		}
	})
}

func TestLocalMCPConservativeRejectsMissingOrCorruptDurableBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, m := completedOriginalMCPFixture(t)
		for _, ref := range []string{m.observation.Observation.ResponseRef, m.operation.RequestRef} {
			var key string
			for candidate := range f.blobs.objects {
				if strings.HasSuffix(candidate, "/"+ref) {
					if key != "" {
						t.Fatal("nonunique exact operation body")
					}
					key = candidate
				}
			}
			if key == "" {
				t.Fatal("exact durable operation body missing from fixture")
			}
			original := f.blobs.objects[key]
			delete(f.blobs.objects, key)
			for _, mode := range []string{"missing", "corrupt"} {
				if mode == "corrupt" {
					f.blobs.objects[key] = []byte("corrupted immutable operation body")
				}
				ctx, finish := ownedAuthorityCallContext(t.Context())
				f.entry.preparationMu.Lock()
				err := f.entry.prepareMCPConservativeLocked(ctx)
				f.entry.preparationMu.Unlock()
				finish()
				if err == nil || m.conservative.Load() != nil {
					t.Fatal("undurable operation body created original witness", mode, ref)
				}
			}
			f.blobs.objects[key] = original
		}
	})
}

func TestLocalMCPConservativePreservesOriginalReadBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, m := completedOriginalMCPFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if err := chargeOwnedAuthorityBody(ctx, ownedAuthorityMaxBytes); err != nil {
			t.Fatal("exhaust original read budget", err)
		}
		opened := f.canonical.opened
		f.entry.preparationMu.Lock()
		err := f.entry.prepareMCPConservativeLocked(ctx)
		f.entry.preparationMu.Unlock()
		if err == nil || m.conservative.Load() != nil || f.canonical.opened != opened || ctx.Value(ownedAuthorityCallContextKey{}) != budget {
			t.Fatalf("original read budget replaced: err=%v witness=%v opened=%d want=%d", err, m.conservative.Load() != nil, f.canonical.opened, opened)
		}
		cancelled, stop := context.WithCancel(t.Context())
		stop()
		f.entry.preparationMu.Lock()
		err = f.entry.prepareMCPConservativeLocked(cancelled)
		f.entry.preparationMu.Unlock()
		if err == nil || m.conservative.Load() != nil || f.canonical.opened != opened {
			t.Fatalf("cancelled read created witness: err=%v witness=%v opened=%d want=%d", err, m.conservative.Load() != nil, f.canonical.opened, opened)
		}
	})
}

func TestLocalMCPConservativeRejectsReboundWitnessAndRows(t *testing.T) {
	for _, name := range []string{"observation", "reservation", "holder_copy", "witness_copy", "spending", "call", "unknown_outcome", "issuer"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, m := completedOriginalMCPFixture(t)
				primeOriginalMCPConservative(t, f)
				r, o := m.reservation.Clone(), m.observation.Observation.Clone()
				switch name {
				case "observation":
					o.ResponseDigest = strings.Repeat("e", 64)
				case "reservation":
					r.Operation.Scope.SessionID = "foreign"
				case "holder_copy":
					copy := *m
					f.entry.preparedMCP.Store(&copy)
				case "witness_copy":
					copy := *m.conservative.Load()
					m.conservative.Store(&copy)
				case "spending":
					m.conservative.Load().data.MCPSpendingDigest = strings.Repeat("e", 64)
				case "call":
					m.operation.CallID = "other"
				case "unknown_outcome":
					o.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
					o.Reason = domain.AttemptOperationReasonTransportUnknownV1
					o.ResponseRef = ""
					o.ResponseDigest = ""
					o.ResponseBytes = 0
				case "issuer":
					m.conservative.Load().observed.Authenticator[0] ^= 1
				}
				if f.entry.originalObservationAllowsSuccessor(r, o) {
					t.Fatal("rebound original witness accepted", name)
				}
			})
		})
	}
}
