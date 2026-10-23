package ports_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func residualV3Fixture(t *testing.T, amount uint64) ports.OwnedOperationResidualInventoryV3 {
	t.Helper()
	parent, source, rows, accounting := accountingV3Fixture(t, amount)
	a := parent.Authority
	h := accountingV3Hash("allocation-data")
	sub := a.SubstrateBinding
	runtime := &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: a.HarnessBinding.Backend.BackendProfileDigest, InstallationID: sub.LocalInstallationID, InstallationRevision: sub.LocalInstallationRevision, WorkerExecutableDigest: sub.OuterHarnessArtifactDigest, HostIdentityDigest: sub.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: sub.LocalNetworkNamespaceIdentityDigest, Limits: domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 64 << 10, Messages: 256, Tools: 16, MaxInputTokens: 32768, MaxOutputTokens: 2048, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second}}
	subDigest, _ := sub.Digest()
	prepared := domain.PreparedAllocationV2{Version: 2, SubstrateBindingDigest: subDigest, ObservedOuterHarnessDigest: sub.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: sub.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: sub.EgressProxyIdentityDigest, WorkloadMode: sub.WorkloadMode, LocalRuntime: runtime}
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: parent.Reservation.ReservedAt, PublishedAt: parent.Reservation.ReservedAt, ExpiresAt: a.InvocationDeadline}
	allocation, err := domain.BuildRegisteredAllocationV2(a, parent.Reservation, prepared, domain.PreparedInvocationDigestV1(h), parent.PreparationExpiresAt, source, "allocation-v3", 1, window)
	if err != nil {
		t.Fatal("allocation structural DATA", err)
	}
	retired := allocation.Clone()
	retired.State = domain.RegisteredPreparationRetiredV1
	retired.RevocationGeneration++
	retired.UpdatedAt = rows[len(rows)-1].Observation.ObservedAt.Add(time.Microsecond)
	v := ports.OwnedOperationResidualInventoryV3{Version: 3, Parent: parent, Source: source, OriginalAllocation: allocation, RetiredAllocation: retired, AccountingDisposition: accounting, PhysicalInventoryDigest: h}
	v.AccountingDisposition.Operations = nil
	ad, _ := allocation.Digest()
	for index, item := range rows {
		r, o := item.Reservation.Clone(), item.Observation.Clone()
		r.Operation.RequestRef = "opbody-v1." + strconv.FormatUint(r.Operation.RequestBytes, 10) + "." + r.Operation.RequestDigest
		o.ResponseRef = "opbody-v1." + strconv.FormatUint(o.ResponseBytes, 10) + "." + o.ResponseDigest
		if index > 0 {
			r.Operation.PredecessorObservationDigest, _ = v.Operations[index-1].Observation.Digest()
		}
		r.OperationDigest, err = r.Operation.Digest()
		if err != nil {
			t.Fatal(err)
		}
		body := domain.OwnedAgentAuthorityBodyRefV1{Ref: "proxy-data", Digest: h, Bytes: 32}
		var proxy domain.RegisteredProxyAttestationV2
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			observation := domain.RegisteredLocalPublicMCPHTTPSProxyV1{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: domain.ConfiguredLocalPublicMCPHTTPSPolicyV1, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, ResolutionSetDigest: h, MCPProfileDigest: h, GrantDigest: source.Spending[2].ScopedGrantDigest, DescriptorDigest: h}
			proxy, err = domain.BuildRegisteredLocalPublicMCPHTTPSProxyAttestationV1(allocation, source, h, sub.EgressPolicyDigest, observation, body, body, window, r.Operation.Sequence, r.OperationDigest)
		} else {
			observation := domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, LiteralPeerDigest: h}
			proxy, err = domain.BuildRegisteredLocalLoopbackProxyAttestationV2(allocation, source, h, sub.EgressPolicyDigest, observation, body, body, window, r.Operation.Sequence, r.OperationDigest)
		}
		if err != nil {
			t.Fatal("proxy structural DATA", index, err)
		}
		r.RegisteredAllocationDigest = ad
		r.RegisteredProxyDigest, _ = proxy.Digest()
		o.OperationDigest = r.OperationDigest
		o.ReservationDigest, err = r.Digest()
		if err != nil {
			t.Fatal(err)
		}
		row, e := ports.BuildOwnedOperationAccountingRowV3(r, o, parent, source)
		if e != nil {
			t.Fatal(e)
		}
		v.AccountingDisposition.Operations = append(v.AccountingDisposition.Operations, row)
		v.Operations = append(v.Operations, ports.OwnedOperationResidualRowV1{Reservation: r, Observation: o})
		v.Proxies = append(v.Proxies, ports.OwnedOperationResidualProxyV1{Sequence: r.Operation.Sequence, Original: proxy, Current: proxy.Clone()})
	}
	raw, err := json.Marshal(accounting.Scope)
	if err != nil {
		t.Fatal(err)
	}
	ns := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	prefix := domain.SessionRunObjectPrefix(accounting.Scope.TenantID, accounting.Scope.SessionID, accounting.Scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(ns[:]) + "/"
	for _, row := range v.Operations {
		for _, b := range []domain.BlobRef{{TenantID: accounting.Scope.TenantID, Key: prefix + row.Reservation.Operation.RequestRef, SHA256: row.Reservation.Operation.RequestDigest, Size: int64(row.Reservation.Operation.RequestBytes)}, {TenantID: accounting.Scope.TenantID, Key: prefix + row.Observation.ResponseRef, SHA256: row.Observation.ResponseDigest, Size: int64(row.Observation.ResponseBytes)}} {
			found := false
			for _, old := range v.Bodies {
				if old.Key == b.Key {
					found = true
				}
			}
			if !found {
				v.Bodies = append(v.Bodies, b)
			}
		}
	}
	if err = v.Validate(); err != nil {
		t.Fatal("residual structural DATA", err)
	}
	return v
}

func TestOwnedOperationResidualV3TruthfulUnknownAndClone(t *testing.T) {
	for _, amount := range []uint64{0, 17} {
		t.Run(strconv.FormatUint(amount, 10), func(t *testing.T) {
			v := residualV3Fixture(t, amount)
			digest, err := v.Digest()
			if err != nil {
				t.Fatal(err)
			}
			body, err := v.IntentBytes()
			if err != nil {
				t.Fatal(err)
			}
			var decoded ports.OwnedOperationResidualInventoryV3
			if err = json.Unmarshal(body, &decoded); err != nil || decoded.Validate() != nil {
				t.Fatal("exact DATA roundtrip", err)
			}
			if again, err := decoded.Digest(); err != nil || again != digest {
				t.Fatal("digest roundtrip", err)
			}
			clone := v.Clone()
			clone.Proxies[1].Original.LocalPublicMCPHTTPS.DescriptorDigest = accountingV3Hash("changed")
			*clone.Source.Spending[2].MaxAmountMicrounits = 99
			clone.Operations[0].Observation.Usage.InputTokens = accountingV3Amount(1)
			clone.AccountingDisposition.Operations[0].Usage.InputTokens = accountingV3Amount(1)
			clone.Bodies[0].Key = "foreign"
			if reflect.DeepEqual(clone.Proxies[1].Original, v.Proxies[1].Original) || *v.Source.Spending[2].MaxAmountMicrounits != amount || v.Operations[0].Observation.Usage.InputTokens != nil || v.AccountingDisposition.Operations[0].Usage.InputTokens != nil || v.Bodies[0].Key == "foreign" {
				t.Fatal("clone aliases original")
			}
			old := ports.OwnedOperationResidualInventoryV2{Version: 2, Parent: v.Parent, Source: v.Source, OriginalAllocation: v.OriginalAllocation, RetiredAllocation: v.RetiredAllocation, Operations: v.Operations, Proxies: v.Proxies, Bodies: v.Bodies, PhysicalInventoryDigest: v.PhysicalInventoryDigest}
			old.AccountingDisposition = ports.OwnedOperationAccountingDispositionV2{Version: 2, Scope: v.AccountingDisposition.Scope, AccountingDigest: v.AccountingDisposition.AccountingDigest, AggregateDebit: v.AccountingDisposition.AggregateDebit, ComponentAmounts: v.AccountingDisposition.ComponentAmounts, ProviderInvoice: v.AccountingDisposition.ProviderInvoice}
			for _, r := range v.Operations {
				row, e := ports.BuildOwnedOperationAccountingRowV2(r.Reservation, r.Observation)
				if e == nil {
					old.AccountingDisposition.Operations = append(old.AccountingDisposition.Operations, row)
				}
			}
			if old.Validate() == nil {
				t.Fatal("V2 accepted new unknown MCP path")
			}
		})
	}
}

func TestOwnedOperationResidualV3RejectsMissingOrForeignEvidence(t *testing.T) {
	v := residualV3Fixture(t, 17)
	for _, name := range []string{"version", "active", "physical", "body_missing", "body_duplicate", "body_foreign", "body_hash", "proxy_missing", "proxy_foreign", "proxy_transport", "history_missing", "predecessor", "observation", "consent", "accounting", "scope", "rows"} {
		t.Run(name, func(t *testing.T) {
			bad := v.Clone()
			switch name {
			case "version":
				bad.Version = 2
			case "active":
				bad.RetiredAllocation = bad.OriginalAllocation
			case "physical":
				bad.PhysicalInventoryDigest = ""
			case "body_missing":
				bad.Bodies = bad.Bodies[:1]
			case "body_duplicate":
				bad.Bodies = append(bad.Bodies, bad.Bodies[0])
			case "body_foreign":
				bad.Bodies[0].TenantID = "other"
			case "body_hash":
				bad.Bodies[0].SHA256 = accountingV3Hash("other")
			case "proxy_missing":
				bad.Proxies = bad.Proxies[:2]
			case "proxy_foreign":
				bad.Proxies[1].Original.GrantDigest = accountingV3Hash("other")
			case "proxy_transport":
				bad.Proxies[1].Original.TransportKind = domain.RegisteredProxyTransportPinnedHTTPSV2
			case "history_missing":
				bad.Operations = bad.Operations[1:]
			case "predecessor":
				bad.Operations[2].Reservation.Operation.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(accountingV3Hash("other"))
			case "observation":
				bad.Operations[1].Observation.ResponseDigest = accountingV3Hash("other")
			case "consent":
				bad.Consents = []domain.WebSearchConsentStateV1{{}}
			case "accounting":
				bad.AccountingDisposition.ComponentAmounts[2]--
			case "scope":
				bad.Source.Scope.SessionID = "other"
			case "rows":
				bad.Operations = append(bad.Operations, make([]ports.OwnedOperationResidualRowV1, 13)...)
			}
			if err := bad.Validate(); err == nil {
				t.Fatal("invalid residual accepted", name)
			}
			if _, err := bad.IntentBytes(); err == nil {
				t.Fatal("invalid residual encoded", name)
			}
			if _, err := bad.Digest(); err == nil {
				t.Fatal("invalid residual digested", name)
			}
		})
	}
}
