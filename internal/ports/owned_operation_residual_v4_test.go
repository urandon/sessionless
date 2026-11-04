package ports_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Structural historical DATA only. No actual Source publication, human
// decision, TLS socket, observed send, erasure or finalizer is claimed here.
func residualV4Fixture(t *testing.T, searches int) ports.OwnedOperationResidualInventoryV4 {
	t.Helper()
	parent, source, rows, accounting := accountingV4Fixture(t, searches)
	a, sub := parent.Authority, parent.Authority.SubstrateBinding
	h := accountingV3Hash("allocation-v4-data")
	runtime := &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: a.HarnessBinding.Backend.BackendProfileDigest, InstallationID: sub.LocalInstallationID, InstallationRevision: sub.LocalInstallationRevision, WorkerExecutableDigest: sub.OuterHarnessArtifactDigest, HostIdentityDigest: sub.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: sub.LocalNetworkNamespaceIdentityDigest, Limits: domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 64 << 10, Messages: 256, Tools: 16, MaxInputTokens: 32768, MaxOutputTokens: 2048, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second}}
	subDigest, _ := sub.Digest()
	prepared := domain.PreparedAllocationV2{Version: 2, SubstrateBindingDigest: subDigest, ObservedOuterHarnessDigest: sub.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: sub.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: sub.EgressProxyIdentityDigest, WorkloadMode: sub.WorkloadMode, LocalRuntime: runtime}
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: parent.Reservation.ReservedAt, PublishedAt: parent.Reservation.ReservedAt, ExpiresAt: a.InvocationDeadline}
	allocation, err := domain.BuildRegisteredAllocationV2(a, parent.Reservation, prepared, domain.PreparedInvocationDigestV1(h), parent.PreparationExpiresAt, source, "allocation-v4", 1, window)
	if err != nil {
		t.Fatal("allocation DATA", err)
	}
	retired := allocation.Clone()
	retired.State, retired.RevocationGeneration = domain.RegisteredPreparationRetiredV1, retired.RevocationGeneration+1
	retired.UpdatedAt = rows[len(rows)-1].Observation.ObservedAt.Add(time.Microsecond)
	v := ports.OwnedOperationResidualInventoryV4{Version: 4, Parent: parent, Source: source, OriginalAllocation: allocation, RetiredAllocation: retired, AccountingDisposition: accounting, PhysicalInventoryDigest: h}
	v.AccountingDisposition.Operations = nil
	ad, _ := allocation.Digest()
	for i, item := range rows {
		r, o := item.Reservation.Clone(), item.Observation.Clone()
		r.Operation.RequestRef = "opbody-v1." + strconv.FormatUint(r.Operation.RequestBytes, 10) + "." + r.Operation.RequestDigest
		o.ResponseRef = "opbody-v1." + strconv.FormatUint(o.ResponseBytes, 10) + "." + o.ResponseDigest
		if i > 0 {
			r.Operation.PredecessorObservationDigest, _ = v.Operations[i-1].Observation.Digest()
		}
		if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			op := &r.Operation
			scope := op.Scope
			args, err := domain.AttemptOperationConsentArgumentsRefV1(scope, op.SearchWire.ArgumentsBytes, op.ArgumentsDigest)
			if err != nil {
				t.Fatal(err)
			}
			request := domain.WebSearchConsentRequestV1{Version: 1, ID: domain.WebSearchConsentRequestIDV1(op.ConsentDecisionRef), Scope: domain.WebSearchConsentScopeV1{TenantID: scope.TenantID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID, RunID: scope.RunID, AttemptID: scope.AttemptID, LeaseID: scope.LeaseID, FenceToken: scope.FenceToken, PhysicalInvocationClaimID: scope.PhysicalInvocationClaimID, EffectReservationDigest: string(scope.EffectReservationDigest), InvocationAuthorityDigest: string(scope.InvocationAuthorityDigest), HarnessBindingDigest: string(scope.HarnessBindingDigest), PolicyDigest: string(scope.AgentLoopPolicyDigest)}, InitiatingUserID: scope.OwnerUserID, ModelResponseDigest: op.ModelResponseDigest, CallID: op.CallID, ArgumentsDigest: op.ArgumentsDigest, QueryDigest: op.QueryDigest, GrantDigest: op.ToolGrantDigest, ConsentRevision: 1, RevocationGeneration: 1, Arguments: args, CreatedAt: v.Operations[i-1].Observation.ObservedAt.Add(time.Microsecond), ExpiresAt: op.Deadline}
			rd, err := request.Digest()
			if err != nil {
				t.Fatal("consent request DATA", err)
			}
			decision := domain.WebSearchConsentDecisionV1{Version: 1, Request: request, RequestDigest: rd, RequestRevision: 1, ActorUserID: request.InitiatingUserID, WebSessionDigest: domain.SecretDigest(h), MembershipSecurityVersion: 1, Choice: domain.WebSearchConsentApproveV1, ObservedAt: request.CreatedAt.Add(time.Microsecond)}
			dd, err := decision.Digest()
			if err != nil {
				t.Fatal("consent decision DATA", err)
			}
			op.ConsentRequestDigest, op.ConsentDecisionDigest = string(rd), string(dd)
			v.Consents = append(v.Consents, domain.WebSearchConsentStateV1{Request: request, Revision: 2, Status: domain.WebSearchConsentApprovedV1, Decision: &decision, UpdatedAt: decision.ObservedAt})
			v.Bodies = append(v.Bodies, args)
		}
		r.OperationDigest, err = r.Operation.Digest()
		if err != nil {
			t.Fatal(err)
		}
		body := domain.OwnedAgentAuthorityBodyRefV1{Ref: "proxy-v4-data", Digest: h, Bytes: 32}
		var proxy domain.RegisteredProxyAttestationV2
		if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			observation := domain.RegisteredLocalSearchHTTPSProxyV1{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: string(domain.RegisteredProxyTransportLocalSearchHTTPSV1), ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, ResolutionSetDigest: h}
			proxy, err = domain.BuildRegisteredLocalSearchHTTPSProxyAttestationV1(allocation, source, h, sub.EgressPolicyDigest, observation, body, body, window, r.Operation.Sequence, r.OperationDigest)
		} else {
			observation := domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, LiteralPeerDigest: h}
			proxy, err = domain.BuildRegisteredLocalLoopbackProxyAttestationV2(allocation, source, h, sub.EgressPolicyDigest, observation, body, body, window, r.Operation.Sequence, r.OperationDigest)
		}
		if err != nil {
			t.Fatal("proxy DATA", err)
		}
		r.RegisteredAllocationDigest = ad
		r.RegisteredProxyDigest, _ = proxy.Digest()
		o.OperationDigest, o.ReservationDigest = r.OperationDigest, ""
		o.ReservationDigest, err = r.Digest()
		if err != nil {
			t.Fatal(err)
		}
		row, err := ports.BuildOwnedOperationAccountingRowV4(r, o, parent, source)
		if err != nil {
			t.Fatal(err)
		}
		v.AccountingDisposition.Operations = append(v.AccountingDisposition.Operations, row)
		v.Operations = append(v.Operations, ports.OwnedOperationResidualRowV1{Reservation: r, Observation: o})
		v.Proxies = append(v.Proxies, ports.OwnedOperationResidualProxyV1{Sequence: r.Operation.Sequence, Original: proxy, Current: proxy.Clone()})
	}
	v.AccountingDisposition.ConsentStatesDigest, err = ports.OwnedSearchConsentStatesDigestV4(v.Consents)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(accounting.Scope)
	ns := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	prefix := domain.SessionRunObjectPrefix(accounting.Scope.TenantID, accounting.Scope.SessionID, accounting.Scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(ns[:]) + "/"
	for _, row := range v.Operations {
		for _, b := range []domain.BlobRef{{TenantID: accounting.Scope.TenantID, Key: prefix + row.Reservation.Operation.RequestRef, SHA256: row.Reservation.Operation.RequestDigest, Size: int64(row.Reservation.Operation.RequestBytes)}, {TenantID: accounting.Scope.TenantID, Key: prefix + row.Observation.ResponseRef, SHA256: row.Observation.ResponseDigest, Size: int64(row.Observation.ResponseBytes)}} {
			found := false
			for _, old := range v.Bodies {
				found = found || old.Key == b.Key
			}
			if !found {
				v.Bodies = append(v.Bodies, b)
			}
		}
	}
	if err := v.Validate(); err != nil {
		t.Fatal("valid residual structural baseline", err)
	}
	return v
}

func TestOwnedOperationResidualV4ConsentCustodyAndFrontiers(t *testing.T) {
	for _, count := range []int{1, 2} {
		v := residualV4Fixture(t, count)
		if _, err := v.Digest(); err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []string{"missing_arguments", "foreign_arguments", "pending", "decision", "scope", "frontier", "refund", "extra_body", "crossed_version", "proxy_kind"} {
			t.Run(strconv.Itoa(count)+"/"+mutation, func(t *testing.T) {
				bad := v.Clone()
				switch mutation {
				case "missing_arguments":
					bad.Bodies = bad.Bodies[1:]
				case "foreign_arguments":
					bad.Bodies[0].Key += "foreign"
				case "pending":
					bad.Consents[0].Status = domain.WebSearchConsentPendingV1
				case "decision":
					bad.Consents[0].Decision = nil
				case "scope":
					bad.Consents[0].Request.Scope.PhysicalInvocationClaimID = "foreign"
				case "frontier":
					bad.Operations[1].Reservation.AggregateDebit.BodyBytes--
				case "refund":
					bad.AccountingDisposition.ComponentAmounts[3]--
				case "extra_body":
					b := bad.Bodies[len(bad.Bodies)-1]
					b.Key += "unused"
					bad.Bodies = append(bad.Bodies, b)
				case "crossed_version":
					bad.AccountingDisposition.Version = 3
				case "proxy_kind":
					bad.Proxies[1].Original.ServiceKind = domain.RegisteredProxyMCPToolV1
				}
				if bad.Validate() == nil {
					t.Fatal("contradiction admitted", mutation)
				}
			})
		}
		encoded, err := v.IntentBytes()
		if err != nil {
			t.Fatal(err)
		}
		var decoded ports.OwnedOperationResidualInventoryV4
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Validate() != nil {
			t.Fatal("canonical DATA round trip", err)
		}
	}
}
