package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const ownedProxyRegistrationSQL = `SELECT
 CASE WHEN proxy_original_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN proxy_original_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN proxy_current_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN proxy_current_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN proxy_policy_body IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN proxy_attestation_body IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(proxy_original_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(proxy_original_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(proxy_current_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(proxy_current_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(proxy_policy_body,0,16385),CAST('' AS String)),
 COALESCE(SUBSTRING(proxy_attestation_body,0,16385),CAST('' AS String))
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

const ownedProxyRegistrationCreateSQL = `UPDATE attempt_effect_reservations
 SET proxy_original_record=$1,proxy_original_digest=$2,proxy_current_record=$1,proxy_current_digest=$2,
 proxy_policy_body=$3,proxy_attestation_body=$4
 WHERE tenant_id=$5 AND attempt_id=$6 AND kind=$7 AND owned_digest=$8
 AND record IS NULL AND allocation_current_digest=$9
 AND proxy_original_record IS NULL AND proxy_original_digest IS NULL
 AND proxy_current_record IS NULL AND proxy_current_digest IS NULL
 AND proxy_policy_body IS NULL AND proxy_attestation_body IS NULL`

const ownedOperationProxyRegistrationSQL = `SELECT
 CASE WHEN original_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN original_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN current_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN current_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN policy_body IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN attestation_body IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(original_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(original_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(current_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(current_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(policy_body,0,16385),CAST('' AS String)),
 COALESCE(SUBSTRING(attestation_body,0,16385),CAST('' AS String)),
 SUBSTRING(CAST(service_kind AS String),0,33)
 FROM attempt_operation_proxies_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND sequence=$3`

type ownedProxySnapshot struct {
	original, current           domain.RegisteredProxyAttestationV2
	policyBody, attestationBody []byte
}

type ownedProxyColumns struct {
	flags                                                                             [6]uint64
	originalRaw, originalDigest, currentRaw, currentDigest, policyRaw, attestationRaw string
	service                                                                           string
}

func (c *ownedProxyColumns) destinations(sequence uint64) []any {
	dest := []any{&c.flags[0], &c.flags[1], &c.flags[2], &c.flags[3], &c.flags[4], &c.flags[5], &c.originalRaw, &c.originalDigest, &c.currentRaw, &c.currentDigest, &c.policyRaw, &c.attestationRaw}
	if sequence > 1 {
		dest = append(dest, &c.service)
	}
	return dest
}

// PublishOwnedProxy preserves observation authenticated by the actual original
// transport producer. It is not registration by startup or permission to send. The
// ordinary Store has no installed owned source keeper and fails closed.
func (store *Store) PublishOwnedProxy(ctx context.Context, request ports.ReserveAttemptEffectRequestV1, proof ports.OwnedProxyPublicationV2) (domain.RegisteredProxyAttestationV2, bool, error) {
	deny := func() (domain.RegisteredProxyAttestationV2, bool, error) {
		return domain.RegisteredProxyAttestationV2{}, false, domain.ErrMembershipDenied
	}
	if !store.validOwnedAllocationContext(ctx) || request.Validate() != nil || store.ownedProxyPublicationVerifier == nil || proof.Validate() != nil {
		return deny()
	}
	// Serialize original local preparation/custody with this real publisher.
	// Nonlocal and the existing explicit preinstalled-body arm are unchanged.
	var localScope domain.AttemptOperationScopeV1
	if store.localBootstrap != nil {
		if entry := store.localBootstrap.entry.Load(); entry != nil {
			localScope = ownedOperationScope(entry.parent)
		}
	}
	unlock, err := store.lockLocalGenericMutation(ctx, localScope)
	if err != nil {
		return deny()
	}
	defer unlock()
	proof = proof.Clone()
	bodies, err := proof.Bodies()
	if err != nil || store.ownedProxyPublicationVerifier.VerifyOwnedProxyPublication(proof.Clone(), proof.ObservedAt) != nil {
		return deny()
	}
	allocationDigest, sourceDigest, observedAt, policyBody, attestationBody := proof.RegisteredAllocationDigest, proof.SourceDigest, proof.ObservedAt, proof.PolicyBody, proof.AttestationBody
	var result domain.RegisteredProxyAttestationV2
	var replayed bool
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result, replayed = domain.RegisteredProxyAttestationV2{}, false
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if store.ownedProxyPublicationVerifier.VerifyOwnedProxyPublication(proof.Clone(), q.at) != nil {
			return q.budget.deny()
		}
		parent, source, burnAt, err := store.readOwnedAllocationParent(ctx, q, request)
		if err != nil {
			return err
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || allocation == nil {
			return q.budget.deny()
		}
		ad, _ := allocation.original.Digest()
		sd, _ := source.Digest()
		pd, _ := bodies.PolicyDigest()
		// Preserve all previously admitted closed transport/service combinations.
		// Only the new local MCP arm has a distinct retained grant-bound policy.
		policyJoin := proof.TransportKind != domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 && pd == allocation.original.Substrate.EgressPolicyDigest
		if proof.ServiceKind == domain.RegisteredProxyMCPToolV1 && proof.TransportKind == domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 && bodies.LocalPublicMCPHTTPS != nil {
			mcpPolicy := bodies.LocalPublicMCPHTTPS.Policy
			profile, profileErr := readOwnedProfileCurrent(ctx, q, source.Profile)
			cost := parent.Authority.AdmissionCostCeiling.MCPToolCost
			grantDigest := source.Spending[2].ScopedGrantDigest
			policyJoin = profileErr == nil && cost != nil && cost.ValidateAt(q.at) == nil && profile.ValidateAt(q.at) == nil &&
				profile.MCPToolGrantDigest == grantDigest && cost.ScopedGrantDigest == grantDigest &&
				mcpPolicy.GrantDigest == grantDigest && mcpPolicy.ConfiguredProfileDigest == allocation.original.Backend.BackendProfileDigest &&
				mcpPolicy.MCPProfileID == cost.ProfileID && mcpPolicy.MCPProfileRevision == cost.ProfileRevision && mcpPolicy.MCPProfileDigest != "" && mcpPolicy.DescriptorDigest != "" &&
				cost.Kind == domain.ExternalServiceMCPToolV1 && cost.TenantID == source.Scope.TenantID && cost.OwnerUserID == source.Scope.OwnerUserID
		}
		if ad != allocationDigest || sd != sourceDigest || proof.Scope != source.Scope || proof.LeaseID != parent.Reservation.LeaseID || proof.FenceToken != parent.Reservation.FenceToken || proof.PhysicalInvocationClaimID != parent.Reservation.PhysicalInvocationClaimID || observedAt.Before(allocation.original.Window.ObservedAt) || observedAt.After(q.at) || !policyJoin {
			return q.budget.deny()
		}
		if proof.OperationSequence > parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxOperations || (proof.OperationSequence == 1 && proof.ServiceKind != domain.RegisteredProxyProviderV1) {
			return q.budget.deny()
		}
		old, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, proof.OperationSequence)
		if err != nil {
			return err
		}
		if old != nil {
			if old.original.TransportKind != proof.TransportKind || old.original.OperationSequence != proof.OperationSequence || old.original.OperationDigest != proof.OperationDigest || old.original.ServiceKind != proof.ServiceKind || !bytes.Equal(old.policyBody, policyBody) || !bytes.Equal(old.attestationBody, attestationBody) || old.original.Window.ObservedAt != observedAt {
				return q.budget.deny()
			}
			result, replayed = old.current.Clone(), true
			return nil // exact retained observation, never new transport or expiry
		}
		if allocation.current.ValidateAt(q.at) != nil || store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil {
			return q.budget.deny()
		}
		if store.ownedProxyPublicationVerifier.VerifyOwnedProxyPublicationCurrent(proof.Clone(), q.at) != nil {
			return q.budget.deny()
		}
		accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
		if err != nil || accounting == nil || !accounting.SetupReservedAt.Equal(burnAt) {
			return q.budget.deny()
		}
		if proof.OperationSequence != accounting.Aggregate.Operations+1 {
			return q.budget.deny()
		}
		if proof.OperationSequence > 1 {
			previous, err := readOwnedOperation(ctx, q, domain.AttemptOperationV1{Scope: ownedOperationScope(parent), Sequence: proof.OperationSequence - 1})
			if err != nil || previous == nil || previous.observation == nil || (!ownedObservationAllowsSuccessor(previous.reservation, *previous.observation) && !store.localMCPProxyContinuation(ctx, q, parent, source, proof, previous)) {
				return q.budget.deny()
			}
		}
		expires := bodies.ExpiresAt().UTC().Truncate(time.Microsecond)
		if proof.OperationDeadline.Before(expires) {
			expires = proof.OperationDeadline
		}
		if allocation.original.Window.ExpiresAt.Before(expires) {
			expires = allocation.original.Window.ExpiresAt
		}
		if !q.at.Before(expires) {
			return q.budget.deny()
		}
		value, err := buildOwnedProxyRecord(bodies, allocation.original, source, proof.ServiceKind, policyBody, attestationBody,
			domain.OwnedAgentAuthorityWindowV1{ObservedAt: observedAt, PublishedAt: q.at, ExpiresAt: expires}, proof.OperationSequence, proof.OperationDigest)
		if err != nil {
			return q.budget.deny()
		}
		raw, err := domain.EncodeRegisteredProxyAttestationV2(value)
		digest, de := value.Digest()
		parentDigest, pe := parent.Digest()
		if err != nil || de != nil || pe != nil {
			return q.budget.deny()
		}
		r := parent.Reservation
		if proof.OperationSequence == 1 {
			if _, err := q.ExecContext(ctx, ownedProxyRegistrationCreateSQL, string(raw), string(digest), string(policyBody), string(attestationBody), r.TenantID, r.AttemptID, r.Kind, parentDigest, string(ad)); err != nil {
				return err
			}
		} else {
			if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operation_proxies_v1 (tenant_id,attempt_id,sequence,service_kind,original_record,original_digest,current_record,current_digest,policy_body,attestation_body) VALUES ($1,$2,$3,$4,$5,$6,$5,$6,$7,$8)`, r.TenantID, r.AttemptID, proof.OperationSequence, proof.ServiceKind, string(raw), string(digest), string(policyBody), string(attestationBody)); err != nil {
				return err
			}
		}
		actual, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, proof.OperationSequence)
		if err != nil || actual == nil {
			return q.budget.deny()
		}
		actualDigest, _ := actual.current.Digest()
		if actualDigest != digest {
			return q.budget.deny()
		}
		result = actual.current.Clone()
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	return result.Clone(), replayed, nil
}

func readOwnedProxySnapshot(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, allocation domain.RegisteredAllocationV2) (*ownedProxySnapshot, error) {
	return readOwnedProxySnapshotAt(ctx, q, parent, source, allocation, 1)
}
func readOwnedProxySnapshotAt(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, allocation domain.RegisteredAllocationV2, sequence uint64) (*ownedProxySnapshot, error) {
	if sequence == 0 || parent.Authority.HarnessBinding.AgentLoopPolicy == nil || sequence > parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxOperations {
		return nil, q.budget.deny()
	}
	var columns ownedProxyColumns
	r := parent.Reservation
	query, args := ownedProxyRegistrationSQL, []any{r.TenantID, r.AttemptID, r.Kind}
	if sequence > 1 {
		query, args = ownedOperationProxyRegistrationSQL, []any{r.TenantID, r.AttemptID, sequence}
	}
	err := q.scanMetadata(ctx, query, args, columns.destinations(sequence)...)
	if err == sql.ErrNoRows && sequence > 1 {
		return nil, nil
	}
	if err != nil {
		return nil, q.budget.deny()
	}
	return columns.decode(q, parent, source, allocation, sequence)
}

func (c ownedProxyColumns) decode(q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, allocation domain.RegisteredAllocationV2, sequence uint64) (*ownedProxySnapshot, error) {
	flags := c.flags
	originalRaw, originalDigest, currentRaw, currentDigest, policyRaw, attestationRaw, service := c.originalRaw, c.originalDigest, c.currentRaw, c.currentDigest, c.policyRaw, c.attestationRaw, c.service
	if flags == [6]uint64{} && originalRaw == "" && originalDigest == "" && currentRaw == "" && currentDigest == "" && policyRaw == "" && attestationRaw == "" {
		return nil, nil
	}
	for _, flag := range flags {
		if flag != 1 {
			return nil, q.budget.deny()
		}
	}
	if len(originalRaw) > ownedAuthorityHeadBytes || len(currentRaw) > ownedAuthorityHeadBytes || len(policyRaw) > 16<<10 || len(attestationRaw) > 16<<10 {
		return nil, q.budget.deny()
	}
	original, oe := domain.DecodeRegisteredProxyAttestationV2([]byte(originalRaw))
	if sequence > 1 && string(original.ServiceKind) != service {
		return nil, q.budget.deny()
	}
	current, ce := domain.DecodeRegisteredProxyAttestationV2([]byte(currentRaw))
	od, ode := original.Digest()
	cd, cde := current.Digest()
	// Initial writer supports no replacement/revocation yet; reject divergent
	// heads rather than normalizing or reconstructing active permission.
	if oe != nil || ce != nil || ode != nil || cde != nil || string(od) != originalDigest || string(cd) != currentDigest || originalRaw != currentRaw || original.OperationSequence != sequence || original.ValidateInitial() != nil || original.ValidateForAllocation(allocation, source) != nil || original.Window.PublishedAt.After(q.at) {
		return nil, q.budget.deny()
	}
	// Decode the same closed canonical DATA bodies as the publication path. This
	// carrier is only a decoder input: it cannot authenticate a producer or grant
	// current/send authority. Historical reads do not require an open socket.
	pb, ab := []byte(policyRaw), []byte(attestationRaw)
	bodies, err := (ports.OwnedProxyPublicationV2{TransportKind: original.TransportKind, ServiceKind: original.ServiceKind, ObservedAt: original.Window.ObservedAt, PolicyBody: pb, AttestationBody: ab}).Bodies()
	if err != nil {
		return nil, q.budget.deny()
	}
	expected, err := buildOwnedProxyRecord(bodies, allocation, source, original.ServiceKind, pb, ab, original.Window, original.OperationSequence, original.OperationDigest)
	expectedDigest, de := expected.Digest()
	if err != nil || de != nil || expectedDigest != od {
		return nil, q.budget.deny()
	}
	return &ownedProxySnapshot{original: original, current: current, policyBody: pb, attestationBody: ab}, nil
}

// One pure DATA mapping is used on write and retained readback. Neither branch
// supplies producer authentication or current permission; Store owns those
// independently. Unknown/mixed bodies are denied before selecting a builder.
func buildOwnedProxyRecord(bodies ports.OwnedProxyBodiesV2, allocation domain.RegisteredAllocationV2, source domain.AttemptExecutionSourceV1, service domain.RegisteredProxyServiceKindV1, policyBody, attestationBody []byte, window domain.OwnedAgentAuthorityWindowV1, sequence uint64, operation domain.AttemptOperationDigestV1) (domain.RegisteredProxyAttestationV2, error) {
	deny := func() (domain.RegisteredProxyAttestationV2, error) {
		return domain.RegisteredProxyAttestationV2{}, domain.ErrMembershipDenied
	}
	policyDigest, err := bodies.PolicyDigest()
	if err != nil || !window.PublishedAt.Before(bodies.ExpiresAt()) || window.ExpiresAt.After(bodies.ExpiresAt()) {
		return deny()
	}
	pb, ab := ports.OwnedProxyBodyReferenceV1("policy", policyBody), ports.OwnedProxyBodyReferenceV1("attestation", attestationBody)
	switch bodies.TransportKind {
	case domain.RegisteredProxyTransportPinnedHTTPSV2:
		b := bodies.PinnedHTTPS
		routeBody, err := json.Marshal(b.Policy.Route)
		if err != nil || b.Attestation.ValidateForPolicy(b.Policy, window.PublishedAt, b.Attestation.ExpiresAt) != nil {
			return deny()
		}
		sum := sha256.Sum256(routeBody)
		return domain.BuildRegisteredPinnedHTTPSProxyAttestationV2(allocation, source, service, hex.EncodeToString(sum[:]), policyDigest, b.Attestation.ResolutionSetDigest, pb, ab, window, sequence, operation)
	case domain.RegisteredProxyTransportLocalLoopbackHTTPV2:
		b := bodies.LocalLoopback
		transport, err := b.Policy.TransportDigest()
		if err != nil || service != domain.RegisteredProxyProviderV1 || !b.Attestation.ObservedAt.Equal(window.ObservedAt) {
			return deny()
		}
		return domain.BuildRegisteredLocalLoopbackProxyAttestationV2(allocation, source, transport, policyDigest, b.Attestation.Observation, pb, ab, window, sequence, operation)
	case domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1:
		b := bodies.LocalPublicMCPHTTPS
		transport, err := b.Policy.TransportDigest()
		if err != nil || service != domain.RegisteredProxyMCPToolV1 || !b.Attestation.ObservedAt.Equal(window.ObservedAt) ||
			b.Attestation.ValidateForPolicy(b.Policy, window.PublishedAt, window.ExpiresAt) != nil {
			return deny()
		}
		return domain.BuildRegisteredLocalPublicMCPHTTPSProxyAttestationV1(allocation, source, transport, policyDigest, b.Attestation.Observation, pb, ab, window, sequence, operation)
	default:
		return deny()
	}
}
