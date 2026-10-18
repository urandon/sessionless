package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Unexported evidence is assembled only from the authenticated original parent,
// immutable completed operation and actual keeper bytes. No public approval
// flag or supplied blob metadata can construct this integration authority.
type ownedFixtureToolEvidence struct {
	model       domain.AttemptOperationReservationV1
	observation domain.AttemptOperationObservationV1
	call        ports.OwnedFixtureModelCallV1
	grant       ports.OwnedFixtureToolGrantV1
	grantDigest string
}
type ownedOperationEvidence struct {
	previous    domain.AttemptOperationReservationV1
	observation domain.AttemptOperationObservationV1
	tool        *ownedFixtureToolEvidence
	price       *ownedProviderPriceEvidence
	compaction  *localCompactionGenerationV1
	model       *localPreparedModelV1
}

func (store *Store) prepareOwnedOperationEvidence(ctx context.Context, op domain.AttemptOperationV1) (*ownedOperationEvidence, error) {
	// A sealed local entry is a handled denial, even when no prepared MODEL or
	// generation exists. Never fall back to fixture/generic evidence grammar.
	if store != nil && store.localBootstrap != nil {
		entry := store.localBootstrap.entry.Load()
		if entry.original() && entry.terminal.Load() != nil {
			return nil, domain.ErrMembershipDenied
		}
	}
	if evidence, handled, err := store.prepareLocalModelEvidence(ctx, op); handled {
		return evidence, err
	}
	if evidence, handled, err := store.prepareLocalCompactionEvidence(ctx, op); handled {
		return evidence, err
	}
	body, err := store.readOwnedOperationBody(ctx, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, 1<<20)
	if err != nil {
		return nil, err
	}
	defer clear(body)
	provider := op.Kind == domain.AttemptOperationModelV1 || op.Kind == domain.AttemptOperationCompactionV1
	if op.Sequence == 1 {
		var profile domain.OwnedAgentProfileCurrentV1
		err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			_, _, source, _, err := store.operationParent(ctx, q, op.Scope)
			if err != nil {
				return q.budget.deny()
			}
			profile, err = readOwnedProfileCurrent(ctx, q, source.Profile)
			return err
		})
		if err != nil || !provider {
			return nil, domain.ErrMembershipDenied
		}
		price, err := store.readOwnedProviderPriceEvidence(ctx, profile)
		if err != nil {
			return nil, err
		}
		return &ownedOperationEvidence{price: price}, nil
	}
	value := &ownedOperationEvidence{}
	var profile domain.OwnedAgentProfileCurrentV1
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		*value = ownedOperationEvidence{}
		row, err := readOwnedOperation(ctx, q, domain.AttemptOperationV1{Scope: op.Scope, Sequence: op.Sequence - 1})
		if err != nil || row == nil || row.observation == nil || !ownedObservationAllowsSuccessor(row.reservation, *row.observation) || op.ValidatePredecessor(row.reservation, *row.observation) != nil || store.operationHistory(ctx, q, row.reservation) != nil {
			return q.budget.deny()
		}
		value.previous, value.observation = row.reservation.Clone(), row.observation.Clone()
		if provider {
			_, _, source, _, err := store.operationParent(ctx, q, op.Scope)
			if err != nil {
				return q.budget.deny()
			}
			profile, err = readOwnedProfileCurrent(ctx, q, source.Profile)
			if err != nil {
				return q.budget.deny()
			}
		}
		return nil
	})
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	if provider {
		value.price, err = store.readOwnedProviderPriceEvidence(ctx, profile)
		if err != nil {
			return nil, err
		}
	}
	if op.Kind == domain.AttemptOperationMCPToolV1 || op.Kind == domain.AttemptOperationWebSearchV1 {
		tool, err := store.readOwnedFixtureToolEvidenceAt(ctx, op.Scope, op.Sequence-1, op.ModelResponseDigest, op.CallID, op.ArgumentsDigest, op.ToolGrantDigest, op.Kind)
		if err != nil || op.RequestDigest != op.ArgumentsDigest || !bytes.Equal(body, tool.call.Arguments) || op.RequestBytes > tool.grant.MaxArgumentBytes || op.ResultBytes > tool.grant.MaxResultBytes || op.Deadline.After(tool.grant.ExpiresAt) {
			return nil, domain.ErrMembershipDenied
		}
		value.tool = &tool
		return value, nil
	}
	previous, err := store.readOwnedOperationBody(ctx, op.Scope, value.observation.ResponseRef, value.observation.ResponseDigest, value.observation.ResponseBytes, 64<<10)
	if err != nil {
		return nil, err
	}
	defer clear(previous)
	if value.previous.Operation.Kind == domain.AttemptOperationModelV1 {
		model, err := ports.ParseOwnedFixtureModelResponseV1(previous)
		if err != nil || len(model.Calls) != 0 {
			return nil, domain.ErrMembershipDenied
		}
	}
	if validateOwnedFixtureContinuation(body, previous, op.Kind) != nil {
		return nil, domain.ErrMembershipDenied
	}
	return value, nil
}
func (store *Store) validateOwnedOperationEvidence(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1, evidence *ownedOperationEvidence, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if op.Kind == domain.AttemptOperationModelV1 || op.Kind == domain.AttemptOperationCompactionV1 {
		if evidence == nil || store.validateOwnedOperationProviderPrice(ctx, q, op, evidence.price, source) != nil {
			return q.budget.deny()
		}
	}
	if op.Sequence == 1 {
		if evidence == nil || evidence.tool != nil || evidence.previous.Version != 0 || evidence.observation.Version != 0 {
			return q.budget.deny()
		}
		return nil
	}
	if evidence == nil || !ownedObservationAllowsSuccessor(evidence.previous, evidence.observation) || op.ValidatePredecessor(evidence.previous, evidence.observation) != nil {
		return q.budget.deny()
	}
	row, err := readOwnedOperation(ctx, q, evidence.previous.Operation)
	if err != nil || row == nil || row.observation == nil {
		return q.budget.deny()
	}
	expected, _ := evidence.previous.Digest()
	actual, _ := row.reservation.Digest()
	od, _ := evidence.observation.Digest()
	actualOD, _ := row.observation.Digest()
	if expected != actual || od != actualOD {
		return q.budget.deny()
	}
	if op.Kind == domain.AttemptOperationMCPToolV1 || op.Kind == domain.AttemptOperationWebSearchV1 {
		if evidence.tool == nil {
			return q.budget.deny()
		}
		modelDigest, modelErr := evidence.tool.model.Digest()
		modelObservationDigest, observationErr := evidence.tool.observation.Digest()
		if modelErr != nil || observationErr != nil || modelDigest != expected || modelObservationDigest != od {
			return q.budget.deny()
		}
		// Search's mandatory consent gate below performs the exact model, grant,
		// and current-price rejoin. MCP has no such gate and must do it here.
		if op.Kind == domain.AttemptOperationMCPToolV1 && store.validateOwnedFixtureToolEvidence(ctx, q, *evidence.tool, parent, source) != nil {
			return q.budget.deny()
		}
		cost := ownedToolCost(parent, op.Kind)
		if cost == nil || op.ExternalChargeEvidenceDigest != cost.PriceEvidenceDigest {
			return q.budget.deny()
		}
		if op.Kind == domain.AttemptOperationMCPToolV1 && denyOwnedMCPSemanticRepeat(ctx, q, op, parent) != nil {
			return q.budget.deny()
		}
	}
	return nil
}
func ownedOperationProxyService(kind domain.AttemptOperationKindV1) domain.RegisteredProxyServiceKindV1 {
	switch kind {
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
		return domain.RegisteredProxyProviderV1
	case domain.AttemptOperationMCPToolV1:
		return domain.RegisteredProxyMCPToolV1
	case domain.AttemptOperationWebSearchV1:
		return domain.RegisteredProxyWebSearchV1
	}
	return ""
}

func ownedOperationScope(parent domain.OwnedAttemptEffectParentV1) domain.AttemptOperationScopeV1 {
	r := parent.Reservation
	rd, _ := r.DigestForAuthority(parent.Authority)
	pd, _ := parent.Authority.HarnessBinding.AgentLoopPolicy.Digest()
	return domain.AttemptOperationScopeV1{TenantID: r.TenantID, SessionID: parent.Scope.SessionID, OwnerUserID: parent.Scope.OwnerUserID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, EffectReservationDigest: rd, InvocationAuthorityDigest: r.InvocationAuthorityDigest, HarnessBindingDigest: r.HarnessBindingDigest, AgentLoopPolicyDigest: pd}
}

func (store *Store) readOwnedOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref, digest string, size, cap uint64) ([]byte, error) {
	if store.attemptOperationBodies == nil || size == 0 || size > cap || cap > 1<<20 {
		return nil, domain.ErrMembershipDenied
	}
	body, err := store.attemptOperationBodies.ReadAttemptOperationBody(ctx, scope, ref, cap)
	if err != nil || ctx.Err() != nil || uint64(len(body)) != size || ports.OwnedFixtureBytesDigestV1(body) != digest {
		return nil, domain.ErrMembershipDenied
	}
	return bytes.Clone(body), nil
}

func ownedObservationAllowsSuccessor(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) bool {
	if o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) {
		return false
	}
	if r.Operation.Kind == domain.AttemptOperationModelV1 || r.Operation.Kind == domain.AttemptOperationCompactionV1 {
		// Known completed outcome is distinct from unknown/reported usage.
		// The Store retains the full pre-send worst-case debit; no report can
		// refund it. This predicate is not token-fit, lineage, current authority,
		// settled billing, final success or residual-disposal evidence.
		return true
	}
	return o.Usage.State == domain.AttemptOperationUsageObservedV1 && o.Usage.AmountMicrounits != nil
}

func ownedToolCost(parent domain.OwnedAttemptEffectParentV1, kind domain.AttemptOperationKindV1) *domain.ExternalServiceCostCeilingV1 {
	if kind == domain.AttemptOperationMCPToolV1 {
		return parent.Authority.AdmissionCostCeiling.MCPToolCost
	}
	if kind == domain.AttemptOperationWebSearchV1 {
		return parent.Authority.AdmissionCostCeiling.WebSearchCost
	}
	return nil
}

// Consent uses sequence zero to read the exact current immutable MODEL head.
// Operation reserve/send use its exact predecessor sequence instead. Bodies are
// materialized outside every retryable SQL callback and rejoined before use.
func (store *Store) readOwnedFixtureToolEvidence(ctx context.Context, scope domain.AttemptOperationScopeV1, modelResponseDigest, callID, argumentsDigest, grantDigest string, kind domain.AttemptOperationKindV1) (ownedFixtureToolEvidence, error) {
	return store.readOwnedFixtureToolEvidenceAt(ctx, scope, 0, modelResponseDigest, callID, argumentsDigest, grantDigest, kind)
}
func (store *Store) readOwnedFixtureToolEvidenceAt(ctx context.Context, scope domain.AttemptOperationScopeV1, sequence uint64, modelResponseDigest, callID, argumentsDigest, grantDigest string, kind domain.AttemptOperationKindV1) (ownedFixtureToolEvidence, error) {
	var evidence ownedFixtureToolEvidence
	var cost domain.ExternalServiceCostCeilingV1
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		evidence = ownedFixtureToolEvidence{}
		cost = domain.ExternalServiceCostCeilingV1{}
		_, parent, source, _, err := store.operationParent(ctx, q, scope)
		if err != nil || parent.Authority.HarnessBinding.AgentLoopPolicy == nil || parent.Authority.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 {
			return q.budget.deny()
		}
		at := sequence
		if at == 0 {
			accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
			if err != nil || accounting == nil {
				return q.budget.deny()
			}
			at = accounting.Aggregate.Operations
		}
		if at == 0 {
			return q.budget.deny()
		}
		row, err := readOwnedOperation(ctx, q, domain.AttemptOperationV1{Scope: scope, Sequence: at})
		if err != nil || row == nil || row.observation == nil || row.reservation.Operation.Kind != domain.AttemptOperationModelV1 || row.observation.ResponseDigest != modelResponseDigest || !ownedObservationAllowsSuccessor(row.reservation, *row.observation) || store.operationHistory(ctx, q, row.reservation) != nil {
			return q.budget.deny()
		}
		component := ownedToolCost(parent, kind)
		if component == nil || component.ScopedGrantDigest != grantDigest {
			return q.budget.deny()
		}
		evidence.model, evidence.observation = row.reservation.Clone(), row.observation.Clone()
		cost = component.Clone()
		return nil
	})
	if err != nil {
		return ownedFixtureToolEvidence{}, domain.ErrMembershipDenied
	}
	response, err := store.readOwnedOperationBody(ctx, scope, evidence.observation.ResponseRef, evidence.observation.ResponseDigest, evidence.observation.ResponseBytes, 64<<10)
	if err != nil {
		return ownedFixtureToolEvidence{}, err
	}
	defer clear(response)
	model, err := ports.ParseOwnedFixtureModelResponseV1(response)
	if err != nil || len(model.Calls) != 1 || model.Calls[0].CallID != callID || model.Calls[0].Kind != kind || ports.OwnedFixtureBytesDigestV1(model.Calls[0].Arguments) != argumentsDigest {
		return ownedFixtureToolEvidence{}, domain.ErrMembershipDenied
	}
	ref, err := ports.OwnedFixtureGrantReferenceV1(grantDigest)
	if err != nil {
		return ownedFixtureToolEvidence{}, err
	}
	grantBody, err := store.attemptOperationBodies.ReadAttemptOperationBody(ctx, scope, ref, 4<<10)
	if err != nil || ctx.Err() != nil || len(grantBody) > 4<<10 || ports.OwnedFixtureBytesDigestV1(grantBody) != grantDigest {
		return ownedFixtureToolEvidence{}, domain.ErrMembershipDenied
	}
	clone := bytes.Clone(grantBody)
	defer clear(clone)
	grant, err := ports.ParseOwnedFixtureToolGrantV1(clone)
	if err != nil || grant.Kind != cost.Kind || grant.TenantID != scope.TenantID || grant.OwnerUserID != scope.OwnerUserID {
		return ownedFixtureToolEvidence{}, domain.ErrMembershipDenied
	}
	evidence.call, evidence.grant, evidence.grantDigest = model.Calls[0], grant, grantDigest
	return evidence, nil
}

func (store *Store) validateOwnedFixtureToolEvidence(ctx context.Context, q *ownedAuthorityQuery, evidence ownedFixtureToolEvidence, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if source.Validate() != nil || parent.Authority.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 || evidence.model.ValidateForParent(parent.Authority, parent.Reservation) != nil || !ownedObservationAllowsSuccessor(evidence.model, evidence.observation) {
		return q.budget.deny()
	}
	row, err := readOwnedOperation(ctx, q, evidence.model.Operation)
	expected, _ := evidence.model.Digest()
	od, _ := evidence.observation.Digest()
	if err != nil || row == nil || row.observation == nil {
		return q.budget.deny()
	}
	actual, _ := row.reservation.Digest()
	actualOD, _ := row.observation.Digest()
	cost := ownedToolCost(parent, evidence.call.Kind)
	if actual != expected || actualOD != od || cost == nil || evidence.grant.ValidateForCost(*cost, evidence.grantDigest, q.at) != nil {
		return q.budget.deny()
	}
	return nil
}

// Strict fixture-only model continuation: its actual request embeds the exact
// immutable previous response, not a caller's claimed context digest. This is
// not the production provider codec owned by #190/#191.
func validateOwnedFixtureContinuation(body, previous []byte, kind domain.AttemptOperationKindV1) error {
	var value struct {
		Kind        domain.AttemptOperationKindV1 `json:"kind"`
		Input       string                        `json:"input"`
		Predecessor json.RawMessage               `json:"predecessor"`
	}
	if json.Unmarshal(body, &value) != nil || value.Kind != kind || value.Input != "fixture" || !bytes.Equal(value.Predecessor, previous) {
		return domain.ErrMembershipDenied
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, body) {
		return domain.ErrMembershipDenied
	}
	return nil
}
