package ydbstore

import (
	"bytes"
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This receipt is only a confirmed original-process postimage, not another
// durable head or send authority. Every next-request read rejoins the actual
// Run head, retained bytes, complete source and concrete fit again.
type localCompactionPublicationV1 struct {
	plan      domain.DerivedContextPlanV1
	postimage domain.RunDerivedContextV1
}

// publishLocalCompaction consumes the original completed generation, never a
// caller-authored plan, summary, reader or approval. The physical pair remains
// one-use; an ambiguous registration, PUT or head commit cannot be retried into
// permission. A separately admitted runtime consumer is still required to send
// the rebuilt MODEL request through the original keeper/Store/issuer.
func publishLocalCompaction(ctx context.Context, g *localCompactionGenerationV1) (*configuredinference.FrozenRequestV1, error) {
	if ctx == nil || ctx.Err() != nil || g == nil || g.source == nil || g.source.state == nil {
		return nil, domain.ErrMembershipDenied
	}
	unlock, err := g.entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer unlock()
	call, finish, err := g.entry.retainedContext(ctx, g.operation)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	g.source.state.mu.Lock()
	if !g.originalForPhase(g.entry, localCompactionCompleted) || !g.entry.owner.store.preparedLocalCompactionLocked(g, g.operation) || g.publicationAttempted || call.Err() != nil {
		g.source.state.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	g.publicationAttempted = true
	store := g.entry.owner.store
	// Upload performs the full completed-source join before registration/PUT.
	// Do not duplicate it here: the same logical call has exactly three bounded
	// phases (registration, head CAS, committed-head read), not a reset budget.
	retained, _, err := g.completedRetainedLocked(call)
	observed := g.observation.Clone()
	g.source.state.mu.Unlock()
	if err != nil || retained == nil || call.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	summary, manifest := retained.SummaryBody(), retained.ManifestBody()
	defer clear(summary)
	defer clear(manifest)
	if chargeOwnedAuthorityBody(call, uint64(len(summary)+len(manifest))) != nil ||
		g.retained.uploadLocked(call, retained.Plan(), observed, summary, manifest) != nil {
		return nil, domain.ErrMembershipDenied
	}
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	if call.Err() != nil || !g.originalForPhase(g.entry, localCompactionCompleted) {
		return nil, domain.ErrMembershipDenied
	}
	// Reread the actual objects, not the buffers just handed to PUT. The typed
	// decoder plus exact original program/fit comparison precede the head CAS.
	if _, _, err := g.readRetainedLocked(call, retained.Plan()); err != nil {
		return nil, domain.ErrMembershipDenied
	}
	attempted := false
	var postimage *domain.RunDerivedContextV1
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if attempted {
			return q.budget.deny()
		}
		postimage = nil
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		run, err := g.rejoinLocked(ctx, q, localCompactionPostRegistration)
		if err != nil || store.validateOwnedOperationProviderPrice(ctx, q, g.operation, g.price, g.entry.source) != nil || run.DerivedContext == nil {
			return q.budget.deny()
		}
		digest, err := retained.Plan().Digest()
		if err != nil {
			return q.budget.deny()
		}
		value := run.DerivedContext.Clone()
		value.Head = &domain.DerivedContextHeadV1{Version: 1, PlanDigest: digest}
		run.DerivedContext = &value
		if run.Validate() != nil {
			return q.budget.deny()
		}
		payload, err := marshalRunPayload(run)
		if err != nil {
			return q.budget.deny()
		}
		postimage = &value
		// Mark before dispatch, including a lost ACK with no persistence. Read
		// retries cannot turn this mutation into a second successful head write.
		attempted = true
		_, err = q.ExecContext(ctx, localRetainedRunUpdateSQL, payload, run.TenantID, run.ID)
		return err
	})
	if err != nil || call.Err() != nil || postimage == nil || !store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: g.price}) {
		return nil, domain.ErrMembershipDenied
	}
	g.publication = &localCompactionPublicationV1{plan: retained.Plan(), postimage: postimage.Clone()}
	return g.readPublishedLocked(call)
}

// Caller holds the original source mutex and has authenticated the completed
// phase. Re-execute concrete generation/summary/next-input fit, using the
// remaining aggregate after this actual debit, not refunded observed usage.
func (g *localCompactionGenerationV1) completedRetainedLocked(ctx context.Context) (*compactioncontext.RetainedV1, *configuredinference.FrozenRequestV1, error) {
	deny := func() (*compactioncontext.RetainedV1, *configuredinference.FrozenRequestV1, error) {
		return nil, nil, domain.ErrMembershipDenied
	}
	if ctx.Err() != nil || !g.originalForPhase(g.entry, localCompactionCompleted) || g.program.Number() < 1 || g.program.Number() > 2 {
		return deny()
	}
	op, observation := g.operation, g.observation.Observation
	if chargeOwnedAuthorityBody(ctx, observation.ResponseBytes+g.operation.RequestBytes) != nil {
		return deny()
	}
	body, err := g.entry.owner.store.readOwnedOperationBody(ctx, op.Scope, observation.ResponseRef, observation.ResponseDigest, observation.ResponseBytes, min(op.ResultBytes, 64<<10))
	if err != nil {
		return deny()
	}
	defer clear(body)
	request := g.frozen.Body()
	defer clear(request)
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(g.entry.owner.configuration, request, body, op.Kind)
	if decoded.Call != nil {
		defer clear(decoded.Call.Arguments)
	}
	usage, usageErr := decoded.Usage.OperationUsage()
	policy := g.entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if err != nil || usageErr != nil || decoded.Call != nil || decoded.Stop != configuredinference.StopFinalV1 || !reflect.DeepEqual(usage, observation.Usage) || policy == nil ||
		uint64(len(decoded.Text)) > min(op.SummaryBytes, policy.Limits.MaxSummaryBytes, uint64(configuredinference.MaxCompactionSummaryBytesV1)) {
		return deny()
	}
	summary := []byte(decoded.Text)
	defer clear(summary)
	if _, err := g.program.ValidateSummary(summary); err != nil {
		return deny()
	}
	source := g.entry.owner.source
	bound, err := source.BoundCompactionSummary(decoded.Text)
	if err != nil || bound == 0 || bound > min(uint64(2048), op.Debit.OutputTokens, policy.Limits.MaxOutputTokensPerCall) {
		return deny()
	}
	configuration := g.entry.owner.configuration
	fit, err := source.FitContext(g.frozen, configuredinference.ContextFitCeilingsV1{InputTokens: op.Debit.InputTokens, OutputTokens: op.Debit.OutputTokens, TotalTokens: configuration.ModelWindowTokens})
	if err != nil || !reflect.DeepEqual(fit, g.fit) {
		return deny()
	}
	accounting, err := debitOwnedOperation(g.source.state.snapshot.Accounting, op, g.entry.parent, g.entry.source, g.reservation.ReservedAt)
	if err != nil || accounting.Aggregate.InputTokens >= policy.Limits.MaxAggregateInputTokens || accounting.Aggregate.OutputTokens >= policy.Limits.MaxAggregateOutputTokens || accounting.Aggregate.Operations >= policy.Limits.MaxOperations || accounting.Aggregate.ModelCalls >= policy.Limits.MaxModelCalls || accounting.Aggregate.ActiveDuration >= policy.Limits.MaxActiveDuration || accounting.Aggregate.BodyBytes >= g.entry.localOperationBodyLimit() {
		return deny()
	}
	next, err := g.program.Rebuild(summary)
	if err != nil {
		return deny()
	}
	nextFit, err := source.FitContext(next, configuredinference.ContextFitCeilingsV1{
		InputTokens:  min(configuration.Limits.MaxInputTokens, policy.Limits.MaxInputTokensPerCall, policy.Limits.MaxAggregateInputTokens-accounting.Aggregate.InputTokens),
		OutputTokens: min(configuration.Limits.MaxOutputTokens, policy.Limits.MaxOutputTokensPerCall, policy.Limits.MaxAggregateOutputTokens-accounting.Aggregate.OutputTokens),
		TotalTokens:  configuration.ModelWindowTokens,
	})
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	retained, err := g.program.BuildRetainedV1(compactioncontext.RetainedEvidenceV1{Reservation: g.reservation, Observation: observation, Summary: summary, GenerationFit: fit, NextFit: nextFit, SummaryTokenUpperBound: bound, Parent: g.parentRetained})
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	return retained, next, nil
}

func (g *localCompactionGenerationV1) readRetainedLocked(ctx context.Context, plan domain.DerivedContextPlanV1) (*compactioncontext.RetainedV1, *configuredinference.FrozenRequestV1, error) {
	deny := func() (*compactioncontext.RetainedV1, *configuredinference.FrozenRequestV1, error) {
		return nil, nil, domain.ErrMembershipDenied
	}
	if plan.Validate() != nil || plan.Scope != g.operation.Scope || plan.SourceViewDigest != g.source.state.digest || ctx.Err() != nil {
		return deny()
	}
	summary, err := g.entry.readCompactionBlob(ctx, plan.Summary, 32<<10)
	if err != nil {
		return deny()
	}
	defer clear(summary)
	manifest, err := g.entry.readCompactionBlob(ctx, plan.Manifest, 128<<10)
	if err != nil {
		return deny()
	}
	defer clear(manifest)
	policy := g.entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil {
		return deny()
	}
	var parent *compactioncontext.RetainedV1
	if g.parentRetained != nil {
		parentPlan := g.parentRetained.Plan()
		pb, err := g.entry.readCompactionBlob(ctx, parentPlan.Summary, 32<<10)
		if err != nil {
			return deny()
		}
		defer clear(pb)
		mb, err := g.entry.readCompactionBlob(ctx, parentPlan.Manifest, 128<<10)
		if err != nil {
			return deny()
		}
		defer clear(mb)
		parent, err = compactioncontext.DecodeRetainedV1(g.entry.owner.configuration, *policy, parentPlan, pb, mb, nil)
		if err != nil {
			return deny()
		}
	}
	decoded, err := compactioncontext.DecodeRetainedV1(g.entry.owner.configuration, *policy, plan, summary, manifest, parent)
	if err != nil || decoded.ValidateForGeneration(g.program) != nil {
		return deny()
	}
	expected, next, err := g.completedRetainedLocked(ctx)
	if err != nil || !reflect.DeepEqual(expected.Plan(), plan) {
		return deny()
	}
	wantSummary, wantManifest := expected.SummaryBody(), expected.ManifestBody()
	defer clear(wantSummary)
	defer clear(wantManifest)
	if !bytes.Equal(summary, wantSummary) || !bytes.Equal(manifest, wantManifest) || ctx.Err() != nil {
		return deny()
	}
	return decoded, next, nil
}

// No receipt alone selects context: read current Run/head and actual objects,
// then reconstruct and fit from those bytes under the same original source.
func (g *localCompactionGenerationV1) readPublishedLocked(ctx context.Context) (*configuredinference.FrozenRequestV1, error) {
	if ctx.Err() != nil || !g.originalForPhase(g.entry, localCompactionPublished) {
		return nil, domain.ErrMembershipDenied
	}
	// Materialize/fit before the final current-head join. A head/frontier change
	// during blob I/O must not escape as a current next-request result.
	_, next, err := g.readRetainedLocked(ctx, g.publication.plan)
	if err != nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	err = g.entry.owner.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		run, err := g.rejoinLocked(ctx, q, localCompactionPublished)
		if err != nil || run.DerivedContext == nil || run.DerivedContext.Head == nil || g.entry.owner.store.validateOwnedOperationProviderPrice(ctx, q, g.operation, g.price, g.entry.source) != nil {
			return q.budget.deny()
		}
		digest, err := g.publication.plan.Digest()
		if err != nil || run.DerivedContext.Head.PlanDigest != digest {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || ctx.Err() != nil || !g.entry.owner.store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: g.price}) {
		return nil, domain.ErrMembershipDenied
	}
	return next, nil
}

func readPublishedLocalCompaction(ctx context.Context, g *localCompactionGenerationV1) (*configuredinference.FrozenRequestV1, error) {
	if ctx == nil || ctx.Err() != nil || g == nil || g.source == nil || g.source.state == nil {
		return nil, domain.ErrMembershipDenied
	}
	unlock, err := g.entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer unlock()
	call, finish, err := g.entry.retainedContext(ctx, g.operation)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	return g.readPublishedLocked(call)
}
