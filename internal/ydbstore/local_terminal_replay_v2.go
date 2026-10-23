package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (c *localTerminalCompletionV2) readRetainedV2(ctx context.Context, body []byte) (*ports.CanonicalEventMaterialSetV1, error) {
	deny := func() (*ports.CanonicalEventMaterialSetV1, error) { return nil, domain.ErrMembershipDenied }
	if !c.original() || ctx == nil || ctx.Err() != nil {
		return deny()
	}
	p := c.cleanup.owner
	if p.capsuleVersion != 2 || p.accounting != nil || p.accountingV3 == nil {
		return deny()
	}
	capsule, err := decodeLocalTerminalCapsuleV2(body)
	if err != nil || capsule.Scope != p.plan.Scope || capsule.FrontierDigest != p.plan.FrontierDigest || capsule.ProfileDigest != c.evidence.LocalV1.ProfileDigest ||
		!domain.EqualRunDerivedContextV1(capsule.DerivedContext, c.run.DerivedContext) || !reflect.DeepEqual(capsule.PublicManifest, c.manifest) || len(capsule.Events) != len(c.drafts) ||
		!reflect.DeepEqual(capsule.Accounting, p.accountingV3.disposition) || len(capsule.Operations) != len(p.accountingV3.operations) {
		return deny()
	}
	digest, err := p.accountingDigest()
	if err != nil || digest != c.evidence.LocalV1.AccountingDispositionDigest || digest != c.custody.Correspondence.AccountingDispositionDigest {
		return deny()
	}
	ops := make([]LocalCompactionOperationV1, len(capsule.Operations))
	for i, item := range capsule.Operations {
		original := p.accountingV3.operations[i]
		if !reflect.DeepEqual(item.Reservation, original.Reservation) || !reflect.DeepEqual(item.Observation, original.Observation) {
			return deny()
		}
		ops[i] = LocalCompactionOperationV1{Reservation: item.Reservation, Observation: item.Observation, Response: item.NormalizedBody}
	}
	// These are the same original admitted product limits, not historical
	// hard-ceiling defaults or permission reconstructed from a caller capsule.
	encoded, err := buildLocalCanonicalPlanV2(c.run, p.parent.source.state.snapshot.Job.Job.Limits, capsule.ProfileDigest, nil, ops, c.manifest.ID)
	if err != nil {
		return deny()
	}
	defer encoded.Release()
	events := encoded.Events()
	if len(events) != len(c.drafts) {
		return deny()
	}
	for i, event := range events {
		draft, err := canonicaloutput.PlanEventDraftV1(c.run, event.ID, event.Kind, event.Payload, c.drafts[i].CreatedAt)
		draft.DisplayText = event.DisplayText
		if err != nil || !reflect.DeepEqual(draft, c.drafts[i]) || capsule.Events[i] != (localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText}) {
			return deny()
		}
	}
	if p.verifyCleanupRetained(ctx) != nil {
		return deny()
	}
	return c.readCanonicalMaterials(ctx)
}
