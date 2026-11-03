package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func readRetainedDeletionFailureProducer(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent) (retainedDeletionProducerData, error) {
	deny := func() (retainedDeletionProducerData, error) { return retainedDeletionProducerData{}, q.budget.deny() }
	v, err := readHistoricalFailure(ctx, q, run, anchor)
	if err != nil {
		return deny()
	}
	parent, err := readOwnedAttemptEffectParent(ctx, q, ownedAllocationParentRequest(v.Parent.Reservation))
	if err != nil || parent == nil || !reflect.DeepEqual(*parent, v.Parent) {
		return deny()
	}
	admission, err := readOwnedAdmission(ctx, q, v.Parent.Scope)
	if err != nil || admission == nil || !reflect.DeepEqual(*admission, v.Admission) {
		return deny()
	}
	burned, at, err := readOwnedPreparationBurnObservation(ctx, q, v.Parent)
	if err != nil || !burned || at.After(run.TerminalFailure.CapturedAt) {
		return deny()
	}
	allocation, err := readOwnedAllocationSnapshots(ctx, q, v.Parent, v.Admission.Source, at)
	if err != nil || allocation == nil || !reflect.DeepEqual(allocation.original, v.Original) || !reflect.DeepEqual(allocation.current, v.Retired) || v.Retired.UpdatedAt.Before(run.TerminalFailure.CapturedAt) || v.Retired.UpdatedAt.After(run.UpdatedAt) {
		return deny()
	}
	snapshot := localTerminalHistorySnapshot{Run: run.Clone(), Anchor: anchor, Parent: v.Parent, Admission: v.Admission, Accounting: v.Accounting, Seal: v.Seal}
	return retainedDeletionProducerData{failure: &v, terminal: snapshot, allocation: *allocation, burnedAt: at}, nil
}

func verifyRetainedDeletionFailureBodies(ctx context.Context, reader SessionDeletionBlobReader, writer retainedDeletionWriterData, ownership retainedDeletionOwnershipData, previous *compactioncontext.RetainedV1) (*compactioncontext.RetainedV1, error) {
	deny := func() (*compactioncontext.RetainedV1, error) { return nil, domain.ErrMembershipDenied }
	v := writer.producer.failure
	if v == nil || !reflect.DeepEqual(ownership.events[v.Anchor.ID], v.Anchor) || (previous != nil && !ownership.contains(previous)) {
		return deny()
	}
	body, err := readRetainedDeletionBlob(ctx, reader, v.Run.TerminalCustody.Plan.Capsule, domain.MaxTerminalCapsuleBytesV1)
	if err != nil {
		return deny()
	}
	defer clear(body)
	c, err := decodeLocalFailureReadCapsule(body)
	if err != nil || len(c.operations()) != len(writer.operations) {
		return deny()
	}
	defer c.release()
	for i, row := range writer.operations {
		if row.observation == nil || !reflect.DeepEqual(row.reservation, c.operations()[i].Reservation) || !reflect.DeepEqual(*row.observation, c.operations()[i].Observation) {
			return deny()
		}
	}
	var records []sessioncontext.EventPayload
	defer func() {
		for _, r := range records {
			clear(r.Payload)
		}
	}()
	for _, event := range ownership.events {
		if event.RunID == nil || *event.RunID != v.Run.ID {
			continue
		}
		payload, err := readRetainedDeletionBlob(ctx, reader, event.Payload, domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			return deny()
		}
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: payload})
	}
	if validateFailureReadHistoryData(*v, c, records) != nil {
		return deny()
	}
	// Every retained ordinary body is independently inventoried from the exact
	// persisted complete operation rows, not a target discovered from capsule
	// bytes. Read/hash all actual retained objects before destructive admission.
	if c.search != nil {
		if validateFailureV4RetainedBodies(ctx, *c.search, v.ProxiesV4, func(ctx context.Context, ref domain.BlobRef, cap uint64) ([]byte, error) {
			return readRetainedDeletionBlob(ctx, reader, ref, cap)
		}) != nil {
			return deny()
		}
	} else {
		for _, ref := range c.bodies() {
			raw, err := readRetainedDeletionBlob(ctx, reader, ref, 1<<20)
			clear(raw)
			if err != nil {
				return deny()
			}
		}
	}
	return verifyRetainedDeletionDerived(ctx, reader, writer.producer.terminal, ownership, previous, c.profile())
}
