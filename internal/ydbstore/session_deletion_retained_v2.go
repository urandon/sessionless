package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func (v retainedDeletionWriterData) matchesCapsuleV2(c localRetainedTerminalCapsuleV2) bool {
	p := v.producer.terminal
	if len(v.operations) < 3 || len(v.operations) != len(c.Operations) || c.Accounting.ValidateFor(p.Parent, p.Admission.Source) != nil || p.Run.TerminalCustody == nil || p.Run.TerminalCustody.Correspondence == nil {
		return false
	}
	d, err := c.Accounting.Digest()
	if err != nil || d != p.Run.TerminalCustody.Correspondence.AccountingDispositionDigest {
		return false
	}
	for i, row := range v.operations {
		if row.observation == nil || !reflect.DeepEqual(row.reservation, c.Operations[i].Reservation) || !reflect.DeepEqual(*row.observation, c.Operations[i].Observation) {
			return false
		}
	}
	return true
}

// Outside SQL, verify actual immutable V2 bytes and EVERY canonical pair body.
// The earlier bounded SQL collection was DATA only. Unknown money cannot make
// a deletion grant until this full V3/seal/correspondence check and the later
// original transactional metadata/current-deletion rejoin have both succeeded.
func verifyRetainedDeletionBodiesV2(ctx context.Context, reader SessionDeletionBlobReader, snapshot localTerminalHistorySnapshot, ownership retainedDeletionOwnershipData, previous *compactioncontext.RetainedV1, writer *retainedDeletionWriterData, body []byte) (*compactioncontext.RetainedV1, error) {
	deny := func() (*compactioncontext.RetainedV1, error) { return nil, domain.ErrMembershipDenied }
	capsule, err := decodeLocalTerminalCapsuleV2(body)
	if err != nil {
		return deny()
	}
	defer func() {
		for _, item := range capsule.Operations {
			clear(item.NormalizedBody)
		}
	}()
	if writer != nil && (!reflect.DeepEqual(writer.producer.terminal, snapshot) || !writer.matchesCapsuleV2(capsule)) {
		return deny()
	}
	if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return deny()
	}
	byID := make(map[domain.SessionEventID]bool, len(capsule.Events))
	for _, item := range capsule.Events {
		if byID[item.ID] {
			return deny()
		}
		byID[item.ID] = true
	}
	count := 0
	for id, e := range ownership.events {
		if e.RunID != nil && *e.RunID == snapshot.Run.ID {
			if !byID[id] {
				return deny()
			}
			count++
		}
	}
	if count != len(capsule.Events) {
		return deny()
	}
	records := make([]sessioncontext.EventPayload, 0, len(capsule.Events))
	defer func() {
		for _, record := range records {
			clear(record.Payload)
		}
	}()
	for _, item := range capsule.Events {
		e, owned := ownership.events[item.ID]
		if !owned || e.Kind != item.Kind || e.Payload != item.Payload || e.IdempotencyKey != item.IdempotencyKey || e.RunID == nil || *e.RunID != snapshot.Run.ID {
			return deny()
		}
		payload, err := readRetainedDeletionBlob(ctx, reader, e.Payload, domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			return deny()
		}
		records = append(records, sessioncontext.EventPayload{Event: e, Payload: payload})
	}
	if validateHistoricalCapsuleDataV2(snapshot, capsule, records, nil) != nil {
		return deny()
	}
	return verifyRetainedDeletionDerived(ctx, reader, snapshot, ownership, previous, capsule.ProfileDigest)
}
