package ydbstore

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// Shared exact proxy read for accounting versions. Caller holds the original
// entry barrier and source mutex. This is the original single transaction and
// query budget, not an extra pass or a new cache allowance.
func readLocalTerminalAccountingProxies(ctx context.Context, h *LocalTerminalV1, count int) ([]ports.OwnedOperationResidualProxyV1, error) {
	entry, s := h.entry, h.source
	var proxies []ports.OwnedOperationResidualProxyV1
	err := entry.owner.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		proxies = nil
		if _, e := s.rejoinRunLocked(ctx, q, nil, nil); e != nil {
			return e
		}
		first, e := readOwnedProxySnapshotAt(ctx, q, entry.parent, entry.source, entry.allocation, 1)
		if e != nil || first == nil {
			return q.budget.deny()
		}
		appendProxy := func(p *ownedProxySnapshot, seq uint64) error {
			if seq != uint64(len(proxies)+1) || seq > uint64(count) {
				return q.budget.deny()
			}
			pd, e := entry.compactionProxyDigest(p, s.state.snapshot.Operations[seq-1])
			if e != nil || pd != s.state.snapshot.ProxyDigests[seq-1] {
				return q.budget.deny()
			}
			proxies = append(proxies, ports.OwnedOperationResidualProxyV1{Sequence: seq, Original: p.original.Clone(), Current: p.current.Clone()})
			return nil
		}
		if appendProxy(first, 1) != nil {
			return q.budget.deny()
		}
		result, e := q.QueryContext(ctx, localCompactionProxiesSQL, entry.parent.Scope.TenantID, entry.parent.Reservation.AttemptID)
		if e != nil {
			return q.budget.deny()
		}
		defer result.Close()
		for result.Next() {
			var seq uint64
			var columns ownedProxyColumns
			dest := append([]any{&seq}, columns.destinations(2)...)
			n, e := scanOwnedCurrentRow(result, dest...)
			if q.budget.materialized(q.callback, n) != nil || e != nil || seq <= 1 || seq > uint64(count) {
				return q.budget.deny()
			}
			p, e := columns.decode(q, entry.parent, entry.source, entry.allocation, seq)
			if e != nil || p == nil || appendProxy(p, seq) != nil {
				return q.budget.deny()
			}
		}
		if result.Err() != nil || result.Close() != nil || len(proxies) != count {
			return q.budget.deny()
		}
		return nil
	})
	return proxies, err
}
