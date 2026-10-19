package ydbstore

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// InspectRetainedDeletionBodiesV1 is a test-only bridge to the historical DATA
// verifier. Input is the durable snapshot of a GENUINE original Complete, not
// a fabricated success marker. SQL/storage are controlled fixtures, not native
// YDB/S3 proof, and this helper grants no lifecycle/destructive authority.
func InspectRetainedDeletionBodiesV1(t *testing.T, seed *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, reader SessionDeletionBlobReader, mutation string) ([]domain.BlobRef, error) {
	t.Helper()
	state := completed.Snapshot(t)
	history := localFreshCommittedHistory(t, seed, state, nil)
	db := sql.OpenDB(freshRunHistoryConnector{base: terminalCompletionConnector{f: completed}, history: history})
	db.SetMaxOpenConns(1)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error("historical DATA fixture close", err)
		}
	}()
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	callback, err := b.begin()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		b.finish(callback)
		t.Fatal(err)
	}
	q := &ownedAuthorityQuery{tx: tx, budget: b, callback: callback, at: time.Now().UTC().Truncate(time.Microsecond)}
	if len(state.Events) != 1 {
		t.Fatal("genuine one-assistant completion required")
	}
	producer, err := readRetainedDeletionProducer(ctx, q, state.Run, state.Events[0])
	rollbackErr := tx.Rollback()
	b.finish(callback)
	if err != nil || rollbackErr != nil {
		t.Fatalf("original historical SQL snapshot: read=%v rollback=%v", err, rollbackErr)
	}
	snapshot := producer.terminal
	events := append(seed.CanonicalSourceEvents(t), state.Events...)
	ownership, err := newRetainedDeletionOwnershipData(state.Run.TenantID, state.Run.SessionID)
	if err != nil {
		t.Fatal("original SQL-owned inventory DATA", err)
	}
	for _, event := range events {
		if err := ownership.addEvent(event); err != nil {
			t.Fatal("original canonical owning SQL row", err)
		}
	}
	if err := ownership.addRun(state.Run); err != nil {
		t.Fatal("original Run owning SQL row", err)
	}
	switch mutation {
	case "missing_anchor_owner":
		delete(ownership.events, snapshot.Anchor.ID)
	case "mismatched_anchor_owner":
		event := ownership.events[snapshot.Anchor.ID]
		event.Payload.SHA256 = strings.Repeat("a", 64)
		ownership.events[event.ID] = event
	case "missing_retained_owner":
		if state.Run.DerivedContext == nil || len(state.Run.DerivedContext.Inventory) != 2 {
			t.Fatal("missing retained owner needs genuine A2")
		}
		digest, _ := state.Run.DerivedContext.Inventory[0].Digest()
		delete(ownership.plans, digest)
	case "changed_seal":
		digest, err := decodeLocalFinalizationMarkerV2(snapshot.Seal)
		if err != nil {
			t.Fatal(err)
		}
		first := "a"
		if strings.HasPrefix(digest, first) {
			first = "b"
		}
		snapshot.Seal, _ = encodeLocalFinalizationMarkerV2(first + digest[1:])
	case "positive":
	default:
		t.Fatal("unknown retained deletion DATA test mutation", mutation)
	}
	_, err = verifyRetainedDeletionBodies(ctx, reader, snapshot, ownership, nil)
	if err != nil {
		return nil, err
	}
	return retainedDeletionRunRefsData(snapshot.Run)
}
