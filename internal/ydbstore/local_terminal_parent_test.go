package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Exercises the PRIVATE parent rejoin, not a cleanup/erasure/completion receipt.
// The original runtime bridge is actually stopped; the test models entry into
// the existing cleanup phase but does not substitute a Keeper stop receipt.
// Full cleanup/completion remain covered by their unchanged connected tests.
func originalStoppedTerminalParentFixture(t *testing.T) (*compactionSourceFixture, *terminalPublicationSQL, *LocalPreparedOutputV1) {
	t.Helper()
	f, sql, p := terminalPublicationFixture(t)
	if err := p.Publish(t.Context()); err != nil {
		t.Fatalf("original successful terminal publication: %v", err)
	}
	if p.parent.burn.IsZero() || !p.parent.burn.Equal(f.parent.sql.burnRow[2].(time.Time)) {
		t.Fatal("successful preparation did not retain exact original authenticated burn")
	}
	ctx, cancel := context.WithDeadline(t.Context(), p.parent.deadline)
	defer cancel()
	if err := f.bridge.StopAndJoinLocalTerminal(ctx, f.entry.allocation); err != nil {
		t.Fatalf("actual original bridge stop/join for private parent test: %v", err)
	}
	p.cleanupAttempted = true // private phase-entry DATA only, not cleanup proof
	return f, sql, p
}

func readOriginalStoppedTerminalParent(t *testing.T, f *compactionSourceFixture, p *LocalPreparedOutputV1) error {
	t.Helper()
	entry := f.entry
	entry.preparationMu.Lock()
	defer entry.preparationMu.Unlock()
	p.parent.source.state.mu.Lock()
	defer p.parent.source.state.mu.Unlock()
	ctx, cancel := context.WithDeadline(t.Context(), p.parent.deadline)
	defer cancel()
	return f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		parent, source, burn, err := p.originalTerminalParent(ctx, q)
		if err == nil && (!reflect.DeepEqual(parent, entry.parent) || !reflect.DeepEqual(source, entry.source) || !burn.Equal(p.parent.burn)) {
			t.Error("private original rejoin returned substituted canonical DATA")
		}
		return err
	})
}

func TestLocalTerminalParentPublishedOwnerSurvivesSourceCloseOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, sql, p := originalStoppedTerminalParentFixture(t)
		if err := readOriginalStoppedTerminalParent(t, f, p); err != nil {
			t.Fatal("genuine original private parent before Source close", err)
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		writes, objects := sql.base.writes, len(f.canonical.objects)
		if err := readOriginalStoppedTerminalParent(t, f, p); err != nil {
			t.Fatal("genuine published private owner lost historical parent after Source close", err)
		}
		// The narrow lane must not weaken the generic history path, even for
		// exactly the same original scope, parent, source and registration.
		err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			_, _, _, err := f.store.readOwnedAllocationParent(ctx, q, ownedAllocationParentRequest(f.entry.parent.Reservation))
			return err
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || sql.base.writes != writes || len(f.canonical.objects) != objects {
			t.Fatalf("private cleanup observation changed generic authentication/effects: err=%v writes=%d/%d objects=%d/%d", err, sql.base.writes, writes, len(f.canonical.objects), objects)
		}
	})
}

func TestLocalTerminalParentOriginalOwnerAndExactFacts(t *testing.T) {
	for _, fault := range []string{"copied-output", "copied-terminal", "foreign-entry", "unentered", "not-published", "cleared", "missing-capture", "missing-burn", "changed-burn", "changed-parent", "changed-source", "changed-plan", "changed-frontier", "expired-deadline"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, sql, p := originalStoppedTerminalParentFixture(t)
				if err := readOriginalStoppedTerminalParent(t, f, p); err != nil {
					t.Fatalf("%s original positive prerequisite: %v", fault, err)
				}
				if err := f.actual.source.Close(); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "copied-output":
					copy := *p // shared mutex pointer, not a copied lock
					p = &copy
				case "copied-terminal":
					original := p.parent
					copy := &LocalTerminalV1{self: original, entry: original.entry, deadline: original.deadline, burn: original.burn, source: original.source}
					p.parent = copy
					t.Cleanup(func() { p.parent = original })
				case "foreign-entry":
					other, _, _ := terminalPublicationFixture(t)
					original := p.parent.entry
					p.parent.entry = other.entry
					t.Cleanup(func() { p.parent.entry = original })
				case "unentered":
					f.entry.owner.entry.Store(nil)
					t.Cleanup(func() { f.entry.owner.entry.Store(f.entry) })
				case "not-published":
					p.published = false
				case "cleared":
					p.cleared = true
				case "missing-capture":
					p.parent.captured.Store(false)
				case "missing-burn":
					p.parent.burn = time.Time{}
				case "changed-burn":
					// Keep the historical SQL burn structurally valid and prove it
					// is earlier than the substituted retained private observation.
					// No allocation.ObservedAt>=burn check can detect this mismatch.
					p.parent.burn = p.parent.burn.Add(time.Microsecond)
					p.parent.source.state.snapshot.Accounting.SetupReservedAt = p.parent.burn
					err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
						parent, _, err := readOwnedAllocationParentData(ctx, q, ownedAllocationParentRequest(f.entry.parent.Reservation))
						if err != nil {
							return err
						}
						burned, burn, err := readOwnedPreparationBurnObservation(ctx, q, parent)
						if err == nil && (!burned || !burn.Before(p.parent.burn)) {
							t.Error("earlier-burn control did not retain a valid distinct original SQL burn")
						}
						return err
					})
					if err != nil {
						t.Fatalf("structurally valid earlier burn prerequisite: %v", err)
					}
				case "changed-parent":
					f.parent.sql.parentRow[4] = "foreign-lease"
				case "changed-source":
					f.parent.base.final[3] = "foreign-session"
				case "changed-plan":
					p.plan.FrontierDigest = strings.Repeat("f", 64)
				case "changed-frontier":
					p.parent.source.state.digest = strings.Repeat("f", 64)
				case "expired-deadline":
					time.Sleep(time.Until(p.parent.deadline))
					synctest.Wait()
				}
				writes, objects := sql.base.writes, len(f.canonical.objects)
				err := readOriginalStoppedTerminalParent(t, f, p)
				if !errors.Is(err, domain.ErrMembershipDenied) || sql.base.writes != writes || len(f.canonical.objects) != objects {
					t.Fatalf("%s substituted/reused private terminal parent: err=%v writes=%d/%d objects=%d/%d", fault, err, sql.base.writes, writes, len(f.canonical.objects), objects)
				}
			})
		})
	}
}
