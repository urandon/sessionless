package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

func materialSetFixture(t *testing.T, id domain.SessionEventID, size int) (domain.SessionEvent, *sessionmaterial.PreparedV1) {
	t.Helper()
	const prefix, suffix = `{"version":1,"text":"`, `"}`
	if size < len(prefix)+len(suffix)+1 {
		t.Fatal("fixture must contain nonempty text")
	}
	body := []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
	t.Cleanup(func() { clear(body) })
	hash := sha256.Sum256(body)
	user, run := domain.UserID("material-user"), domain.RunID("material-run")
	event := domain.SessionEvent{ID: id, TenantID: "material-tenant", SessionID: "material-session",
		Kind: domain.SessionEventUserMessage, AuthorUserID: &user, RunID: &run, IdempotencyKey: domain.IdempotencyKey(id),
		Payload:  domain.BlobRef{TenantID: "material-tenant", Key: domain.SessionEventObjectPrefix("material-tenant", "material-session", id) + "uploads/source/message.json", Size: int64(size), SHA256: hex.EncodeToString(hash[:])},
		Sequence: 1, CreatedAt: time.Date(2026, 10, 11, 1, 2, 3, 0, time.UTC)}
	prepared, err := sessionmaterial.PrepareV1(event, body, nil)
	if err != nil {
		t.Fatalf("prepare fixture id=%s size=%d: %v", id, size, err)
	}
	t.Cleanup(prepared.Release)
	return event, prepared
}

func TestCanonicalMaterialSetOwnershipRejectionAndAliases(t *testing.T) {
	set := NewCanonicalEventMaterialSetV1()
	t.Cleanup(set.Release)
	event, first := materialSetFixture(t, "owned-event", 64)
	if !set.Adopt(event.ID, first) || !set.Adopt(event.ID, first) || set.ForEvent(event.ID) != first {
		t.Fatal("first adoption or exact pointer re-adoption failed")
	}
	_, replacement := materialSetFixture(t, event.ID, 80)
	if set.Adopt(event.ID, replacement) || set.ForEvent(event.ID) != first {
		t.Fatal("duplicate ID replaced original preparation")
	}
	if n, live := replacement.OwnedBytesV1(); live || n != 0 {
		t.Fatal("rejected preparation retained owned bytes")
	}
	if n, live := first.OwnedBytesV1(); !live || n != 64 {
		t.Fatalf("duplicate rejection invalidated original: size=%d live=%v", n, live)
	}
	alias := *set
	alias.Release()
	set.Release()
	if set.ForEvent(event.ID) != nil || alias.ForEvent(event.ID) != nil {
		t.Fatal("value alias resurrected released set")
	}
	if body, _, err := first.CopyForFinalV1(event); body != nil || err == nil {
		t.Fatal("borrowed preparation survived owner's release")
	}
	_, late := materialSetFixture(t, "late-event", 64)
	if set.Adopt("late-event", late) {
		t.Fatal("released set accepted new ownership")
	}
	if _, live := late.OwnedBytesV1(); live {
		t.Fatal("late rejected preparation retained bytes")
	}
}

func TestCanonicalMaterialSetExactInspectionBudgets(t *testing.T) {
	t.Run("event count", func(t *testing.T) {
		set := NewCanonicalEventMaterialSetV1()
		t.Cleanup(set.Release)
		for i := range MaxCanonicalPreparedEventsV1 + 1 {
			id := domain.SessionEventID(fmt.Sprintf("count-event-%d", i))
			_, prepared := materialSetFixture(t, id, 64)
			accepted := set.Adopt(id, prepared)
			if accepted != (i < MaxCanonicalPreparedEventsV1) {
				t.Fatalf("index=%d accepted=%v", i, accepted)
			}
			if _, live := prepared.OwnedBytesV1(); live != accepted {
				t.Fatalf("index=%d live=%v accepted=%v", i, live, accepted)
			}
		}
	})
	t.Run("actual owned bytes", func(t *testing.T) {
		set := NewCanonicalEventMaterialSetV1()
		t.Cleanup(set.Release)
		for i := range MaxCanonicalPreparedBytesV1 / sessionmaterial.MaxPayloadBytesV1 {
			id := domain.SessionEventID(fmt.Sprintf("byte-event-%d", i))
			_, prepared := materialSetFixture(t, id, sessionmaterial.MaxPayloadBytesV1)
			if !set.Adopt(id, prepared) {
				t.Fatalf("exact byte boundary rejected event=%d", i)
			}
		}
		_, excess := materialSetFixture(t, "byte-excess", 64)
		if set.Adopt("byte-excess", excess) {
			t.Fatal("inspection byte budget exceeded")
		}
		if _, live := excess.OwnedBytesV1(); live {
			t.Fatal("over-budget preparation was not released")
		}
	})
}

func TestCanonicalMaterialSetZeroPrivacyAndConcurrentRelease(t *testing.T) {
	var zero CanonicalEventMaterialSetV1
	for _, set := range []*CanonicalEventMaterialSetV1{nil, &zero} {
		set.Release()
		_, p := materialSetFixture(t, "zero-event", 64)
		if set.Adopt("zero-event", p) || set.ForEvent("zero-event") != nil {
			t.Fatal("nil/zero set accepted material")
		}
		if _, live := p.OwnedBytesV1(); live {
			t.Fatal("nil/zero rejection did not release material")
		}
	}
	set := NewCanonicalEventMaterialSetV1()
	t.Cleanup(set.Release)
	event, p := materialSetFixture(t, "private-event", 64)
	if !set.Adopt(event.ID, p) {
		t.Fatal("adoption failed")
	}
	for _, value := range []any{set, *set, zero} {
		if body, err := json.Marshal(value); err == nil || body != nil {
			t.Fatal("private set serialized")
		}
		for _, format := range []string{"%v", "%+v", "%#v", "%x", "%q"} {
			if got := fmt.Sprintf(format, value); got != "ports.CanonicalEventMaterialSetV1" {
				t.Fatalf("private set leaked using format=%s", format)
			}
		}
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			<-start
			for range 16 {
				borrowed := set.ForEvent(event.ID)
				if borrowed != nil {
					body, _, err := borrowed.CopyForFinalV1(event)
					if err == nil && len(body) != 64 {
						t.Errorf("concurrent copy returned partial body length=%d", len(body))
					}
					clear(body)
				}
			}
		})
	}
	close(start)
	set.Release()
	group.Wait()
	if _, live := p.OwnedBytesV1(); live {
		t.Fatal("concurrent release left live material")
	}
}
