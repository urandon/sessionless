package ydbstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestWebSearchResourceGrantActualSourceStoreCustody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, store, f, receipt, manifest := localSearchOperatorStore(t)
		store.ownedWebSearchResourceGrantCustody = source
		for _, suffix := range []string{"read-one", "read-two"} {
			publication, err := source.SpendingPublication(t.Context(), localSearchScope(receipt.Profile, suffix), domain.AttemptSpendingWebSearchV1)
			if err != nil {
				t.Fatal(err)
			}
			registerActualFundingKey(f, publication)
			f.at = publication.IssuedAt
			if _, err := store.PublishAttemptSpending(t.Context(), publication); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			evidence, err := store.readOwnedWebSearchResourceGrant(ctx, receipt.Profile, f.at)
			cancel()
			if err != nil || evidence == nil {
				t.Fatalf("actual Source/profile/search spending -> Store body custody denied: %v", err)
			}
			digest, _ := evidence.grant.Digest()
			if digest != receipt.Profile.WebSearchGrantDigest || digest == manifest.SearchExpenditure.GrantBody.Digest || evidence.profile.WebSearchResourceGrant == nil || *evidence.profile.WebSearchResourceGrant != *receipt.Profile.WebSearchResourceGrant {
				t.Fatal("raw/semantic/original body-ref association lost")
			}
		}
		before, err := source.ReadWebSearchResourceGrant(t.Context(), domain.OwnedAgentProfileReferenceV1{Locator: receipt.Profile.Locator, RevocationGeneration: receipt.Profile.RevocationGeneration, Digest: receipt.Digest}, 8192)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(before)
		for _, kind := range []string{"foreign-profile", "nil-custody", "cancel", "closed-source"} {
			p := receipt.Profile.Clone()
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			switch kind {
			case "foreign-profile":
				p.WebSearchResourceGrant.Body.Ref = "foreign-original-profile-body"
			case "nil-custody":
				store.ownedWebSearchResourceGrantCustody = nil
			case "cancel":
				cancel()
			case "closed-source":
				_ = source.Close()
			}
			if got, err := store.readOwnedWebSearchResourceGrant(ctx, p, f.at); !errors.Is(err, domain.ErrMembershipDenied) || got != nil {
				t.Fatalf("%s returned usable grant DATA: %+v err=%v", kind, got, err)
			}
			cancel()
			store.ownedWebSearchResourceGrantCustody = source
		}
		if len(before) == 0 || !bytes.Contains(before, []byte(`"version":2`)) {
			t.Fatal("later Source Close destroyed an already-transferred caller buffer")
		}
	})
}

var _ ports.OwnedWebSearchResourceGrantCustodyV2 = (*resourceConsentCustodyFixture)(nil)

// A bounded private fake for current-consent SQL joining only; NOT genuine
// MODEL provenance or a replacement installation/source verifier.
type resourceConsentCustodyFixture struct {
	f        *privateConsentViewFixture
	body     []byte
	ref      domain.OwnedAgentProfileReferenceV1
	reads    int
	caps     []uint64
	fault    func(context.Context)
	closed   bool
	lifetime context.Context
	cancel   context.CancelFunc
}

func (c *resourceConsentCustodyFixture) close() {
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
}
func (c *resourceConsentCustodyFixture) ExecutionLifetime() (context.Context, error) {
	if c.closed || (c.lifetime != nil && c.lifetime.Err() != nil) {
		return nil, domain.ErrMembershipDenied
	}
	return c.lifetime, nil
}

// The scripted database clock is authoritative; this passive fake does not
// create a real timer for a historical DB timestamp. Its Values must never be
// retained or inspected by grant consumers.
type resourceConsentLifetime struct {
	context.Context
	deadline time.Time
}

func (c resourceConsentLifetime) Deadline() (time.Time, bool) { return c.deadline, true }
func (c resourceConsentLifetime) Value(any) any {
	panic("Source lifetime Values are not consumer authority")
}

type resourceConsentNoDeadline struct{ context.Context }

func (resourceConsentNoDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *resourceConsentCustodyFixture) ReadWebSearchResourceGrant(ctx context.Context, ref domain.OwnedAgentProfileReferenceV1, cap uint64) ([]byte, error) {
	c.reads++
	c.caps = append(c.caps, cap)
	if c.closed || ctx.Err() != nil || ref != c.ref || cap == 0 || cap > 8192 || c.f.allocation.parent.sql.activeTransactions != 0 {
		return nil, domain.ErrMembershipDenied
	}
	b, ok := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	if !ok {
		return nil, domain.ErrMembershipDenied
	}
	b.mu.Lock()
	charged, active, callbacks := b.bytes, b.active, b.callbacks
	b.mu.Unlock()
	if active || callbacks != 1 || charged < c.f.intentBytes+4*cap {
		return nil, errors.New("V2 grant read must reserve original return/copy/parser/canonical budget before I/O")
	}
	if c.fault != nil {
		c.fault(ctx)
	}
	return bytes.Clone(c.body), nil
}
