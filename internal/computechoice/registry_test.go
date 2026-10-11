package computechoice

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func testScope() Scope {
	return Scope{TenantID: "tenant-a", UserID: "owner-a", SessionID: "session-a", MembershipSecurityVersion: 1}
}

func testRegistrations(count int) []Registration {
	entries := make([]Registration, count)
	for index := range entries {
		entries[index] = Registration{TenantID: "tenant-a", OwnerUserID: "owner-a", ChoiceID: fmt.Sprintf("choice-%02d", index), RegistrationRevision: 1, DisclosureRevision: strings.Repeat("a", 64)}
	}
	return entries
}

func mustScoped(t testing.TB, scope Scope, entries []Registration) *ScopedRegistry {
	t.Helper()
	registry, err := NewRegistry(entries)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	scoped, err := registry.Scoped(scope)
	if err != nil {
		t.Fatalf("Scoped: %v", err)
	}
	return scoped
}

func TestRegistryRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]Registration) []Registration
	}{
		{"global_overflow", func([]Registration) []Registration { return testRegistrations(65) }},
		{"duplicate_same_scope", func(e []Registration) []Registration { return append(e, e[0]) }},
		{"missing_tenant", func(e []Registration) []Registration { e[0].TenantID = ""; return e }},
		{"missing_owner", func(e []Registration) []Registration { e[0].OwnerUserID = ""; return e }},
		{"missing_choice", func(e []Registration) []Registration { e[0].ChoiceID = ""; return e }},
		{"long_choice", func(e []Registration) []Registration { e[0].ChoiceID = strings.Repeat("c", 129); return e }},
		{"zero_registration_revision", func(e []Registration) []Registration { e[0].RegistrationRevision = 0; return e }},
		{"missing_disclosure_revision", func(e []Registration) []Registration { e[0].DisclosureRevision = ""; return e }},
		{"uppercase_disclosure_revision", func(e []Registration) []Registration { e[0].DisclosureRevision = strings.Repeat("A", 64); return e }},
		{"nonhex_disclosure_revision", func(e []Registration) []Registration { e[0].DisclosureRevision = strings.Repeat("z", 64); return e }},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry, err := NewRegistry(test.mutate(testRegistrations(1)))
			if registry != nil || !errors.Is(err, ErrInvalidRegistry) {
				t.Fatalf("got registry=%v err=%v; want nil/content-free invalid registry", registry, err)
			}
			if err.Error() != "compute choice registry invalid" {
				t.Errorf("unexpected error disclosure: %q", err)
			}
		})
	}
	entries := testRegistrations(64)
	entries[0].ChoiceID = strings.Repeat("c", 128)
	if _, err := NewRegistry(entries); err != nil {
		t.Fatalf("exact global/ID bounds rejected: %v", err)
	}
	entries = testRegistrations(1)
	other := entries[0]
	other.OwnerUserID = "owner-b"
	entries = append(entries, other)
	other.TenantID = "tenant-b"
	if _, err := NewRegistry(append(entries, other)); err != nil {
		t.Fatalf("same ID in distinct scopes rejected: %v", err)
	}
}

func TestRegistryScopeValidationAndFraming(t *testing.T) {
	registry, err := NewRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Scope){func(s *Scope) { s.TenantID = "" }, func(s *Scope) { s.UserID = "" }, func(s *Scope) { s.SessionID = "" }, func(s *Scope) { s.MembershipSecurityVersion = 0 }} {
		scope := testScope()
		mutate(&scope)
		if _, err := registry.Scoped(scope); !errors.Is(err, ErrInvalidScope) {
			t.Errorf("invalid scope got %v", err)
		}
	}
	if _, err := (*Registry)(nil).Scoped(testScope()); !errors.Is(err, ErrInvalidScope) {
		t.Errorf("nil registry: %v", err)
	}
	first := testScope()
	first.TenantID, first.UserID = "ab", "c"
	second := first
	second.TenantID, second.UserID = "a", "bc"
	if scopeHash(first) == scopeHash(second) {
		t.Error("scope framing collapsed distinct identities")
	}
	base := mustScoped(t, testScope(), nil)
	for _, mutate := range []func(*Scope){func(s *Scope) { s.TenantID = "tenant-b" }, func(s *Scope) { s.UserID = "owner-b" }, func(s *Scope) { s.SessionID = "session-b" }, func(s *Scope) { s.MembershipSecurityVersion++ }} {
		scope := testScope()
		mutate(&scope)
		if mustScoped(t, scope, nil).Revision() == base.Revision() {
			t.Errorf("scope change did not seal revision: %+v", scope)
		}
	}
}

func TestRegistryForeignOnlyChangesPreserveSubset(t *testing.T) {
	own := testRegistrations(5)
	foreign := own[0]
	foreign.TenantID, foreign.OwnerUserID, foreign.ChoiceID = "foreign-tenant", "foreign-owner", "000-foreign"
	base := mustScoped(t, testScope(), append(append([]Registration(nil), own...), foreign))
	want, err := base.Page(0, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		entries []Registration
	}{
		{"add", append(append(append([]Registration(nil), own...), foreign), Registration{TenantID: "second-foreign", OwnerUserID: "foreign-owner", ChoiceID: "000-addition", RegistrationRevision: 1, DisclosureRevision: foreign.DisclosureRevision})},
		{"remove", append([]Registration(nil), own...)},
		{"edit", append(append([]Registration(nil), own...), Registration{TenantID: foreign.TenantID, OwnerUserID: foreign.OwnerUserID, ChoiceID: "zzz-edited", RegistrationRevision: 9, DisclosureRevision: strings.Repeat("b", 64)})},
		{"other_owner_same_tenant", append(append([]Registration(nil), own...), Registration{TenantID: own[0].TenantID, OwnerUserID: "owner-b", ChoiceID: "000-other-owner", RegistrationRevision: 1, DisclosureRevision: foreign.DisclosureRevision})},
	} {
		t.Run(test.name, func(t *testing.T) {
			scoped := mustScoped(t, testScope(), test.entries)
			got, err := scoped.Page(0, 4)
			if err != nil || scoped.Revision() != base.Revision() || !reflect.DeepEqual(got, want) {
				t.Fatalf("foreign change altered own subset: revision=%s page=%+v err=%v", scoped.Revision(), got, err)
			}
			if _, err := scoped.Lookup(foreign.ChoiceID); !errors.Is(err, ErrChoiceUnavailable) {
				t.Errorf("foreign lookup: %v", err)
			}
		})
	}
	for _, mutate := range []func([]Registration) []Registration{
		func(e []Registration) []Registration { e[0].RegistrationRevision++; return e },
		func(e []Registration) []Registration { e[0].DisclosureRevision = strings.Repeat("b", 64); return e },
		func(e []Registration) []Registration { e[0].ChoiceID = "changed"; return e },
		func(e []Registration) []Registration { return e[1:] },
		func(e []Registration) []Registration {
			extra := e[0]
			extra.ChoiceID = "extra"
			return append(e, extra)
		},
	} {
		if got := mustScoped(t, testScope(), mutate(append([]Registration(nil), own...))).Revision(); got == base.Revision() {
			t.Error("own change did not change revision")
		}
	}
}

func TestRegistryPageBoundAndDefensiveCopies(t *testing.T) {
	entries := testRegistrations(9)
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	registry, err := NewRegistry(entries)
	if err != nil {
		t.Fatal(err)
	}
	entries[0].ChoiceID, entries[0].RegistrationRevision = "caller-mutated", 999
	scoped, err := registry.Scoped(testScope())
	if err != nil {
		t.Fatal(err)
	}
	if scoped.Revision() != mustScoped(t, testScope(), testRegistrations(9)).Revision() {
		t.Error("input order/mutation changed canonical registry")
	}
	for position := uint32(0); position < 9; {
		page, err := scoped.Page(position, 4)
		if err != nil {
			t.Fatal(err)
		}
		wantCount := uint32(9) - position
		if wantCount > 4 {
			wantCount = 4
		}
		if uint32(len(page.Registrations)) != wantCount || page.LastPosition != position+wantCount || page.More != (page.LastPosition < 9) {
			t.Fatalf("after=%d page=%+v", position, page)
		}
		for index, entry := range page.Registrations {
			if want := fmt.Sprintf("choice-%02d", int(position)+index); entry.ChoiceID != want {
				t.Errorf("got %q want %q", entry.ChoiceID, want)
			}
		}
		// A future current reader may hide every returned candidate. It must
		// retain this progress, not ask this component to fill from more rows.
		position = page.LastPosition
		page.Registrations[0].ChoiceID = "response-mutated"
	}
	got, err := scoped.Lookup("choice-00")
	if err != nil {
		t.Fatal(err)
	}
	got.ChoiceID = "lookup-mutated"
	if again, err := scoped.Lookup("choice-00"); err != nil || again.ChoiceID != "choice-00" {
		t.Fatalf("defensive lookup got %+v %v", again, err)
	}
	for _, limit := range []uint32{1, 2, 3, 4} {
		if page, err := scoped.Page(0, limit); err != nil || len(page.Registrations) != int(limit) {
			t.Errorf("limit=%d: %+v %v", limit, page, err)
		}
	}
	for _, test := range []struct{ after, limit uint32 }{{0, 0}, {0, 5}, {0, ^uint32(0)}, {10, 1}, {^uint32(0), 4}} {
		if _, err := scoped.Page(test.after, test.limit); !errors.Is(err, ErrInvalidPage) {
			t.Errorf("bad page %+v: %v", test, err)
		}
	}
	empty := mustScoped(t, testScope(), nil)
	if page, err := empty.Page(0, 4); err != nil || len(page.Registrations) != 0 || page.More || page.LastPosition != 0 {
		t.Errorf("empty page %+v %v", page, err)
	}
	if _, err := scoped.Lookup("unknown"); !errors.Is(err, ErrChoiceUnavailable) {
		t.Errorf("missing lookup %v", err)
	}
	if _, err := (*ScopedRegistry)(nil).Page(0, 1); !errors.Is(err, ErrInvalidPage) {
		t.Errorf("nil page %v", err)
	}
}

func TestPrivateRegistrationCannotLeakViaJSON(t *testing.T) {
	for _, value := range []any{testScope(), testRegistrations(1)[0], CandidatePage{Registrations: testRegistrations(1)}} {
		body, err := json.Marshal(value)
		if !errors.Is(err, ErrPrivateRegistrationNotSerializable) || len(body) != 0 {
			t.Errorf("private metadata encoded: body=%q err=%v", body, err)
		}
	}
}

func TestRegistryConcurrentReads(t *testing.T) {
	registry, err := NewRegistry(testRegistrations(64))
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 16 {
				scoped, err := registry.Scoped(testScope())
				if err != nil {
					t.Error(err)
					return
				}
				page, err := scoped.Page(0, 4)
				if err != nil || len(page.Registrations) != 4 {
					t.Errorf("concurrent page %+v %v", page, err)
					return
				}
				page.Registrations[0].TenantID = domain.TenantID("mutated")
			}
		})
	}
	group.Wait()
}
