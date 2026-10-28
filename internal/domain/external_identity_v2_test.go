package domain_test

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func identityV2TestTime() time.Time { return time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC) }

func identityV2Fixture(provider domain.IdentityProvider, revision domain.AuthRevisionV2) domain.ExternalIdentityV2 {
	return domain.ExternalIdentityV2{Version: 2, Subject: domain.ExternalSubject{Provider: provider, Subject: "123"}, UserID: "user-v2", CreatedAt: identityV2TestTime(), State: domain.ExternalIdentityActiveV2, Revision: revision, UpdatedAt: identityV2TestTime()}
}

func TestExternalIdentityV2SetValidation(t *testing.T) {
	yandex, telegram := identityV2Fixture(domain.IdentityProviderYandex, 7), identityV2Fixture(domain.IdentityProviderTelegram, 7)
	for _, test := range []struct {
		name string
		rows []domain.ExternalIdentityV2
		want bool
	}{
		{name: "single", rows: []domain.ExternalIdentityV2{yandex}, want: true},
		{name: "namespace isolated equal subjects", rows: []domain.ExternalIdentityV2{yandex, telegram}, want: true},
		{name: "empty"},
		{name: "three", rows: []domain.ExternalIdentityV2{yandex, telegram, yandex}},
		{name: "duplicate provider", rows: []domain.ExternalIdentityV2{yandex, yandex}},
		{name: "different revision", rows: []domain.ExternalIdentityV2{yandex, identityV2Fixture(domain.IdentityProviderTelegram, 8)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := domain.ValidateExternalIdentitySetV2("user-v2", test.rows)
			if (err == nil) != test.want || (test.want && got != 7) || (!test.want && got != 0) {
				t.Errorf("ValidateExternalIdentitySetV2(%+v) = (%v, %v), want valid=%v", test.rows, got, err, test.want)
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*domain.ExternalIdentityV2)
	}{
		{name: "wrong version", mutate: func(v *domain.ExternalIdentityV2) { v.Version = 1 }},
		{name: "unknown provider", mutate: func(v *domain.ExternalIdentityV2) { v.Subject.Provider = "other" }},
		{name: "empty subject", mutate: func(v *domain.ExternalIdentityV2) { v.Subject.Subject = "" }},
		{name: "foreign user", mutate: func(v *domain.ExternalIdentityV2) { v.UserID = "other" }},
		{name: "unlinked", mutate: func(v *domain.ExternalIdentityV2) { v.State = domain.ExternalIdentityUnlinkedV2 }},
		{name: "unknown state", mutate: func(v *domain.ExternalIdentityV2) { v.State = "suspended" }},
		{name: "zero revision", mutate: func(v *domain.ExternalIdentityV2) { v.Revision = 0 }},
		{name: "zero creation", mutate: func(v *domain.ExternalIdentityV2) { v.CreatedAt = time.Time{} }},
		{name: "backwards update", mutate: func(v *domain.ExternalIdentityV2) { v.UpdatedAt = v.CreatedAt.Add(-time.Nanosecond) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			row := yandex
			test.mutate(&row)
			if got, err := domain.ValidateExternalIdentitySetV2("user-v2", []domain.ExternalIdentityV2{row}); err == nil || got != 0 {
				t.Errorf("invalid set %+v = (%v, %v), want zero/error", row, got, err)
			}
		})
	}
}

func TestExternalIdentityV2OwnershipImmutable(t *testing.T) {
	original := identityV2Fixture(domain.IdentityProviderYandex, 4)
	for _, test := range []struct {
		name   string
		mutate func(*domain.ExternalIdentityV2)
	}{
		{name: "provider", mutate: func(v *domain.ExternalIdentityV2) { v.Subject.Provider = domain.IdentityProviderTelegram }},
		{name: "subject", mutate: func(v *domain.ExternalIdentityV2) { v.Subject.Subject = "456" }},
		{name: "user", mutate: func(v *domain.ExternalIdentityV2) { v.UserID = "other" }},
		{name: "creation", mutate: func(v *domain.ExternalIdentityV2) { v.CreatedAt = v.CreatedAt.Add(-time.Second) }},
		{name: "state without advance", mutate: func(v *domain.ExternalIdentityV2) { v.State = domain.ExternalIdentityUnlinkedV2 }},
		{name: "revision backwards", mutate: func(v *domain.ExternalIdentityV2) { v.Revision = 3 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := original
			test.mutate(&next)
			if err := domain.ValidateExternalIdentityOwnershipV2(original, next); err == nil {
				t.Errorf("ownership update %+v -> %+v accepted", original, next)
			}
		})
	}
	next := original
	next.State, next.Revision, next.UpdatedAt = domain.ExternalIdentityUnlinkedV2, 5, original.UpdatedAt.Add(time.Second)
	if err := domain.ValidateExternalIdentityOwnershipV2(original, next); err != nil {
		t.Errorf("ownership-preserving unlink = %v", err)
	}
}

func TestPlanExternalIdentityChangeV2BothDirectionsAndTombstone(t *testing.T) {
	for _, sourceProvider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(sourceProvider), func(t *testing.T) {
			targetProvider := domain.IdentityProviderTelegram
			if sourceProvider == targetProvider {
				targetProvider = domain.IdentityProviderYandex
			}
			source := identityV2Fixture(sourceProvider, 7)
			targetSubject := domain.ExternalSubject{Provider: targetProvider, Subject: "123"}
			enabled := []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram}
			at := source.CreatedAt.Add(time.Minute)
			active := []domain.ExternalIdentityV2{source}
			plan, err := domain.PlanExternalIdentityChangeV2(source.UserID, active, targetSubject, nil, domain.IdentityOperationLinkV2, enabled, at)
			if err != nil {
				t.Fatalf("link %s -> %s = %v", sourceProvider, targetProvider, err)
			}
			if len(plan.Active) != 2 || plan.Unlinked != nil || plan.BeforeRevision != 7 || plan.AfterRevision != 8 {
				t.Fatalf("link plan = %+v, want two active revision 8", plan)
			}
			if active[0] != source || plan.Active[0].CreatedAt != source.CreatedAt || plan.Active[1].CreatedAt != at {
				t.Errorf("link mutated input or creation history: source=%+v active=%+v plan=%+v", source, active, plan)
			}
			target := plan.Active[1]
			unlinked, err := domain.PlanExternalIdentityChangeV2(source.UserID, plan.Active, targetSubject, &target, domain.IdentityOperationUnlinkV2, enabled, at.Add(time.Minute))
			if err != nil || len(unlinked.Active) != 1 || unlinked.Unlinked == nil {
				t.Fatalf("unlink = (%+v, %v), want one survivor and tombstone", unlinked, err)
			}
			if unlinked.AfterRevision != 9 || unlinked.Active[0].Revision != 9 || unlinked.Unlinked.Revision != 9 || unlinked.Unlinked.CreatedAt != target.CreatedAt || unlinked.Unlinked.State != domain.ExternalIdentityUnlinkedV2 {
				t.Errorf("unlink revisions/history = %+v", unlinked)
			}
			tombstone := *unlinked.Unlinked
			tombstone.Revision = 99
			relinked, err := domain.PlanExternalIdentityChangeV2(source.UserID, unlinked.Active, targetSubject, &tombstone, domain.IdentityOperationLinkV2, enabled, at.Add(2*time.Minute))
			if err != nil || relinked.AfterRevision != 100 || relinked.Active[1].CreatedAt != target.CreatedAt {
				t.Fatalf("relink higher tombstone = (%+v, %v), want revision100 immutable creation", relinked, err)
			}
			if tombstone.State != domain.ExternalIdentityUnlinkedV2 || plan.Active[0].Revision != 8 || unlinked.Active[0].Revision != 9 {
				t.Error("planning mutated an input/tombstone or aliased earlier plans")
			}
		})
	}
}

func TestPlanExternalIdentityChangeV2Denials(t *testing.T) {
	yandex := identityV2Fixture(domain.IdentityProviderYandex, 7)
	telegram := identityV2Fixture(domain.IdentityProviderTelegram, 7)
	at := identityV2TestTime().Add(time.Minute)
	both := []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram}
	for _, test := range []struct {
		name    string
		active  []domain.ExternalIdentityV2
		target  domain.ExternalIdentityV2
		kind    domain.IdentityOperationKindV2
		enabled []domain.IdentityProvider
		at      time.Time
		wantErr error
	}{
		{name: "foreign target", active: []domain.ExternalIdentityV2{yandex}, target: func() domain.ExternalIdentityV2 {
			v := telegram
			v.UserID = "other"
			v.State = domain.ExternalIdentityUnlinkedV2
			return v
		}(), kind: domain.IdentityOperationLinkV2, enabled: both, at: at, wantErr: domain.ErrExternalIdentityConflict},
		{name: "active target", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: both, at: at, wantErr: domain.ErrExternalIdentityConflict},
		{name: "occupied provider", active: []domain.ExternalIdentityV2{yandex, telegram}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: both, at: at, wantErr: domain.ErrExternalIdentityConflict},
		{name: "unenabled target", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: []domain.IdentityProvider{domain.IdentityProviderYandex}, at: at, wantErr: domain.ErrIdentityProviderUnavailableV2},
		{name: "only method", active: []domain.ExternalIdentityV2{yandex}, target: yandex, kind: domain.IdentityOperationUnlinkV2, enabled: both, at: at, wantErr: domain.ErrIdentityLastMethodV2},
		{name: "disabled survivor", active: []domain.ExternalIdentityV2{yandex, telegram}, target: yandex, kind: domain.IdentityOperationUnlinkV2, enabled: []domain.IdentityProvider{domain.IdentityProviderYandex}, at: at, wantErr: domain.ErrIdentityLastMethodV2},
		{name: "target not active", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationUnlinkV2, enabled: both, at: at, wantErr: domain.ErrExternalIdentityConflict},
		{name: "changed target", active: []domain.ExternalIdentityV2{yandex, telegram}, target: func() domain.ExternalIdentityV2 { v := telegram; v.Revision++; return v }(), kind: domain.IdentityOperationUnlinkV2, enabled: both, at: at, wantErr: domain.ErrExternalIdentityConflict},
		{name: "overflow", active: []domain.ExternalIdentityV2{identityV2Fixture(domain.IdentityProviderYandex, math.MaxUint64)}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: both, at: at, wantErr: domain.ErrExternalIdentityConflict},
		{name: "unknown kind", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: "merge", enabled: both, at: at},
		{name: "duplicate enabled", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderYandex}, at: at},
		{name: "unknown enabled", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: []domain.IdentityProvider{"other"}, at: at},
		{name: "empty enabled", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, at: at},
		{name: "zero time", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: both},
		{name: "time before source", active: []domain.ExternalIdentityV2{yandex}, target: telegram, kind: domain.IdentityOperationLinkV2, enabled: both, at: identityV2TestTime().Add(-time.Nanosecond)},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := append([]domain.ExternalIdentityV2(nil), test.active...)
			plan, err := domain.PlanExternalIdentityChangeV2("user-v2", test.active, test.target.Subject, &test.target, test.kind, test.enabled, test.at)
			if err == nil || !reflect.DeepEqual(plan, domain.ExternalIdentityChangePlanV2{}) || !reflect.DeepEqual(test.active, original) {
				t.Errorf("denied plan = (%+v, %v), input=%+v original=%+v", plan, err, test.active, original)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("plan error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestPlanExternalIdentityChangeV2OverflowAndNewProviderSlot(t *testing.T) {
	source := identityV2Fixture(domain.IdentityProviderYandex, math.MaxUint64)
	target := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "456"}
	enabled := []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram}
	at := identityV2TestTime().Add(time.Minute)
	if plan, err := domain.PlanExternalIdentityChangeV2(source.UserID, []domain.ExternalIdentityV2{source}, target, nil, domain.IdentityOperationLinkV2, enabled, at); !errors.Is(err, domain.ErrAuthRevisionOverflowV2) || plan.Active != nil {
		t.Errorf("overflow plan = (%+v, %v), want no plan and overflow", plan, err)
	}
	source.Revision = 7
	plan, err := domain.PlanExternalIdentityChangeV2(source.UserID, []domain.ExternalIdentityV2{source}, target, nil, domain.IdentityOperationLinkV2, enabled, at)
	if err != nil || plan.Active[1].Subject != target {
		t.Errorf("new unused provider subject = (%+v, %v)", plan, err)
	}
}
