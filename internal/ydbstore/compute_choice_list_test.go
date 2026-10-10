package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func computeChoiceListFixture(t *testing.T, count int) (*computeChoiceSourceFixture, *computeChoiceRateFixture, *computeChoiceAttachedCatalog) {
	t.Helper()
	f, fixture := newComputeChoiceRateFixture(t)
	codec, err := computechoice.NewCursorCodec([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	pin := computeChoiceAttachedPin{tenantID: f.cookie.ActiveTenantID, ownerUserID: f.cookie.UserID, subscriptionID: "owned-subscription", actorID: "owned-actor", provider: "codex", workerID: "owned-worker", connectionID: "owned-connection", enrollmentGeneration: 1, connectionGeneration: 1, workerRevision: 1, connectionRevision: 1, capabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("c", 64))}
	entries := make([]computeChoiceAttachedRegistration, 0, count+1)
	for i := 0; i < count; i++ {
		entries = append(entries, computeChoiceAttachedRegistration{enumeration: computechoice.Registration{TenantID: pin.tenantID, OwnerUserID: pin.ownerUserID, ChoiceID: "choice-" + string(rune('a'+i)), RegistrationRevision: 1, DisclosureRevision: strings.Repeat("d", 64)}, pin: pin})
	}
	foreign := entries[0]
	foreign.enumeration.OwnerUserID = "other-owner"
	foreign.enumeration.ChoiceID = "choice-0-foreign"
	foreign.pin.ownerUserID = "other-owner"
	foreign.pin.workerID = "foreign-worker"
	entries = append(entries, foreign)
	catalog, err := newComputeChoiceAttachedCatalog(f.pins, codec, entries)
	if err != nil {
		t.Fatal(err)
	}
	// Configuration input mutation cannot alter the constructed registry/pins.
	entries[0].pin.workerID = "caller-mutated-worker"
	entries[0].enumeration.ChoiceID = "caller-mutated-choice"
	f.driver.expected[computeChoiceAttachedSubscriptionSQL] = []driver.Value{string(pin.tenantID), string(pin.subscriptionID)}
	f.driver.expected[computeChoiceAttachedActorSQL] = []driver.Value{string(pin.tenantID), string(pin.actorID)}
	f.driver.expected[computeChoiceAttachedWorkerSQL] = []driver.Value{string(pin.tenantID), string(pin.ownerUserID), string(pin.workerID)}
	f.driver.expected[computeChoiceAttachedConnectionSQL] = []driver.Value{string(pin.tenantID), string(pin.ownerUserID), string(pin.workerID)}
	f.driver.expected[computeChoiceAttachedSlotSQL] = []driver.Value{string(pin.tenantID), string(pin.subscriptionID)}
	f.driver.expected[computeChoiceAttachedCountersSQL] = []driver.Value{string(pin.tenantID)}
	// All candidate sources absent: each must stay missing/unknown, without
	// initialization or invented capacity. The reader's observed case and source
	// corruption are tested separately with a coherent local protocol fixture.
	return f, fixture, catalog
}

func TestComputeChoiceRatedSourcePageBoundedOwnPositions(t *testing.T) {
	f, fixture, catalog := computeChoiceListFixture(t, 5)
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, computeChoiceRateInput(), "page-first", catalog)
	if err != nil {
		t.Fatal(err)
	}
	if page.rateLimited || len(page.candidates) != 4 || page.lastPosition != 4 || !page.more || !page.readAt.Equal(f.at) || page.throughSequence != 41 || len(page.registryRevision) != 64 {
		t.Fatalf("bounded own-position source page: %+v", page)
	}
	for i, candidate := range page.candidates {
		if candidate.registration.OwnerUserID != f.cookie.UserID || candidate.registration.ChoiceID != "choice-"+string(rune('a'+i)) || candidate.sources.state != computeChoiceAttachedMissing || candidate.sources.slot != nil || candidate.sources.counters != nil {
			t.Fatalf("position=%d leaked foreign/initialized missing source: %+v", i, candidate)
		}
	}
	if len(fixture.statements) != 35 || fixture.writes != 1 || fixture.commits != 1 || fixture.deadlineMissing || len(fixture.isolations) != 1 || fixture.isolations[0] != driverpkgSerializable() {
		t.Fatalf("6 auth +4 finite probes +1debit +4*6 sources SAMEtx: %+v", fixture.ratedSQLFixture)
	}
	if _, err := json.Marshal(page); !errors.Is(err, computechoice.ErrPrivateRegistrationNotSerializable) {
		t.Fatalf("private snapshot must not become public eligibility/permission JSON: %v", err)
	}
	// Exact-key replay rereads every candidate and current auth, not a cached page.
	fixture.statements = nil
	page, err = f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, computeChoiceRateInput(), "page-first", catalog)
	if err != nil || len(page.candidates) != 4 || len(fixture.statements) != 34 || fixture.writes != 1 {
		t.Fatalf("replay must have fresh resources/no second debit: page=%+v sql=%d writes=%d err=%v", page, len(fixture.statements), fixture.writes, err)
	}
}

func TestComputeChoiceRatedSourcePageCursorAndRateDenial(t *testing.T) {
	f, fixture, catalog := computeChoiceListFixture(t, 5)
	scope := computechoice.Scope{TenantID: f.cookie.ActiveTenantID, UserID: f.cookie.UserID, SessionID: f.session.ID, MembershipSecurityVersion: f.member.SecurityVersion}
	scoped, err := catalog.registry.Scoped(scope)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := catalog.cursor.Encode(scope, scoped, computechoice.InputText, 4, f.at, f.at.Add(30*time.Second), f.cookie.IdleExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	input := computeChoiceRateInput()
	input.Cursor = cursor
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, input, "page-next", catalog)
	if err != nil || len(page.candidates) != 1 || page.candidates[0].registration.ChoiceID != "choice-e" || page.more || page.lastPosition != 5 || len(fixture.statements) != 17 {
		t.Fatalf("cursor advances examined own positions only: page=%+v SQL=%d err=%v", page, len(fixture.statements), err)
	}
	if _, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, input, "burst-second", catalog); err != nil {
		t.Fatal(err)
	}
	fixture.statements = nil
	page, err = f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, input, "burst-third", catalog)
	if err != nil || !page.rateLimited || page.retryAfter != 5*time.Second || len(page.candidates) != 0 || page.registryRevision != "" || !page.readAt.IsZero() || page.more || len(fixture.statements) != 10 || fixture.writes != 2 {
		t.Fatalf("denied list must disclose zero candidates/cursor/source reads: page=%+v SQL=%d writes=%d err=%v", page, len(fixture.statements), fixture.writes, err)
	}
}

func TestComputeChoiceRatedSourcePageClearsSpeculationAndReauthorizes(t *testing.T) {
	for _, mode := range []string{"unknown", "revoked retry", "candidate corruption"} {
		t.Run(mode, func(t *testing.T) {
			f, fixture, catalog := computeChoiceListFixture(t, 5)
			switch mode {
			case "unknown":
				fixture.commitModes = []string{"unknown"}
			case "revoked retry":
				fixture.commitModes = []string{"ambiguous"}
				fixture.afterCommit = func() {
					at := f.at
					f.cookie.RevokedAt = &at
					f.syncRows(t)
				}
			case "candidate corruption":
				f.driver.rows[computeChoiceAttachedActorSQL] = []driver.Value{"foreign-current-user"}
			}
			page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, computeChoiceRateInput(), "page-uncertain", catalog)
			if err == nil || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) {
				t.Fatalf("%s leaked speculative page: page=%+v err=%v", mode, page, err)
			}
			if mode == "candidate corruption" && (len(fixture.slots) != 0 || fixture.commits != 0 || fixture.rollbacks != 1) {
				t.Fatalf("candidate corruption must roll back debit: %+v", fixture.ratedSQLFixture)
			}
			if mode == "revoked retry" && (!errors.Is(err, domain.ErrWebSessionRevoked) || fixture.writes != 1) {
				t.Fatalf("retry must reauthorize despite debit receipt: writes=%d err=%v", fixture.writes, err)
			}
		})
	}
}

func TestComputeChoiceRatedSourcePageSyntaxAndCatalogDenyWithoutSQL(t *testing.T) {
	f, fixture, catalog := computeChoiceListFixture(t, 1)
	input := computeChoiceRateInput()
	input.Limit = 5
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, input, "invalid-query", catalog)
	if !errors.Is(err, computechoice.ErrInvalidQuery) || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || len(fixture.statements) != 0 {
		t.Fatalf("bounded syntax precedes all authority work: page=%+v statements=%d err=%v", page, len(fixture.statements), err)
	}
	entries := []computeChoiceAttachedRegistration{{enumeration: computechoice.Registration{TenantID: f.cookie.ActiveTenantID, OwnerUserID: f.cookie.UserID, ChoiceID: "valid-choice", RegistrationRevision: 1, DisclosureRevision: strings.Repeat("d", 64)}, pin: computeChoiceAttachedPin{tenantID: "different-tenant"}}}
	if got, err := newComputeChoiceAttachedCatalog(f.pins, catalog.cursor, entries); got != nil || !errors.Is(err, errComputeChoiceSource) {
		t.Fatalf("mismatching registration/pin accepted: catalog=%+v err=%v", got, err)
	}
}

func TestComputeChoiceRatedSourcePageRereadsCoherentProtocolAfterLostAck(t *testing.T) {
	attached := newComputeChoiceAttachedFixture(t)
	f := attached.source
	_, keys, err := computechoice.ListRateIdentityKeys(f.cookie.ActiveTenantID, f.cookie.UserID)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &computeChoiceRateFixture{computeChoiceSQLFixture: f.driver, keys: keys}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store = &Store{db: db}
	codec, err := computechoice.NewCursorCodec([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := newComputeChoiceAttachedCatalog(f.pins, codec, []computeChoiceAttachedRegistration{{enumeration: computechoice.Registration{TenantID: f.cookie.ActiveTenantID, OwnerUserID: f.cookie.UserID, ChoiceID: "observed-choice", RegistrationRevision: 1, DisclosureRevision: strings.Repeat("d", 64)}, pin: attached.pin}})
	if err != nil {
		t.Fatal(err)
	}
	fixture.commitModes = []string{"ambiguous"}
	fixture.afterCommit = func() {
		// A coherent recorded offline connection keeps its locally restorable
		// protocol, but can no longer satisfy current placement availability.
		attached.worker.ObservedState = domain.AttachedWorkerObservedOffline
		attached.connection.State = domain.AttachedWorkerConnectionOffline
		attached.syncRows(t)
	}
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, computeChoiceRateInput(), "protocol-retry", catalog)
	if err != nil || len(page.candidates) != 1 || page.candidates[0].sources.state != computeChoiceAttachedUnavailable || page.candidates[0].sources.connection == nil || page.candidates[0].sources.connection.State != domain.AttachedWorkerConnectionOffline || fixture.writes != 1 || fixture.commits != 2 || len(fixture.statements) != 33 {
		t.Fatalf("lost ACK returned old placement/substituted resource/redebited: page=%+v SQL=%d writes=%d commits=%d err=%v", page, len(fixture.statements), fixture.writes, fixture.commits, err)
	}
}

func TestComputeChoiceRatedSourcePageRetryAggregateCeiling(t *testing.T) {
	f, fixture, catalog := computeChoiceListFixture(t, 5)
	fixture.commitModes = []string{"rollback", "rollback", "rollback"}
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, computeChoiceRateInput(), "page-budget-retry", catalog)
	if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || len(fixture.statements) != computechoice.MaxStatementsTotal || len(fixture.slots) != 0 || fixture.commits != 2 {
		t.Fatalf("full candidate graph retry must stop at96 with no partial page/debit: page=%+v SQL=%d slots=%d commits=%d err=%v", page, len(fixture.statements), len(fixture.slots), fixture.commits, err)
	}
}

func TestComputeChoiceRatedSourcePageForeignCursorIsNotPermission(t *testing.T) {
	f, fixture, catalog := computeChoiceListFixture(t, 5)
	foreignSession := computechoice.Scope{TenantID: f.cookie.ActiveTenantID, UserID: f.cookie.UserID, SessionID: "other-session", MembershipSecurityVersion: f.member.SecurityVersion}
	scoped, err := catalog.registry.Scoped(foreignSession)
	if err != nil {
		t.Fatal(err)
	}
	token, err := catalog.cursor.Encode(foreignSession, scoped, computechoice.InputText, 4, f.at, f.at.Add(30*time.Second), f.cookie.IdleExpiresAt)
	if err != nil {
		t.Fatal(err)
	}
	input := computeChoiceRateInput()
	input.Cursor = token
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, input, "foreign-cursor", catalog)
	if !errors.Is(err, computechoice.ErrChoiceStale) || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || fixture.commits != 0 || len(fixture.slots) != 0 {
		t.Fatalf("MACed different-Session cursor leaked candidates/committed debit: page=%+v commits=%d slots=%d error=%v", page, fixture.commits, len(fixture.slots), err)
	}
	for _, statement := range fixture.statements {
		if statement == computeChoiceAttachedWorkerSQL || statement == computeChoiceAttachedConnectionSQL || statement == computeChoiceAttachedSubscriptionSQL {
			t.Fatal("foreign cursor reached candidate sources")
		}
	}
}
