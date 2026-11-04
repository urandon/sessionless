package ydbstore

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// LocalSearchBFFSeed exposes persisted DATA and constructor inputs, not a worker
// Store or its body owner. Source intake/funding, reserve/burn/allocation and
// argument storage are real. Canonical running rows and the pending request are
// explicitly structural SQL DATA: this is NOT a MODEL producer or send proof.
type LocalSearchBFFSeed struct {
	Installation *LocalSearchSourceFixture
	ReceiptPath  string
	Blobs        ports.BlobStore
	Input        ports.ReadPrivateWebSearchConsentRequestV1
	Query        string
	MaxResults   *uint8
	view         *privateConsentViewFixture
}

func NewLocalSearchBFFSeed(t *testing.T, maxResults *uint8) *LocalSearchBFFSeed {
	t.Helper()
	f := NewLocalSearchSourceFixture(t, "http://127.0.0.1:11434/v1", time.Time{})
	scope := localSearchScope(f.Receipt.Profile, "private-bff")
	pkg := f.PublishPackage(t, scope)
	state := currentWorkerAuthorityFixture(t)
	state.Run.TenantID, state.Run.ID, state.Run.SessionID = scope.TenantID, scope.RunID, scope.SessionID
	state.Run.SubscriptionConnectionID = ""
	association := &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, ResourceID: f.Receipt.Profile.Resource.ResourceID}
	state.Run.LocalCapacityAssociation = association
	state.Job.TenantID, state.Job.RunID, state.Job.SessionID, state.Job.AttemptID, state.Job.CredentialOwnerUserID = scope.TenantID, scope.RunID, scope.SessionID, scope.AttemptID, scope.OwnerUserID
	state.Job.LocalCapacityAssociation = state.Run.Clone().LocalCapacityAssociation
	state.Job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 1}
	state.Job.ContextSnapshot.TenantID = scope.TenantID
	state.Job.ContextSnapshot.Key = domain.SessionObjectPrefix(scope.TenantID, scope.SessionID) + "context"
	state.Job.DeliveryChat.TenantID = scope.TenantID
	state.Attempt.TenantID, state.Attempt.RunID, state.Attempt.ID = scope.TenantID, scope.RunID, scope.AttemptID
	state.Reservation.TenantID, state.Reservation.RunID, state.Reservation.SubscriptionConnectionID = scope.TenantID, scope.RunID, ""
	state.Reservation.LocalCapacityAssociation = state.Run.Clone().LocalCapacityAssociation
	state.InputManifest.TenantID, state.InputManifest.RunID = scope.TenantID, scope.RunID
	parent := newOwnedAttemptEffectParentFixture(t, ownedFinalPrebuild{at: pkg.Source.IssuedAt, state: &state, sources: &ownedFinalSources{profile: f.Receipt.Profile, spending: pkg.Source.Spending}})
	declareLocalSessionCapacityRows(t, parent)
	if f.Source.VerifyOwnedAttemptEffectSource(parent.source) != nil || !reflect.DeepEqual(parent.source.Spending, pkg.Source.Spending) {
		t.Fatal("BFF seed rewrote an original signed spending publication")
	}
	operate, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return parent.graph.at }, strings.NewReader(strings.Repeat("B", 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := NewLocalOwnedStoreBootstrapV1(f.Source, parent.issuer.issuer, operate)
	if err != nil {
		t.Fatal(err)
	}
	allocation := ownedAllocationFixture{parent: parent, sql: &allocationSQLFixture{parent: parent.sql}}
	view := &privateConsentViewFixture{allocation: allocation}
	workerDB := sql.OpenDB(privateConsentViewConnector{view})
	workerDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = workerDB.Close() })
	workerStore, err := bootstrap.CreateStore(workerDB, Options{OwnedProfilePublicationVerifier: f.Source}, &localBootstrapDenyKeeper{identity: 1})
	if err != nil {
		t.Fatal(err)
	}
	parent.store = workerStore
	bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(parent.issuer.issuer, parent.parent.Authority.HarnessBinding.Backend, parent.parent.Authority.SubstrateBinding, f.Source, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bridge.Close() })
	if err := bootstrap.Complete(bridge, nil); err != nil {
		t.Fatal(err)
	}
	reserved := parent.reserve(t, parent.request)
	if reserved.Grant == nil {
		t.Fatal("BFF seed lacks original sole-parent grant")
	}
	publication, err := bridge.PrepareAllocationPublication(t.Context(), reserved.Grant.Clone())
	if err != nil {
		t.Fatalf("BFF seed original runtime preparation: %v", err)
	}
	if _, err := workerStore.publishBoundedOwnedAllocation(t.Context(), publication); err != nil {
		t.Fatalf("BFF seed original allocation publication: %v", err)
	}
	view.allocation.publication, view.allocation.bridge = publication, bridge
	query := "public compiler documentation — \"exact\"\nsecond line"
	args, err := json.Marshal(struct {
		Query      string `json:"query"`
		MaxResults *uint8 `json:"max_results,omitempty"`
	}{query, maxResults})
	if err != nil {
		t.Fatal(err)
	}
	blobs := &localEntryBlobs{}
	bodyOwner, err := operationbody.NewV1(ownedOperationScope(parent.parent), parent.parent.Authority.HarnessBinding.AgentLoopPolicy.Profile, parent.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits, blobs)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := bodyOwner.WriteConsentArguments(t.Context(), args)
	if err != nil {
		t.Fatal(err)
	}
	at := parent.graph.at
	grant := f.GrantV2
	r := domain.WebSearchConsentRequestV1{Version: 1, ID: "private-bff-consent", Scope: ownedConsentScope(parent.parent), InitiatingUserID: scope.OwnerUserID, ModelResponseDigest: strings.Repeat("6", 64), CallID: "private-bff-call", ArgumentsDigest: ownedConsentTestHash(args), QueryDigest: ownedConsentTestHash([]byte(query)), GrantDigest: f.Receipt.Profile.WebSearchGrantDigest, ConsentRevision: grant.ConsentRevision, RevocationGeneration: grant.RevocationGeneration, Arguments: ref, CreatedAt: at, ExpiresAt: at.Add(time.Second)}
	if err := r.ValidateArguments(args); err != nil {
		t.Fatal("BFF structural pending DATA invalid", err)
	}
	view.state = domain.WebSearchConsentStateV1{Request: r, Revision: 1, Status: domain.WebSearchConsentPendingV1, UpdatedAt: at}
	view.web = domain.WebSession{SessionDigest: domain.DigestSecret("private-bff-cookie"), CSRFTokenDigest: domain.DigestSecret("private-bff-csrf"), UserID: scope.OwnerUserID, ActiveTenantID: scope.TenantID, AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "owned-yandex-subject"}, MembershipSecurityVersion: 1, IssuedAt: at, LastSeenAt: at, IdleExpiresAt: at.Add(time.Hour), AbsoluteExpiresAt: at.Add(2 * time.Hour)}
	view.input = ports.ReadPrivateWebSearchConsentRequestV1{SessionID: scope.SessionID, RunID: scope.RunID, AttemptID: scope.AttemptID, RequestID: r.ID, WebSessionDigest: view.web.SessionDigest}
	var run domain.Run
	if json.Unmarshal([]byte(parent.graph.rows["runs"][0].(string)), &run) != nil {
		t.Fatal("BFF seed missing canonical run")
	}
	author, runID := scope.OwnerUserID, scope.RunID
	event := domain.SessionEvent{ID: run.TriggerEventID, TenantID: scope.TenantID, SessionID: scope.SessionID, Sequence: 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &author, RunID: &runID, IdempotencyKey: run.IdempotencyKey, CreatedAt: run.CreatedAt, Payload: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionEventObjectPrefix(scope.TenantID, scope.SessionID, run.TriggerEventID) + "message.json", Size: 1, SHA256: strings.Repeat("d", 64)}}
	if event.Validate() != nil {
		t.Fatal("BFF canonical initiating event DATA invalid")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	view.triggerSequence, view.triggerRecord = event.Sequence, &text
	path := filepath.Join(f.Root, "bff-receipt.json")
	slot, err := localoperator.ReserveProfileReceipt(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slot.Close() })
	if err := slot.WriteReceipt(t.Context(), f.Receipt); err != nil {
		t.Fatal(err)
	}
	return &LocalSearchBFFSeed{Installation: f, ReceiptPath: path, Blobs: blobs, Input: view.input, Query: query, MaxResults: maxResults, view: view}
}

func (s *LocalSearchBFFSeed) NewBFFDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(privateConsentViewConnector{s.view})
	db.SetMaxOpenConns(1)
	if db == s.view.allocation.parent.store.db {
		t.Fatal("BFF borrowed the worker database owner")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// These faults change only modeled current canonical rows, never a Source,
// signed publication, sealed parent or the original pending description.
func (s *LocalSearchBFFSeed) RevokeCurrentMembership(t *testing.T) {
	t.Helper()
	row := s.view.allocation.parent.base.membership
	var m domain.TenantMembership
	if json.Unmarshal([]byte(row[0].(string)), &m) != nil {
		t.Fatal("missing canonical membership DATA")
	}
	m.SecurityVersion++
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	s.view.allocation.parent.base.membership = []driver.Value{string(raw), string(m.Role), string(m.Status), int64(m.SecurityVersion)}
}

func (s *LocalSearchBFFSeed) CorruptStoredArguments() {
	s.Blobs.(*localEntryBlobs).objects[s.view.state.Request.Arguments.Key][0] ^= 1
}

func (s *LocalSearchBFFSeed) CanonicalWriteCount() int {
	return s.view.allocation.parent.graph.writes
}
