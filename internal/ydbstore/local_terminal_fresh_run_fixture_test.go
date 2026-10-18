package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// TEST ONLY keyed durable DATA. A completely new Source/factory/Store/issuer
// consumes these rows. The previous private cleanup/accounting owners, TTL
// Attempt/Job/lease rows and ordinary request bodies are not installed here.
type LocalFreshRunHistorySQLFixture struct {
	runs                                 map[domain.RunID]string
	details                              map[domain.AttemptID][]driver.Value
	markers                              map[domain.RunID][2]string
	manifests                            map[domain.ArtifactManifestID]string
	identities                           map[domain.AttemptID]freshRunHistoryIdentity
	DetailReads                          map[domain.RunID]int
	legacyHeads                          map[domain.RunID]string
	legacyEvidence                       map[domain.AttemptID][]driver.Value
	LegacyHeadReads, LegacyEvidenceReads int
	tenant                               domain.TenantID
	session                              domain.SessionID
	oldRun                               domain.RunID
	oldAttempt                           domain.AttemptID
	oldLease                             domain.LeaseID
	oldClaim                             string
	HistoryReads, TTLReads               int
	changeDiscoveryAfterDetail           bool
	DiscoveryChanges                     int
	changeLegacyAfterDetail              bool
	LegacyChanges                        int
}

// Immutable storage locators only, never the prior Source/issuer/owner.
type freshRunHistoryIdentity struct {
	run      domain.RunID
	lease    domain.LeaseID
	claim    string
	manifest domain.ArtifactManifestID
}

type freshRunHistoryConnector struct {
	base    driver.Connector
	history *LocalFreshRunHistorySQLFixture
}

func (c freshRunHistoryConnector) Driver() driver.Driver { return c.base.Driver() }
func (c freshRunHistoryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &freshRunHistoryConn{Conn: conn, history: c.history}, nil
}

type freshRunHistoryConn struct {
	driver.Conn
	history *LocalFreshRunHistorySQLFixture
}

func (c *freshRunHistoryConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}
func (c *freshRunHistoryConn) ExecContext(ctx context.Context, s string, a []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, s, a)
}
func (c *freshRunHistoryConn) QueryContext(ctx context.Context, s string, a []driver.NamedValue) (driver.Rows, error) {
	f := c.history
	if s == localTerminalHistoryRunsSQL(len(a)-3) {
		if len(a) < 4 || a[0].Value != string(f.tenant) || a[1].Value != string(f.session) || ctx.Err() != nil {
			return nil, errors.New("fresh history discovery scope")
		}
		cap, ok := a[len(a)-1].Value.(int64)
		if !ok || cap < 1 || cap > 32769 || cap*int64(len(a)-2) > 1<<20 {
			return nil, errors.New("fresh discovery transfer bound")
		}
		ids := make([]string, 0, len(a)-3)
		seen := make(map[string]bool)
		for _, arg := range a[2 : len(a)-1] {
			id, ok := arg.Value.(string)
			if !ok || seen[id] {
				return nil, errors.New("fresh discovery exact keys")
			}
			seen[id] = true
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var rows [][]driver.Value
		for _, id := range ids {
			if raw, ok := f.runs[domain.RunID(id)]; ok {
				if int64(len(raw)) > cap {
					raw = raw[:cap]
				}
				marker := f.markers[domain.RunID(id)]
				rows = append(rows, []driver.Value{id, raw, marker[0][:min(len(marker[0]), 33)], marker[1][:min(len(marker[1]), 74)]})
			}
		}
		f.HistoryReads++
		return &compactionSourceRows{rows: rows}, nil
	}
	if s == localTerminalLegacyManifestsSQL(len(a)-2) {
		return canonicalHistoryManifestRows(f.tenant, f.manifests, a)
	}
	if s == localTerminalLegacyHeadSQL {
		if len(a) != 2 || ctx.Err() != nil || a[0].Value != string(f.tenant) {
			return nil, errors.New("legacy head exact tenant/key")
		}
		id, ok := a[1].Value.(string)
		if !ok {
			return nil, errors.New("legacy head Run key")
		}
		f.LegacyHeadReads++
		raw, found := f.legacyHeads[domain.RunID(id)]
		if !found {
			return &compactionSourceRows{}, nil
		}
		return &explanationFixtureRows{values: []driver.Value{raw[:min(len(raw), 8193)]}}, nil
	}
	if s == localTerminalLegacyEvidenceSQL {
		if len(a) != 3 || ctx.Err() != nil || a[0].Value != string(f.tenant) || a[2].Value != string(domain.ProviderEffectTurnV1) {
			return nil, errors.New("legacy evidence exact tenant/kind")
		}
		id, ok := a[1].Value.(string)
		if !ok {
			return nil, errors.New("legacy evidence Attempt key")
		}
		f.LegacyEvidenceReads++
		row, found := f.legacyEvidence[domain.AttemptID(id)]
		if !found {
			return &compactionSourceRows{}, nil
		}
		copy := append([]driver.Value(nil), row...)
		if len(copy) != 7 {
			return nil, errors.New("legacy evidence seven exact bounded cells")
		}
		for i, cap := range []int{161, 161, 161, 65, 32769, 32769} {
			raw, ok := copy[i].(string)
			if !ok {
				return nil, errors.New("legacy evidence String cell")
			}
			copy[i] = raw[:min(len(raw), cap)]
		}
		return &explanationFixtureRows{values: copy}, nil
	}
	if s == localTerminalHistorySQL {
		if len(a) != 4 || ctx.Err() != nil || a[0].Value != string(f.tenant) || a[2].Value != string(domain.ProviderEffectTurnV1) {
			return nil, errors.New("fresh immutable terminal exact keys")
		}
		attempt, ok := a[1].Value.(string)
		identity, found := f.identities[domain.AttemptID(attempt)]
		if !ok || !found || a[3].Value != string(identity.manifest) {
			return nil, errors.New("fresh immutable terminal attempt/manifest association")
		}
		row := f.details[domain.AttemptID(attempt)]
		f.HistoryReads++
		f.DetailReads[identity.run]++
		original := append([]driver.Value(nil), row...)
		if f.changeDiscoveryAfterDetail {
			if len(original) != 11 || f.markers[f.oldRun][1] != original[7] {
				return nil, errors.New("cached-history fault requires original matching snapshot")
			}
			marker := f.markers[f.oldRun]
			digest, err := decodeLocalFinalizationMarkerV2(marker[1])
			if err != nil {
				return nil, err
			}
			replacement := "a" + digest[1:]
			if replacement == digest {
				replacement = "b" + digest[1:]
			}
			marker[1], err = encodeLocalFinalizationMarkerV2(replacement)
			if err != nil {
				return nil, err
			}
			// Raw durable DATA changes only AFTER this exact original snapshot
			// has been read. The first source history installation remains valid;
			// its later Store reserve/send rejoin must reject the disagreement.
			f.markers[f.oldRun] = marker
			f.changeDiscoveryAfterDetail = false
			f.DiscoveryChanges++
		}
		if f.changeLegacyAfterDetail {
			var selected domain.Run
			for id, raw := range f.runs {
				if id == f.oldRun {
					continue
				}
				var run domain.Run
				if json.Unmarshal([]byte(raw), &run) != nil || run.Validate() != nil || run.TerminalCustody != nil || run.DerivedContext != nil || domain.ValidateSHA256Digest("fixture.legacy", f.markers[id][1]) != nil || selected.ID != "" {
					return nil, errors.New("legacy currentness fault requires exactly one valid canonical V1 Run")
				}
				selected = run
			}
			if selected.ID == "" {
				return nil, errors.New("legacy currentness fault missing original V1 Run")
			}
			// CreatedAt is not in V1's output seal. Preserve marker, manifest
			// and canonical bytes exactly; change only a valid full Run field.
			selected.CreatedAt = selected.CreatedAt.Add(-time.Microsecond)
			if selected.Validate() != nil {
				return nil, errors.New("legacy currentness mutation must remain valid")
			}
			raw, err := json.Marshal(selected)
			if err != nil {
				return nil, err
			}
			f.runs[selected.ID] = string(raw)
			f.changeLegacyAfterDetail = false
			f.LegacyChanges++
		}
		return &explanationFixtureRows{values: original}, nil
	}
	// A historical dependency on operational TTL rows is a test failure, not
	// a convenient copied old fixture. New Run's own rows still use production.
	if strings.Contains(s, "FROM attempts") || strings.Contains(s, "FROM worker_jobs") || strings.Contains(s, "FROM lease_heads") || strings.Contains(s, "FROM leases") {
		for _, arg := range a {
			for attempt, identity := range f.identities {
				if arg.Value == string(identity.run) || arg.Value == string(attempt) || arg.Value == string(identity.lease) || arg.Value == identity.claim {
					f.TTLReads++
					return nil, errors.New("old TTL state is gone")
				}
			}
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, s, a)
}

// NewFreshRunAfterTerminalSeed opens the unchanged private installation through
// its real OpenExisting API. All new scope/funding/canonical rows are declared
// BEFORE the caller constructs NewLocalV1; no producer or fit result is injected.
func NewFreshRunAfterTerminalSeed(t *testing.T, old *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, blobs ports.BlobStore, name string, previous *LocalFreshRunHistorySQLFixture) (*LocalTerminalPublicationSessionConnectedSeed, *LocalFreshRunHistorySQLFixture, *LocalTerminalCompletionSQLFixture) {
	t.Helper()
	oldState := completed.Snapshot(t)
	if name == "" || completed.cleanup.seed != old || oldState.Run.Status != domain.RunSucceeded || oldState.Finalizations != 1 || oldState.Run.TerminalCustody == nil || len(oldState.Events) != 1 {
		t.Fatal("genuine successful producing Run required")
	}
	history := localFreshCommittedHistory(t, old, oldState, previous)
	events := append(old.CanonicalSourceEvents(t), oldState.Events...)
	return newFreshRunFromCommittedHistory(t, old.LocalSessionConnectedSeed, oldState.Session, events, history, blobs, name)
}

// The common intake consumes committed STORAGE DATA and installation files,
// never a prior live Source, issuer, prepared owner, lease or grant.
func newFreshRunFromCommittedHistory(t *testing.T, installation *LocalSessionConnectedSeed, session domain.Session, events []domain.SessionEvent, history *LocalFreshRunHistorySQLFixture, blobs ports.BlobStore, name string) (*LocalTerminalPublicationSessionConnectedSeed, *LocalFreshRunHistorySQLFixture, *LocalTerminalCompletionSQLFixture) {
	t.Helper()
	if name == "" || len(events) == 0 || session.LastEventSequence != events[len(events)-1].Sequence || history.tenant != session.TenantID || history.session != session.ID {
		t.Fatal("complete committed prefix required before fresh intake")
	}
	source, err := localoperator.OpenExisting(t.Context(), installation.installationRoot, installation.executable, installation.receipt)
	if err != nil {
		t.Fatal("new process original installation intake", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	cfg, err := source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	profile := installation.receipt.Profile.Clone()
	state := currentWorkerAuthorityFixture(t)
	at := time.Now().UTC().Truncate(time.Microsecond)
	state.Run.ID, state.Run.IdempotencyKey, state.Run.TriggerEventID = domain.RunID(name+"-run"), domain.IdempotencyKey(name+"-idempotency"), domain.SessionEventID(name+"-user")
	state.Run.CreatedAt, state.Run.UpdatedAt = at, at
	state.Job.RunID, state.Job.TriggerEventID, state.Job.AttemptID, state.Job.ReservationID, state.Job.InputManifestID = state.Run.ID, state.Run.TriggerEventID, domain.AttemptID(name+"-attempt"), domain.QuotaReservationID(name+"-reservation"), domain.ArtifactManifestID(name+"-input-manifest")
	state.Attempt.ID, state.Attempt.RunID, state.Attempt.CreatedAt, state.Attempt.UpdatedAt = state.Job.AttemptID, state.Run.ID, at, at
	state.Reservation.ID, state.Reservation.RunID, state.Reservation.HeldAt, state.Reservation.UpdatedAt, state.Reservation.ExpiresAt = state.Job.ReservationID, state.Run.ID, at, at, at.Add(time.Hour)
	state.InputManifest.ID, state.InputManifest.RunID, state.InputManifest.CreatedAt = state.Job.InputManifestID, state.Run.ID, at
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: state.Run.TenantID, SessionID: state.Run.SessionID, OwnerUserID: state.Job.CredentialOwnerUserID, RunID: state.Run.ID, AttemptID: state.Attempt.ID}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range ownedSpendingKinds {
		if i < 2 {
			grant, e := source.SpendingPublication(t.Context(), scope, kind)
			if e != nil {
				t.Fatal(e)
			}
			spending[i] = grant.Authorization
		} else {
			spending[i] = localRuntimeStructuralSpending(t, profile, scope, kind)
		}
	}
	// Actual Source publication observes its own wall clock. Sample the SAME
	// authority only after all genuine grants exist, so their ObservedAt cannot
	// be in the future of the parent/current snapshot. Earlier canonical Run
	// creation remains unchanged; no lifetime or grant field is rewritten.
	at = time.Now().UTC().Truncate(time.Microsecond)
	parent := newOwnedAttemptEffectParentFixture(t, ownedFinalPrebuild{at: at, state: &state, sources: &ownedFinalSources{profile: profile, spending: spending}})
	operate, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, strings.NewReader(strings.Repeat("N", 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	operationSQL := &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{&allocationSQLFixture{parent: parent.sql}}}, sampleWallClock: true}
	queries := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: operationSQL}}
	engine := &compactionPublicationSQL{compactionEffectSQL: &compactionEffectSQL{compactionSourceSQL: queries}}
	var job domain.WorkerJob
	if json.Unmarshal([]byte(parent.graph.rows["worker_jobs"][0].(string)), &job) != nil {
		t.Fatal("new original job")
	}
	anchor := events[len(events)-1]
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: anchor.Sequence + 1}
	ratedFixturePut(t, parent.graph.ratedSQLFixture, "worker_jobs", job)
	for i, event := range events {
		if event.Sequence != uint64(i+1) || event.TenantID != scope.TenantID || event.SessionID != scope.SessionID || event.Validate() != nil {
			t.Fatal("committed prefix must be complete and same scope")
		}
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		queries.events = append(queries.events, []driver.Value{string(raw)})
	}
	body := `{"version":1,"text":"continue the original task; preserve all constraints"}`
	ref, err := blobs.Put(t.Context(), state.Run.TenantID, domain.SessionEventObjectPrefix(state.Run.TenantID, state.Run.SessionID, state.Run.TriggerEventID)+"message.json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	author := scope.OwnerUserID
	event := domain.SessionEvent{ID: state.Run.TriggerEventID, TenantID: scope.TenantID, SessionID: scope.SessionID, Sequence: anchor.Sequence + 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &author, IdempotencyKey: domain.IdempotencyKey(name + "-user-key"), Payload: ref, CreatedAt: at}
	if event.Validate() != nil {
		t.Fatal("new original user ingress")
	}
	raw, _ := json.Marshal(event)
	queries.events = append(queries.events, []driver.Value{string(raw)})
	session.LastEventSequence, session.UpdatedAt = event.Sequence, at
	raw, err = json.Marshal(session)
	if err != nil || session.Validate() != nil || session.Status != domain.SessionActive || session.ArchivedAt != nil {
		t.Fatal("fresh current canonical Session", err)
	}
	// The current-principal SELECT projects JSON AND scalars from the SAME
	// Session. Preserve the real historical creator (which need not be this
	// caller); replacing only JSON/sequence left the fixture's old creator
	// scalar and correctly failed the production correspondence check.
	parent.base.session = []driver.Value{string(raw), string(session.CreatedBy), string(session.Status), int64(session.LastEventSequence), int64(0)}
	// These are this fresh fixture's independently declared CURRENT principal
	// facts, not authority imported from the historical execution/creator.
	var member domain.TenantMembership
	var participant domain.SessionParticipant
	if len(parent.base.membership) != 4 || len(parent.base.participant) != 4 ||
		json.Unmarshal([]byte(parent.base.membership[0].(string)), &member) != nil ||
		json.Unmarshal([]byte(parent.base.participant[0].(string)), &participant) != nil ||
		member.Authorize(scope.OwnerUserID, scope.TenantID, domain.TenantPermissionWrite) != nil ||
		participant.Authorize(scope.TenantID, scope.SessionID, scope.OwnerUserID, true) != nil ||
		parent.base.membership[1] != string(member.Role) || parent.base.membership[2] != string(member.Status) || parent.base.membership[3] != int64(member.SecurityVersion) ||
		parent.base.participant[1] != string(participant.Role) || parent.base.participant[2] != string(participant.Status) ||
		member.UpdatedAt.After(at) || participant.UpdatedAt.After(at) {
		t.Fatal("fresh current membership/participation projection")
	}
	seed := terminalPublicationSessionSeed(t, &LocalSessionConnectedSeed{Source: source, Options: Options{OwnedAttemptEffectSourceVerifier: parent.verifier, OwnedProfilePublicationVerifier: source}, Prepare: parent.issuer.issuer, Operate: operate, Backend: parent.parent.Authority.HarnessBinding.Backend, Substrate: parent.parent.Authority.SubstrateBinding, Request: parent.request, Profile: cfg, Scope: scope, parent: parent, sql: operationSQL, compaction: engine, installationRoot: installation.installationRoot, executable: installation.executable, receipt: installation.receipt})
	completion := seed.InstallTerminalCompletionSQL(t)
	db := sql.OpenDB(freshRunHistoryConnector{base: terminalCompletionConnector{f: completion}, history: history})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	seed.DB = db
	return seed, history, completion
}

func localFreshCommittedHistory(t *testing.T, old *LocalTerminalPublicationSessionConnectedSeed, oldState LocalTerminalCompletionSnapshot, previous *LocalFreshRunHistorySQLFixture) *LocalFreshRunHistorySQLFixture {
	t.Helper()
	oldRaw, _ := json.Marshal(oldState.Run)
	manifestRaw, _ := json.Marshal(oldState.Manifest)
	parentRow, admissionRow, accountingRow := old.parent.sql.parentRow, old.parent.base.final, old.parent.sql.accountingRow
	if len(parentRow) != 15 || len(admissionRow) != 7 || len(accountingRow) != 4 {
		t.Fatal("missing genuine retained durable rows")
	}
	history := &LocalFreshRunHistorySQLFixture{tenant: oldState.Run.TenantID, session: oldState.Run.SessionID, oldRun: oldState.Run.ID, oldAttempt: oldState.Attempt.ID, oldLease: old.Request.LeaseID, oldClaim: old.Request.PhysicalInvocationClaimID, runs: map[domain.RunID]string{oldState.Run.ID: string(oldRaw)}, details: map[domain.AttemptID][]driver.Value{oldState.Attempt.ID: {parentRow[13], parentRow[14], admissionRow[0], admissionRow[1], oldState.EvidenceRecord, oldState.EvidenceDigest, oldState.FinalStatus, oldState.FinalDigest, string(manifestRaw), accountingRow[2], accountingRow[3]}}}
	history.markers = map[domain.RunID][2]string{oldState.Run.ID: {oldState.FinalStatus, oldState.FinalDigest}}
	history.manifests = map[domain.ArtifactManifestID]string{oldState.Manifest.ID: string(manifestRaw)}
	history.identities = map[domain.AttemptID]freshRunHistoryIdentity{oldState.Attempt.ID: {run: oldState.Run.ID, lease: old.Request.LeaseID, claim: old.Request.PhysicalInvocationClaimID, manifest: oldState.Manifest.ID}}
	history.DetailReads = make(map[domain.RunID]int)
	history.legacyHeads = make(map[domain.RunID]string)
	history.legacyEvidence = make(map[domain.AttemptID][]driver.Value)
	if previous != nil {
		if previous.tenant != history.tenant || previous.session != history.session || len(previous.identities) != 1 || previous.runs[oldState.Run.ID] != "" {
			t.Fatal("fresh chain requires exactly one distinct earlier committed producing Run")
		}
		for id, raw := range previous.runs {
			history.runs[id] = raw
		}
		for id, row := range previous.details {
			history.details[id] = append([]driver.Value(nil), row...)
		}
		for id, marker := range previous.markers {
			history.markers[id] = marker
		}
		for id, raw := range previous.manifests {
			history.manifests[id] = raw
		}
		for id, identity := range previous.identities {
			history.identities[id] = identity
		}
		for id, raw := range previous.legacyHeads {
			history.legacyHeads[id] = raw
		}
		for id, row := range previous.legacyEvidence {
			history.legacyEvidence[id] = append([]driver.Value(nil), row...)
		}
	}
	for id, raw := range old.compaction.historyRuns {
		history.runs[id] = raw
		history.markers[id] = old.compaction.historyMarkers[id]
	}
	for id, raw := range old.compaction.historyManifests {
		history.manifests[id] = raw
	}
	return history
}

func (f *LocalFreshRunHistorySQLFixture) Corrupt(t *testing.T, kind string) {
	t.Helper()
	switch kind {
	case "cached_legacy_run":
		if f.changeLegacyAfterDetail || f.LegacyChanges != 0 {
			t.Fatal("cached legacy currentness fault must be armed once")
		}
		f.changeLegacyAfterDetail = true
	case "cached_discovery_marker":
		if f.changeDiscoveryAfterDetail || f.DiscoveryChanges != 0 || !isLocalFinalizationMarkerV2(f.markers[f.oldRun][1]) {
			t.Fatal("cached marker fault must be armed once against a valid original marker")
		}
		f.changeDiscoveryAfterDetail = true
	case "marker":
		for id, row := range f.details {
			row = append([]driver.Value(nil), row...)
			row[7] = strings.Repeat("f", 64)
			f.details[id] = row
		}
		f.markers[f.oldRun] = [2]string{string(domain.RunSucceeded), strings.Repeat("f", 64)}
	case "stripped_tag", "stripped_tag_omitted_custody", "missing_marker", "unknown_marker", "oversize_marker", "status_mismatch":
		marker := f.markers[f.oldRun]
		switch kind {
		case "stripped_tag", "stripped_tag_omitted_custody":
			marker[1] = strings.TrimPrefix(marker[1], localFinalizationPrefixV2)
		case "missing_marker":
			marker = [2]string{}
		case "unknown_marker":
			marker[1] = "local_v3:" + strings.Repeat("a", 64)
		case "oversize_marker":
			marker[1] += "a"
		case "status_mismatch":
			marker[0] = string(domain.RunCancelled)
		}
		f.markers[f.oldRun] = marker
		if kind == "stripped_tag_omitted_custody" {
			f.Corrupt(t, "a0_omitted_custody")
		}
	case "missing_evidence":
		f.details = map[domain.AttemptID][]driver.Value{}
	case "missing_run":
		delete(f.runs, f.oldRun) // Preserve legacy rows: deny specifically A's absence.
	case "omitted_custody", "a0_omitted_custody":
		var run domain.Run
		if json.Unmarshal([]byte(f.runs[f.oldRun]), &run) != nil || run.TerminalCustody == nil {
			t.Fatal("omitted-custody fixture requires actual terminal A")
		}
		if kind == "a0_omitted_custody" {
			if run.DerivedContext != nil {
				t.Fatal("A0 omission fixture must have no derived metadata")
			}
		} else if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 2 {
			t.Fatal("A2 omission fixture requires two genuine generations")
		}
		run.TerminalCustody = nil
		if err := run.Validate(); err != nil {
			t.Fatalf("omitted-custody negative must remain structurally valid: %v", err)
		}
		raw, err := json.Marshal(run)
		if err != nil {
			t.Fatal(err)
		}
		f.runs[f.oldRun] = string(raw)
	default:
		panic(fmt.Sprintf("unknown bounded history fault %q", kind))
	}
}

// Mutate only this fresh consumer's archived storage DATA. The original
// producer and its exact committed postimages remain available to other cases.
func (f *LocalFreshRunHistorySQLFixture) CorruptManagedV1(t *testing.T, run domain.RunID, kind string) {
	t.Helper()
	var attempt domain.AttemptID
	var identity freshRunHistoryIdentity
	for id, v := range f.identities {
		if v.run == run {
			attempt, identity = id, v
		}
	}
	if attempt == "" || len(f.legacyEvidence[attempt]) != 7 || f.legacyHeads[run] == "" {
		t.Fatal("actual archived managed producer required for fault")
	}
	switch kind {
	case "missing_marker":
		delete(f.markers, run)
	case "corrupt_marker":
		f.markers[run] = [2]string{string(domain.RunSucceeded), strings.Repeat("f", 64)}
	case "missing_manifest":
		delete(f.manifests, identity.manifest)
	case "corrupt_manifest":
		var v domain.ArtifactManifest
		if json.Unmarshal([]byte(f.manifests[identity.manifest]), &v) != nil || len(v.Artifacts) != 1 {
			t.Fatal("actual managed manifest artifact")
		}
		v.Artifacts = nil
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		f.manifests[identity.manifest] = string(raw)
	case "missing_head":
		delete(f.legacyHeads, run)
	case "corrupt_head":
		f.legacyHeads[run] = "{}"
	case "missing_evidence":
		delete(f.legacyEvidence, attempt)
	case "corrupt_evidence":
		f.legacyEvidence[attempt][4] = "{}"
	case "locator_attempt":
		var head runexplanation.HeadV1
		if json.Unmarshal([]byte(f.legacyHeads[run]), &head) != nil {
			t.Fatal("original head")
		}
		head.SelectedAttemptID = "foreign-historical-attempt"
		if head.Validate() != nil {
			t.Fatal("locator negative must remain structurally valid")
		}
		raw, err := json.Marshal(head)
		if err != nil {
			t.Fatal(err)
		}
		f.legacyHeads[run] = string(raw)
	case "evidence_claim":
		f.legacyEvidence[attempt][2] = "foreign-historical-claim"
	default:
		t.Fatal("unknown managed historical fault", kind)
	}
}
