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

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// TEST ONLY keyed durable DATA. A completely new Source/factory/Store/issuer
// consumes these rows. The previous private cleanup/accounting owners, TTL
// Attempt/Job/lease rows and ordinary request bodies are not installed here.
type LocalFreshRunHistorySQLFixture struct {
	runs                       map[domain.RunID]string
	details                    map[domain.AttemptID][]driver.Value
	markers                    map[domain.RunID][2]string
	manifests                  map[domain.ArtifactManifestID]string
	tenant                     domain.TenantID
	session                    domain.SessionID
	oldRun                     domain.RunID
	oldAttempt                 domain.AttemptID
	oldLease                   domain.LeaseID
	oldClaim                   string
	HistoryReads, TTLReads     int
	changeDiscoveryAfterDetail bool
	DiscoveryChanges           int
	changeLegacyAfterDetail    bool
	LegacyChanges              int
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
				rows = append(rows, []driver.Value{id, raw, marker[0], marker[1]})
			}
		}
		f.HistoryReads++
		return &compactionSourceRows{rows: rows}, nil
	}
	if s == localTerminalLegacyManifestsSQL(len(a)-2) {
		return canonicalHistoryManifestRows(f.tenant, f.manifests, a)
	}
	if s == localTerminalHistorySQL {
		if len(a) != 4 || a[0].Value != string(f.tenant) || a[2].Value != string(domain.ProviderEffectTurnV1) || a[3].Value != string(canonicaloutput.ManifestIDV1(f.oldRun)) {
			return nil, errors.New("fresh immutable terminal exact keys")
		}
		row := f.details[domain.AttemptID(a[1].Value.(string))]
		f.HistoryReads++
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
			if arg.Value == string(f.oldRun) || arg.Value == string(f.oldAttempt) || arg.Value == string(f.oldLease) || arg.Value == f.oldClaim {
				f.TTLReads++
				return nil, errors.New("old TTL state is gone")
			}
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, s, a)
}

// NewFreshRunAfterTerminalSeed opens the unchanged private installation through
// its real OpenExisting API. All new scope/funding/canonical rows are declared
// BEFORE the caller constructs NewLocalV1; no producer or fit result is injected.
func NewFreshRunAfterTerminalSeed(t *testing.T, old *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, blobs ports.BlobStore) (*LocalSessionConnectedSeed, *LocalFreshRunHistorySQLFixture) {
	t.Helper()
	oldState := completed.Snapshot(t)
	if oldState.Run.Status != domain.RunSucceeded || oldState.Run.TerminalCustody == nil || len(oldState.Events) != 1 {
		t.Fatal("genuine successful producing Run required")
	}
	source, err := localoperator.OpenExisting(t.Context(), old.installationRoot, old.executable, old.receipt)
	if err != nil {
		t.Fatal("new process original installation intake", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	cfg, err := source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	profile := old.receipt.Profile.Clone()
	state := currentWorkerAuthorityFixture(t)
	at := time.Now().UTC().Truncate(time.Microsecond)
	state.Run.ID, state.Run.IdempotencyKey, state.Run.TriggerEventID = "fresh-run", "fresh-idempotency", "fresh-user"
	state.Run.CreatedAt, state.Run.UpdatedAt = at, at
	state.Job.RunID, state.Job.TriggerEventID, state.Job.AttemptID, state.Job.ReservationID, state.Job.InputManifestID = "fresh-run", "fresh-user", "fresh-attempt", "fresh-reservation", "fresh-input-manifest"
	state.Attempt.ID, state.Attempt.RunID, state.Attempt.CreatedAt, state.Attempt.UpdatedAt = "fresh-attempt", "fresh-run", at, at
	state.Reservation.ID, state.Reservation.RunID, state.Reservation.HeldAt, state.Reservation.UpdatedAt, state.Reservation.ExpiresAt = "fresh-reservation", "fresh-run", at, at, at.Add(time.Hour)
	state.InputManifest.ID, state.InputManifest.RunID, state.InputManifest.CreatedAt = "fresh-input-manifest", "fresh-run", at
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
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: oldState.Events[0].Sequence + 1}
	ratedFixturePut(t, parent.graph.ratedSQLFixture, "worker_jobs", job)
	queries.events = cloneGenerationRows(old.compaction.events)
	anchor := oldState.Events[0]
	raw, _ := json.Marshal(anchor)
	queries.events = append(queries.events, []driver.Value{string(raw)})
	body := `{"version":1,"text":"continue the original task; preserve all constraints"}`
	ref, err := blobs.Put(t.Context(), state.Run.TenantID, domain.SessionEventObjectPrefix(state.Run.TenantID, state.Run.SessionID, state.Run.TriggerEventID)+"message.json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	author := scope.OwnerUserID
	event := domain.SessionEvent{ID: state.Run.TriggerEventID, TenantID: scope.TenantID, SessionID: scope.SessionID, Sequence: anchor.Sequence + 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &author, IdempotencyKey: "fresh-user-key", Payload: ref, CreatedAt: at}
	if event.Validate() != nil {
		t.Fatal("new original user ingress")
	}
	raw, _ = json.Marshal(event)
	queries.events = append(queries.events, []driver.Value{string(raw)})
	session := oldState.Session
	session.LastEventSequence, session.UpdatedAt = event.Sequence, at
	raw, _ = json.Marshal(session)
	parent.base.session[0], parent.base.session[3] = string(raw), int64(event.Sequence)
	oldRaw, _ := json.Marshal(oldState.Run)
	manifestRaw, _ := json.Marshal(oldState.Manifest)
	parentRow, admissionRow, accountingRow := old.parent.sql.parentRow, old.parent.base.final, old.parent.sql.accountingRow
	if len(parentRow) != 15 || len(admissionRow) != 7 || len(accountingRow) != 4 {
		t.Fatal("missing genuine retained durable rows")
	}
	history := &LocalFreshRunHistorySQLFixture{tenant: scope.TenantID, session: scope.SessionID, oldRun: oldState.Run.ID, oldAttempt: oldState.Attempt.ID, oldLease: old.Request.LeaseID, oldClaim: old.Request.PhysicalInvocationClaimID, runs: map[domain.RunID]string{oldState.Run.ID: string(oldRaw)}, details: map[domain.AttemptID][]driver.Value{oldState.Attempt.ID: {parentRow[13], parentRow[14], admissionRow[0], admissionRow[1], oldState.EvidenceRecord, oldState.EvidenceDigest, oldState.FinalStatus, oldState.FinalDigest, string(manifestRaw), accountingRow[2], accountingRow[3]}}}
	history.markers = map[domain.RunID][2]string{oldState.Run.ID: {oldState.FinalStatus, oldState.FinalDigest}}
	history.manifests = map[domain.ArtifactManifestID]string{oldState.Manifest.ID: string(manifestRaw)}
	for id, raw := range old.compaction.historyRuns {
		history.runs[id] = raw
		history.markers[id] = old.compaction.historyMarkers[id]
	}
	for id, raw := range old.compaction.historyManifests {
		history.manifests[id] = raw
	}
	db := sql.OpenDB(freshRunHistoryConnector{base: compactionPublicationConnector{engine}, history: history})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	return &LocalSessionConnectedSeed{Source: source, DB: db, Options: Options{OwnedAttemptEffectSourceVerifier: parent.verifier, OwnedProfilePublicationVerifier: source}, Prepare: parent.issuer.issuer, Operate: operate, Backend: parent.parent.Authority.HarnessBinding.Backend, Substrate: parent.parent.Authority.SubstrateBinding, Request: parent.request, Profile: cfg, Scope: scope, parent: parent, sql: operationSQL, compaction: engine, installationRoot: old.installationRoot, executable: old.executable, receipt: old.receipt}, history
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
