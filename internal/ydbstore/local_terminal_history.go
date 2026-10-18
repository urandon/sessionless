package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Exact immutable Session-owned parents, not TTL Attempt/WorkerJob/lease rows.
// Every cell is bounded before transfer. No historical receipt or current
// execution capability can be reconstructed from this DATA join.
const localTerminalHistorySQL = `SELECT
 SUBSTRING(p.owned_record,0,32769),SUBSTRING(CAST(p.owned_digest AS String),0,65),
 SUBSTRING(a.record,0,32769),SUBSTRING(CAST(a.digest AS String),0,65),
 SUBSTRING(CAST(e.record AS String),0,32769),SUBSTRING(CAST(e.evidence_digest AS String),0,65),
 SUBSTRING(CAST(f.terminal_status AS String),0,33),SUBSTRING(CAST(f.content_digest AS String),0,74),
 SUBSTRING(CAST(m.payload AS String),0,32769),
 SUBSTRING(p.operation_accounting_record,0,32769),SUBSTRING(CAST(p.operation_accounting_digest AS String),0,65)
 FROM attempt_effect_reservations AS p
 INNER JOIN attempt_execution_admissions_v1 AS a ON a.tenant_id=p.tenant_id AND a.attempt_id=p.attempt_id
 INNER JOIN substrate_execution_evidence AS e ON e.tenant_id=p.tenant_id AND e.attempt_id=p.attempt_id
 INNER JOIN run_finalizations AS f ON f.tenant_id=p.tenant_id AND f.run_id=p.run_id
 INNER JOIN artifact_manifests AS m ON m.tenant_id=p.tenant_id AND m.artifact_manifest_id=$4
 WHERE p.tenant_id=$1 AND p.attempt_id=$2 AND p.kind=$3`

type localTerminalHistorySnapshot struct {
	Run        domain.Run
	Anchor     domain.SessionEvent
	Parent     domain.OwnedAttemptEffectParentV1
	Admission  domain.AttemptExecutionAdmissionV1
	Evidence   domain.ExecutionEvidenceV2
	Manifest   domain.ArtifactManifest
	Accounting ownedAttemptAccounting
	Seal       string
}

type localTerminalHistoryV1 struct {
	snapshots []localTerminalHistorySnapshot
	legacy    []localTerminalLegacySnapshot
	runs      []compactioncontext.HistoricalRunV1
	parents   []*compactioncontext.RetainedV1
}

// Both classification arms are part of the immutable source identity; neither
// a marker nor a digest substitutes for their original current rejoin.
type localTerminalHistoryCommitmentV1 struct {
	Version uint32
	Local   []localTerminalHistorySnapshot
	Legacy  []localTerminalLegacySnapshot
}

func (h *localTerminalHistoryV1) commitmentV1() localTerminalHistoryCommitmentV1 {
	if h == nil {
		return localTerminalHistoryCommitmentV1{} // Invalid, never an empty-history fallback.
	}
	return localTerminalHistoryCommitmentV1{Version: 1, Local: h.snapshots, Legacy: h.legacy}
}

func localTerminalHistoryRunsSQL(count int) string {
	if count < 1 || count > 4096 {
		return ""
	}
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("$%d", i+3)
	}
	return `SELECT SUBSTRING(CAST(r.run_id AS String),0,161),SUBSTRING(CAST(r.payload AS String),0,CAST($` + fmt.Sprint(count+3) + ` AS Uint32)),
 COALESCE(SUBSTRING(CAST(f.terminal_status AS String),0,33),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(f.content_digest AS String),0,74),CAST('' AS String))
 FROM runs AS r LEFT JOIN run_finalizations AS f ON f.tenant_id=r.tenant_id AND f.run_id=r.run_id
 WHERE r.tenant_id=$1 AND r.session_id=$2 AND r.run_id IN (` + strings.Join(keys, ",") + `) ORDER BY r.run_id LIMIT ` + fmt.Sprint(count+1)
}

func (h *localTerminalHistoryV1) clear() {
	if h == nil {
		return
	}
	for _, run := range h.runs {
		for _, op := range run.Operations {
			clear(op.Normalized)
		}
	}
	h.snapshots, h.legacy, h.runs, h.parents = nil, nil, nil, nil
}

// The source owns view and history under its original mutex. The first call is
// made in preparation's EXISTING second transaction; later calls compare the
// complete postimage, including each marker, rather than trusting cached hashes.
func (s *LocalCompactionSourceV1) rejoinHistoryLocked(ctx context.Context, q *ownedAuthorityQuery) error {
	if s.state.view == nil || q.budget.materialized(q.callback, s.state.view.Bytes()) != nil {
		return q.budget.deny()
	}
	records := s.state.view.Records()
	defer func() {
		for i := range records {
			clear(records[i].Payload)
		}
	}()
	anchors := make(map[domain.RunID]domain.SessionEvent)
	byProducing := make(map[domain.RunID][]sessioncontext.EventPayload)
	var ids []domain.RunID
	for _, record := range records {
		e := record.Event
		if e.RunID != nil {
			byProducing[*e.RunID] = append(byProducing[*e.RunID], record)
		}
		if e.Kind == domain.SessionEventAssistantMessage && e.RunID != nil && *e.RunID != s.entry.parent.Scope.RunID {
			if _, exists := anchors[*e.RunID]; exists {
				return q.budget.deny()
			}
			anchors[*e.RunID] = e
			ids = append(ids, *e.RunID)
		}
	}
	if len(ids) > 4096 {
		return q.budget.deny()
	}
	byRun := make(map[domain.RunID]domain.Run)
	markers := make(map[domain.RunID]string)
	if len(ids) > 0 {
		args := []any{s.entry.parent.Scope.TenantID, s.entry.parent.Scope.SessionID}
		for _, id := range ids {
			args = append(args, id)
		}
		// Bound the whole result's possible transfer to <=1MiB, including
		// overflow sentinels. Large histories may explicitly exceed admission.
		rowBudget := (ownedAuthorityMaxBytes / 4) / uint64(len(ids)+1)
		if rowBudget <= 268 {
			return q.budget.deny()
		}
		cell := min(uint64(32769), rowBudget-268)
		args = append(args, cell)
		rows, err := q.QueryContext(ctx, localTerminalHistoryRunsSQL(len(ids)), args...)
		if err != nil {
			return q.budget.deny()
		}
		for rows.Next() {
			var id, raw, status, marker string
			n, err := scanOwnedCurrentRow(rows, &id, &raw, &status, &marker)
			if q.budget.materialized(q.callback, n) != nil || err != nil || len(id) > 160 || uint64(len(raw)) >= cell || len(byRun) >= len(ids) {
				rows.Close()
				return q.budget.deny()
			}
			var run domain.Run
			_, expected := anchors[domain.RunID(id)]
			_, duplicate := byRun[domain.RunID(id)]
			if !expected || duplicate || decodeOwnedEffectCanonicalJSON(raw, &run) != nil || run.Validate() != nil || string(run.ID) != id || run.TenantID != s.entry.parent.Scope.TenantID || run.SessionID != s.entry.parent.Scope.SessionID || status != string(domain.RunSucceeded) || run.Status != domain.RunSucceeded || run.FinishedAt == nil || !run.FinishedAt.Equal(run.UpdatedAt) || run.UpdatedAt.After(q.at) || (domain.ValidateSHA256Digest("legacy.marker", marker) != nil && !isLocalFinalizationMarkerV2(marker)) {
				rows.Close()
				return q.budget.deny()
			}
			byRun[run.ID] = run
			markers[run.ID] = marker
		}
		if rows.Err() != nil || rows.Close() != nil {
			return q.budget.deny()
		}
	}
	legacyMaterials, err := prepareLocalTerminalLegacyMaterials(q, ids, byProducing, byRun, markers)
	if err != nil {
		return err
	}
	legacyManifests, err := readLocalTerminalLegacyManifests(ctx, q, ids, legacyMaterials, byRun)
	if err != nil {
		return err
	}
	var snapshots []localTerminalHistorySnapshot
	var legacy []localTerminalLegacySnapshot
	for _, id := range ids {
		event := anchors[id]
		run, exists := byRun[id]
		// Every discovered canonical producing Run must remain present. Missing
		// durable history cannot silently become a legacy/canonical-only route.
		if !exists {
			return q.budget.deny()
		}
		if run.ID != *event.RunID || run.TenantID != event.TenantID || run.SessionID != event.SessionID {
			return q.budget.deny()
		}
		if run.TerminalCustody == nil {
			if run.DerivedContext != nil || isLocalFinalizationMarkerV2(markers[id]) {
				return q.budget.deny()
			}
			v, err := readLocalTerminalLegacySnapshot(ctx, q, run, markers[id], legacyMaterials[id], legacyManifests)
			if err != nil {
				return err
			}
			legacy = append(legacy, v)
			continue
		}
		if !isLocalFinalizationMarkerV2(markers[id]) {
			return q.budget.deny()
		}
		trigger := uint64(0)
		for _, original := range records {
			if original.Event.ID == run.TriggerEventID {
				trigger = original.Event.Sequence
			}
		}
		if trigger == 0 || trigger >= event.Sequence {
			return q.budget.deny()
		}
		// No guessed ordering across concurrently appended user/system DATA.
		// The original canonical boundary and terminal anchor delimit this
		// first supported producing segment exactly.
		for _, original := range records {
			if original.Event.Sequence > trigger && original.Event.Sequence < event.Sequence {
				return q.budget.deny()
			}
		}
		v, err := s.entry.readTerminalHistorySnapshot(ctx, q, run, event)
		if err != nil {
			return err
		}
		if v.Seal != markers[id] {
			return q.budget.deny()
		}
		snapshots = append(snapshots, v)
	}
	if s.state.history == nil {
		s.state.history = &localTerminalHistoryV1{snapshots: snapshots, legacy: legacy}
	} else if !reflect.DeepEqual(s.state.history.snapshots, snapshots) || !reflect.DeepEqual(s.state.history.legacy, legacy) {
		return q.budget.deny()
	}
	return nil
}

func (entry *LocalInvocationEntryV1) readTerminalHistorySnapshot(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent) (localTerminalHistorySnapshot, error) {
	// Keep the original consumer's foreign-owner denial before any historical
	// SQL read. The shared DATA reader has no current entry ownership itself.
	if run.TerminalCustody == nil || run.TerminalCustody.Plan.Scope.OwnerUserID != entry.parent.Scope.OwnerUserID {
		return localTerminalHistorySnapshot{}, q.budget.deny()
	}
	v, err := readHistoricalTerminalSnapshot(ctx, q, run, anchor)
	profile, fe := entry.owner.configuration.Digest()
	if err != nil || fe != nil || v.Run.TerminalCustody.Plan.Scope.OwnerUserID != entry.parent.Scope.OwnerUserID || v.Evidence.LocalV1.ProfileDigest != profile {
		return localTerminalHistorySnapshot{}, q.budget.deny()
	}
	return v, nil
}

// Immutable producer validation shared with deletion. Current Source/profile
// authorization remains in the original consumer above, never reactivated here.
func readHistoricalTerminalSnapshot(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent) (localTerminalHistorySnapshot, error) {
	deny := func() (localTerminalHistorySnapshot, error) { return localTerminalHistorySnapshot{}, q.budget.deny() }
	custody := run.TerminalCustody
	if run.Validate() != nil || run.Status != domain.RunSucceeded || run.CancellationRequestedAt != nil || custody == nil || custody.Correspondence == nil || custody.LocalEvidence == nil ||
		custody.Correspondence.TerminalAssistantEventID != anchor.ID || run.FinishedAt == nil || !run.FinishedAt.Equal(run.UpdatedAt) || run.UpdatedAt.After(q.at) {
		return deny()
	}
	scope := custody.Plan.Scope
	var parentRaw, parentDigest, admissionRaw, admissionDigest, evidenceRaw, evidenceDigest, status, seal, manifestRaw, accountingRaw, accountingDigest string
	if q.scanMetadata(ctx, localTerminalHistorySQL, []any{scope.TenantID, scope.AttemptID, domain.ProviderEffectTurnV1, canonicaloutput.ManifestIDV1(run.ID)},
		&parentRaw, &parentDigest, &admissionRaw, &admissionDigest, &evidenceRaw, &evidenceDigest, &status, &seal, &manifestRaw, &accountingRaw, &accountingDigest) != nil {
		return deny()
	}
	for _, raw := range []string{parentRaw, admissionRaw, evidenceRaw, manifestRaw, accountingRaw} {
		if len(raw) == 0 || len(raw) > ownedAuthorityHeadBytes {
			return deny()
		}
	}
	v := localTerminalHistorySnapshot{Run: run.Clone(), Anchor: anchor, Seal: seal}
	var err error
	v.Parent, err = domain.DecodeOwnedAttemptEffectParentV1([]byte(parentRaw))
	if err != nil || decodeOwnedEffectCanonicalJSON(admissionRaw, &v.Admission) != nil || decodeOwnedEffectCanonicalJSON(evidenceRaw, &v.Evidence) != nil || decodeOwnedEffectCanonicalJSON(manifestRaw, &v.Manifest) != nil || decodeOwnedEffectCanonicalJSON(accountingRaw, &v.Accounting) != nil {
		return deny()
	}
	pd, pe := v.Parent.Digest()
	ad, ae := v.Admission.Digest()
	sd, se := v.Admission.Source.Digest()
	_, actualAccounting, ce := accountingBytes(v.Accounting)
	if pe != nil || ae != nil || se != nil || ce != nil || pd != parentDigest || ad != admissionDigest || actualAccounting != accountingDigest ||
		v.Parent.ValidateForAdmission(v.Admission) != nil || ownedOperationScope(v.Parent) != scope || v.Accounting.validate(v.Parent, v.Admission.Source, q.at) != nil ||
		status != string(domain.RunSucceeded) || !isLocalFinalizationMarkerV2(seal) || v.Manifest.ValidateForRun(run) != nil || v.Manifest.ID != canonicaloutput.ManifestIDV1(run.ID) ||
		v.Evidence.Version != 2 || v.Evidence.Kind != domain.ExecutionEvidenceLocalV1 || v.Evidence.ManagedV1 != nil || v.Evidence.LocalV1 == nil || v.Evidence.LocalV1.Validate() != nil ||
		!reflect.DeepEqual(v.Evidence.LocalV1, custody.LocalEvidence) || evidenceDigest != custody.LocalEvidence.EvidenceDigest || custody.LocalEvidence.ParentDigest != pd || custody.LocalEvidence.SourceDigest != sd || custody.LocalEvidence.ProfileDigest != v.Parent.Authority.HarnessBinding.Backend.BackendProfileDigest {
		return deny()
	}
	return v, nil
}

func (s *LocalCompactionSourceV1) materializeHistoryLocked(ctx context.Context) error {
	if s.state.history == nil {
		return domain.ErrMembershipDenied
	}
	h := s.state.history
	if len(h.runs) != 0 || len(h.snapshots) == 0 {
		return nil
	}
	var previous *compactioncontext.RetainedV1
	for _, snapshot := range h.snapshots {
		body, err := s.entry.readCompactionBlob(ctx, snapshot.Run.TerminalCustody.Plan.Capsule, domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			return err
		}
		if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
			clear(body)
			return domain.ErrMembershipDenied
		}
		var capsule localRetainedTerminalCapsuleV1
		err = decodeOwnedEffectCanonicalJSON(string(body), &capsule)
		if err == nil {
			if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
				clear(body)
				return domain.ErrMembershipDenied
			}
			ops := make([]LocalCompactionOperationV1, len(capsule.Operations))
			for i, item := range capsule.Operations {
				ops[i] = LocalCompactionOperationV1{Reservation: item.Reservation, Observation: item.Observation, Response: item.NormalizedBody}
			}
			encoded, e := encodeLocalTerminalCapsule(capsule.Scope, capsule.ProfileDigest, capsule.FrontierDigest, ops, capsule.DerivedContext, capsule.Events, capsule.PublicManifest, domain.MaxTerminalCapsuleBytesV1)
			if e != nil || !bytes.Equal(encoded, body) {
				err = domain.ErrMembershipDenied
			}
			clear(encoded)
		}
		clear(body)
		if err != nil || s.validateHistoricalCapsule(ctx, snapshot, capsule) != nil {
			return domain.ErrMembershipDenied
		}
		run := compactioncontext.HistoricalRunV1{Scope: capsule.Scope, Anchor: snapshot.Anchor.ID}
		for _, item := range capsule.Operations {
			run.Operations = append(run.Operations, compactioncontext.HistoricalOperationV1{Reservation: item.Reservation.Clone(), Observation: item.Observation.Clone(), Normalized: item.NormalizedBody, Projection: item.Projection})
		}
		h.runs = append(h.runs, run)
		parent, err := s.readHistoricalDerived(ctx, snapshot, previous)
		if err != nil {
			return err
		}
		h.parents = append(h.parents, parent)
		if parent != nil {
			previous = parent
		}
	}
	return nil
}

func (s *LocalCompactionSourceV1) validateHistoricalCapsule(ctx context.Context, v localTerminalHistorySnapshot, capsule localRetainedTerminalCapsuleV1) error {
	run, custody := v.Run, v.Run.TerminalCustody
	if capsule.Schema != "sessionless.terminal-custody.v1" || capsule.CodecVersion != 1 || capsule.Scope != custody.Plan.Scope || capsule.ProfileDigest != custody.LocalEvidence.ProfileDigest || capsule.FrontierDigest != custody.Plan.FrontierDigest ||
		!domain.EqualRunDerivedContextV1(capsule.DerivedContext, run.DerivedContext) || !reflect.DeepEqual(capsule.PublicManifest, v.Manifest) || len(capsule.Events) != 1 || len(capsule.Operations) == 0 || len(capsule.Operations) > 12 || uint64(len(capsule.Operations)) != v.Accounting.Aggregate.Operations {
		return domain.ErrMembershipDenied
	}
	e := capsule.Events[0]
	a := v.Anchor
	draft := domain.SessionEventDraft{ID: e.ID, Kind: e.Kind, IdempotencyKey: e.IdempotencyKey, Payload: e.Payload, DisplayText: e.DisplayText, CreatedAt: a.CreatedAt}
	if draft.ValidateForRun(run) != nil || e.ID != a.ID || e.Kind != a.Kind || e.IdempotencyKey != a.IdempotencyKey || e.Payload != a.Payload || e.Kind != domain.SessionEventAssistantMessage || a.RunID == nil || *a.RunID != run.ID {
		return domain.ErrMembershipDenied
	}
	drafts := []domain.SessionEventDraft{draft}
	identities, err := terminalCanonicalEventsDigestV1(drafts)
	seal, se := runFinalizationDigestV2(run.Status, &v.Manifest, drafts, &v.Evidence, run.DerivedContext, custody)
	if se == nil {
		seal, se = encodeLocalFinalizationMarkerV2(seal)
	}
	if err != nil || se != nil || identities != custody.Correspondence.CanonicalEventsDigest || seal != v.Seal {
		return domain.ErrMembershipDenied
	}
	accounting := v.Accounting
	accounting.Aggregate = domain.AttemptOperationDebitV1{BodyBytes: ownedSetupBodyBytes(v.Parent)}
	accounting.Amounts = [4]uint64{0, *v.Admission.Source.Spending[1].MaxAmountMicrounits, 0, 0}
	accounting.UpdatedAt = accounting.SetupReservedAt
	var previous domain.AttemptOperationObservationDigestV1
	var normalized uint64
	for i, item := range capsule.Operations {
		r, o := item.Reservation, item.Observation
		if r.Operation.Scope != capsule.Scope || r.Operation.Sequence != uint64(i+1) || r.Operation.PredecessorObservationDigest != previous || r.ValidateForParent(v.Parent.Authority, v.Parent.Reservation) != nil || !ownedObservationAllowsSuccessor(r, o) || r.ReservedAt.Before(accounting.UpdatedAt) || o.ObservedAt.After(custody.LocalEvidence.StoppedAt) || uint64(len(item.NormalizedBody)) > (384<<10)-normalized {
			return domain.ErrMembershipDenied
		}
		normalized += uint64(len(item.NormalizedBody))
		response, err := configuredinference.DecodeRetainedNormalizedDataV1(s.entry.owner.configuration, r, o, item.NormalizedBody)
		if err != nil || response.Call != nil || response.Stop != configuredinference.StopFinalV1 {
			return domain.ErrMembershipDenied
		}
		expected := "retained_only"
		if r.Operation.Kind == domain.AttemptOperationCompactionV1 {
			expected = "compaction"
		}
		if i == len(capsule.Operations)-1 {
			expected = "assistant"
			if r.Operation.Kind != domain.AttemptOperationModelV1 || len(item.EventIDs) != 1 || item.EventIDs[0] != a.ID {
				return domain.ErrMembershipDenied
			}
			// Original canonical payload was already hash-verified by ProjectCanonical.
			if chargeOwnedAuthorityBody(ctx, s.state.view.Bytes()) != nil {
				return domain.ErrMembershipDenied
			}
			for _, record := range s.state.view.Records() {
				if record.Event.ID == a.ID {
					var payload struct {
						Summary            string                    `json:"summary"`
						ArtifactManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
					}
					if json.Unmarshal(record.Payload, &payload) != nil || payload.Summary != canonicaloutput.NormalizedSummary(response.Text) || payload.ArtifactManifestID != v.Manifest.ID {
						return domain.ErrMembershipDenied
					}
				}
				clear(record.Payload)
			}
		} else if len(item.EventIDs) != 0 {
			return domain.ErrMembershipDenied
		}
		if item.Projection != expected {
			return domain.ErrMembershipDenied
		}
		accounting, err = debitOwnedOperation(accounting, r.Operation, v.Parent, v.Admission.Source, r.ReservedAt)
		if err != nil || accounting.Aggregate != r.AggregateDebit || accounting.Amounts != r.ComponentAmounts {
			return domain.ErrMembershipDenied
		}
		previous, _ = o.Digest()
	}
	if !reflect.DeepEqual(accounting, v.Accounting) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (s *LocalCompactionSourceV1) readHistoricalDerived(ctx context.Context, v localTerminalHistorySnapshot, previous *compactioncontext.RetainedV1) (*compactioncontext.RetainedV1, error) {
	metadata := v.Run.DerivedContext
	if metadata == nil {
		return nil, nil
	}
	if metadata.Head == nil || v.Parent.Authority.HarnessBinding.AgentLoopPolicy == nil {
		return nil, domain.ErrMembershipDenied
	}
	var last string
	for _, plan := range metadata.Inventory {
		if plan.Scope != ownedOperationScope(v.Parent) {
			return nil, domain.ErrMembershipDenied
		}
		summary, err := s.entry.readCompactionBlob(ctx, plan.Summary, 32<<10)
		if err != nil {
			return nil, err
		}
		manifest, err := s.entry.readCompactionBlob(ctx, plan.Manifest, 128<<10)
		if err != nil {
			clear(summary)
			return nil, err
		}
		decoded, err := compactioncontext.DecodeRetainedV1(s.entry.owner.configuration, *v.Parent.Authority.HarnessBinding.AgentLoopPolicy, plan, summary, manifest, previous)
		clear(summary)
		clear(manifest)
		if err != nil {
			return nil, domain.ErrMembershipDenied
		}
		previous = decoded
		last, _ = plan.Digest()
	}
	if last != metadata.Head.PlanDigest {
		return nil, domain.ErrMembershipDenied
	}
	return previous, nil
}

// Historical source selection never discovers ancestors recursively. Every
// parent comes from an independently verified original Run anchored in view.
func (s *LocalCompactionSourceV1) historicalParent() *compactioncontext.RetainedV1 {
	if s.state.history == nil {
		return nil
	}
	var parent *compactioncontext.RetainedV1
	for _, p := range s.state.history.parents {
		if p != nil {
			parent = p
		}
	}
	return parent
}

func (s *LocalCompactionSourceV1) policyForRetainedPlan(plan domain.DerivedContextPlanV1) (domain.AgentLoopPolicyV1, error) {
	if plan.Scope == ownedOperationScope(s.entry.parent) && s.entry.parent.Authority.HarnessBinding.AgentLoopPolicy != nil {
		return s.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Clone(), nil
	}
	if s.state.history != nil {
		for _, old := range s.state.history.snapshots {
			if old.Run.DerivedContext != nil {
				for _, original := range old.Run.DerivedContext.Inventory {
					if original == plan && old.Parent.Authority.HarnessBinding.AgentLoopPolicy != nil {
						return old.Parent.Authority.HarnessBinding.AgentLoopPolicy.Clone(), nil
					}
				}
			}
		}
	}
	return domain.AgentLoopPolicyV1{}, domain.ErrMembershipDenied
}

func (s *LocalCompactionSourceV1) reloadHistoricalParent(ctx context.Context) (*compactioncontext.RetainedV1, error) {
	if s == nil || s.self != s || s.state == nil || s.state.cleared || s.state.history == nil {
		return nil, domain.ErrMembershipDenied
	}
	var parent *compactioncontext.RetainedV1
	for _, snapshot := range s.state.history.snapshots {
		p, err := s.readHistoricalDerived(ctx, snapshot, parent)
		if err != nil {
			return nil, err
		}
		if p != nil {
			parent = p
		}
	}
	return parent, nil
}
