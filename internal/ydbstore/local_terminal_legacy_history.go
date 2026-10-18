package ydbstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// Retained classification DATA, compared in full on every original source
// rejoin. A projection can locate evidence; it cannot approve history or sends.
type localTerminalLegacySnapshot struct {
	Run      domain.Run
	Marker   string
	Manifest domain.ArtifactManifest
	Head     *runexplanation.HeadV1
	Effect   *attemptEffectRecordV1
	Evidence *domain.SubstrateExecutionEvidenceV1
}

type localTerminalLegacyMaterial struct {
	Drafts     []domain.SessionEventDraft
	ManifestID domain.ArtifactManifestID
}

const localTerminalLegacyHeadSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193)
 FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`

// Historical immutable evidence/authority only: no old TTL execution state,
// current-price reauthorization or revived physical capability.
const localTerminalLegacyEvidenceSQL = `SELECT
 SUBSTRING(CAST(e.run_id AS String),0,161),SUBSTRING(CAST(e.attempt_id AS String),0,161),
 SUBSTRING(CAST(e.physical_invocation_claim_id AS String),0,161),
 SUBSTRING(CAST(e.evidence_digest AS String),0,65),SUBSTRING(CAST(e.record AS String),0,32769),
 SUBSTRING(CAST(p.record AS String),0,32769),
 CASE WHEN p.owned_record IS NOT NULL OR p.owned_digest IS NOT NULL
 OR p.preparation_started_at IS NOT NULL OR p.preparation_parent_digest IS NOT NULL
 OR p.allocation_original_record IS NOT NULL OR p.allocation_original_digest IS NOT NULL
 OR p.allocation_current_record IS NOT NULL OR p.allocation_current_digest IS NOT NULL
 OR p.allocation_publication_digest IS NOT NULL OR p.allocation_transition_parent_digest IS NOT NULL
 OR p.proxy_original_record IS NOT NULL OR p.proxy_original_digest IS NOT NULL
 OR p.proxy_current_record IS NOT NULL OR p.proxy_current_digest IS NOT NULL
 OR p.proxy_policy_body IS NOT NULL OR p.proxy_attestation_body IS NOT NULL
 OR p.operation_accounting_record IS NOT NULL OR p.operation_accounting_digest IS NOT NULL
 OR p.web_search_consent_record IS NOT NULL OR p.web_search_consent_digest IS NOT NULL
 THEN CAST(1 AS Uint64) ELSE CAST(0 AS Uint64) END
 FROM substrate_execution_evidence AS e INNER JOIN attempt_effect_reservations AS p
 ON p.tenant_id=e.tenant_id AND p.attempt_id=e.attempt_id AND p.kind=$3
 WHERE e.tenant_id=$1 AND e.attempt_id=$2`

func localTerminalLegacyManifestsSQL(count int) string {
	if count < 1 || count > 4096 {
		return ""
	}
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("$%d", i+2)
	}
	return `SELECT SUBSTRING(CAST(artifact_manifest_id AS String),0,161),SUBSTRING(CAST(payload AS String),0,CAST($` + fmt.Sprint(count+2) + ` AS Uint32))
 FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id IN (` + strings.Join(keys, ",") + `) ORDER BY artifact_manifest_id LIMIT ` + fmt.Sprint(count+1)
}

// The complete ordered finalizer input comes from the already authenticated
// admitted canonical material. No normalization or missing-event fallback.
func localTerminalLegacyDrafts(q *ownedAuthorityQuery, run domain.Run, records []sessioncontext.EventPayload) ([]domain.SessionEventDraft, domain.ArtifactManifestID, error) {
	if q == nil || q.budget == nil {
		return nil, "", domain.ErrMembershipDenied
	}
	var drafts []domain.SessionEventDraft
	var manifest domain.ArtifactManifestID
	for _, r := range records {
		e := r.Event
		if e.RunID == nil || *e.RunID != run.ID {
			continue
		}
		if len(drafts) >= 25 || e.TenantID != run.TenantID || e.SessionID != run.SessionID || e.CreatedAt.After(run.UpdatedAt) {
			return nil, "", domain.ErrMembershipDenied
		}
		// Charge BEFORE PrepareV1 clones payload or decodes/renders anything.
		// Five payload-sized logical arms cover its owned body, inspection
		// strings/RawMessage, renderer strings, rendered frame, and the exact
		// assistant projection decoded below. Assistant framing adds <=14;
		// tool rendering is unsupported and allocates no frame. This is a
		// conservative materialization bound, not a general Go heap ceiling.
		if len(r.Payload) == 0 || len(r.Payload) > sessionmaterial.MaxPayloadBytesV1 {
			return nil, "", q.budget.deny()
		}
		metadata := uint64(len(e.ID) + len(e.TenantID) + len(e.SessionID) + len(e.Kind) + len(e.IdempotencyKey) + len(e.Payload.TenantID) + len(e.Payload.Key) + len(e.Payload.SHA256) + len(run.ID))
		if metadata > sessionmaterial.MaxIdentityMetadataBytesV1 || q.budget.materialized(q.callback, 5*uint64(len(r.Payload))+14, metadata) != nil {
			return nil, "", domain.ErrMembershipDenied
		}
		prepared, err := sessionmaterial.PrepareV1(e, r.Payload, nil)
		if err != nil {
			return nil, "", domain.ErrMembershipDenied
		}
		prepared.Release()
		d := domain.SessionEventDraft{ID: e.ID, Kind: e.Kind, IdempotencyKey: e.IdempotencyKey, Payload: e.Payload, CreatedAt: e.CreatedAt}
		if e.Kind == domain.SessionEventAssistantMessage {
			var payload struct {
				Schema   string                    `json:"schema"`
				Summary  string                    `json:"summary"`
				Manifest domain.ArtifactManifestID `json:"artifact_manifest_id"`
			}
			if manifest != "" || json.Unmarshal(r.Payload, &payload) != nil || payload.Manifest.Validate() != nil {
				return nil, "", domain.ErrMembershipDenied
			}
			manifest, d.DisplayText = payload.Manifest, payload.Summary
		}
		if d.ValidateForRun(run) != nil {
			return nil, "", domain.ErrMembershipDenied
		}
		drafts = append(drafts, d)
	}
	if manifest == "" || validateCanonicalFinalizationEvents(domain.RunSucceeded, drafts) != nil {
		return nil, "", domain.ErrMembershipDenied
	}
	return drafts, manifest, nil
}

func prepareLocalTerminalLegacyMaterials(q *ownedAuthorityQuery, ids []domain.RunID, byProducing map[domain.RunID][]sessioncontext.EventPayload, runs map[domain.RunID]domain.Run, markers map[domain.RunID]string) (map[domain.RunID]localTerminalLegacyMaterial, error) {
	result := make(map[domain.RunID]localTerminalLegacyMaterial)
	for _, runID := range ids {
		run, exists := runs[runID]
		if !exists || run.TerminalCustody != nil || isLocalFinalizationMarkerV2(markers[runID]) {
			continue
		}
		if run.DerivedContext != nil {
			return nil, q.budget.deny()
		}
		drafts, id, err := localTerminalLegacyDrafts(q, run, byProducing[runID])
		if err != nil {
			return nil, q.budget.deny()
		}
		result[runID] = localTerminalLegacyMaterial{Drafts: drafts, ManifestID: id}
	}
	return result, nil
}

func readLocalTerminalLegacyManifests(ctx context.Context, q *ownedAuthorityQuery, runIDs []domain.RunID, materials map[domain.RunID]localTerminalLegacyMaterial, runs map[domain.RunID]domain.Run) (map[domain.ArtifactManifestID]domain.ArtifactManifest, error) {
	ids := make([]domain.ArtifactManifestID, 0)
	expected := make(map[domain.ArtifactManifestID]domain.Run)
	// Canonical order, not map iteration, gives stable bounded SQL keys.
	for _, runID := range runIDs {
		material, exists := materials[runID]
		if !exists {
			continue
		}
		run, id := runs[runID], material.ManifestID
		if _, duplicate := expected[id]; duplicate {
			return nil, q.budget.deny()
		}
		expected[id], ids = run, append(ids, id)
	}
	result := make(map[domain.ArtifactManifestID]domain.ArtifactManifest)
	if len(ids) == 0 {
		return result, nil
	}
	rowBudget := (ownedAuthorityMaxBytes / 4) / uint64(len(ids)+1)
	if rowBudget <= 161 {
		return nil, q.budget.deny()
	}
	cell := min(uint64(32769), rowBudget-161)
	args := []any{expected[ids[0]].TenantID}
	for _, id := range ids {
		args = append(args, id)
	}
	args = append(args, cell)
	rows, err := q.QueryContext(ctx, localTerminalLegacyManifestsSQL(len(ids)), args...)
	if err != nil {
		return nil, q.budget.deny()
	}
	defer rows.Close()
	for rows.Next() {
		var id, raw string
		n, err := scanOwnedCurrentRow(rows, &id, &raw)
		run, exists := expected[domain.ArtifactManifestID(id)]
		_, duplicate := result[domain.ArtifactManifestID(id)]
		var manifest domain.ArtifactManifest
		if err != nil || q.budget.materialized(q.callback, n) != nil || len(id) > 160 || uint64(len(raw)) >= cell || !exists || duplicate || len(result) >= len(ids) || decodeOwnedEffectCanonicalJSON(raw, &manifest) != nil || manifest.ID != domain.ArtifactManifestID(id) || manifest.ValidateForRun(run) != nil || manifest.CreatedAt.After(run.UpdatedAt) {
			return nil, q.budget.deny()
		}
		result[manifest.ID] = manifest
	}
	if rows.Err() != nil || rows.Close() != nil || len(result) != len(ids) {
		return nil, q.budget.deny()
	}
	return result, nil
}

func readLocalTerminalLegacySnapshot(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, marker string, material localTerminalLegacyMaterial, manifests map[domain.ArtifactManifestID]domain.ArtifactManifest) (localTerminalLegacySnapshot, error) {
	deny := func() (localTerminalLegacySnapshot, error) { return localTerminalLegacySnapshot{}, q.budget.deny() }
	if run.TerminalCustody != nil || run.DerivedContext != nil || run.Status != domain.RunSucceeded || domain.ValidateSHA256Digest("legacy.seal", marker) != nil {
		return deny()
	}
	drafts := material.Drafts
	manifest, found := manifests[material.ManifestID]
	if len(drafts) == 0 || !found {
		return deny()
	}
	v := localTerminalLegacySnapshot{Run: run.Clone(), Marker: marker, Manifest: manifest}
	digest, err := runFinalizationDigest(run.Status, &manifest, drafts, nil, nil)
	if err == nil && digest == marker {
		return v, nil
	}
	var headRaw string
	if q.scanMetadata(ctx, localTerminalLegacyHeadSQL, []any{run.TenantID, run.ID}, &headRaw) != nil || len(headRaw) > 8192 {
		return deny()
	}
	var head runexplanation.HeadV1
	if decodeOwnedEffectCanonicalJSON(headRaw, &head) != nil || head.Validate() != nil || head.TenantID != run.TenantID || head.RunID != run.ID || head.SessionID != run.SessionID {
		return deny()
	}
	var evidenceRun, attempt, claim, evidenceDigest, evidenceRaw, effectRaw string
	var owned uint64
	if q.scanMetadata(ctx, localTerminalLegacyEvidenceSQL, []any{run.TenantID, head.SelectedAttemptID, domain.ProviderEffectTurnV1}, &evidenceRun, &attempt, &claim, &evidenceDigest, &evidenceRaw, &effectRaw, &owned) != nil || owned != 0 || len(evidenceRaw) > 32768 || len(effectRaw) > 32768 || evidenceRun != string(run.ID) || attempt != string(head.SelectedAttemptID) || len(claim) > 160 || len(evidenceDigest) != 64 {
		return deny()
	}
	var evidence domain.SubstrateExecutionEvidenceV1
	var effect attemptEffectRecordV1
	if decodeOwnedEffectCanonicalJSON(evidenceRaw, &evidence) != nil || decodeOwnedEffectCanonicalJSON(effectRaw, &effect) != nil || effect.validate() != nil || effect.Reservation.TenantID != run.TenantID || effect.Reservation.RunID != run.ID || effect.Reservation.AttemptID != head.SelectedAttemptID || effect.Reservation.PhysicalInvocationClaimID != claim || string(evidence.EvidenceDigest) != evidenceDigest || evidence.ValidateForPersistedAuthority(effect.Authority, effect.Reservation) != nil {
		return deny()
	}
	digest, err = runFinalizationDigest(run.Status, &manifest, drafts, &evidence, nil)
	if err != nil || digest != marker {
		return deny()
	}
	v.Head, v.Effect, v.Evidence = &head, &effect, &evidence
	return v, nil
}
