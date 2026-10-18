package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The original outer primary key/kind is the physical claim fence for BOTH
// profiles. Null flags distinguish an absent representation from empty bytes.
const ownedAttemptEffectParentSQL = `SELECT
 SUBSTRING(CAST(tenant_id AS String),0,161), SUBSTRING(CAST(attempt_id AS String),0,161),
 SUBSTRING(CAST(kind AS String),0,161), SUBSTRING(CAST(run_id AS String),0,161),
 SUBSTRING(CAST(lease_id AS String),0,161), fence_token,
 SUBSTRING(CAST(physical_invocation_claim_id AS String),0,161), effect_sequence,
 SUBSTRING(CAST(invocation_authority_digest AS String),0,65), reserved_at,
 CASE WHEN record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(owned_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(owned_digest AS String),0,65),CAST('' AS String))
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

const ownedAttemptEffectLeaseSQL = `SELECT
 SUBSTRING(CAST(h.tenant_id AS String),0,161), SUBSTRING(CAST(h.run_id AS String),0,161),
 SUBSTRING(CAST(h.lease_id AS String),0,161), SUBSTRING(CAST(h.attempt_id AS String),0,161),
 SUBSTRING(CAST(h.worker_id AS String),0,161), h.fence_token, h.expires_at, h.updated_at,
 SUBSTRING(CAST(l.tenant_id AS String),0,161), SUBSTRING(CAST(l.lease_id AS String),0,161),
 SUBSTRING(CAST(l.run_id AS String),0,161), SUBSTRING(CAST(l.attempt_id AS String),0,161),
 SUBSTRING(CAST(l.worker_id AS String),0,161), l.fence_token, l.acquired_at, l.expires_at,
 SUBSTRING(CAST(l.payload AS String),0,8193)
 FROM lease_heads AS h INNER JOIN leases AS l ON l.tenant_id=h.tenant_id AND l.lease_id=h.lease_id
 WHERE h.tenant_id=$1 AND h.run_id=$2`

// Private composition only. Public admission and both serving registries still
// deny owned execution. This fence grants preparation, NOT an operation/send,
// monetary debit, allocation registration or production source approval.
func (store *Store) reserveBoundedOwnedAttemptEffect(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (ports.ReserveAttemptEffectResultV1, error) {
	if store == nil || store.db == nil || request.Validate() != nil || store.attemptEffectGrantIssuer == nil || store.ownedAttemptEffectSourceVerifier == nil {
		return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
	}
	var candidate domain.OwnedAttemptEffectParentV1
	var status ports.AttemptEffectReservationStatusV1
	var at time.Time
	writeAttempted := false
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		candidate, status, at = domain.OwnedAttemptEffectParentV1{}, "", time.Time{}
		// An attempted INSERT whose ACK was lost cannot become a fresh
		// reservation on a transaction callback retry, even if it did not persist.
		if writeAttempted {
			return q.budget.deny()
		}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, err := readOwnedAttemptEffectParent(ctx, q, request)
		if err != nil {
			return err
		}
		if parent != nil {
			final, err := readOwnedAdmission(ctx, q, parent.Scope)
			if err != nil || final == nil || parent.ValidateForAdmission(*final) != nil || store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(final.Source.Clone()) != nil {
				return q.budget.deny()
			}
			if parent.Reservation.ReservedAt.After(q.at) || !attemptEffectRequestMatchesRecord(request, attemptEffectRecordV1{Authority: parent.Authority, Reservation: parent.Reservation}) {
				return q.budget.deny()
			}
			burned, err := readOwnedPreparationBurn(ctx, q, *parent)
			if err != nil {
				return err
			}
			// Retained, authenticated history is observation-only for a losing
			// claimant or an exhausted ORIGINAL preparation window. No current
			// source replacement, clock renewal, preparation or cancellation right.
			if burned || parent.Reservation.PhysicalInvocationClaimID != request.PhysicalInvocationClaimID || !q.at.Before(parent.PreparationExpiresAt) {
				candidate, status, at = parent.Clone(), ports.AttemptEffectReconcileOnlyV1, q.at
				return nil
			}
			if err := store.validateCurrentOwnedAttemptParent(ctx, q, request, *parent); err != nil {
				return err
			}
			candidate, status, at = parent.Clone(), ports.AttemptEffectReplayedV1, q.at
			return nil
		}
		authority, final, err := store.currentOwnedAttemptEffectAuthority(ctx, q, request)
		if err != nil {
			return err
		}
		value, err := domain.BuildOwnedAttemptEffectParentV1(final, authority, request.PhysicalInvocationClaimID, request.UpstreamIdempotencyKeyDigest, q.at)
		if err != nil {
			return q.budget.deny()
		}
		raw, err := domain.EncodeOwnedAttemptEffectParentV1(value)
		digest, digestErr := value.Digest()
		if err != nil || digestErr != nil {
			return q.budget.deny()
		}
		r := value.Reservation
		writeAttempted = true
		_, err = q.ExecContext(ctx, `INSERT INTO attempt_effect_reservations
 (tenant_id,attempt_id,kind,run_id,lease_id,fence_token,physical_invocation_claim_id,effect_sequence,invocation_authority_digest,reserved_at,owned_record,owned_digest)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, r.TenantID, r.AttemptID, r.Kind, r.RunID, r.LeaseID, r.FenceToken, r.PhysicalInvocationClaimID, r.EffectSequence, r.InvocationAuthorityDigest, r.ReservedAt, string(raw), digest)
		if err != nil {
			return err
		}
		candidate, status, at = value.Clone(), ports.AttemptEffectOwnedV1, q.at
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
	}
	result := ports.ReserveAttemptEffectResultV1{Status: status, Reservation: candidate.Reservation.Clone()}
	if status == ports.AttemptEffectReconcileOnlyV1 {
		grant, err := store.mintAttemptEffectObservationGrant(attemptEffectRecordV1{Authority: candidate.Authority, Reservation: candidate.Reservation}, at)
		if err != nil {
			return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
		}
		result.ObservationGrant = &grant
	} else {
		grant, err := store.attemptEffectGrantIssuer.MintAttemptEffectOwnershipGrant(candidate.Authority, candidate.Reservation, candidate.PreparationExpiresAt)
		if err != nil {
			return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
		}
		result.Grant = &grant
	}
	if result.Validate() != nil || ctx.Err() != nil {
		return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

// Observation never creates a missing reservation or recovers preparation
// ownership. In particular, a previous uncertain INSERT cannot be retried here.
func (store *Store) observeBoundedOwnedAttemptEffect(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (ports.ReserveAttemptEffectResultV1, error) {
	deny := func() (ports.ReserveAttemptEffectResultV1, error) {
		return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
	}
	if store == nil || store.db == nil || ctx == nil || ctx.Err() != nil || request.Validate() != nil || store.attemptEffectGrantIssuer == nil || store.ownedAttemptEffectSourceVerifier == nil {
		return deny()
	}
	var parent domain.OwnedAttemptEffectParentV1
	var at time.Time
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		parent, at = domain.OwnedAttemptEffectParentV1{}, time.Time{}
		original, err := readOwnedAttemptEffectParent(ctx, q, request)
		if err != nil || original == nil || original.Reservation.ReservedAt.After(q.at) || !attemptEffectRequestMatchesRecord(request, attemptEffectRecordV1{Authority: original.Authority, Reservation: original.Reservation}) {
			return q.budget.deny()
		}
		final, err := readOwnedAdmission(ctx, q, original.Scope)
		if err != nil || final == nil || original.ValidateForAdmission(*final) != nil || store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(final.Source.Clone()) != nil {
			return q.budget.deny()
		}
		parent, at = original.Clone(), q.at
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	grant, err := store.mintAttemptEffectObservationGrant(attemptEffectRecordV1{Authority: parent.Authority, Reservation: parent.Reservation}, at)
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	result := ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectReconcileOnlyV1, Reservation: parent.Reservation.Clone(), ObservationGrant: &grant}
	if result.Validate() != nil {
		return deny()
	}
	return result, nil
}

func readOwnedAttemptEffectParent(ctx context.Context, q *ownedAuthorityQuery, request ports.ReserveAttemptEffectRequestV1) (*domain.OwnedAttemptEffectParentV1, error) {
	var tenant, attempt, kind, run, lease, claim, authorityDigest, raw, digest string
	var fence, sequence, legacyPresent, ownedPresent, digestPresent uint64
	var reservedAt time.Time
	err := q.scanMetadata(ctx, ownedAttemptEffectParentSQL, []any{request.TenantID, request.AttemptID, domain.ProviderEffectTurnV1},
		&tenant, &attempt, &kind, &run, &lease, &fence, &claim, &sequence, &authorityDigest, &reservedAt, &legacyPresent, &ownedPresent, &digestPresent, &raw, &digest)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil || legacyPresent != 0 || ownedPresent != 1 || digestPresent != 1 || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || len(authorityDigest) != 64 {
		return nil, q.budget.deny()
	}
	for _, value := range []string{tenant, attempt, kind, run, lease, claim} {
		if len(value) > 160 {
			return nil, q.budget.deny()
		}
	}
	p, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(raw))
	d, digestErr := p.Digest()
	r := p.Reservation
	if err != nil || digestErr != nil || d != digest || tenant != string(request.TenantID) || attempt != string(request.AttemptID) || kind != string(domain.ProviderEffectTurnV1) ||
		tenant != string(r.TenantID) || attempt != string(r.AttemptID) || kind != string(r.Kind) || run != string(r.RunID) || lease != string(r.LeaseID) || fence != r.FenceToken ||
		claim != r.PhysicalInvocationClaimID || sequence != r.EffectSequence || authorityDigest != string(r.InvocationAuthorityDigest) || !reservedAt.Equal(r.ReservedAt) {
		return nil, q.budget.deny()
	}
	return &p, nil
}

func (store *Store) currentOwnedAttemptEffectAuthority(ctx context.Context, q *ownedAuthorityQuery, request ports.ReserveAttemptEffectRequestV1) (domain.ServerlessInvocationAuthorityV1, domain.AttemptExecutionAdmissionV1, error) {
	deny := func() (domain.ServerlessInvocationAuthorityV1, domain.AttemptExecutionAdmissionV1, error) {
		return domain.ServerlessInvocationAuthorityV1{}, domain.AttemptExecutionAdmissionV1{}, q.budget.deny()
	}
	loaded, err := loadOwnedEffectWorkerJob(ctx, q, request)
	if err != nil || loaded.Job.HarnessBinding.Profile != domain.HarnessExecutionOwnedAgentV2 || loaded.Job.HarnessBinding.AgentLoopPolicy == nil || loaded.Job.SubstrateBinding == nil || loaded.Job.AdmissionCostCeiling == nil || loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementManaged {
		return deny()
	}
	job := loaded.Job
	// Closed preparation classes only. A local tuple is not approval: the
	// original authenticated source, Final, installed profile/funding heads,
	// Session/principal and lease/fence checks below remain mandatory. Public
	// serving composition and all per-operation send gates remain separate.
	backend, resource, substrate := job.HarnessBinding.Backend, job.HarnessBinding.Resource, *job.SubstrateBinding
	fixture := backend.BackendKind == domain.HarnessBackendDeterministicFixtureV1 && resource.Kind == domain.ProviderResourceCredentiallessFixtureV1
	local := backend.BackendKind == domain.HarnessBackendDirectOpenAICompatibleV1 && backend.ProviderContractKind == domain.ProviderContractConfiguredEndpointV1 &&
		resource.Kind == domain.ProviderResourceLocalEndpointV1 && substrate.Kind == domain.SubstrateLocalDevelopmentV1 && substrate.WorkloadMode == domain.SubstrateWorkloadInProcessDirectV1
	if !fixture && !local {
		return deny()
	}
	source, err := domain.DecodeAttemptExecutionSourceV1([]byte(job.OwnedExecutionSourceRecord))
	if err != nil || source.Scope.TenantID != request.TenantID || source.Scope.RunID != request.RunID || source.Scope.AttemptID != request.AttemptID || source.Scope.SessionID != job.SessionID || source.Scope.OwnerUserID != job.CredentialOwnerUserID {
		return deny()
	}
	final, err := readOwnedAdmission(ctx, q, source.Scope)
	if err != nil || !ownedAdmissionMatches(final, source, job.OwnedExecutionSourceRecord, loaded.Reservation, job) || store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(source.Clone()) != nil {
		return deny()
	}
	if readOwnedAdmissionPrincipal(ctx, q, source.Scope) != nil {
		return deny()
	}
	if _, err := readOwnedAdmissionSources(ctx, q, source); err != nil {
		return deny()
	}
	lease, err := readOwnedAttemptEffectLease(ctx, q, request)
	if err != nil || validateCurrentServerlessLease(loaded, lease, request, q.at) != nil || loaded.Reservation.Status != domain.ReservationHeld || !loaded.Reservation.ExpiresAt.After(q.at) || loaded.Reservation.UpdatedAt.After(q.at) {
		return deny()
	}
	contextDigest, inputDigest, err := domain.ServerlessWorkerJobDigestsV1(job, loaded.InputManifest)
	if err != nil {
		return deny()
	}
	deadline := lease.AcquiredAt.Add(job.SubstrateBinding.Limits.InvocationTimeout)
	for _, limit := range []time.Time{lease.ExpiresAt, loaded.Reservation.ExpiresAt, source.ExpiresAt, lease.AcquiredAt.Add(job.HarnessBinding.AgentLoopPolicy.Limits.MaxActiveDuration), lease.AcquiredAt.Add(job.Limits.MaxRuntime)} {
		if limit.Before(deadline) {
			deadline = limit
		}
	}
	authority := domain.ServerlessInvocationAuthorityV1{Version: domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: job.HarnessBinding.Clone(), ExecutionPlacementV2: job.ExecutionPlacementV2,
		SubstrateBinding: *job.SubstrateBinding, AdmissionCostCeiling: job.AdmissionCostCeiling.Clone(),
		Lease: lease, ContextManifestDigest: contextDigest, InputManifestDigest: inputDigest, InvocationDeadline: deadline.UTC()}
	if authority.ValidateAt(q.at) != nil || final.ValidateForExecution(authority.ExecutionPlacementV2, authority.HarnessBinding, authority.SubstrateBinding, authority.AdmissionCostCeiling) != nil {
		return deny()
	}
	return authority, final.Clone(), nil
}

func readOwnedAttemptEffectLease(ctx context.Context, q *ownedAuthorityQuery, request ports.ReserveAttemptEffectRequestV1) (domain.Lease, error) {
	var hTenant, hRun, hLease, hAttempt, hWorker, lTenant, lLease, lRun, lAttempt, lWorker, raw string
	var hFence, lFence uint64
	var hExpires, hUpdated, lAcquired, lExpires time.Time
	err := q.scanMetadata(ctx, ownedAttemptEffectLeaseSQL, []any{request.TenantID, request.RunID}, &hTenant, &hRun, &hLease, &hAttempt, &hWorker, &hFence, &hExpires, &hUpdated, &lTenant, &lLease, &lRun, &lAttempt, &lWorker, &lFence, &lAcquired, &lExpires, &raw)
	for _, value := range []string{hTenant, hRun, hLease, hAttempt, hWorker, lTenant, lLease, lRun, lAttempt, lWorker} {
		if len(value) > 160 {
			return domain.Lease{}, q.budget.deny()
		}
	}
	var lease domain.Lease
	fields := map[string]bool{"id": true, "tenant_id": true, "run_id": true, "attempt_id": true, "worker_id": true, "fence_token": true, "acquired_at": true, "expires_at": true}
	if err != nil || decodeCurrentExecutionRecord(raw, &lease, fields) != nil || hTenant != string(request.TenantID) || hRun != string(request.RunID) || hLease != string(request.LeaseID) || hAttempt != string(request.AttemptID) || hFence != request.FenceToken ||
		hTenant != lTenant || hRun != lRun || hLease != lLease || hAttempt != lAttempt || hWorker != lWorker || hFence != lFence || !hExpires.Equal(lExpires) || hUpdated.Before(lAcquired) || hUpdated.After(q.at) ||
		lTenant != string(lease.TenantID) || lRun != string(lease.RunID) || lLease != string(lease.ID) || lAttempt != string(lease.AttemptID) || lWorker != lease.WorkerID || lFence != lease.FenceToken || !lAcquired.Equal(lease.AcquiredAt) || !lExpires.Equal(lease.ExpiresAt) || lease.AcquiredAt.After(q.at) ||
		!ownedEffectTime(lease.AcquiredAt) || !ownedEffectTime(lease.ExpiresAt) || !ownedEffectTime(hUpdated) {
		return domain.Lease{}, q.budget.deny()
	}
	return lease, nil
}

func loadOwnedEffectWorkerJob(ctx context.Context, q *ownedAuthorityQuery, request ports.ReserveAttemptEffectRequestV1) (ports.WorkerJobState, error) {
	var v ports.WorkerJobState
	for _, item := range []struct {
		statement string
		args      []any
		target    any
	}{
		{"SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", []any{request.TenantID, request.RunID}, &v.Run},
		{"SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2", []any{request.TenantID, request.RunID}, &v.Job},
		{"SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2", []any{request.TenantID, request.AttemptID}, &v.Attempt},
	} {
		raw, err := q.readCanonicalJSON(ctx, item.statement, item.args...)
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, item.target) != nil {
			return ports.WorkerJobState{}, q.budget.deny()
		}
	}
	for _, item := range []struct {
		statement string
		args      []any
		target    any
	}{
		{"SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", []any{request.TenantID, v.Job.ReservationID}, &v.Reservation},
		{"SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2", []any{request.TenantID, v.Job.InputManifestID}, &v.InputManifest},
	} {
		raw, err := q.readCanonicalJSON(ctx, item.statement, item.args...)
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, item.target) != nil {
			return ports.WorkerJobState{}, q.budget.deny()
		}
	}
	if v.Run.TenantID != request.TenantID || v.Run.ID != request.RunID || v.Job.AttemptID != request.AttemptID || v.InputManifest.CreatedAt.After(q.at) ||
		(v.Run.StartedAt != nil && v.Run.StartedAt.After(q.at)) || validateLoadedWorkerJob(v) != nil {
		return ports.WorkerJobState{}, q.budget.deny()
	}
	return v, nil
}

// Existing JsonDocument rows may reorder fields/whitespace. Preserve those
// representations but reject duplicated, unknown, case-folded, omitted/null
// substitutions or non-producer shape at EVERY nested level. Never compare
// via float64: these authority envelopes contain integers and exact digests.
func decodeOwnedEffectCanonicalJSON(raw string, target any) error {
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	if validateOwnedEffectJSONTokens(d, 0) != nil {
		return domain.ErrMembershipDenied
	}
	if _, err := d.Token(); err != io.EOF {
		return domain.ErrMembershipDenied
	}
	d = json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return domain.ErrMembershipDenied
	}
	expected, err := json.Marshal(target)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	var originalShape, producerShape any
	for _, item := range []struct {
		raw    []byte
		target *any
	}{{[]byte(raw), &originalShape}, {expected, &producerShape}} {
		decoder := json.NewDecoder(bytes.NewReader(item.raw))
		decoder.UseNumber()
		if decoder.Decode(item.target) != nil {
			return domain.ErrMembershipDenied
		}
	}
	if !reflect.DeepEqual(originalShape, producerShape) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func validateOwnedEffectJSONTokens(d *json.Decoder, depth int) error {
	if depth > 64 {
		return domain.ErrMembershipDenied
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] {
				return domain.ErrMembershipDenied
			}
			seen[name] = true
			if err := validateOwnedEffectJSONTokens(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return domain.ErrMembershipDenied
		}
	case json.Delim('['):
		for d.More() {
			if err := validateOwnedEffectJSONTokens(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return domain.ErrMembershipDenied
		}
	case json.Delim('}'), json.Delim(']'):
		return domain.ErrMembershipDenied
	}
	return nil
}

func ownedEffectTime(at time.Time) bool {
	return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 && at.Nanosecond()%1000 == 0
}
