package ydbstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const piPolicyRecordMax = 64 << 10
const piPolicyReadSQL = `SELECT SUBSTRING(record,0,65537),generation,SUBSTRING(CAST(mapping_digest AS String),0,65),SUBSTRING(CAST(state AS String),0,17),updated_at FROM attached_pi_api_policy_v1 WHERE tenant_id=$1 AND owner_user_id=$2 AND locator_digest=$3 AND record_key=$4`

type piPolicyHead struct {
	Version     uint32                                              `json:"version"`
	Generation  uint64                                              `json:"generation"`
	State       string                                              `json:"state"`
	Mapping     domain.AttachedPiAPIInvocationPolicyMappingDigestV1 `json:"mapping"`
	DecisionKey string                                              `json:"decision_key"`
	UpdatedAt   time.Time                                           `json:"updated_at"`
}
type piPolicyDecision struct {
	Version            uint32                               `json:"version"`
	Locator            ports.AttachedPiAPIPolicyLocatorV1   `json:"locator"`
	MutationID         domain.IdempotencyKey                `json:"mutation_id"`
	ExpectedGeneration uint64                               `json:"expected_generation"`
	Selection          ports.AttachedPiAPIPolicySelectionV1 `json:"selection"`
	Decision           ports.AttachedPiAPIPolicyDecisionV1  `json:"decision"`
}

func piPolicyDecode(raw string, target any) error {
	if len(raw) == 0 || len(raw) > piPolicyRecordMax || decodeOwnedEffectCanonicalJSON(raw, target) != nil {
		return domain.ErrMembershipDenied
	}
	// String storage preserves canonical producer bytes. Do not accept an
	// alternate object/order/number representation for immutable R/M decisions.
	b, err := json.Marshal(target)
	if err != nil || !bytes.Equal([]byte(raw), b) {
		return domain.ErrMembershipDenied
	}
	return nil
}
func readPiPolicyRecord(ctx context.Context, q rowQuery, l ports.AttachedPiAPIPolicyLocatorV1, key string, target any) (bool, uint64, string, string, time.Time, error) {
	digest, err := l.Digest()
	if err != nil {
		return false, 0, "", "", time.Time{}, err
	}
	var raw, mapping, state string
	var generation uint64
	var at time.Time
	err = q.QueryRowContext(ctx, piPolicyReadSQL, l.TenantID, l.OwnerUserID, digest, key).Scan(&raw, &generation, &mapping, &state, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return false, 0, "", "", time.Time{}, nil
	}
	if err != nil || piPolicyDecode(raw, target) != nil || generation == 0 || len(mapping) != 64 || !piPolicyTime(at) {
		return false, 0, "", "", time.Time{}, domain.ErrMembershipDenied
	}
	return true, generation, mapping, state, at, nil
}
func piPolicyTime(at time.Time) bool {
	return !at.IsZero() && at.Location() == time.UTC && at.Nanosecond()%1000 == 0 && at.Year() >= 1 && at.Year() <= 9999
}
func readPiPolicyHead(ctx context.Context, q rowQuery, l ports.AttachedPiAPIPolicyLocatorV1, now time.Time) (*piPolicyHead, error) {
	var h piPolicyHead
	found, generation, mapping, state, at, err := readPiPolicyRecord(ctx, q, l, "head", &h)
	if err != nil || !found {
		return nil, err
	}
	if h.Version != 1 || h.Generation != generation || string(h.Mapping) != mapping || h.State != state || !h.UpdatedAt.Equal(at) ||
		h.UpdatedAt.After(now) || !piPolicyTime(h.UpdatedAt) || (h.State != "active" && h.State != "revoked") || h.Mapping.Validate() != nil || !strings.HasPrefix(h.DecisionKey, "decision:") || len(h.DecisionKey) > 105 {
		return nil, domain.ErrMembershipDenied
	}
	return &h, nil
}
func piDecisionKey(id domain.IdempotencyKey) (string, error) {
	if id.Validate() != nil || len(id) > 96 {
		return "", domain.ErrMembershipDenied
	}
	return "decision:" + string(id), nil
}
func readPiPolicyDecision(ctx context.Context, q rowQuery, l ports.AttachedPiAPIPolicyLocatorV1, key string, now time.Time) (*piPolicyDecision, error) {
	var d piPolicyDecision
	found, generation, mapping, state, at, err := readPiPolicyRecord(ctx, q, l, key, &d)
	if err != nil || !found {
		return nil, err
	}
	r := d.Decision.Receipt
	wantKey, keyErr := piDecisionKey(d.MutationID)
	md, digestErr := d.Decision.Mapping.Digest()
	if d.Version != 1 || d.Locator != l || keyErr != nil || wantKey != key || d.ExpectedGeneration == math.MaxUint64 || d.Decision.Generation != d.ExpectedGeneration+1 ||
		d.Decision.Generation != generation || digestErr != nil || md != d.Decision.MappingDigest || string(md) != mapping || state != "decision" || d.Decision.Replayed ||
		r.IssuerID != ports.AttachedPiAPIOwnerPolicyIssuerV1 || r.DecisionSourceID != "attached-pi-api:"+string(d.MutationID) ||
		r.DecisionID == "" || !r.IssuedAt.Equal(at) || r.IssuedAt.After(now) || !piPolicyTime(r.IssuedAt) || !piPolicyTime(r.ExpiresAt) ||
		d.Selection.Terms != d.Decision.Terms || !l.Matches(d.Decision.Terms) || d.Decision.Mapping.ValidateFor(d.Decision.Terms, r) != nil {
		return nil, domain.ErrMembershipDenied
	}
	return &d, nil
}
func writePiPolicyRecord(ctx context.Context, tx stateSQLExecutor, l ports.AttachedPiAPIPolicyLocatorV1, key string, value any, generation uint64, mapping domain.AttachedPiAPIInvocationPolicyMappingDigestV1, state string, at time.Time, immutable bool) error {
	digest, err := l.Digest()
	b, e := json.Marshal(value)
	if err != nil || e != nil || len(b) == 0 || len(b) > piPolicyRecordMax {
		return domain.ErrMembershipDenied
	}
	op := "UPSERT"
	if immutable {
		op = "INSERT"
	}
	_, err = tx.ExecContext(ctx, op+` INTO attached_pi_api_policy_v1 (tenant_id,owner_user_id,locator_digest,record_key,record,generation,mapping_digest,state,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		l.TenantID, l.OwnerUserID, digest, key, string(b), generation, mapping, state, at)
	return err
}

// Current browser/session/CSRF and membership are read in the SAME transaction
// as API resource, immutable owner decision and current-head CAS; BFF preflight
// is required but cannot substitute this recheck. These reads do not refresh a
// browser lifetime. The native/provider credential itself is never read here.
func piPolicyActor(ctx context.Context, q rowQuery, l ports.AttachedPiAPIPolicyLocatorV1, sessionDigest, csrf domain.SecretDigest, mutate bool) (domain.TenantMembership, time.Time, error) {
	if l.Validate() != nil || sessionDigest.Validate("pi_policy.session") != nil || mutate && csrf.Validate("pi_policy.csrf") != nil {
		return domain.TenantMembership{}, time.Time{}, domain.ErrMembershipDenied
	}
	var at time.Time
	if err := q.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		return domain.TenantMembership{}, at, err
	}
	at = at.UTC().Truncate(time.Microsecond)
	bucket, err := webBucket(string(sessionDigest))
	if err != nil || !piPolicyTime(at) {
		return domain.TenantMembership{}, at, domain.ErrMembershipDenied
	}
	var raw string
	if err := q.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, sessionDigest).Scan(&raw); err != nil {
		return domain.TenantMembership{}, at, domain.ErrMembershipDenied
	}
	var session domain.WebSession
	if len(raw) > 8192 || decodeOwnedEffectCanonicalJSON(raw, &session) != nil || session.SessionDigest != sessionDigest || session.UserID != l.OwnerUserID || session.ActiveTenantID != l.TenantID ||
		session.IssuedAt.After(at) || session.LastSeenAt.After(at) || mutate && session.CSRFTokenDigest != csrf {
		return domain.TenantMembership{}, at, domain.ErrMembershipDenied
	}
	member, err := piPolicyMembership(ctx, q, l, at)
	if err != nil || session.Authorize(member, domain.TenantPermissionWrite, at) != nil {
		return domain.TenantMembership{}, at, domain.ErrMembershipDenied
	}
	return member, at, nil
}
func piPolicyMembership(ctx context.Context, q rowQuery, l ports.AttachedPiAPIPolicyLocatorV1, at time.Time) (domain.TenantMembership, error) {
	bucket, err := webBucket(string(l.OwnerUserID))
	if err != nil {
		return domain.TenantMembership{}, domain.ErrMembershipDenied
	}
	var raw, role, status string
	var security uint64
	if err := q.QueryRowContext(ctx, computeChoiceMembershipSQL, bucket, l.OwnerUserID, l.TenantID).Scan(&raw, &role, &status, &security); err != nil {
		return domain.TenantMembership{}, domain.ErrMembershipDenied
	}
	var m domain.TenantMembership
	if decodeCurrentExecutionRecord(raw, &m, computeChoiceMembershipFields) != nil || m.SecurityVersion != security || string(m.Role) != role || string(m.Status) != status || m.UpdatedAt.After(at) || m.Authorize(l.OwnerUserID, l.TenantID, domain.TenantPermissionWrite) != nil {
		return domain.TenantMembership{}, domain.ErrMembershipDenied
	}
	return m, nil
}
func (store *Store) selectPiPolicy(ctx context.Context, q rowQuery, l ports.AttachedPiAPIPolicyLocatorV1, at time.Time) (ports.AttachedPiAPIPolicySelectionV1, time.Time, error) {
	if store.attachedPiAPIPolicyCatalog == nil {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	s, err := store.attachedPiAPIPolicyCatalog.SelectAttachedPiAPIPolicy(l, at)
	if err != nil || !l.Matches(s.Terms) || domain.ValidateOpaqueID("pi_policy.source", s.SourceID) != nil || len(s.Evidence.Catalog.Models) > 64 || len(s.Evidence.Route.Routes) > 64 || len(s.Evidence.Privacy.AllowedDataClasses) > 3 || len(s.Evidence.Policy.AllowedDataClasses) > 3 {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	until, err := domain.ValidateAttachedPiAPITermsV1(s.Profile, s.Evidence, s.Terms, at)
	if err != nil {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	current, found, err := readProviderCredential(ctx, q, ports.ProviderCredentialLocatorV1{TenantID: l.TenantID, OwnerUserID: l.OwnerUserID, ResourceKind: domain.ProviderResourceAPIAccountV1, ResourceID: l.ResourceID})
	if err != nil || !found || current.UpdatedAt.After(at) {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	resource, err := current.ResourceBinding()
	if err != nil || resource != s.Terms.Scope.Resource {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(s)
	if err != nil || len(raw) > piPolicyRecordMax {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	var owned ports.AttachedPiAPIPolicySelectionV1
	if piPolicyDecode(string(raw), &owned) != nil {
		return ports.AttachedPiAPIPolicySelectionV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	return owned, until, nil
}

func (store *Store) ReadAttachedPiAPIPolicy(ctx context.Context, r ports.ReadAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyOfferV1, error) {
	if store == nil || store.db == nil || ctx == nil || ctx.Err() != nil || store.attachedPiAPIPolicyCatalog == nil {
		return ports.AttachedPiAPIPolicyOfferV1{}, domain.ErrMembershipDenied
	}
	var result ports.AttachedPiAPIPolicyOfferV1
	err := store.Transact(ctx, r.Locator.TenantID, func(st ports.StateTx) error {
		result = ports.AttachedPiAPIPolicyOfferV1{}
		tx := st.(*stateTx)
		_, at, err := piPolicyActor(ctx, tx.sqlTx, r.Locator, r.WebSessionDigest, "", false)
		if err != nil {
			return err
		}
		s, _, err := store.selectPiPolicy(ctx, tx.sqlTx, r.Locator, at)
		if err != nil {
			return err
		}
		h, err := readPiPolicyHead(ctx, tx.sqlTx, r.Locator, at)
		if err != nil {
			return err
		}
		digest, err := s.Terms.Digest()
		if err != nil {
			return err
		}
		result = ports.AttachedPiAPIPolicyOfferV1{Terms: s.Terms, TermsDigest: digest}
		if h != nil {
			result.CurrentGeneration = h.Generation
			if h.State == "active" {
				result.CurrentMapping = h.Mapping
			}
		}
		return nil
	})
	if err != nil {
		return ports.AttachedPiAPIPolicyOfferV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

func (store *Store) ConfirmAttachedPiAPIPolicy(ctx context.Context, r ports.ConfirmAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyDecisionV1, error) {
	key, err := piDecisionKey(r.MutationID)
	if store == nil || store.db == nil || ctx == nil || ctx.Err() != nil || err != nil || r.ExpectedGeneration == math.MaxUint64 || domain.ValidateSHA256Digest("pi_policy.terms", string(r.TermsDigest)) != nil {
		return ports.AttachedPiAPIPolicyDecisionV1{}, domain.ErrMembershipDenied
	}
	decisionID := "piad_" + rand.Text() // Identifier only, not a signature/permission.
	var result ports.AttachedPiAPIPolicyDecisionV1
	err = store.Transact(ctx, r.Locator.TenantID, func(st ports.StateTx) error {
		result = ports.AttachedPiAPIPolicyDecisionV1{}
		tx := st.(*stateTx)
		member, at, err := piPolicyActor(ctx, tx.sqlTx, r.Locator, r.WebSessionDigest, r.CSRFTokenDigest, true)
		if err != nil {
			return err
		}
		prior, err := readPiPolicyDecision(ctx, tx.sqlTx, r.Locator, key, at)
		if err != nil {
			return err
		}
		if prior != nil {
			td, _ := prior.Decision.Terms.Digest()
			if td != r.TermsDigest || prior.ExpectedGeneration != r.ExpectedGeneration {
				return domain.ErrMembershipDenied
			}
			result = prior.Decision
			result.Replayed = true
			return nil // Historical receipt only: NEVER rewrite/revive a head.
		}
		s, until, err := store.selectPiPolicy(ctx, tx.sqlTx, r.Locator, at)
		if err != nil {
			return err
		}
		until = until.UTC().Truncate(time.Microsecond)
		if !until.After(at) {
			return domain.ErrMembershipDenied
		}
		td, err := s.Terms.Digest()
		if err != nil || td != r.TermsDigest {
			return domain.ErrMembershipDenied
		}
		h, err := readPiPolicyHead(ctx, tx.sqlTx, r.Locator, at)
		if err != nil || h == nil && r.ExpectedGeneration != 0 || h != nil && h.Generation != r.ExpectedGeneration {
			return domain.ErrMembershipDenied
		}
		receipt := domain.AttachedPiAPIOwnerAuthorizationReceiptV1{Version: 1, IssuerID: ports.AttachedPiAPIOwnerPolicyIssuerV1, DecisionID: decisionID, TenantID: r.Locator.TenantID, OwnerUserID: r.Locator.OwnerUserID,
			DecisionSourceID: "attached-pi-api:" + string(r.MutationID), SubjectTermsDigest: td, Decision: "authorize", IssuedAt: at, ExpiresAt: until, MembershipSecurityVersion: member.SecurityVersion}
		rd, err := receipt.Digest()
		if err != nil {
			return domain.ErrMembershipDenied
		}
		mapping := domain.AttachedPiAPIInvocationPolicyMappingV1{Version: 1, TermsDigest: td, OwnerAuthorizationReceiptDigest: rd}
		md, err := mapping.Digest()
		if err != nil {
			return domain.ErrMembershipDenied
		}
		result = ports.AttachedPiAPIPolicyDecisionV1{Terms: s.Terms, Receipt: receipt, Mapping: mapping, MappingDigest: md, Generation: r.ExpectedGeneration + 1}
		d := piPolicyDecision{Version: 1, Locator: r.Locator, MutationID: r.MutationID, ExpectedGeneration: r.ExpectedGeneration, Selection: s, Decision: result}
		next := piPolicyHead{Version: 1, Generation: result.Generation, State: "active", Mapping: md, DecisionKey: key, UpdatedAt: at}
		if err := writePiPolicyRecord(ctx, tx.sqlTx, r.Locator, key, d, next.Generation, md, "decision", at, true); err != nil {
			return err
		}
		return writePiPolicyRecord(ctx, tx.sqlTx, r.Locator, "head", next, next.Generation, md, next.State, at, false)
	})
	if err != nil {
		return ports.AttachedPiAPIPolicyDecisionV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

func (store *Store) RevokeAttachedPiAPIPolicy(ctx context.Context, r ports.RevokeAttachedPiAPIPolicyV1) error {
	if store == nil || store.db == nil || ctx == nil || ctx.Err() != nil || r.ExpectedGeneration == 0 || r.ExpectedGeneration == math.MaxUint64 || r.ExpectedMapping.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	return store.Transact(ctx, r.Locator.TenantID, func(st ports.StateTx) error {
		tx := st.(*stateTx)
		_, at, err := piPolicyActor(ctx, tx.sqlTx, r.Locator, r.WebSessionDigest, r.CSRFTokenDigest, true)
		if err != nil {
			return err
		}
		h, err := readPiPolicyHead(ctx, tx.sqlTx, r.Locator, at)
		if err != nil || h == nil || h.Mapping != r.ExpectedMapping {
			return domain.ErrMembershipDenied
		}
		if h.State == "revoked" && h.Generation == r.ExpectedGeneration+1 {
			return nil
		}
		if h.State != "active" || h.Generation != r.ExpectedGeneration {
			return domain.ErrMembershipDenied
		}
		h.Generation++
		h.State = "revoked"
		h.UpdatedAt = at
		return writePiPolicyRecord(ctx, tx.sqlTx, r.Locator, "head", h, h.Generation, h.Mapping, h.State, at, false)
	})
}

// Called by actual original dispatch/use/publication transactions, NOT a
// caller-supplied receipt. Exact authoritative locator, registered issuer,
// current head, immutable R and current membership/API/catalog all revalidate.
// Historical ACK/reconciliation must retain their separate original predicates.
func (store *Store) currentAttachedPiAPIPolicyTx(ctx context.Context, q rowQuery, binding domain.HarnessBindingV2, at time.Time) (ports.AttachedPiAPIPolicySelectionV1, domain.AttachedPiAPIOwnerAuthorizationReceiptV1, domain.AttachedPiAPIInvocationPolicyMappingV1, time.Time, error) {
	deny := func() (ports.AttachedPiAPIPolicySelectionV1, domain.AttachedPiAPIOwnerAuthorizationReceiptV1, domain.AttachedPiAPIInvocationPolicyMappingV1, time.Time, error) {
		return ports.AttachedPiAPIPolicySelectionV1{}, domain.AttachedPiAPIOwnerAuthorizationReceiptV1{}, domain.AttachedPiAPIInvocationPolicyMappingV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	l := ports.AttachedPiAPIPolicyLocatorV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceID: binding.Resource.ResourceID, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, InputDataClass: binding.InputDataClass}
	if binding.Resource.Kind != domain.ProviderResourceAPIAccountV1 || binding.AttachedPiAPIInvocationPolicyDigest.Validate() != nil {
		return deny()
	}
	h, err := readPiPolicyHead(ctx, q, l, at)
	if err != nil || h == nil || h.State != "active" || h.Mapping != binding.AttachedPiAPIInvocationPolicyDigest {
		return deny()
	}
	d, err := readPiPolicyDecision(ctx, q, l, h.DecisionKey, at)
	if err != nil || d == nil || d.Decision.MappingDigest != h.Mapping || d.Decision.Generation != h.Generation {
		return deny()
	}
	m, err := piPolicyMembership(ctx, q, l, at)
	if err != nil || m.SecurityVersion != d.Decision.Receipt.MembershipSecurityVersion {
		return deny()
	}
	s, _, err := store.selectPiPolicy(ctx, q, l, at)
	if err != nil {
		return deny()
	}
	old, err := json.Marshal(d.Selection)
	current, e := json.Marshal(s)
	if err != nil || e != nil || !bytes.Equal(old, current) {
		return deny()
	}
	until, err := domain.ValidateAttachedPiAPIInvocationPolicyV1(s.Profile, s.Evidence, s.Terms, d.Decision.Receipt, d.Decision.Mapping, binding, at)
	if err != nil {
		return deny()
	}
	return s, d.Decision.Receipt, d.Decision.Mapping, until, nil
}

var _ ports.AttachedPiAPIOwnerPolicyStoreV1 = (*Store)(nil)
