package compactioncontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const MaxRetainedSummaryBytesV1 = 32 << 10
const MaxRetainedManifestBytesV1 = 128 << 10

// RetainedEvidenceV1 is DATA from the original publisher. Neither supplying
// these records nor successful decoding proves their issuer, current authority,
// calibrated fit, upload, factual fidelity, custody or publication.
type RetainedEvidenceV1 struct {
	Reservation            domain.AttemptOperationReservationV1
	Observation            domain.AttemptOperationObservationV1
	Summary                []byte
	GenerationFit, NextFit configuredinference.ContextFitV1
	SummaryTokenUpperBound uint64
	Parent                 *RetainedV1
}

type retainedOperationV1 struct {
	Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation domain.AttemptOperationObservationV1 `json:"observation"`
}

// Only original identity/immutable-reference metadata is retained here, never
// original source text. Canonical and operational identities stay disjoint.
type retainedSourceV1 struct {
	SourceID  string               `json:"source_id"`
	Canonical *domain.SessionEvent `json:"canonical"`
	Operation *retainedOperationV1 `json:"operation"`
}

type retainedCutV1 struct {
	Sources   []retainedSourceV1 `json:"sources"`
	Cut       uint32             `json:"cut"`
	Protected []uint32           `json:"protected"`
	Covered   []uint32           `json:"covered"`
	Suffix    []uint32           `json:"suffix"`
}

type retainedFitV1 struct {
	RequestDigest     string `json:"request_digest"`
	CalibrationDigest string `json:"calibration_digest"`
	InputUpperBound   uint64 `json:"input_upper_bound"`
	OutputReserve     uint64 `json:"output_reserve"`
	TotalUpperBound   uint64 `json:"total_upper_bound"`
}

func retainedFit(v configuredinference.ContextFitV1) retainedFitV1 {
	return retainedFitV1{v.RequestDigest, v.CalibrationDigest, v.InputUpperBound, v.OutputReserve, v.TotalUpperBound}
}

// Core has no own object references: the plan seals encoded summary/manifest
// bytes; the manifest refers only to the already encoded summary, never itself.
type retainedCoreV1 struct {
	Version                  uint32                                     `json:"version"`
	Scope                    domain.AttemptOperationScopeV1             `json:"scope"`
	Generation               uint64                                     `json:"generation"`
	OperationSequence        uint64                                     `json:"operation_sequence"`
	ReservationDigest        domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest        domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	NormalizedResponseDigest string                                     `json:"normalized_response_digest"`
	SourceViewDigest         string                                     `json:"source_view_digest"`
	CanonicalDigest          string                                     `json:"canonical_digest"`
	FirstCanonicalSequence   uint64                                     `json:"first_canonical_sequence"`
	ThroughCanonicalSequence uint64                                     `json:"through_canonical_sequence"`
	ThroughCanonicalEventID  domain.SessionEventID                      `json:"through_canonical_event_id"`
	ProfileDigest            string                                     `json:"profile_digest"`
	PolicyDigest             string                                     `json:"policy_digest"`
	BundleDigest             string                                     `json:"bundle_digest"`
	CompatibilityDigest      string                                     `json:"compatibility_digest"`
	CalibrationDigest        string                                     `json:"calibration_digest"`
	GenerationRequestDigest  string                                     `json:"generation_request_digest"`
	NextRequestDigest        string                                     `json:"next_request_digest"`
	CutDigest                string                                     `json:"cut_digest"`
	SummaryDigest            string                                     `json:"summary_digest"`
	SummaryTokenUpperBound   uint64                                     `json:"summary_token_upper_bound"`
}

type retainedSummaryV1 struct {
	Schema  string          `json:"schema"`
	Core    retainedCoreV1  `json:"core"`
	Summary json.RawMessage `json:"summary"`
}

type retainedManifestV1 struct {
	Schema        string                               `json:"schema"`
	Core          retainedCoreV1                       `json:"core"`
	Summary       domain.BlobRef                       `json:"summary"`
	Cut           retainedCutV1                        `json:"cut"`
	Reservation   domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation   domain.AttemptOperationObservationV1 `json:"observation"`
	GenerationFit retainedFitV1                        `json:"generation_fit"`
	NextFit       retainedFitV1                        `json:"next_fit"`
	Parent        *domain.DerivedContextPlanV1         `json:"parent"`
	Closure       uint32                               `json:"closure"`
}

// RetainedV1 owns strictly validated immutable DATA. An original publisher must
// independently rejoin source/current authority, execute Source fit checks and
// verify actual retained I/O. A decoded value is never a publication receipt.
type RetainedV1 struct {
	plan                      domain.DerivedContextPlanV1
	summaryBody, manifestBody []byte
	summary                   retainedSummaryV1
	manifest                  retainedManifestV1
}

func (RetainedV1) String() string               { return "RetainedCompactionV1{content:[redacted]}" }
func (v RetainedV1) GoString() string           { return v.String() }
func (v RetainedV1) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, v.String()) }
func (RetainedV1) MarshalJSON() ([]byte, error) { return nil, ErrCanonical }
func (v *RetainedV1) Plan() domain.DerivedContextPlanV1 {
	if v == nil {
		return domain.DerivedContextPlanV1{}
	}
	return v.plan
}
func (v *RetainedV1) SummaryBody() []byte {
	if v == nil {
		return nil
	}
	return bytes.Clone(v.summaryBody)
}
func (v *RetainedV1) ManifestBody() []byte {
	if v == nil {
		return nil
	}
	return bytes.Clone(v.manifestBody)
}
func (v *RetainedV1) SummaryText() []byte {
	if v == nil {
		return nil
	}
	return bytes.Clone(v.summary.Summary)
}
func (v *RetainedV1) Parent() *ParentV1 {
	if v == nil {
		return nil
	}
	ids := make([]string, 0, len(v.manifest.Cut.Covered))
	for _, index := range v.manifest.Cut.Covered {
		ids = append(ids, v.manifest.Cut.Sources[index].SourceID)
	}
	parent := &ParentV1{Plan: v.plan, Summary: v.SummaryText(), CoveredSourceIDs: ids, CompatibilityDigest: v.manifest.Core.CompatibilityDigest}
	if v.manifest.Parent != nil {
		parent.PriorPlans = []domain.DerivedContextPlanV1{*v.manifest.Parent}
	}
	for _, index := range v.manifest.Cut.Protected {
		parent.ProtectedSourceIDs = append(parent.ProtectedSourceIDs, v.manifest.Cut.Sources[index].SourceID)
	}
	return parent
}

func retainedDigest(body []byte) string { h := sha256.Sum256(body); return hex.EncodeToString(h[:]) }
func retainedValidDigest(value string) bool {
	return domain.ValidateSHA256Digest("retained.digest", value) == nil
}

func (p *GenerationV1) retainedCut() retainedCutV1 {
	c := retainedCutV1{Sources: p.sources, Cut: p.cut, Protected: []uint32{}, Covered: []uint32{}, Suffix: []uint32{}}
	protected := make(map[string]bool, len(p.protected))
	for _, frame := range p.protected {
		protected[frame.SourceID] = true
	}
	for i, source := range p.sources {
		switch {
		case uint32(i) >= p.cut:
			c.Suffix = append(c.Suffix, uint32(i))
		case protected[source.SourceID]:
			c.Protected = append(c.Protected, uint32(i))
		default:
			c.Covered = append(c.Covered, uint32(i))
		}
	}
	return c
}

// BuildRetainedV1 seals this exact program's cut and generation evidence. The
// caller's fit records are checked as DATA only; the original Source must have
// computed and must recheck them before the real publication transaction.
func (p *GenerationV1) BuildRetainedV1(e RetainedEvidenceV1) (*RetainedV1, error) {
	if p == nil || p.frozen == nil || len(p.sources) == 0 || len(e.Summary) == 0 || len(e.Summary) > configuredinference.MaxCompactionSummaryBytesV1 {
		return nil, ErrCanonical
	}
	if _, err := p.ValidateSummary(e.Summary); err != nil {
		return nil, ErrCanonical
	}
	cut := p.retainedCut()
	if !boundedRetainedCut(cut) {
		return nil, ErrCanonical
	}
	cutBody, err := json.Marshal(cut)
	if err != nil || len(cutBody) > MaxRetainedManifestBytesV1 {
		return nil, ErrCanonical
	}
	pd, _ := p.profile.Digest()
	policyDigest, _ := p.policy.Digest()
	rd, re := e.Reservation.Digest()
	od, oe := e.Observation.Digest()
	if re != nil || oe != nil {
		return nil, ErrCanonical
	}
	first, through, eventID := canonicalThrough(cut.Sources)
	next, err := p.Rebuild(e.Summary)
	if err != nil {
		return nil, ErrCanonical
	}
	core := retainedCoreV1{Version: 1, Scope: e.Reservation.Operation.Scope, Generation: p.generation, OperationSequence: e.Reservation.Operation.Sequence,
		ReservationDigest: rd, ObservationDigest: od, NormalizedResponseDigest: e.Observation.ResponseDigest, SourceViewDigest: p.source,
		CanonicalDigest: p.canonicalDigest, FirstCanonicalSequence: first, ThroughCanonicalSequence: through, ThroughCanonicalEventID: eventID,
		ProfileDigest: pd, PolicyDigest: string(policyDigest), BundleDigest: CompactionPolicyDigestV1(), CompatibilityDigest: p.compatibility,
		CalibrationDigest: p.profile.CalibrationDigest, GenerationRequestDigest: p.frozen.Digest(), NextRequestDigest: next.Digest(), CutDigest: retainedDigest(cutBody), SummaryDigest: retainedDigest(e.Summary), SummaryTokenUpperBound: e.SummaryTokenUpperBound}
	summary := retainedSummaryV1{Schema: "sessionless.retained-compaction-summary.v1", Core: core, Summary: bytes.Clone(e.Summary)}
	sb, err := encodeRetainedSummary(summary)
	if err != nil || len(sb) > MaxRetainedSummaryBytesV1 {
		return nil, ErrCanonical
	}
	plan := domain.DerivedContextPlanV1{Version: 1, Scope: core.Scope, Generation: core.Generation, OperationSequence: core.OperationSequence,
		ReservationDigest: rd, ObservationDigest: od, NormalizedResponseDigest: core.NormalizedResponseDigest, SourceViewDigest: p.source}
	plan.Summary, err = retainedRef(plan, "summary", sb)
	if err != nil {
		return nil, ErrCanonical
	}
	m := retainedManifestV1{Schema: "sessionless.compaction-manifest.v1", Core: core, Summary: plan.Summary, Cut: cut,
		Reservation: e.Reservation.Clone(), Observation: e.Observation.Clone(), GenerationFit: retainedFit(e.GenerationFit), NextFit: retainedFit(e.NextFit), Closure: 1}
	if e.Parent != nil {
		value := e.Parent.Plan()
		m.Parent = &value
		m.Closure = e.Parent.manifest.Closure + 1
	}
	mb, err := json.Marshal(m)
	if err != nil || len(mb) > MaxRetainedManifestBytesV1 {
		return nil, ErrCanonical
	}
	plan.Manifest, err = retainedRef(plan, "manifest", mb)
	if err != nil {
		return nil, ErrCanonical
	}
	v, err := DecodeRetainedV1(p.profile, p.policy, plan, sb, mb, e.Parent)
	if err != nil || v.ValidateForGeneration(p) != nil {
		return nil, ErrCanonical
	}
	return v, nil
}

func retainedRef(plan domain.DerivedContextPlanV1, role string, body []byte) (domain.BlobRef, error) {
	digest := retainedDigest(body)
	key, err := domain.DerivedContextObjectKeyV1(plan.Scope, plan.Generation, role, digest)
	return domain.BlobRef{TenantID: plan.Scope.TenantID, Key: key, Size: int64(len(body)), SHA256: digest}, err
}

// strictRetained requires our one canonical encoding. This additionally rejects
// duplicate names, case aliases, missing fields, invalid Unicode, alternate
// number spellings and trailing values instead of allowing json.Unmarshal's
// replacement/last-key-wins behavior. Input is bounded before allocation.
func strictRetained(body []byte, limit int, dst any) error {
	if len(body) == 0 || len(body) > limit {
		return ErrCanonical
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil {
		return ErrCanonical
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ErrCanonical
	}
	canonical, err := json.Marshal(dst)
	if err != nil || !bytes.Equal(canonical, body) {
		return ErrCanonical
	}
	return nil
}

// DecodeRetainedV1 verifies encoded DATA and its exact plan, not storage origin,
// issuer/current authority or semantic fidelity. Parent is an already decoded
// DATA dependency; the original reader must verify its head and entire closure.
func DecodeRetainedV1(profile domain.ConfiguredInferenceProfileV1, policy domain.AgentLoopPolicyV1, plan domain.DerivedContextPlanV1,
	summaryArtifact, manifest []byte, parent *RetainedV1) (*RetainedV1, error) {
	if profile.Validate() != nil || policy.Validate() != nil || plan.Validate() != nil ||
		len(summaryArtifact) > MaxRetainedSummaryBytesV1 || len(manifest) > MaxRetainedManifestBytesV1 ||
		!retainedBodyMatches(plan.Summary, summaryArtifact) || !retainedBodyMatches(plan.Manifest, manifest) {
		return nil, ErrCanonical
	}
	var s retainedSummaryV1
	var m retainedManifestV1
	if decodeRetainedSummary(summaryArtifact, &s) != nil || strictRetained(manifest, MaxRetainedManifestBytesV1, &m) != nil ||
		s.Schema != "sessionless.retained-compaction-summary.v1" || m.Schema != "sessionless.compaction-manifest.v1" || s.Core != m.Core || m.Summary != plan.Summary {
		return nil, ErrCanonical
	}
	c := m.Core
	r, o := m.Reservation, m.Observation
	op := r.Operation
	pd, _ := profile.Digest()
	policyDigest, _ := policy.Digest()
	rd, re := r.Digest()
	od, oe := o.Digest()
	if c.Version != 1 || c.Generation < 1 || c.Generation > 2 || c.Scope != plan.Scope || c.Generation != plan.Generation || c.OperationSequence != plan.OperationSequence ||
		c.ReservationDigest != plan.ReservationDigest || c.ObservationDigest != plan.ObservationDigest || c.NormalizedResponseDigest != plan.NormalizedResponseDigest || c.SourceViewDigest != plan.SourceViewDigest ||
		re != nil || oe != nil || rd != c.ReservationDigest || od != c.ObservationDigest || r.Validate() != nil || o.ValidateForReservation(r) != nil ||
		op.Scope != c.Scope || op.Sequence != c.OperationSequence || op.Kind != domain.AttemptOperationCompactionV1 || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(op.Debit) ||
		o.ResponseDigest != c.NormalizedResponseDigest || op.Scope.AgentLoopPolicyDigest != policyDigest || op.Scope.TenantID != profile.TenantID || op.Scope.OwnerUserID != profile.OwnerUserID || op.Scope.RunID != policy.RunID || op.Scope.AttemptID != policy.AttemptID ||
		c.ProfileDigest != pd || c.PolicyDigest != string(policyDigest) || !retainedPolicyMatches(profile, policy) || c.BundleDigest != CompactionPolicyDigestV1() || policy.InstructionVersion != InstructionVersionV1 || policy.CompactionPolicyDigest != c.BundleDigest ||
		c.CompatibilityDigest != sourceKey("compatibility", pd, c.BundleDigest) || c.CalibrationDigest != profile.CalibrationDigest || c.SummaryDigest != retainedDigest(s.Summary) ||
		!retainedValidDigest(c.SourceViewDigest) || !retainedValidDigest(c.CanonicalDigest) || !retainedValidDigest(c.NormalizedResponseDigest) ||
		len(s.Summary) == 0 || uint64(len(s.Summary)) > min(uint64(configuredinference.MaxCompactionSummaryBytesV1), op.SummaryBytes, policy.Limits.MaxSummaryBytes) ||
		c.SummaryTokenUpperBound <= uint64(len(s.Summary)) || c.SummaryTokenUpperBound > min(uint64(2048), op.Debit.OutputTokens, policy.Limits.MaxOutputTokensPerCall) ||
		r.AggregateDebit.ValidateForPolicy(policy) != nil {
		return nil, ErrCanonical
	}
	if !retainedFitValid(m.GenerationFit, profile, policy) || m.GenerationFit.RequestDigest != c.GenerationRequestDigest || m.GenerationFit.InputUpperBound != op.Debit.InputTokens || m.GenerationFit.OutputReserve != op.Debit.OutputTokens ||
		!retainedFitValid(m.NextFit, profile, policy) || m.NextFit.RequestDigest != c.NextRequestDigest {
		return nil, ErrCanonical
	}
	refs, err := validateRetainedCut(m.Cut, c.Scope)
	cutBody, _ := json.Marshal(m.Cut)
	first, through, eventID := canonicalThrough(m.Cut.Sources)
	if err != nil || c.CutDigest != retainedDigest(cutBody) || first != c.FirstCanonicalSequence || through != c.ThroughCanonicalSequence || eventID != c.ThroughCanonicalEventID {
		return nil, ErrCanonical
	}
	for _, row := range m.Cut.Sources {
		if row.Operation != nil && row.Operation.Reservation.Operation.Sequence >= op.Sequence {
			return nil, ErrCanonical
		}
	}
	if _, err := configuredinference.DecodeCompactionSummaryV1(s.Summary, refs); err != nil {
		return nil, ErrCanonical
	}
	if parent == nil {
		if m.Parent != nil || m.Closure != 1 || c.Generation != 1 {
			return nil, ErrCanonical
		}
	} else {
		if m.Parent == nil || *m.Parent != parent.plan || m.Closure != parent.manifest.Closure+1 || m.Closure > 2 || c.Generation != 2 || parent.manifest.Core.Generation != 1 ||
			parent.manifest.Core.Scope != c.Scope || parent.manifest.Core.CompatibilityDigest != c.CompatibilityDigest || parent.plan == plan {
			return nil, ErrCanonical
		}
		currentCovered := make(map[string]bool, len(m.Cut.Covered))
		for _, i := range m.Cut.Covered {
			currentCovered[m.Cut.Sources[i].SourceID] = true
		}
		for _, i := range parent.manifest.Cut.Covered {
			row := parent.manifest.Cut.Sources[i]
			if !currentCovered[row.SourceID] || !hasExactRetainedSource(m.Cut.Sources, row) {
				return nil, ErrCanonical
			}
		}
		if len(m.Cut.Covered) <= len(parent.manifest.Cut.Covered) {
			return nil, ErrCanonical
		}
	}
	return &RetainedV1{plan: plan, summaryBody: bytes.Clone(summaryArtifact), manifestBody: bytes.Clone(manifest), summary: s, manifest: m}, nil
}

func retainedBodyMatches(ref domain.BlobRef, body []byte) bool {
	return int64(len(body)) == ref.Size && retainedDigest(body) == ref.SHA256
}

// Bound both the count and all variable-size source metadata BEFORE encoding
// the complete cut. Each row is then separately bounded and charged; a large
// admitted history cannot cause an unbounded all-history Marshal allocation.
func boundedRetainedCut(c retainedCutV1) bool {
	if len(c.Sources) < 2 || len(c.Sources) > 4096 || len(c.Protected)+len(c.Covered)+len(c.Suffix) != len(c.Sources) {
		return false
	}
	remaining := MaxRetainedManifestBytesV1 - 256 - 24*len(c.Sources)
	if remaining <= 0 {
		return false
	}
	for _, row := range c.Sources {
		if !retainedValidDigest(row.SourceID) || (row.Canonical == nil) == (row.Operation == nil) {
			return false
		}
		if row.Canonical != nil {
			if len(row.Canonical.Payload.Key) > MaxRetainedManifestBytesV1 || row.Canonical.Validate() != nil {
				return false
			}
		} else if row.Operation.Reservation.Validate() != nil || row.Operation.Observation.ValidateForReservation(row.Operation.Reservation) != nil {
			return false
		}
		body, err := json.Marshal(row)
		if err != nil || len(body) > remaining {
			return false
		}
		remaining -= len(body)
	}
	return true
}

// RawMessage's standard MarshalJSON compacts its contents. Preserve the exact
// model summary bytes, including whitespace, instead: the fixed canonical
// envelope owns the entire final raw-value region. The summary decoder still
// lexically rejects duplicate keys, invalid Unicode and trailing values.
func encodeRetainedSummary(s retainedSummaryV1) ([]byte, error) {
	prefix, err := json.Marshal(struct {
		Schema string         `json:"schema"`
		Core   retainedCoreV1 `json:"core"`
	}{s.Schema, s.Core})
	if err != nil {
		return nil, err
	}
	if len(prefix)+len(s.Summary)+12 > MaxRetainedSummaryBytesV1 {
		return nil, ErrCanonical
	}
	out := append(prefix[:len(prefix)-1], []byte(`,"summary":`)...)
	out = append(out, s.Summary...)
	return append(out, '}'), nil
}

func decodeRetainedSummary(body []byte, s *retainedSummaryV1) error {
	if len(body) == 0 || len(body) > MaxRetainedSummaryBytesV1 {
		return ErrCanonical
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(s) != nil {
		return ErrCanonical
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return ErrCanonical
	}
	prefix, err := json.Marshal(struct {
		Schema string         `json:"schema"`
		Core   retainedCoreV1 `json:"core"`
	}{s.Schema, s.Core})
	if err != nil {
		return ErrCanonical
	}
	prefix = append(prefix[:len(prefix)-1], []byte(`,"summary":`)...)
	if len(body) <= len(prefix) || !bytes.HasPrefix(body, prefix) || body[len(body)-1] != '}' {
		return ErrCanonical
	}
	s.Summary = bytes.Clone(body[len(prefix) : len(body)-1])
	encoded, err := encodeRetainedSummary(*s)
	if err != nil || !bytes.Equal(encoded, body) {
		return ErrCanonical
	}
	return nil
}

func retainedPolicyMatches(p domain.ConfiguredInferenceProfileV1, policy domain.AgentLoopPolicyV1) bool {
	d, _ := p.Digest()
	return policy.TenantID == p.TenantID && policy.OwnerUserID == p.OwnerUserID && policy.Backend.BackendProfileDigest == d &&
		policy.ProfileRevision == p.ProfileRevision && policy.Resource == p.Resource && policy.ModelVendorID == p.ModelVendorID && policy.ModelID == p.ModelID &&
		policy.ProviderCatalogDigest == p.ProviderCatalogDigest && policy.ProviderRouteDigest == p.ProviderRouteDigest && policy.CapabilityEvidenceDigest == p.CapabilityEvidence.Digest &&
		policy.PrivacyPolicyDigest == p.PrivacyPolicyDigest && policy.EffectivePolicyDigest == p.EffectivePolicyDigest
}
func retainedFitValid(f retainedFitV1, p domain.ConfiguredInferenceProfileV1, policy domain.AgentLoopPolicyV1) bool {
	return retainedValidDigest(f.RequestDigest) && f.CalibrationDigest == p.CalibrationDigest && f.InputUpperBound > 0 && f.OutputReserve > 0 &&
		f.InputUpperBound <= min(p.Limits.MaxInputTokens, policy.Limits.MaxInputTokensPerCall) && f.OutputReserve <= min(p.Limits.MaxOutputTokens, policy.Limits.MaxOutputTokensPerCall) &&
		f.InputUpperBound <= math.MaxUint64-f.OutputReserve && f.TotalUpperBound == f.InputUpperBound+f.OutputReserve && f.TotalUpperBound <= p.ModelWindowTokens
}

func canonicalThrough(sources []retainedSourceV1) (uint64, uint64, domain.SessionEventID) {
	var first, through uint64
	var id domain.SessionEventID
	for _, row := range sources {
		if row.Canonical != nil {
			if first == 0 {
				first = row.Canonical.Sequence
			}
			through, id = row.Canonical.Sequence, row.Canonical.ID
		}
	}
	return first, through, id
}

func validateRetainedCut(c retainedCutV1, scope domain.AttemptOperationScopeV1) ([]configuredinference.CompactionReferenceV1, error) {
	if len(c.Sources) < 2 || len(c.Sources) > 4096 || c.Cut == 0 || uint64(c.Cut) >= uint64(len(c.Sources)) || len(c.Covered) == 0 || len(c.Suffix) != len(c.Sources)-int(c.Cut) ||
		c.Protected == nil || c.Covered == nil || c.Suffix == nil {
		return nil, ErrCanonical
	}
	seen := make(map[string]bool, len(c.Sources))
	seenEvents := make(map[domain.SessionEventID]bool, len(c.Sources))
	parts := make([]uint8, len(c.Sources))
	protectedIndices := make([]bool, len(c.Sources))
	for kind, indices := range [][]uint32{c.Protected, c.Covered, c.Suffix} {
		for j, i := range indices {
			if uint64(i) >= uint64(len(parts)) || (j > 0 && i <= indices[j-1]) || parts[i] != 0 || (kind < 2 && i >= c.Cut) || (kind == 2 && i != c.Cut+uint32(j)) {
				return nil, ErrCanonical
			}
			parts[i] = uint8(kind + 1)
		}
	}
	var lastCanonical, lastOperation uint64
	operationSeen := false
	refs := make([]configuredinference.CompactionReferenceV1, 0, len(c.Sources))
	for i, row := range c.Sources {
		if parts[i] == 0 || !retainedValidDigest(row.SourceID) || seen[row.SourceID] || (row.Canonical == nil) == (row.Operation == nil) {
			return nil, ErrCanonical
		}
		seen[row.SourceID] = true
		kind := configuredinference.CompactionCanonicalReferenceV1
		if row.Canonical != nil {
			e := *row.Canonical
			key, err := CanonicalSourceKeyV1(e)
			if err != nil || key != row.SourceID || seenEvents[e.ID] || e.TenantID != scope.TenantID || e.SessionID != scope.SessionID || operationSeen || (lastCanonical == 0 && e.Sequence != 1) || (lastCanonical != 0 && e.Sequence != lastCanonical+1) {
				return nil, ErrCanonical
			}
			seenEvents[e.ID] = true
			lastCanonical = e.Sequence
			// Protected correlation components may contain tools/assistants too;
			// every original user/system is nevertheless always protected.
			protected := e.Kind == domain.SessionEventUserMessage || e.Kind == domain.SessionEventSystemNotice
			protectedIndices[i] = protected
			if protected && parts[i] == 2 {
				return nil, ErrCanonical
			}
		} else {
			operationSeen = true
			kind = configuredinference.CompactionOperationReferenceV1
			r, o := row.Operation.Reservation, row.Operation.Observation
			key, err := OperationSourceKeyV1(r, o)
			if err != nil || key != row.SourceID || r.Operation.Scope != scope || r.Operation.Kind != domain.AttemptOperationModelV1 || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.ExceedsReservation(r.Operation.Debit) || r.Operation.Sequence <= lastOperation || r.Operation.Sequence >= scopeSequenceLimit || parts[i] == 1 {
				return nil, ErrCanonical
			}
			lastOperation = r.Operation.Sequence
		}
		if parts[i] < 3 || (row.Canonical != nil && (row.Canonical.Kind == domain.SessionEventUserMessage || row.Canonical.Kind == domain.SessionEventSystemNotice)) {
			refs = append(refs, configuredinference.CompactionReferenceV1{SourceID: row.SourceID, Kind: kind})
		}
	}
	if lastCanonical == 0 {
		return nil, ErrCanonical
	}
	// Recheck the same conservative per-producing-Run component boundaries
	// from identity metadata. Actual call/result payload correlation remains the
	// original program/source validator's responsibility, not invented here.
	spans := make(map[domain.RunID]UnitV1)
	for i, row := range c.Sources {
		if row.Canonical == nil {
			continue
		}
		e := row.Canonical
		if e.Kind != domain.SessionEventAssistantMessage && e.Kind != domain.SessionEventToolCall && e.Kind != domain.SessionEventToolResult {
			continue
		}
		if e.RunID == nil {
			return nil, ErrCanonical
		}
		span, ok := spans[*e.RunID]
		if !ok {
			span.Start = i
		}
		span.End = i + 1
		spans[*e.RunID] = span
	}
	intervals := make([]UnitV1, 0, len(spans))
	for _, span := range spans {
		intervals = append(intervals, span)
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].Start < intervals[j].Start })
	merged := make([]UnitV1, 0, len(intervals))
	for _, span := range intervals {
		if n := len(merged); n > 0 && span.Start < merged[n-1].End {
			merged[n-1].End = max(merged[n-1].End, span.End)
		} else {
			merged = append(merged, span)
		}
	}
	for _, span := range merged {
		if span.Start < int(c.Cut) && span.End > int(c.Cut) {
			return nil, ErrCanonical
		}
		protected := false
		for _, row := range c.Sources[span.Start:span.End] {
			if row.Canonical.Kind == domain.SessionEventUserMessage || row.Canonical.Kind == domain.SessionEventSystemNotice {
				protected = true
			}
		}
		if protected {
			for i := span.Start; i < span.End; i++ {
				protectedIndices[i] = true
				if span.End <= int(c.Cut) && parts[i] != 1 {
					return nil, ErrCanonical
				}
			}
		}
	}
	refs = refs[:0]
	for i, row := range c.Sources {
		if parts[i] < 3 || protectedIndices[i] {
			kind := configuredinference.CompactionCanonicalReferenceV1
			if row.Operation != nil {
				kind = configuredinference.CompactionOperationReferenceV1
			}
			refs = append(refs, configuredinference.CompactionReferenceV1{SourceID: row.SourceID, Kind: kind})
		}
	}
	return refs, nil
}

const scopeSequenceLimit = uint64(13)

func hasExactRetainedSource(rows []retainedSourceV1, wanted retainedSourceV1) bool {
	for _, row := range rows {
		if row.SourceID == wanted.SourceID {
			return reflect.DeepEqual(row, wanted)
		}
	}
	return false
}

// ValidateForGeneration binds a reread retained DATA object to the actual
// original program. The original publisher still owns source/authority and the
// independent concrete Source fit calculations, not this comparison.
func (v *RetainedV1) ValidateForGeneration(p *GenerationV1) error {
	if v == nil || p == nil || p.frozen == nil || v.manifest.Core.SourceViewDigest != p.source || v.manifest.Core.Generation != p.generation ||
		v.manifest.Core.CanonicalDigest != p.canonicalDigest || v.manifest.Core.CompatibilityDigest != p.compatibility ||
		!reflect.DeepEqual(v.manifest.Cut, p.retainedCut()) || !reflect.DeepEqual(v.manifest.Parent, p.parentPlan) || v.manifest.GenerationFit.RequestDigest != p.frozen.Digest() {
		return ErrCanonical
	}
	next, err := p.Rebuild(v.summary.Summary)
	if err != nil || next.Digest() != v.manifest.NextFit.RequestDigest {
		return ErrCanonical
	}
	for _, pair := range []struct {
		frozen *configuredinference.FrozenRequestV1
		fit    retainedFitV1
	}{{p.frozen, v.manifest.GenerationFit}, {next, v.manifest.NextFit}} {
		var request struct {
			MaxTokens uint64 `json:"max_tokens"`
		}
		body := pair.frozen.Body()
		if pair.fit.InputUpperBound <= uint64(len(body)) || json.Unmarshal(body, &request) != nil || request.MaxTokens != pair.fit.OutputReserve {
			clear(body)
			return ErrCanonical
		}
		clear(body)
		if pair.fit.InputUpperBound > p.limits.MaxInputTokens || pair.fit.OutputReserve > p.limits.MaxOutputTokens {
			return ErrCanonical
		}
	}
	generationBody := p.frozen.Body()
	defer clear(generationBody)
	if v.manifest.Reservation.Operation.RequestDigest != retainedDigest(generationBody) || v.manifest.Reservation.Operation.RequestBytes != uint64(len(generationBody)) {
		return ErrCanonical
	}
	return nil
}
