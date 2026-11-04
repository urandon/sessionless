package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/strictjson"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// The new arm is deliberately not projected into the old accounting/capsule
// type. Only the original private SEARCH failure owner may produce it.
type localFailureCapsuleV4 struct {
	Schema         string                                  `json:"schema"`
	Version        uint32                                  `json:"version"`
	Parent         domain.OwnedAttemptEffectParentV1       `json:"parent"`
	Source         domain.AttemptExecutionSourceV1         `json:"source"`
	ProfileDigest  string                                  `json:"profile_digest"`
	FrontierDigest string                                  `json:"frontier_digest"`
	Stop           string                                  `json:"stop"`
	Cleanup        string                                  `json:"cleanup"`
	CapturedAt     time.Time                               `json:"captured_at"`
	Accounting     ports.OwnedOperationFailureAccountingV4 `json:"accounting"`
	Operations     []localFailureOperationV1               `json:"operations"`
	Events         []localCapsuleEventV1                   `json:"events"`
	Bodies         []domain.BlobRef                        `json:"retained_bodies"`
	Derived        *domain.RunDerivedContextV1             `json:"derived_context,omitempty"`
	Consents       []domain.WebSearchConsentStateV1        `json:"consents"`
	ModelCatalogs  []localFailureModelCatalogV4            `json:"model_catalogs"`
}

// Raw array preserves omitted versus explicitly empty. Explicit null is never
// a catalog. These are original frozen request DATA, not a current tool surface.
type localFailureModelCatalogV4 struct {
	Sequence uint64          `json:"sequence"`
	Tools    json.RawMessage `json:"tools,omitempty"`
}

// The new V4 minimum catalog language has the one already supported public
// MCP slot. This does not widen or rename any legacy grant/profile language.
const localFailureV4MCPToolName = "search_openai_docs"

// Complete child-proxy DATA from the existing bounded SQL range. It is part of
// the transient history commitment, never a serving holder or current grant.
type localFailureProxyV4 struct {
	Original        domain.RegisteredProxyAttestationV2
	Current         domain.RegisteredProxyAttestationV2
	PolicyBody      []byte
	AttestationBody []byte
}

func failureV4ProxyData(p *ownedProxySnapshot) localFailureProxyV4 {
	return localFailureProxyV4{Original: p.original, Current: p.current, PolicyBody: p.policyBody, AttestationBody: p.attestationBody}
}

func readFailureV4ProxyRange(ctx context.Context, q *ownedAuthorityQuery, v localFailureHistoryV1) ([]localFailureProxyV4, error) {
	count := v.Accounting.Aggregate.Operations
	if count < 2 || count > 12 {
		return nil, q.budget.deny()
	}
	rows, err := q.QueryContext(ctx, localCompactionProxiesSQL, v.Parent.Scope.TenantID, v.Parent.Scope.AttemptID)
	if err != nil {
		return nil, q.budget.deny()
	}
	defer rows.Close()
	var out []localFailureProxyV4
	for rows.Next() {
		var sequence uint64
		var c ownedProxyColumns
		n, e := scanOwnedCurrentRow(rows, append([]any{&sequence}, c.destinations(2)...)...)
		// Charge both materialized columns and their owned decoded/copy data;
		// this is the same logical-call budget, never a renewed allowance.
		copies := uint64(len(c.originalRaw) + len(c.currentRaw) + len(c.policyRaw) + len(c.attestationRaw))
		if e != nil || q.budget.materialized(q.callback, n, copies) != nil || sequence != uint64(len(out)+2) || sequence > count {
			return nil, q.budget.deny()
		}
		p, e := c.decode(q, v.Parent, v.Admission.Source, v.Original, sequence)
		if e != nil || p == nil {
			return nil, q.budget.deny()
		}
		out = append(out, failureV4ProxyData(p))
	}
	if rows.Err() != nil || rows.Close() != nil || uint64(len(out))+1 != count {
		return nil, q.budget.deny()
	}
	return out, nil
}

type localFailureReadCapsule struct {
	legacy *localFailureCapsule
	search *localFailureCapsuleV4
}

func localFailureV4Digest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	defer clear(b)
	sum := sha256.Sum256(append([]byte("sessionless.search-failure.v4\x00"), b...))
	return hex.EncodeToString(sum[:]), nil
}

func decodeLocalFailureReadCapsule(body []byte) (localFailureReadCapsule, error) {
	var tag struct {
		Schema  string `json:"schema"`
		Version uint32 `json:"version"`
	}
	if _, err := strictjson.Parse(body, operationbody.LocalTerminalCapsuleBytesV1, 64, 16384); err != nil || json.Unmarshal(body, &tag) != nil {
		return localFailureReadCapsule{}, domain.ErrMembershipDenied
	}
	if tag.Schema != "sessionless.terminal-failure-custody.v4" {
		c, err := decodeLocalFailureCapsule(body)
		if err != nil {
			return localFailureReadCapsule{}, err
		}
		return localFailureReadCapsule{legacy: &c}, nil
	}
	var c localFailureCapsuleV4
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if tag.Version != 4 || d.Decode(&c) != nil {
		return localFailureReadCapsule{}, domain.ErrMembershipDenied
	}
	raw, err := encodeLocalFailureCapsuleV4(c, operationbody.LocalTerminalCapsuleBytesV1)
	defer clear(raw)
	if err != nil || !bytes.Equal(raw, body) || !localFailureV4BodyLanguage(c) {
		clearFailureOperations(c.Operations)
		return localFailureReadCapsule{}, domain.ErrMembershipDenied
	}
	return localFailureReadCapsule{search: &c}, nil
}

func encodeLocalFailureCapsuleV4(c localFailureCapsuleV4, limit uint64) ([]byte, error) {
	if limit == 0 || limit > operationbody.LocalTerminalCapsuleBytesV1 || !localFailureV4BodyLanguage(c) {
		return nil, domain.ErrMembershipDenied
	}
	sink := &localCapsuleSink{limit: limit, emit: true}
	if err := encodeLocalFailureCapsuleV4To(sink, c, -1, 0); err != nil {
		clear(sink.body)
		return nil, err
	}
	return sink.body, nil
}

// All metadata keeps the exact existing json.Marshal field order/escaping.
// Only a normalized body is inserted raw: its original bytes are committed by
// the observation, and the SEARCH codec intentionally does not HTML-escape.
// The actual encoder above and strict reader validate those raw identities.
// A non-emitting sink may count one private future scalar instead of "null";
// no hypothetical bytes/observation are returned, signed or persisted.
func encodeLocalFailureCapsuleV4To(sink *localCapsuleSink, c localFailureCapsuleV4, futureIndex int, futureBytes uint64) error {
	if sink == nil || sink.limit == 0 || sink.limit > operationbody.LocalTerminalCapsuleBytesV1 || c.Schema != "sessionless.terminal-failure-custody.v4" || c.Version != 4 || len(c.Operations) < 1 || len(c.Operations) > 12 || futureIndex < -1 || futureIndex >= len(c.Operations) || futureIndex == -1 && futureBytes != 0 || futureIndex >= 0 && (sink.emit || futureBytes < 4 || futureBytes > 64<<10) {
		return domain.ErrMembershipDenied
	}
	add, value := sink.add, sink.value
	add([]byte(`{"schema":`))
	value(c.Schema)
	add([]byte(`,"version":`))
	value(c.Version)
	add([]byte(`,"parent":`))
	value(c.Parent)
	add([]byte(`,"source":`))
	value(c.Source)
	add([]byte(`,"profile_digest":`))
	value(c.ProfileDigest)
	add([]byte(`,"frontier_digest":`))
	value(c.FrontierDigest)
	add([]byte(`,"stop":`))
	value(c.Stop)
	add([]byte(`,"cleanup":`))
	value(c.Cleanup)
	add([]byte(`,"captured_at":`))
	value(c.CapturedAt)
	add([]byte(`,"accounting":`))
	value(c.Accounting)
	add([]byte(`,"operations":[`))
	var normalized uint64
	for i, item := range c.Operations {
		n := uint64(len(item.Normalized))
		if i == futureIndex {
			if !bytes.Equal(item.Normalized, []byte("null")) {
				return domain.ErrMembershipDenied
			}
			n = futureBytes
		}
		var compact bytes.Buffer
		compactErr := json.Compact(&compact, item.Normalized)
		canonicalRaw := compactErr == nil && bytes.Equal(compact.Bytes(), item.Normalized)
		clear(compact.Bytes())
		if n == 0 || n > 64<<10 || n > (384<<10)-normalized || !canonicalRaw {
			return domain.ErrMembershipDenied
		}
		normalized += n
		if i > 0 {
			add([]byte(","))
		}
		add([]byte(`{"reservation":`))
		value(item.Reservation)
		add([]byte(`,"observation":`))
		value(item.Observation)
		add([]byte(`,"normalized":`))
		if i == futureIndex {
			sink.count(n)
		} else {
			add(item.Normalized)
		}
		add([]byte("}"))
	}
	add([]byte(`],"events":`))
	value(c.Events)
	add([]byte(`,"retained_bodies":`))
	value(c.Bodies)
	if c.Derived != nil {
		add([]byte(`,"derived_context":`))
		value(c.Derived)
	}
	add([]byte(`,"consents":`))
	value(c.Consents)
	add([]byte(`,"model_catalogs":`))
	value(c.ModelCatalogs)
	add([]byte("}"))
	if sink.failed {
		return ErrTerminalCapacity
	}
	return nil
}

func clearFailureOperations(ops []localFailureOperationV1) {
	for _, op := range ops {
		clear(op.Normalized)
	}
}
func (c localFailureReadCapsule) release() {
	if c.legacy != nil {
		clearLocalFailureNormalized(*c.legacy)
	}
	if c.search != nil {
		clearFailureOperations(c.search.Operations)
	}
}
func (c localFailureReadCapsule) operations() []localFailureOperationV1 {
	if c.search != nil {
		return c.search.Operations
	}
	if c.legacy != nil {
		return c.legacy.Operations
	}
	return nil
}
func (c localFailureReadCapsule) bodies() []domain.BlobRef {
	if c.search != nil {
		return c.search.Bodies
	}
	if c.legacy != nil {
		return c.legacy.Bodies
	}
	return nil
}
func (c localFailureReadCapsule) profile() string {
	if c.search != nil {
		return c.search.ProfileDigest
	}
	if c.legacy != nil {
		return c.legacy.ProfileDigest
	}
	return ""
}

func localFailureV4BodyLanguage(c localFailureCapsuleV4) bool {
	if c.Schema != "sessionless.terminal-failure-custody.v4" || c.Version != 4 || c.Accounting.Version != 4 || !domain.SearchFailureStopV4(c.Stop) || c.Cleanup != "joined_retained" || len(c.Operations) < 1 || len(c.Operations) > 12 || c.Consents == nil || len(c.Consents) < 1 || len(c.Consents) > 2 || c.ModelCatalogs == nil || c.Events == nil || c.Bodies == nil {
		return false
	}
	for i, op := range c.Operations {
		if op.Observation.Outcome == domain.AttemptOperationCompletedV1 {
			if !localFailureBytes(op.Normalized, op.Observation.ResponseBytes, op.Observation.ResponseDigest) {
				return false
			}
		} else if i != len(c.Operations)-1 || !bytes.Equal(op.Normalized, []byte("null")) || op.Observation.ResponseRef != "" || op.Observation.ResponseDigest != "" || op.Observation.ResponseBytes != 0 {
			return false
		}
	}
	return true
}

func localFailureV4Frontier(c localFailureCapsuleV4) (string, error) {
	pd, err := c.Parent.Digest()
	if err != nil {
		return "", err
	}
	ad, err := c.Accounting.Digest()
	if err != nil {
		return "", err
	}
	rows := make([]localFailureOperationV1, len(c.Operations))
	for i, op := range c.Operations {
		rows[i] = localFailureOperationV1{Reservation: op.Reservation, Observation: op.Observation}
	}
	return localFailureV4Digest(struct {
		Parent     string
		Accounting string
		Operations []localFailureOperationV1
		Catalogs   []localFailureModelCatalogV4
	}{pd, ad, rows, c.ModelCatalogs})
}

func localFailureV4Metadata(plan domain.TerminalCustodyPlanV1, c localFailureCapsuleV4, retired string) (domain.RunTerminalFailureV1, error) {
	ad, err := c.Accounting.Digest()
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	bd, err := operationbody.RetainedBodiesDigestV1(c.Bodies)
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	pd, err := c.Parent.Digest()
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	ed, err := localFailureV4Digest(c.Events)
	if err != nil {
		return domain.RunTerminalFailureV1{}, err
	}
	return domain.RunTerminalFailureV1{Version: 1, Scope: c.Accounting.Scope, ParentDigest: pd, Plan: plan, CapsuleSchema: c.Schema, AccountingDigest: ad, EventsDigest: ed, RetainedBodiesDigest: bd, RetiredAllocationDigest: retired, CapturedAt: c.CapturedAt, Stop: c.Stop, Cleanup: c.Cleanup}, nil
}

func localFailureV4ConsentFrontiers(c localFailureCapsuleV4) ([]uint64, error) {
	pd, _ := c.Parent.Digest()
	v := ownedConsentStates{Version: 1, ParentDigest: pd, States: c.Consents}
	if v.validate(c.Parent, c.CapturedAt) != nil {
		return nil, domain.ErrMembershipDenied
	}
	frontiers := make([]uint64, len(c.Consents))
	var reserved uint64
	for i, state := range c.Consents {
		r := state.Request
		ref, err := domain.ParseAttemptOperationConsentArgumentsRefV1(c.Accounting.Scope, r.Arguments.Key, uint64(r.Arguments.Size))
		if err != nil || ref != r.Arguments || state.Status == domain.WebSearchConsentPendingV1 {
			return nil, domain.ErrMembershipDenied
		}
		for _, op := range c.Operations {
			if op.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || op.Observation.ResponseDigest != r.ModelResponseDigest {
				continue
			}
			m, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, op.Reservation, op.Observation, op.Normalized)
			matches := err == nil && m.Call != nil && m.Stop == configuredinference.StopToolV1 && m.Call.ID == r.CallID && m.Call.Name == "web_search" && rawLocalFailureHash(m.Call.Arguments) == r.ArgumentsDigest && r.ValidateArguments(m.Call.Arguments) == nil && !r.CreatedAt.Before(op.Observation.ObservedAt)
			if m.Call != nil {
				clear(m.Call.Arguments)
			}
			if !matches || frontiers[i] != 0 {
				return nil, domain.ErrMembershipDenied
			}
			frontiers[i] = op.Reservation.Operation.Sequence
		}
		if frontiers[i] == 0 || i > 0 && frontiers[i] <= frontiers[i-1] {
			return nil, domain.ErrMembershipDenied
		}
		cost := c.Parent.Authority.AdmissionCostCeiling.WebSearchCost
		if cost == nil || r.GrantDigest != cost.ScopedGrantDigest || r.RevocationGeneration != cost.RevocationGeneration {
			return nil, domain.ErrMembershipDenied
		}
		reserved += ownedConsentMetadataReservationBytes + uint64(r.Arguments.Size)
	}
	digest, err := localFailureV4Digest(c.Consents)
	if err != nil || digest != c.Accounting.ConsentStatesDigest || reserved != c.Accounting.ConsentReservedBytes {
		return nil, domain.ErrMembershipDenied
	}
	return frontiers, nil
}

// Reconstruct the existing single ledger, including intervening and final
// unreserved BODY-only consent charges. Its actual UpdatedAt is authenticated
// by the retained SQL row digest; it is never replaced with a reservation time.
func validateFailureV4Frontiers(c localFailureCapsuleV4, actual ownedAttemptAccounting, frontiers []uint64) error {
	if actual.validate(c.Parent, c.Source, c.CapturedAt) != nil || len(frontiers) != len(c.Consents) {
		return domain.ErrMembershipDenied
	}
	_, digest, err := accountingBytes(actual)
	if err != nil || digest != c.Accounting.AccountingDigest || actual.Aggregate != c.Accounting.AggregateDebit || actual.Amounts != c.Accounting.ComponentAmounts {
		return domain.ErrMembershipDenied
	}
	aggregate := domain.AttemptOperationDebitV1{BodyBytes: ownedSetupBodyBytes(c.Parent)}
	amounts := [4]uint64{0, *c.Source.Spending[1].MaxAmountMicrounits, 0, 0}
	var previous time.Time = actual.SetupReservedAt
	for i, item := range c.Operations {
		r := item.Reservation
		for j, frontier := range frontiers {
			if frontier == uint64(i) {
				aggregate, err = aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: ownedConsentMetadataReservationBytes + uint64(c.Consents[j].Request.Arguments.Size)})
				if err != nil || c.Consents[j].Request.CreatedAt.After(r.ReservedAt) {
					return domain.ErrMembershipDenied
				}
			}
		}
		if r.ReservedAt.Before(previous) {
			return domain.ErrMembershipDenied
		}
		aggregate, err = aggregate.Add(r.Operation.Debit)
		if err != nil {
			return domain.ErrMembershipDenied
		}
		index := 0
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			index = 2
		} else if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			index = 3
		}
		if amounts[index] > ^uint64(0)-r.Operation.Debit.AmountMicrounits {
			return domain.ErrMembershipDenied
		}
		amounts[index] += r.Operation.Debit.AmountMicrounits
		if r.AggregateDebit != aggregate || r.ComponentAmounts != amounts {
			return domain.ErrMembershipDenied
		}
		previous = item.Observation.ObservedAt
	}
	last := c.Operations[len(c.Operations)-1].Reservation.ReservedAt
	for j, frontier := range frontiers {
		if frontier == uint64(len(c.Operations)) {
			aggregate, err = aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: ownedConsentMetadataReservationBytes + uint64(c.Consents[j].Request.Arguments.Size)})
			if err != nil {
				return domain.ErrMembershipDenied
			}
		}
		if c.Consents[j].Request.CreatedAt.After(last) {
			last = c.Consents[j].Request.CreatedAt
		}
	}
	if aggregate != actual.Aggregate || amounts != actual.Amounts || actual.UpdatedAt.Before(last) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func localFailureV4Bodies(c localFailureCapsuleV4) ([]domain.BlobRef, error) {
	var out []domain.BlobRef
	for _, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		ref, err := operationbody.RetainedOperationBodyRefV1(r.Operation.Scope, r.Operation.RequestRef, r.Operation.RequestBytes, r.Operation.RequestDigest)
		if err != nil {
			return nil, domain.ErrMembershipDenied
		}
		out = append(out, ref)
		if o.Outcome == domain.AttemptOperationCompletedV1 {
			ref, err = operationbody.RetainedOperationBodyRefV1(r.Operation.Scope, o.ResponseRef, o.ResponseBytes, o.ResponseDigest)
			if err != nil {
				return nil, domain.ErrMembershipDenied
			}
			out = append(out, ref)
		}
	}
	for _, state := range c.Consents {
		out = append(out, state.Request.Arguments)
	}
	// Content-addressed ordinary bodies may share a key. Consent states cannot.
	unique := out[:0]
	for _, ref := range out {
		found := false
		for _, old := range unique {
			if old.Key == ref.Key {
				if old != ref {
					return nil, domain.ErrMembershipDenied
				}
				found = true
			}
		}
		if !found {
			unique = append(unique, ref)
		}
	}
	if len(unique) > 24 {
		return nil, domain.ErrMembershipDenied
	}
	var total uint64
	for _, ref := range unique {
		if ref.Validate() != nil || ref.Size <= 0 || uint64(ref.Size) > (1<<20)-total {
			return nil, domain.ErrMembershipDenied
		}
		total += uint64(ref.Size)
	}
	return unique, nil
}

func localFailureV4Approved(c localFailureCapsuleV4, previous, item localFailureOperationV1, arguments []byte) bool {
	op := item.Reservation.Operation
	if op.SearchWire == nil || op.SearchWire.ArgumentsBytes != uint64(len(arguments)) {
		return false
	}
	for _, state := range c.Consents {
		r := state.Request
		rd, _ := r.Digest()
		if string(rd) != op.ConsentRequestDigest {
			continue
		}
		if state.Decision == nil || state.Decision.Choice != domain.WebSearchConsentApproveV1 || r.Arguments.Size != int64(len(arguments)) {
			return false
		}
		dd, err := state.Decision.Digest()
		return err == nil && string(dd) == op.ConsentDecisionDigest && string(r.ID) == op.ConsentDecisionRef && r.CallID == op.CallID && r.ArgumentsDigest == op.ArgumentsDigest && r.QueryDigest == op.QueryDigest && r.GrantDigest == op.ToolGrantDigest && r.ModelResponseDigest == previous.Observation.ResponseDigest && !state.Decision.ObservedAt.After(item.Reservation.ReservedAt) && item.Reservation.ReservedAt.Before(r.ExpiresAt) && !op.Deadline.After(r.ExpiresAt) && (state.Status == domain.WebSearchConsentApprovedV1 || !state.UpdatedAt.Before(item.Reservation.ReservedAt))
	}
	return false
}

func localFailureV4Catalogs(c localFailureCapsuleV4) error {
	index := 0
	var schemaLimits domain.ConfiguredInferenceLimitsV1
	for _, item := range c.Operations {
		if item.Reservation.Operation.Kind == domain.AttemptOperationModelV1 && item.Observation.Outcome == domain.AttemptOperationCompletedV1 {
			var envelope struct {
				Limits domain.ConfiguredInferenceLimitsV1 `json:"limits"`
			}
			if json.Unmarshal(item.Normalized, &envelope) != nil || envelope.Limits.Validate() != nil {
				return domain.ErrMembershipDenied
			}
			schemaLimits = envelope.Limits
			break
		}
	}
	for _, item := range c.Operations {
		if item.Reservation.Operation.Kind != domain.AttemptOperationModelV1 {
			continue
		}
		if index >= len(c.ModelCatalogs) || c.ModelCatalogs[index].Sequence != item.Reservation.Operation.Sequence {
			return domain.ErrMembershipDenied
		}
		raw := c.ModelCatalogs[index].Tools
		index++
		if len(raw) == 0 {
			if item.Observation.Outcome == domain.AttemptOperationCompletedV1 {
				m, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, item.Reservation, item.Observation, item.Normalized)
				if m.Call != nil {
					clear(m.Call.Arguments)
					return domain.ErrMembershipDenied
				}
				if err != nil {
					return domain.ErrMembershipDenied
				}
			}
			continue
		}
		if len(raw) > 64<<10 || bytes.Equal(raw, []byte("null")) {
			return domain.ErrMembershipDenied
		}
		var tools []struct {
			Type     string `json:"type"`
			Function struct {
				Name        string          `json:"name"`
				Description string          `json:"description"`
				Parameters  json.RawMessage `json:"parameters"`
			} `json:"function"`
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if d.Decode(&tools) != nil || tools == nil || len(tools) > 2 {
			return domain.ErrMembershipDenied
		}
		canonical, err := json.Marshal(tools)
		if err != nil || !bytes.Equal(canonical, raw) {
			return domain.ErrMembershipDenied
		}
		seen := map[string]bool{}
		search := websearch.ModelToolV1()
		for _, tool := range tools {
			if tool.Type != "function" || domain.ValidateOpaqueID("catalog.name", tool.Function.Name) != nil || seen[tool.Function.Name] || len(tool.Function.Description) > 4096 || configuredinference.ValidateToolSchemaV1(tool.Function.Parameters, schemaLimits) != nil {
				return domain.ErrMembershipDenied
			}
			if tool.Function.Name != search.Name && tool.Function.Name != localFailureV4MCPToolName {
				return domain.ErrMembershipDenied
			}
			if tool.Function.Name == search.Name && (tool.Function.Description != search.Description || !bytes.Equal(tool.Function.Parameters, search.Parameters)) {
				return domain.ErrMembershipDenied
			}
			seen[tool.Function.Name] = true
		}
		if item.Observation.Outcome == domain.AttemptOperationCompletedV1 {
			m, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, item.Reservation, item.Observation, item.Normalized)
			if err != nil {
				return domain.ErrMembershipDenied
			}
			if m.Call != nil {
				found := seen[m.Call.Name]
				clear(m.Call.Arguments)
				if !found {
					return domain.ErrMembershipDenied
				}
			}
		}
	}
	if index != len(c.ModelCatalogs) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func validateFailureV4ProxyData(v localFailureHistoryV1, c localFailureCapsuleV4) error {
	if len(v.ProxiesV4)+1 != len(c.Operations) {
		return domain.ErrMembershipDenied
	}
	for i, data := range v.ProxiesV4 {
		r := c.Operations[i+1].Reservation
		p := data.Original
		d, err := p.Digest()
		if err != nil || !reflect.DeepEqual(p, data.Current) || p.ValidateForAllocation(v.Original, c.Source) != nil || p.OperationSequence != uint64(i+2) || p.OperationDigest != r.OperationDigest || p.ServiceKind != ownedOperationProxyService(r.Operation.Kind) || d != r.RegisteredProxyDigest {
			return domain.ErrMembershipDenied
		}
		var rebuilt domain.RegisteredProxyAttestationV2
		if p.TransportKind == domain.RegisteredProxyTransportLocalSearchHTTPSV1 {
			rebuilt, err = readLocalSearchProxyDataV1(p, c.Parent, v.Original, c.Source, data.PolicyBody, data.AttestationBody)
		} else {
			b, e := (ports.OwnedProxyPublicationV2{TransportKind: p.TransportKind, ServiceKind: p.ServiceKind, ObservedAt: p.Window.ObservedAt, PolicyBody: data.PolicyBody, AttestationBody: data.AttestationBody}).Bodies()
			if e != nil {
				return domain.ErrMembershipDenied
			}
			rebuilt, err = buildOwnedProxyRecord(b, v.Original, c.Source, p.ServiceKind, data.PolicyBody, data.AttestationBody, p.Window, p.OperationSequence, p.OperationDigest)
		}
		actual, e := rebuilt.Digest()
		if err != nil || e != nil || actual != d {
			return domain.ErrMembershipDenied
		}
		if r.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			policy, _, e := ports.DecodeLocalSearchHTTPSProxyDataV1(data.PolicyBody, data.AttestationBody)
			matched := false
			for _, state := range c.Consents {
				rd, _ := state.Request.Digest()
				if string(rd) == r.Operation.ConsentRequestDigest {
					request := state.Request
					gd, ge := policy.Grant.Digest()
					matched = e == nil && ge == nil && request.GrantDigest == gd && request.ConsentRevision == policy.Grant.ConsentRevision && request.RevocationGeneration == policy.Grant.RevocationGeneration && !request.CreatedAt.Before(policy.Grant.IssuedAt) && !request.ExpiresAt.After(policy.Grant.ExpiresAt)
				}
			}
			if !matched || validateRetainedSearchProxyOperationV4(&ownedProxySnapshot{original: p, policyBody: data.PolicyBody, attestationBody: data.AttestationBody}, r) != nil {
				return domain.ErrMembershipDenied
			}
		}
	}
	return nil
}

// Read every actual retained body once through the existing hash/length/budget
// reader. Original MODEL requests are owned in joined_retained failures, unlike
// erased successful requests. Catalogs here are immutable DATA, not send caps.
func validateFailureV4RetainedBodies(ctx context.Context, c localFailureCapsuleV4, proxies []localFailureProxyV4, read func(context.Context, domain.BlobRef, uint64) ([]byte, error)) error {
	deny := func() error { return domain.ErrMembershipDenied }
	bodies := make(map[string][]byte, len(c.Bodies))
	defer func() {
		for _, raw := range bodies {
			clear(raw)
		}
	}()
	for _, ref := range c.Bodies {
		raw, err := read(ctx, ref, 1<<20)
		if err != nil || !localRetainedBytes(ref, raw) {
			clear(raw)
			return deny()
		}
		bodies[ref.Key] = raw
	}
	requestBody := func(op domain.AttemptOperationV1) []byte {
		ref, err := operationbody.RetainedOperationBodyRefV1(op.Scope, op.RequestRef, op.RequestBytes, op.RequestDigest)
		if err != nil {
			return nil
		}
		return bodies[ref.Key]
	}
	catalog := 0
	for i, item := range c.Operations {
		op := item.Reservation.Operation
		if op.Kind == domain.AttemptOperationModelV1 {
			raw := requestBody(op)
			if _, err := strictjson.Parse(raw, 1<<20, 64, 16384); err != nil || catalog >= len(c.ModelCatalogs) {
				return deny()
			}
			var request map[string]json.RawMessage
			if json.Unmarshal(raw, &request) != nil || request == nil || c.ModelCatalogs[catalog].Sequence != op.Sequence || !bytes.Equal(request["tools"], c.ModelCatalogs[catalog].Tools) {
				return deny()
			}
			for key := range request {
				if key != "tools" && strings.EqualFold(key, "tools") {
					return deny()
				}
			}
			catalog++
			continue
		}
		if i == 0 || i-1 >= len(proxies) {
			return deny()
		}
		if op.Kind == domain.AttemptOperationWebSearchV1 {
			p, _, err := ports.DecodeLocalSearchHTTPSProxyDataV1(proxies[i-1].PolicyBody, proxies[i-1].AttestationBody)
			if err != nil || c.Parent.Authority.AdmissionCostCeiling.WebSearchCost == nil {
				return deny()
			}
			var consent *domain.WebSearchConsentRequestV1
			for j := range c.Consents {
				d, _ := c.Consents[j].Request.Digest()
				if string(d) == op.ConsentRequestDigest {
					consent = &c.Consents[j].Request
				}
			}
			if consent == nil {
				return deny()
			}
			binding := websearch.BindingV1{Scope: op.Scope, Sequence: op.Sequence, CallID: op.CallID, ModelResponseDigest: op.ModelResponseDigest, InitiatingUserID: consent.InitiatingUserID, ParentCostCeilingDigestV2: op.AdmissionCostCeilingDigest, ParentPriceRevision: op.PriceRevision}
			frozen, err := websearch.CompileV2(p.Grant, *c.Parent.Authority.AdmissionCostCeiling.WebSearchCost, binding, bodies[consent.Arguments.Key], item.Reservation.ReservedAt)
			if err != nil || frozen.ValidateOperation(op) != nil || frozen.ValidateConsentRequest(*consent) != nil {
				return deny()
			}
			wire := frozen.Body()
			valid := bytes.Equal(wire, requestBody(op))
			clear(wire)
			if !valid {
				return deny()
			}
		} else if op.Kind == domain.AttemptOperationMCPToolV1 {
			// The normalized result already authenticates the complete original
			// grant. Optional descriptor metadata is not inverted from a hash.
			if _, err := mcpadmission.DecodeSealedToolResultDataV1(item.Reservation, item.Observation, item.Normalized); err != nil {
				return deny()
			}
			var envelope struct {
				Grant json.RawMessage `json:"grant"`
			}
			if json.Unmarshal(item.Normalized, &envelope) != nil {
				return deny()
			}
			grant, err := mcpadmission.ParseMCPToolGrantV1(envelope.Grant)
			if err != nil {
				return deny()
			}
			descriptor, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
			proxy := proxies[i-1].Original
			gd, ge := grant.Digest()
			if err != nil || ge != nil || proxy.GrantDigest != gd || proxy.LocalPublicMCPHTTPS == nil || proxy.LocalPublicMCPHTTPS.DescriptorDigest != descriptor.Digest() {
				return deny()
			}
			var request struct {
				Tools []struct {
					Function struct {
						Name        string          `json:"name"`
						Description string          `json:"description"`
						Parameters  json.RawMessage `json:"parameters"`
					} `json:"function"`
				} `json:"tools"`
			}
			if json.Unmarshal(requestBody(c.Operations[i-1].Reservation.Operation), &request) != nil {
				return deny()
			}
			matched := false
			for _, tool := range request.Tools {
				if tool.Function.Name == descriptor.Name() {
					matched = tool.Function.Description == grant.ModelDescription && bytes.Equal(tool.Function.Parameters, descriptor.InputSchema())
				}
			}
			if !matched {
				return deny()
			}
		}
	}
	if catalog != len(c.ModelCatalogs) {
		return deny()
	}
	return nil
}

func localFailureV4Plan(run domain.Run, c localFailureCapsuleV4, limits domain.ProductLimits) (*canonicaloutput.FailurePlanV1, error) {
	deny := func() (*canonicaloutput.FailurePlanV1, error) { return nil, domain.ErrMembershipDenied }
	if !localFailureV4BodyLanguage(c) || c.Parent.Validate() != nil || c.Accounting.ValidateForStop(c.Parent, c.Source, c.Stop) != nil || len(c.Accounting.Operations) != len(c.Operations) || c.ProfileDigest != c.Parent.Authority.HarnessBinding.Backend.BackendProfileDigest || c.CapturedAt != c.CapturedAt.UTC().Truncate(time.Microsecond) || c.CapturedAt.IsZero() || c.Parent.Scope.TenantID != run.TenantID || c.Parent.Scope.SessionID != run.SessionID || c.Parent.Scope.RunID != run.ID || !domain.EqualRunDerivedContextV1(c.Derived, run.DerivedContext) || localFailureV4Catalogs(c) != nil {
		return deny()
	}
	fd, err := localFailureV4Frontier(c)
	if err != nil || fd != c.FrontierDigest {
		return deny()
	}
	frontiers, err := localFailureV4ConsentFrontiers(c)
	if err != nil {
		return deny()
	}
	for j, frontier := range frontiers {
		if frontier < uint64(len(c.Operations)) {
			next := c.Operations[frontier].Reservation.Operation
			if next.Kind != domain.AttemptOperationWebSearchV1 || next.ConsentDecisionRef != string(c.Consents[j].Request.ID) {
				return deny()
			}
		} else if c.Stop != domain.SearchConsentStoppedV4 {
			return deny()
		}
	}
	bodies, err := localFailureV4Bodies(c)
	if err != nil || !sameFailureBodies(bodies, c.Bodies) {
		return deny()
	}
	var events []ports.ExecutionToolEvent
	defer func() {
		for _, event := range events {
			clear(event.Payload)
		}
	}()
	var normalizedBytes uint64
	knownSearch := false
	for i, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		row, err := ports.BuildOwnedOperationFailureRowV4(r, o, c.Parent, c.Source)
		if err != nil || !reflect.DeepEqual(row, c.Accounting.Operations[i]) || r.Operation.Sequence != uint64(i+1) || r.Operation.Scope != c.Accounting.Scope || o.ObservedAt.After(c.CapturedAt) || len(item.Normalized) > 64<<10 || uint64(len(item.Normalized)) > (384<<10)-normalizedBytes || i > 0 && r.Operation.ValidatePredecessor(c.Operations[i-1].Reservation, c.Operations[i-1].Observation) != nil {
			return deny()
		}
		normalizedBytes += uint64(len(item.Normalized))
		if r.Operation.Kind == domain.AttemptOperationModelV1 || r.Operation.Kind == domain.AttemptOperationCompactionV1 {
			if o.Outcome != domain.AttemptOperationCompletedV1 {
				if i != len(c.Operations)-1 || c.Stop != domain.ModelAfterSearchFailureV4 || !knownSearch {
					return deny()
				}
				continue
			}
			m, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, r, o, item.Normalized)
			if err != nil {
				return deny()
			}
			if m.Call != nil {
				valid := i+1 < len(c.Operations) && (c.Operations[i+1].Reservation.Operation.Kind == domain.AttemptOperationMCPToolV1 || c.Operations[i+1].Reservation.Operation.Kind == domain.AttemptOperationWebSearchV1) || i == len(c.Operations)-1 && c.Stop == domain.SearchConsentStoppedV4 && m.Call.Name == "web_search"
				clear(m.Call.Arguments)
				if !valid {
					return deny()
				}
			} else if m.Stop != configuredinference.StopFinalV1 {
				return deny()
			}
			continue
		}
		if i == 0 {
			return deny()
		}
		previous := c.Operations[i-1]
		model, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, previous.Reservation, previous.Observation, previous.Normalized)
		if err != nil || model.Call == nil {
			return deny()
		}
		call := model.Call
		if call.ID != r.Operation.CallID || rawLocalFailureHash(call.Arguments) != r.Operation.ArgumentsDigest {
			clear(call.Arguments)
			return deny()
		}
		if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
			tool, err := mcpadmission.DecodeSealedToolResultDataV1(r, o, item.Normalized)
			if err != nil || tool.IsError() || tool.Name() != call.Name || tool.ModelResponseDigest() != previous.Observation.ResponseDigest {
				clear(call.Arguments)
				return deny()
			}
			pair, err := localMCPCanonicalToolEvents(call.ID, call.Name, call.Arguments, tool)
			clear(call.Arguments)
			if err != nil {
				return deny()
			}
			events = append(events, pair...)
			continue
		}
		if call.Name != "web_search" || !localFailureV4Approved(c, previous, item, call.Arguments) {
			clear(call.Arguments)
			return deny()
		}
		clear(call.Arguments)
		callBody, err := json.Marshal(struct {
			CallID          string `json:"call_id"`
			Name            string `json:"name"`
			ArgumentsDigest string `json:"arguments_digest"`
			QueryDigest     string `json:"query_digest"`
		}{call.ID, call.Name, r.Operation.ArgumentsDigest, r.Operation.QueryDigest})
		if err != nil {
			return deny()
		}
		var result []byte
		if o.Outcome == domain.AttemptOperationCompletedV1 {
			typed, err := domain.DecodeWebSearchResultV1(item.Normalized)
			valid := err == nil && typed.ValidateForOperation(r.Operation) == nil && typed.ProfileRevision == c.Source.Spending[3].ProfileRevision && !typed.RetrievedAt.Before(r.ReservedAt) && !typed.RetrievedAt.After(o.ObservedAt)
			if i == len(c.Operations)-1 && c.Stop == domain.SearchServiceErrorV4 {
				valid = valid && typed.Outcome == "known_service_error"
			} else {
				valid = valid && (typed.Outcome == "known_results" || typed.Outcome == "known_empty")
			}
			if !valid {
				clear(callBody)
				return deny()
			}
			knownSearch = true
			result, err = json.Marshal(struct {
				Schema    string                          `json:"schema"`
				Outcome   string                          `json:"outcome"`
				Operation domain.AttemptOperationDigestV1 `json:"operation_digest"`
				Sources   []domain.WebSearchSourceV1      `json:"sources"`
				Usage     domain.WebSearchUsageV1         `json:"usage"`
				Warnings  []domain.WebSearchWarningV1     `json:"warnings"`
			}{"sessionless.search-result.v4", typed.Outcome, typed.OperationDigest, typed.Sources, typed.Usage, typed.Warnings})
		} else {
			result, err = canonicaloutput.EncodeSearchOperationStatusV4(r, o)
		}
		if err != nil {
			clear(callBody)
			return deny()
		}
		events = append(events, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: call.ID, ToolName: call.Name, Payload: callBody}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: call.ID, ToolName: call.Name, Payload: result})
	}
	if c.Stop == domain.SearchConsentStoppedV4 {
		found := false
		for i, frontier := range frontiers {
			if frontier == uint64(len(c.Operations)) {
				status := c.Consents[i].Status
				found = status != domain.WebSearchConsentApprovedV1 && status != domain.WebSearchConsentPendingV1
			}
		}
		if !found {
			return deny()
		}
	}
	last := c.Operations[len(c.Operations)-1]
	return canonicaloutput.EncodeSearchFailureV4(run.ID, limits, events, c.Stop, last.Reservation, last.Observation)
}

func validateFailureReadHistoryData(v localFailureHistoryV1, c localFailureReadCapsule, records []sessioncontext.EventPayload) error {
	if c.legacy != nil && c.search == nil && v.ConsentsV4 == nil && v.ProxiesV4 == nil {
		return validateFailureHistoryData(v, *c.legacy, records)
	}
	if c.search == nil || c.legacy != nil {
		return domain.ErrMembershipDenied
	}
	return validateFailureHistoryDataV4(v, *c.search, records)
}

func validateFailureHistoryDataV4(v localFailureHistoryV1, c localFailureCapsuleV4, records []sessioncontext.EventPayload) error {
	deny := func() error { return domain.ErrMembershipDenied }
	run := v.Run
	if run.Validate() != nil || run.TerminalFailure == nil || run.TerminalCustody == nil || v.ConsentsV4 == nil || !reflect.DeepEqual(c.Parent, v.Parent) || !reflect.DeepEqual(c.Source, v.Admission.Source) || !reflect.DeepEqual(c.Consents, v.ConsentsV4.States) || v.ConsentsV4.validate(v.Parent, c.CapturedAt) != nil {
		return deny()
	}
	if validateFailureV4ProxyData(v, c) != nil {
		return deny()
	}
	metadata, err := localFailureV4Metadata(run.TerminalCustody.Plan, c, run.TerminalFailure.RetiredAllocationDigest)
	if err != nil || metadata != *run.TerminalFailure {
		return deny()
	}
	frontiers, err := localFailureV4ConsentFrontiers(c)
	if err != nil || validateFailureV4Frontiers(c, v.Accounting, frontiers) != nil {
		return deny()
	}
	p, err := localFailureV4Plan(run, c, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: domain.MaxTerminalCapsuleBytesV1})
	if err != nil {
		return deny()
	}
	defer p.Release()
	events := p.Events()
	if len(events) < 1 || len(events) > 9 || len(events)%2 != 1 || len(c.Events) != len(events) || events[len(events)-1].ID != v.Anchor.ID || v.Anchor.Sequence < uint64(len(events)) {
		return deny()
	}
	actual := map[domain.SessionEventID]sessioncontext.EventPayload{}
	for _, record := range records {
		if record.Event.RunID == nil || *record.Event.RunID != run.ID {
			continue
		}
		switch record.Event.Kind {
		case domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventSystemNotice, domain.SessionEventAssistantMessage:
			if _, duplicate := actual[record.Event.ID]; duplicate {
				return deny()
			}
			actual[record.Event.ID] = record
		}
	}
	if len(actual) != len(events) {
		return deny()
	}
	for i, event := range events {
		record, found := actual[event.ID]
		identity := c.Events[i]
		if !found || record.Event.Validate() != nil || identity.ID != event.ID || identity.Kind != event.Kind || identity.Payload != record.Event.Payload || identity.IdempotencyKey != record.Event.IdempotencyKey || identity.DisplayText != "" || record.Event.TenantID != run.TenantID || record.Event.SessionID != run.SessionID || record.Event.Kind != event.Kind || record.Event.Sequence != v.Anchor.Sequence-uint64(len(events)-1-i) || !record.Event.CreatedAt.Equal(v.Anchor.CreatedAt) || !bytes.Equal(record.Payload, event.Payload) {
			return deny()
		}
		ref := record.Event.Payload
		if ref.Size != int64(len(record.Payload)) || ref.SHA256 != rawLocalFailureHash(record.Payload) {
			return deny()
		}
	}
	seal, err := localFailureSeal(run, c.Events)
	if err != nil || seal != v.Seal {
		return deny()
	}
	original, err := v.Original.Digest()
	if err != nil {
		return deny()
	}
	for _, item := range c.Operations {
		if item.Reservation.RegisteredAllocationDigest != original {
			return deny()
		}
	}
	return nil
}

// Explicit V4 arm of the existing deletion collector. It retains the same SQL
// range, index/call/proxy/reconciliation joins and transfer bounds. Body/argument
// and canonical evidence are then independently checked by its mounted verifier.
func readRetainedDeletionFailureOperationsV4(ctx context.Context, q *ownedAuthorityQuery, p retainedDeletionProducerData) ([]ownedOperationRow, error) {
	if p.failure == nil || p.failure.ConsentsV4 == nil || p.failure.Run.TerminalFailure == nil {
		return nil, q.budget.deny()
	}
	v := p.terminal
	scope := ownedOperationScope(v.Parent)
	rows, err := q.QueryContext(ctx, localCompactionOperationsSQL, scope.TenantID, scope.AttemptID)
	if err != nil {
		return nil, q.budget.deny()
	}
	defer rows.Close()
	var out []ownedOperationRow
	var operations []localFailureOperationV1
	accounting := ports.OwnedOperationFailureAccountingV4{Version: 4, Scope: scope, AggregateDebit: v.Accounting.Aggregate, ComponentAmounts: v.Accounting.Amounts}
	_, accounting.AccountingDigest, err = accountingBytes(v.Accounting)
	if err != nil {
		return nil, q.budget.deny()
	}
	accounting.ConsentStatesDigest, err = localFailureV4Digest(p.failure.ConsentsV4.States)
	if err != nil {
		return nil, q.budget.deny()
	}
	for _, state := range p.failure.ConsentsV4.States {
		accounting.ConsentReservedBytes += ownedConsentMetadataReservationBytes + uint64(state.Request.Arguments.Size)
	}
	for rows.Next() {
		var columns ownedOperationColumns
		var sequence, indexed uint64
		var indexedDigest string
		dest := append([]any{&sequence}, columns.destinations()...)
		dest = append(dest, &indexed, &indexedDigest)
		n, e := scanOwnedCurrentRow(rows, dest...)
		if q.budget.materialized(q.callback, n) != nil || e != nil || len(out) >= 12 || sequence != uint64(len(out)+1) || sequence > v.Accounting.Aggregate.Operations {
			return nil, q.budget.deny()
		}
		row, e := columns.decode(q, scope, sequence)
		if e != nil || row == nil || row.observation == nil || indexed != sequence || indexedDigest != string(row.reservation.OperationDigest) || row.observation.ObservedAt.After(p.terminalBound()) || row.reservation.Operation.Scope != scope || len(out) > 0 && row.reservation.Operation.ValidatePredecessor(out[len(out)-1].reservation, *out[len(out)-1].observation) != nil {
			return nil, q.budget.deny()
		}
		projected, e := ports.BuildOwnedOperationFailureRowV4(row.reservation, *row.observation, v.Parent, v.Admission.Source)
		if e != nil {
			return nil, q.budget.deny()
		}
		accounting.Operations = append(accounting.Operations, projected)
		operations = append(operations, localFailureOperationV1{Reservation: row.reservation, Observation: *row.observation})
		out = append(out, *row)
	}
	if rows.Err() != nil || rows.Close() != nil || uint64(len(out)) != v.Accounting.Aggregate.Operations || accounting.ValidateForStop(v.Parent, v.Admission.Source, p.failure.Run.TerminalFailure.Stop) != nil {
		return nil, q.budget.deny()
	}
	frontiers := make([]uint64, len(p.failure.ConsentsV4.States))
	for i, state := range p.failure.ConsentsV4.States {
		for _, row := range out {
			if row.reservation.Operation.Kind == domain.AttemptOperationModelV1 && row.observation.ResponseDigest == state.Request.ModelResponseDigest {
				if frontiers[i] != 0 {
					return nil, q.budget.deny()
				}
				frontiers[i] = row.reservation.Operation.Sequence
			}
		}
		if frontiers[i] == 0 || i > 0 && frontiers[i] <= frontiers[i-1] {
			return nil, q.budget.deny()
		}
	}
	c := localFailureCapsuleV4{Parent: v.Parent, Source: v.Admission.Source, CapturedAt: p.terminalBound(), Accounting: accounting, Operations: operations, Consents: p.failure.ConsentsV4.States}
	if validateFailureV4Frontiers(c, v.Accounting, frontiers) != nil {
		return nil, q.budget.deny()
	}
	return out, nil
}
