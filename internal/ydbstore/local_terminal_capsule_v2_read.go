package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Distinct retained grammar: V1's decoder/meaning is never a fallback. Full
// accounting facts survive erasure, rather than just an opaque disposition hash.
type localRetainedTerminalCapsuleV2 struct {
	Schema         string                                      `json:"schema"`
	Scope          domain.AttemptOperationScopeV1              `json:"scope"`
	ProfileDigest  string                                      `json:"profile_digest"`
	CodecVersion   uint32                                      `json:"codec_version"`
	FrontierDigest string                                      `json:"frontier_digest"`
	Operations     []localRetainedTerminalOperationV1          `json:"operations"`
	DerivedContext *domain.RunDerivedContextV1                 `json:"derived_context"`
	Events         []localCapsuleEventV1                       `json:"events"`
	PublicManifest domain.ArtifactManifest                     `json:"public_manifest"`
	Accounting     ports.OwnedOperationAccountingDispositionV3 `json:"accounting_disposition"`
}

// This validates closed immutable DATA only; the reader still authenticates
// the actual capsule reference, terminal seal and financial parent/admission.
func decodeLocalTerminalCapsuleV2(body []byte) (localRetainedTerminalCapsuleV2, error) {
	deny := func() (localRetainedTerminalCapsuleV2, error) {
		return localRetainedTerminalCapsuleV2{}, domain.ErrMembershipDenied
	}
	var c localRetainedTerminalCapsuleV2
	if len(body) == 0 || uint64(len(body)) > domain.MaxTerminalCapsuleBytesV1 || decodeOwnedEffectCanonicalJSON(string(body), &c) != nil ||
		c.Schema != "sessionless.terminal-custody.v2" || c.CodecVersion != 2 || c.Scope.Validate() != nil ||
		domain.ValidateSHA256Digest("capsule.profile", c.ProfileDigest) != nil || domain.ValidateSHA256Digest("capsule.frontier", c.FrontierDigest) != nil ||
		len(c.Operations) < 3 || len(c.Operations) > 12 || c.Accounting.Version != 3 || c.Accounting.Scope != c.Scope || len(c.Accounting.Operations) != len(c.Operations) {
		return deny()
	}
	if _, err := c.Accounting.Digest(); err != nil {
		return deny()
	}
	materials := make([]localCapsuleOperationMaterial, len(c.Operations))
	for i, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		if r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 ||
			len(item.NormalizedBody) == 0 || len(item.NormalizedBody) > 64<<10 || uint64(len(item.NormalizedBody)) != o.ResponseBytes {
			return deny()
		}
		sum := sha256.Sum256(item.NormalizedBody)
		rd, re := r.Digest()
		od, oe := o.Digest()
		row := c.Accounting.Operations[i]
		if re != nil || oe != nil || hex.EncodeToString(sum[:]) != o.ResponseDigest || row.Sequence != r.Operation.Sequence || row.Kind != r.Operation.Kind || row.OperationDigest != r.OperationDigest || row.ReservationDigest != rd || row.ObservationDigest != od || row.Reserved != r.Operation.Debit || !reflect.DeepEqual(row.Usage, o.Usage) {
			return deny()
		}
		materials[i] = localCapsuleOperationMaterial{reservation: r, observation: o, normalized: item.NormalizedBody, bytes: uint64(len(item.NormalizedBody))}
	}
	projections, err := localCapsuleProjectionsV2(materials, c.Events)
	if err != nil {
		return deny()
	}
	for i, p := range projections {
		if c.Operations[i].Projection != p.kind || !reflect.DeepEqual(c.Operations[i].EventIDs, p.events) {
			return deny()
		}
	}
	sink := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1, emit: true}
	if err := encodeLocalTerminalCapsuleV2To(&sink, c.Scope, c.ProfileDigest, c.FrontierDigest, materials, c.DerivedContext, c.Events, 0, c.PublicManifest, c.Accounting); err != nil {
		clear(sink.body)
		return deny()
	}
	exact := bytes.Equal(sink.body, body)
	clear(sink.body)
	if !exact {
		return deny()
	}
	return c, nil
}

func terminalCapsuleSchema(body []byte) string {
	// Discriminator only, never validation. Each selected decoder separately
	// enforces duplicate/unknown/case-sensitive canonical JSON and exact bytes.
	var h struct {
		Schema string `json:"schema"`
	}
	if len(body) == 0 || uint64(len(body)) > domain.MaxTerminalCapsuleBytesV1 || json.Unmarshal(body, &h) != nil {
		return ""
	}
	return h.Schema
}
