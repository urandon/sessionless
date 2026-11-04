package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type localRetainedTerminalCapsuleV3 struct {
	Schema         string                                      `json:"schema"`
	Scope          domain.AttemptOperationScopeV1              `json:"scope"`
	ProfileDigest  string                                      `json:"profile_digest"`
	CodecVersion   uint32                                      `json:"codec_version"`
	FrontierDigest string                                      `json:"frontier_digest"`
	Operations     []localRetainedTerminalOperationV1          `json:"operations"`
	DerivedContext *domain.RunDerivedContextV1                 `json:"derived_context"`
	Events         []localCapsuleEventV1                       `json:"events"`
	PublicManifest domain.ArtifactManifest                     `json:"public_manifest"`
	Accounting     ports.OwnedOperationAccountingDispositionV4 `json:"accounting_disposition"`
	Consents       []domain.WebSearchConsentStateV1            `json:"consents"`
	ModelCatalogs  []compactioncontext.ModelCatalogV3          `json:"model_catalogs"`
}

// Strict immutable DATA decoding only. Mounted consumers must separately join
// the actual parent/admission/accounting/retired allocation and canonical seal.
func decodeLocalTerminalCapsuleV3(body []byte) (localRetainedTerminalCapsuleV3, error) {
	deny := func() (localRetainedTerminalCapsuleV3, error) {
		return localRetainedTerminalCapsuleV3{}, domain.ErrMembershipDenied
	}
	var c localRetainedTerminalCapsuleV3
	if len(body) == 0 || uint64(len(body)) > domain.MaxTerminalCapsuleBytesV1 || decodeOwnedEffectCanonicalJSON(string(body), &c) != nil || c.Schema != "sessionless.terminal-custody.v3" || c.CodecVersion != 3 || c.Scope.Validate() != nil || domain.ValidateSHA256Digest("capsule.profile", c.ProfileDigest) != nil || domain.ValidateSHA256Digest("capsule.frontier", c.FrontierDigest) != nil || len(c.Operations) < 3 || len(c.Operations) > 12 || c.Accounting.Scope != c.Scope || len(c.Accounting.Operations) != len(c.Operations) {
		return deny()
	}
	if _, err := c.Accounting.Digest(); err != nil {
		return deny()
	}
	consents, err := ports.OwnedSearchConsentStatesDigestV4(c.Consents)
	if err != nil || consents != c.Accounting.ConsentStatesDigest {
		return deny()
	}
	materials := make([]localCapsuleOperationMaterial, len(c.Operations))
	for i, item := range c.Operations {
		r, o := item.Reservation, item.Observation
		if r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || len(item.NormalizedBody) == 0 || len(item.NormalizedBody) > 64<<10 || uint64(len(item.NormalizedBody)) != o.ResponseBytes {
			return deny()
		}
		hash := sha256.Sum256(item.NormalizedBody)
		rd, re := r.Digest()
		od, oe := o.Digest()
		row := c.Accounting.Operations[i]
		if re != nil || oe != nil || hex.EncodeToString(hash[:]) != o.ResponseDigest || row.Sequence != r.Operation.Sequence || row.Kind != r.Operation.Kind || row.OperationDigest != r.OperationDigest || row.ReservationDigest != rd || row.ObservationDigest != od || row.Reserved != r.Operation.Debit || !reflect.DeepEqual(row.Usage, o.Usage) {
			return deny()
		}
		materials[i] = localCapsuleOperationMaterial{reservation: r, observation: o, normalized: item.NormalizedBody, bytes: uint64(len(item.NormalizedBody))}
	}
	if localSuccessCatalogsV3(c.ProfileDigest, c.Operations, c.ModelCatalogs) != nil {
		return deny()
	}
	projections, err := localCapsuleProjectionsV3(materials, c.Events)
	if err != nil {
		return deny()
	}
	for i, projection := range projections {
		if c.Operations[i].Projection != projection.kind || !reflect.DeepEqual(c.Operations[i].EventIDs, projection.events) {
			return deny()
		}
	}
	sink := localCapsuleSink{limit: domain.MaxTerminalCapsuleBytesV1, emit: true}
	defer func() { clear(sink.body) }()
	if encodeLocalTerminalCapsuleV3To(&sink, c.Scope, c.ProfileDigest, c.FrontierDigest, materials, c.DerivedContext, c.Events, 0, c.PublicManifest, c.Accounting, c.Consents, c.ModelCatalogs) != nil || !bytes.Equal(sink.body, body) {
		return deny()
	}
	return c, nil
}
