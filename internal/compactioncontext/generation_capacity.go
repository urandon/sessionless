package compactioncontext

import (
	"errors"
	"strings"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
)

// MinimumSuccessorRequest renders a minimum STRUCTURALLY valid summary DATA
// witness. It is not a generated answer, quality/custody evidence or permission.
// Every accepted summary has the seven closed fields, at least one substantive
// one-byte text and one actual known 64-hex source ID. The original owner fits
// this necessary lower bound using its REAL retained calibration.
func (p *GenerationV1) MinimumSuccessorRequest() (*configuredinference.FrozenRequestV1, error) {
	summary, err := p.MinimumSummaryData()
	if err != nil {
		return nil, err
	}
	return p.Rebuild(summary)
}

// MinimumSummaryData returns an independent structural DATA witness, never a
// generated result, citation-quality assertion or publication permission.
func (p *GenerationV1) MinimumSummaryData() ([]byte, error) {
	if p == nil || p.frozen == nil || len(p.tools) != 0 || len(p.references) == 0 {
		return nil, ErrCanonical
	}
	body := []byte(`{"schema":"` + configuredinference.CompactionSummarySchemaV1 + `","task":[{"text":"x","source_ids":["` + p.references[0].SourceID + `"]}],"constraints":[],"facts":[],"unresolved":[],"artifacts":[],"omissions":[]}`)
	if _, err := p.ValidateSummary(body); err != nil {
		return nil, err
	}
	return body, nil
}

func (p *GenerationV1) SuccessorOutputReserve() uint64 {
	if p == nil || p.frozen == nil {
		return 0
	}
	return min(uint64(2048), p.limits.MaxOutputTokens)
}

// SuccessorRequestUpperBytes counts potential wire DATA using the SAME renderer
// as Rebuild. Minimum witness/worst text below is not actual model output and cannot
// grant a send, publication, or permission. The original reconstructed request
// must still pass its actual summary validator and Source.FitContext.
// maxAdmissibleWire must be independently derived from original budget/fit
// ceilings; clipping never attests that every summary will fit those ceilings.
func (p *GenerationV1) SuccessorRequestUpperBytes(summaryBytes, maxAdmissibleWire uint64) (uint64, error) {
	if p == nil || p.frozen == nil || summaryBytes == 0 || summaryBytes > configuredinference.MaxCompactionSummaryBytesV1 || maxAdmissibleWire == 0 || len(p.tools) != 0 {
		return 0, ErrCanonical
	}
	maxAdmissibleWire = min(maxAdmissibleWire, p.limits.RequestBytes)
	format, err := p.MinimumSummaryData()
	if err != nil {
		return 0, err
	}
	if summaryBytes < uint64(len(format)) {
		return 0, configuredinference.ErrContextNoFit
	}
	minimum, err := p.MinimumSuccessorRequest()
	if err != nil {
		return 0, err
	}
	body := minimum.Body()
	minimumBytes := uint64(len(body))
	clear(body)
	if minimumBytes > maxAdmissibleWire {
		return 0, configuredinference.ErrContextNoFit
	}
	// Each raw UTF8 byte expands by at most six bytes in the inner JSON string
	// and at most seven through both JSON layers. A low control byte attains
	// that maximum. This is length-only DATA, never a claimed semantic summary.
	maximum, err := p.successorRequestV1(strings.Repeat("\x01", int(summaryBytes)))
	if errors.Is(err, configuredinference.ErrContextNoFit) || errors.Is(err, configuredinference.ErrRequestBytes) {
		return maxAdmissibleWire, nil
	}
	if err != nil {
		return 0, err
	}
	body = maximum.Body()
	n := uint64(len(body))
	clear(body)
	return min(n, maxAdmissibleWire), nil
}
