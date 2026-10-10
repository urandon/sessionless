package sessioncontext

import (
	"errors"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// FinalMaterialMeasureV1 binds transient verified preparation to the actual
// final codec record. It retains no payload or preparation pointer. It is one
// contribution, not persisted prefix coverage, authorization or eligibility.
type FinalMaterialMeasureV1 struct {
	record   RecordMeasureV1
	measures sessionmaterial.MeasuresV1
	verified bool
}

func MeasurePreparedRecordV1(event domain.SessionEvent, prepared *sessionmaterial.PreparedV1) (FinalMaterialMeasureV1, error) {
	body, measures, err := prepared.CopyForFinalV1(event)
	if err != nil {
		return FinalMaterialMeasureV1{}, ErrRecordNotMeasurable
	}
	defer clear(body)
	record, err := MeasureRecordV1(event, body)
	if err != nil {
		return FinalMaterialMeasureV1{}, ErrRecordNotMeasurable
	}
	return FinalMaterialMeasureV1{record: record, measures: measures, verified: true}, nil
}

func (measure FinalMaterialMeasureV1) Matches(event domain.SessionEvent) bool {
	return measure.verified && measure.record.Matches(event)
}

func (measure FinalMaterialMeasureV1) Record() (RecordMeasureV1, bool) {
	return measure.record, measure.verified
}
func (measure FinalMaterialMeasureV1) SourceMeasures() (sessionmaterial.MeasuresV1, bool) {
	return measure.measures, measure.verified
}
func (FinalMaterialMeasureV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("canonical material measure cannot be serialized")
}
func (FinalMaterialMeasureV1) Format(state fmt.State, verb rune) {
	_, _ = fmt.Fprint(state, "sessioncontext.FinalMaterialMeasureV1")
}
