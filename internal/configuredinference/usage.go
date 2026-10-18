package configuredinference

import "gitcode.com/urandon/sessionless/internal/domain"

// OperationUsage projects DATA into the existing durable observation shape.
// A provider report is never a measurement, bill, refund or authority. The
// caller still owns complete-response validation and authenticated persistence.
func (v UsageV1) OperationUsage() (domain.AttemptOperationUsageV1, error) {
	if v.Provenance == UsageUnknownV1 {
		if v.InputTokens != nil || v.OutputTokens != nil || v.TotalTokens != nil {
			return domain.AttemptOperationUsageV1{}, ErrContract
		}
		return domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, nil
	}
	if v.Provenance != UsageProviderReportedV1 || (v.InputTokens == nil && v.OutputTokens == nil) {
		return domain.AttemptOperationUsageV1{}, ErrContract
	}
	if v.TotalTokens != nil && (v.InputTokens == nil || v.OutputTokens == nil ||
		*v.InputTokens > ^uint64(0)-*v.OutputTokens || *v.InputTokens+*v.OutputTokens != *v.TotalTokens) {
		return domain.AttemptOperationUsageV1{}, ErrContract
	}
	usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1,
		InputTokens: v.InputTokens, OutputTokens: v.OutputTokens}
	return usage.Clone(), nil
}
