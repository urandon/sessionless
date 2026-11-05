package ydbstore

import (
	"context"
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// A current forward-row point read inside the SAME private LIST transaction.
// This is not a generic auth preflight or SEND permission. Unlike the ordinary
// identity reader, every returned byte (including the oversize sentinel) is
// charged to this compute-choice callback/call BEFORE decode or size denial.
func readComputeChoiceIdentityV2(ctx context.Context, query *computeChoiceQuery, subject domain.ExternalSubject) (domain.ExternalIdentityV2, bool, error) {
	if subject.Validate() != nil {
		return domain.ExternalIdentityV2{}, false, errComputeChoiceSource
	}
	bucket, err := webBucket(subject.String())
	if err != nil {
		return domain.ExternalIdentityV2{}, false, errComputeChoiceSource
	}
	var raw string
	err = query.QueryRowContext(ctx, webIdentityRowSQLV2, bucket, subject.Provider, subject.Subject).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ExternalIdentityV2{}, false, nil
	}
	if err != nil {
		return domain.ExternalIdentityV2{}, false, query.readError(err)
	}
	if err := query.budget.RecordMaterialized(computechoice.SourceOrdinary, int64(len(raw))); err != nil {
		return domain.ExternalIdentityV2{}, false, err
	}
	identity, err := domain.DecodeExternalIdentityV2([]byte(raw))
	if err != nil || identity.Subject != subject {
		return domain.ExternalIdentityV2{}, false, errComputeChoiceSource
	}
	return identity, true, nil
}
