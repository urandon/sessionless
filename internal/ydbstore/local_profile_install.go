package ydbstore

import (
	"context"
	"database/sql"

	"gitcode.com/urandon/sessionless/internal/localoperator"
)

// InstallLocalProfile is the trusted local CLI composition, not a Web/API or
// worker entry point. Its caller establishes DB readiness before entry so the
// original short-lived publication grant is not spent waiting for connection.
// There is exactly one Fresh Source and one publication call. The existing
// Store owns bounded transactional retries; this owner never restarts it.
func InstallLocalProfile(ctx context.Context, db *sql.DB, root, executable, receiptPath string) error {
	if ctx == nil || ctx.Err() != nil || db == nil {
		return localoperator.ErrDenied
	}
	output, err := localoperator.ReserveProfileReceipt(ctx, receiptPath)
	if err != nil {
		return localoperator.ErrDenied
	}
	defer output.Close() // retain even empty output on every error
	source, err := localoperator.OpenFresh(ctx, root, executable)
	if err != nil {
		return localoperator.ErrDenied
	}
	defer source.Close()
	grant, err := source.Publication()
	if err != nil {
		return localoperator.ErrDenied
	}
	store, err := New(db, Options{OwnedProfilePublicationVerifier: source})
	if err != nil {
		return localoperator.ErrDenied
	}
	receipt, err := store.PublishOwnedProfile(ctx, grant)
	if err != nil || source.AcceptReceipt(receipt) != nil {
		return localoperator.ErrDenied
	}
	return output.WriteReceipt(ctx, receipt)
}
