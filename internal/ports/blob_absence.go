package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// BlobAbsenceVerifierV1 is an explicit storage capability, not a fallback from
// an arbitrary Open/Delete error. Nil requires a successful tenant-scoped,
// exact-object absence check under the caller's finite deadline. Permission,
// transport, cancellation, malformed response and unsupported capability fail.
// Absence at one instant does NOT prove writer quiescence or that an ambiguous
// PUT cannot commit later. The original cleanup owner must independently fence
// and join its known writers before relying on this storage fact.
type BlobAbsenceVerifierV1 interface {
	RequireBlobAbsent(context.Context, domain.TenantID, domain.BlobRef) error
}
