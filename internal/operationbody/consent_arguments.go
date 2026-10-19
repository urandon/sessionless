package operationbody

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// WriteConsentArguments writes only bounded content DATA through this original
// physical holder. A returned BlobRef is not consent, an operation reservation,
// a send capability or a physical/durable retirement receipt.
func (s *StoreV1) WriteConsentArguments(ctx context.Context, body []byte) (domain.BlobRef, error) {
	return s.writeBody(ctx, body, true)
}
