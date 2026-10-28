//go:build !darwin && !linux

package localoperator

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/ports"
)

type ProfileReceiptFile struct{}

func ReadProfileReceipt(context.Context, string) (ports.OwnedProfilePublicationReceiptV1, error) {
	return ports.OwnedProfilePublicationReceiptV1{}, ErrDenied
}

func ReserveProfileReceipt(context.Context, string) (*ProfileReceiptFile, error) {
	return nil, ErrDenied
}
func (*ProfileReceiptFile) WriteReceipt(context.Context, ports.OwnedProfilePublicationReceiptV1) error {
	return ErrDenied
}
func (*ProfileReceiptFile) Close() error { return nil }
