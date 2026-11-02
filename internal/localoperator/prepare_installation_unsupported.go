//go:build !darwin && !linux

package localoperator

import "context"

func writePreparedInstallation(context.Context, string, []byte, []byte) error {
	return ErrDenied
}
