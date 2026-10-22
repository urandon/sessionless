//go:build !darwin && !linux

package attachedworkerstack

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
)

func newNativeLauncherV3(context.Context, attachedworkeroci.Config) (nativeReconcilingLauncherV3, error) {
	return nil, ErrIsolationUnsupported
}
