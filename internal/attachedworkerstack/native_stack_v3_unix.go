//go:build darwin || linux

package attachedworkerstack

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
)

func newNativeLauncherV3(ctx context.Context, config attachedworkeroci.Config) (nativeReconcilingLauncherV3, error) {
	return attachedworkeroci.NewNativeLauncherV3(ctx, config)
}
