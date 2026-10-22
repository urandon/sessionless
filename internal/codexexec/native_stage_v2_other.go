//go:build !darwin && !linux

package codexexec

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

type nativeInputStagePlatformV2 struct{}

func stageNativeInputV2(*preparedHistory, *mediaInput, *nativeFrozenProfileV2) (*nativeInputStageV2, error) {
	return nil, ErrContract
}
func (*nativeInputStageV2) verifyPlatform(context.Context) error { return ErrContract }
func (*nativeInputStageV2) executionPlatform(context.Context, attachedworkerdaemon.NativeLaunchV3) (*attachedworkerdaemon.NativeExecutionV3, error) {
	return nil, ErrContract
}
func (*nativeInputStageV2) remove() error { return ErrContract }
