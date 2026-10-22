package codexexec

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// nativeInputStageV2 belongs to the original preparedHistory. It owns input
// custody, not permission to dispatch: the original consumed entry supplies it.
// The original serialized lifecycle alone sets dispatched and quiesced from
// actual supervisor dispatch and successful complete cleanup observations.
type nativeInputStageV2 struct {
	self                 *nativeInputStageV2
	history              *preparedHistory
	profile              *nativeFrozenProfileV2
	media                *mediaInput
	frozen               nativeFrozenProfileV2
	platform             nativeInputStagePlatformV2
	readset              *attachedworkerdaemon.NativeExecutionV3
	dispatched, quiesced bool
}

func (s *nativeInputStageV2) verify(ctx context.Context, profile *nativeFrozenProfileV2, media *mediaInput) error {
	if ctx == nil || ctx.Err() != nil || s == nil || s.self != s || s.history == nil || s.history.self != s.history || s.history.nativeV2 != s ||
		s.profile != profile || s.media != media || profile == nil || media == nil {
		return ErrContract
	}
	return s.verifyPlatform(ctx)
}

func (s *nativeInputStageV2) execution(ctx context.Context, bundle attachedworkerdaemon.NativeLaunchV3) (*attachedworkerdaemon.NativeExecutionV3, error) {
	if s == nil || s.verify(ctx, s.profile, s.media) != nil {
		return nil, ErrContract
	}
	return s.executionPlatform(ctx, bundle)
}
