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
	profileV3            *nativeFrozenProfileV3
	frozenV3             nativeFrozenProfileV3
	platform             nativeInputStagePlatformV2
	readset              *attachedworkerdaemon.NativeExecutionV3
	dispatched, quiesced bool
}

func (s *nativeInputStageV2) verify(ctx context.Context, profile *nativeFrozenProfileV2, media *mediaInput) error {
	if ctx == nil || ctx.Err() != nil || s == nil || s.self != s || s.history == nil || s.history.self != s.history || s.history.nativeV2 != s ||
		s.profile != profile || s.profileV3 != nil || s.media != media || profile == nil || media == nil {
		return ErrContract
	}
	return s.verifyPlatform(ctx)
}

func (s *nativeInputStageV2) verifyV3(ctx context.Context, profile *nativeFrozenProfileV3, media *mediaInput) error {
	if ctx == nil || ctx.Err() != nil || s == nil || s.self != s || s.history == nil || s.history.self != s.history || s.history.nativeV2 != s ||
		s.profile != nil || s.profileV3 != profile || s.media != media || profile == nil || media == nil {
		return ErrContract
	}
	return s.verifyPlatform(ctx)
}

func (s *nativeInputStageV2) verifyOwned(ctx context.Context) error {
	if s == nil {
		return ErrContract
	}
	if s.profileV3 != nil {
		return s.verifyV3(ctx, s.profileV3, s.media)
	}
	return s.verify(ctx, s.profile, s.media)
}

func (s *nativeInputStageV2) frozenLaunch() attachedworkerdaemon.NativeLaunchV3 {
	if s.profileV3 != nil && s.profile == nil {
		return s.frozenV3.launch
	}
	if s.profile != nil && s.profileV3 == nil {
		return s.frozen.launch
	}
	return attachedworkerdaemon.NativeLaunchV3{}
}

func (s *nativeInputStageV2) execution(ctx context.Context, bundle attachedworkerdaemon.NativeLaunchV3) (*attachedworkerdaemon.NativeExecutionV3, error) {
	if s == nil || s.verifyOwned(ctx) != nil {
		return nil, ErrContract
	}
	return s.executionPlatform(ctx, bundle)
}
