package webbff

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
)

func (handler *Handler) observeFailure(ctx context.Context, signal opssignal.SignalID, stage opssignal.Stage, class opssignal.ErrorClass, err error) {
	if webapi.ExpectedOperationalDenial(err) {
		return
	}
	if class == opssignal.Provider &&
		!errors.Is(err, telegramoidc.ErrProviderUnavailable) && !errors.Is(err, yandexoauth.ErrProviderUnavailable) &&
		(errors.Is(err, telegramoidc.ErrProviderResponse) || errors.Is(err, yandexoauth.ErrProviderResponse)) {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		class = opssignal.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		class = opssignal.DeadlineExceeded
	}
	opssignal.Emit(ctx, handler.config.OpsSignals, signal, stage, class)
}
