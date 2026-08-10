// Package syntheticfrontend is a deterministic non-Telegram adapter for
// canonical ingress contract and integration tests.
package syntheticfrontend

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
)

const Frontend domain.Frontend = "synthetic"

type Adapter struct {
	service *sessioningress.Service
	actor   sessioningress.Actor
}

func New(
	service *sessioningress.Service,
	tenantID domain.TenantID,
	userID domain.UserID,
	externalConversationID string,
) *Adapter {
	return &Adapter{service: service, actor: sessioningress.Actor{
		TenantID: tenantID, UserID: userID, Frontend: Frontend,
		ExternalConversationID: externalConversationID,
	}}
}

func (adapter *Adapter) EnsureSession(ctx context.Context, at time.Time) (ports.FrontendSessionState, error) {
	return adapter.service.EnsureSession(ctx, adapter.actor, at)
}

func (adapter *Adapter) NewSession(
	ctx context.Context,
	expectedRevision uint64,
	externalRequestID string,
	at time.Time,
) (ports.FrontendSessionState, error) {
	return adapter.service.NewSession(ctx, adapter.actor, expectedRevision, externalRequestID, at)
}

func (adapter *Adapter) Send(
	ctx context.Context,
	externalEventID string,
	text string,
	connectionID domain.SubscriptionConnectionID,
	at time.Time,
) (ports.CanonicalUserEventResult, error) {
	return adapter.service.Ingest(ctx, sessioningress.UserInput{
		Actor: adapter.actor, ExternalEventID: externalEventID, ReceivedAt: at,
		Text: text, SubscriptionConnectionID: connectionID,
	})
}
