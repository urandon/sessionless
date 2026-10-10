package ports

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

var ErrRunExplanationNotFound = errors.New("run explanation not found")
var ErrRunExplanationUnavailable = errors.New("run explanation temporarily unavailable")

// RunExplanationAuthorizationV1 is resolved by the server, never by a client.
// The resource transaction rechecks all fields without refreshing activity.
type RunExplanationAuthorizationV1 struct {
	TenantID                  domain.TenantID
	UserID                    domain.UserID
	MembershipSecurityVersion uint64
	WebSessionDigest          domain.SecretDigest
}

type RunExplanationReadStoreV1 interface {
	ReadRunExplanationV1(context.Context, RunExplanationAuthorizationV1, domain.RunID) (webcontract.RunExplanationV1, error)
}
