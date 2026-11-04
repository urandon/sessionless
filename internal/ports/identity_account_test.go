package ports_test

import (
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestIdentityAccountV2EphemeralBoundaries(t *testing.T) {
	for _, value := range []any{
		ports.IdentityAccountReadAuthV2{SessionCookie: "private-session"},
		ports.IdentityAccountMutationAuthV2{PresentedCSRF: "private-csrf"},
		ports.IdentityAccountCallbackAuthV2{OperationCookie: "private-browser"},
		ports.IdentityAccountCallbackPurposeRequestV2{State: "private-state"},
		ports.IdentityAccountStartResultV2{OperationCookie: "private-browser"},
		ports.IdentityAccountContinueResultV2{AuthorizationURL: "https://provider.example/?state=private-state"},
		ports.IdentityAccountConfirmResultV2{Cookies: &ports.IdentityAccountSessionCookiesV2{SessionCookie: "private-session"}},
		ports.IdentityAccountMethodsRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "private-session"}},
		ports.IdentityAccountCallbackRequestV2{Code: "private-code"},
	} {
		if raw, err := json.Marshal(value); err == nil || raw != nil {
			t.Errorf("secret-bearing %T serialized", value)
		}
	}
	view := ports.IdentityAccountMethodsViewV2{Revision: domain.AuthRevisionV2(1)}
	if _, err := json.Marshal(view); err != nil {
		t.Error("safe view failed:", err)
	}
}

func TestIdentityAccountV2ContentFreeFailures(t *testing.T) {
	if ports.IdentityAccountUnavailableV2.Error() != "temporarily_unavailable" || ports.IdentityAccountErrorV2(255).Error() != "temporarily_unavailable" || ports.IdentityAccountUnauthenticatedV2.Error() != "unauthenticated" {
		t.Error("unknown/storage/auth failures lost finite classification")
	}
	for value := ports.IdentityAccountAccessDeniedV2; value <= ports.IdentityAccountExpiredV2; value++ {
		if strings.Contains(value.Error(), "private") {
			t.Error("failure exposed implementation data")
		}
	}
}
