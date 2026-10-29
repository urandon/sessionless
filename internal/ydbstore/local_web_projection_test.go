package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/domain"
	"testing"
)

func TestLocalWebRunProviderUsesOriginalLocalArmWithoutSubscriptionQuery(t *testing.T) {
	r := currentWorkerAuthorityFixture(t).Run
	r.SubscriptionConnectionID = ""
	r.LocalCapacityAssociation = &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: r.TenantID, OwnerUserID: "owner-test", ResourceID: "local-resource"}
	tx := &stateTx{tenantID: r.TenantID} // no SQL connection: any subscription lookup is invalid
	if p, e := readRunProviderTx(t.Context(), tx, r); e != nil || p != "local_model" {
		t.Fatal("original local provider projection", p, e)
	}
	r.SubscriptionConnectionID = "mixed-subscription"
	if _, e := readRunProviderTx(t.Context(), tx, r); e == nil {
		t.Fatal("mixed local history accepted")
	}
}
