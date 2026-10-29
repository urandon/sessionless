package webapi

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func TestLocalComputeProjectionIsClosedAndContainsNoSubscriptionOrPrivateIdentity(t *testing.T) {
	a := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: "private-tenant", OwnerUserID: "private-owner", ResourceID: "private-resource"}
	c := ports.ComputeConnectionState{LocalCapacityAssociation: &a, Provider: "local_model", ObservedAt: time.Date(2026, 10, 29, 14, 0, 0, 0, time.UTC)}
	p, e := projectComputeConnection(c)
	if e != nil || p.Kind != webcontract.ComputeLocalDevelopment {
		t.Fatal("local compute projection", e)
	}
	b, e := json.Marshal(p)
	if e != nil {
		t.Fatal(e)
	}
	for _, private := range []string{"entitlement", "quota", "subscription", "private-tenant", "private-owner", "private-resource"} {
		if strings.Contains(string(b), private) {
			t.Fatal("local public projection leaked other arm/private identity", private)
		}
	}
	for _, fault := range []string{"subscription", "entitlement", "quota", "provider", "time"} {
		t.Run(fault, func(t *testing.T) {
			v := c
			switch fault {
			case "subscription":
				v.ID = "subscription"
			case "entitlement":
				v.Entitlement = domain.EntitlementActive
			case "quota":
				v.Quota = domain.ProviderQuotaAvailable
			case "provider":
				v.Provider = "remote"
			case "time":
				v.ObservedAt = time.Time{}
			}
			if _, e := projectComputeConnection(v); e == nil {
				t.Fatal("mixed or invalid local arm accepted")
			}
		})
	}
}
