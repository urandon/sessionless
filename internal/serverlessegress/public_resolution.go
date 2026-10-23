package serverlessegress

import "net/netip"

// PublicResolutionDataV1 applies the existing conservative pinned-HTTPS address
// classifier to the entire finite result. Returned literals/hash are DATA only:
// they do not perform DNS, dial, TLS, publication or authorize an effect.
func PublicResolutionDataV1(addresses []netip.Addr) ([]string, string, error) {
	return pinnedPublicAddresses(addresses)
}
