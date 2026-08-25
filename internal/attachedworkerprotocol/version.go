package attachedworkerprotocol

const (
	ProtocolVersionV1 ProtocolVersion = 1
	maxVersionWidth                   = 8
)

type ProtocolVersion uint32

type VersionWindow struct {
	Minimum ProtocolVersion `json:"minimum"`
	Maximum ProtocolVersion `json:"maximum"`
}

type VersionOfferV1 struct {
	Window    VersionWindow     `json:"window"`
	Supported []ProtocolVersion `json:"supported"`
}

func (offer VersionOfferV1) Validate() error {
	if offer.Window.Validate() != nil || len(offer.Supported) == 0 || len(offer.Supported) > maxVersionWidth {
		return protocolError(ErrorUnsupportedVersion)
	}
	var previous ProtocolVersion
	for _, version := range offer.Supported {
		if version < offer.Window.Minimum || version > offer.Window.Maximum || version <= previous {
			return protocolError(ErrorUnsupportedVersion)
		}
		previous = version
	}
	return nil
}

func (window VersionWindow) Validate() error {
	if window.Minimum == 0 || window.Maximum < window.Minimum ||
		uint64(window.Maximum)-uint64(window.Minimum)+1 > maxVersionWidth {
		return protocolError(ErrorUnsupportedVersion)
	}
	return nil
}

// NegotiateVersion selects the highest explicitly implemented version in the
// intersection. A version merely falling inside both windows is insufficient.
func NegotiateVersion(local, peer VersionWindow, implemented []ProtocolVersion) (ProtocolVersion, error) {
	if local.Validate() != nil || peer.Validate() != nil || len(implemented) == 0 || len(implemented) > maxVersionWidth {
		return 0, protocolError(ErrorUnsupportedVersion)
	}
	seen := make(map[ProtocolVersion]struct{}, len(implemented))
	var selected ProtocolVersion
	for _, version := range implemented {
		if version == 0 {
			return 0, protocolError(ErrorUnsupportedVersion)
		}
		if _, duplicate := seen[version]; duplicate {
			return 0, protocolError(ErrorUnsupportedVersion)
		}
		seen[version] = struct{}{}
		if version >= local.Minimum && version <= local.Maximum &&
			version >= peer.Minimum && version <= peer.Maximum && version > selected {
			selected = version
		}
	}
	if selected == 0 {
		return 0, protocolError(ErrorUnsupportedVersion)
	}
	return selected, nil
}

func NegotiateOffers(local, peer VersionOfferV1, implemented []ProtocolVersion) (ProtocolVersion, error) {
	if local.Validate() != nil || peer.Validate() != nil || len(implemented) == 0 || len(implemented) > maxVersionWidth {
		return 0, protocolError(ErrorUnsupportedVersion)
	}
	implementedSet := make(map[ProtocolVersion]struct{}, len(implemented))
	for _, version := range implemented {
		if version == 0 {
			return 0, protocolError(ErrorUnsupportedVersion)
		}
		if _, duplicate := implementedSet[version]; duplicate {
			return 0, protocolError(ErrorUnsupportedVersion)
		}
		implementedSet[version] = struct{}{}
	}
	peerSet := make(map[ProtocolVersion]struct{}, len(peer.Supported))
	for _, version := range peer.Supported {
		peerSet[version] = struct{}{}
	}
	var selected ProtocolVersion
	for _, version := range local.Supported {
		_, peerSupports := peerSet[version]
		_, explicitlyImplemented := implementedSet[version]
		if peerSupports && explicitlyImplemented && version > selected {
			selected = version
		}
	}
	if selected == 0 {
		return 0, protocolError(ErrorUnsupportedVersion)
	}
	return selected, nil
}

func highestCommonOfferedVersion(local, peer VersionOfferV1) (ProtocolVersion, error) {
	if local.Validate() != nil || peer.Validate() != nil {
		return 0, protocolError(ErrorUnsupportedVersion)
	}
	peerSet := make(map[ProtocolVersion]struct{}, len(peer.Supported))
	for _, version := range peer.Supported {
		peerSet[version] = struct{}{}
	}
	var selected ProtocolVersion
	for _, version := range local.Supported {
		if _, ok := peerSet[version]; ok && version > selected {
			selected = version
		}
	}
	if selected == 0 {
		return 0, protocolError(ErrorUnsupportedVersion)
	}
	return selected, nil
}
