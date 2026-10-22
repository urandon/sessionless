package attachedworkeroci

import "gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"

// Private lowering only, after the versioned launch constructor has verified
// its original envelope. V2 never populates input mappings or changes labels;
// V3 supplies only its original, finite descriptor-derived read-only files.
// This is not an admitted workload or public alternate launcher interface.
type nativeContainerEnvelope struct {
	workload          attachedworkerdaemon.LaunchSpec
	relay             attachedworkerdaemon.NativeArtifactV2
	socket            *attachedworkerdaemon.NativeSocketV2
	socketDestination string
	profile           string
	inputs            []attachedworkerdaemon.NativeInputMappingV3
}
