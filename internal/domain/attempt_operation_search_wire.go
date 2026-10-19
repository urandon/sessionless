package domain

// AttemptOperationSearchWireV1 describes the distinct immutable search request
// encoding. It is DATA, never proof of the original call or send authority.
type AttemptOperationSearchWireV1 struct {
	Version                uint32 `json:"version"`
	APIProfile             string `json:"api_profile"`
	ArgumentsBytes         uint64 `json:"arguments_bytes"`
	CompilerArtifactDigest string `json:"compiler_artifact_digest"`
}

func (v AttemptOperationSearchWireV1) Validate() error {
	if v.Version != 1 || v.APIProfile != WebSearchAPIProfileV1 || v.ArgumentsBytes == 0 || v.ArgumentsBytes > WebSearchArgumentsBytesV1 {
		return operationError("search_wire", "requires the closed version 1 Tavily Basic profile and bounded original argument count")
	}
	return ValidateSHA256Digest("attempt_operation.search_wire.compiler_artifact_digest", v.CompilerArtifactDigest)
}

// The absent arm preserves the original argument-sized tool request contract.
// New search encodings keep argument size distinct from physical request size;
// this accessor never infers missing arguments for an incomplete new arm.
func (v AttemptOperationV1) toolArgumentBytes() uint64 {
	if v.SearchWire != nil {
		return v.SearchWire.ArgumentsBytes
	}
	return v.RequestBytes
}
