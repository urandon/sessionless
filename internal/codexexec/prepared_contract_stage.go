package codexexec

import "errors"

type preparedContractStage uint8

const (
	preparedContractStageIdentity preparedContractStage = iota + 1
	preparedContractStageVerifiedInput
	preparedContractStageNativeGeneration
	preparedContractStageNativeFixed
	preparedContractStageDescriptor
	preparedContractStageIngress
	preparedContractStageUnsupportedInput
	preparedContractStageDriver
	preparedContractStagePreflight
	preparedContractStageIngressShape
	preparedContractStageIngressManifest
	preparedContractStageIngressRecords
	preparedContractStageIngressTrigger
	preparedContractStageIngressSource
	preparedContractStageIngressHistory
	preparedContractStageIngressUploads
)

type preparedContractStageError struct {
	stage preparedContractStage
}

func (err *preparedContractStageError) Error() string { return "prepared invocation contract failure" }
func (err *preparedContractStageError) Unwrap() error { return ErrContract }

func preparedContractFailure(stage preparedContractStage) error {
	return &preparedContractStageError{stage: stage}
}

// PreparedContractStageV1 returns only the closed diagnostic stage associated
// with a classified prepared invocation contract failure. It never returns
// source error text or data.
func PreparedContractStageV1(err error) string {
	var classified *preparedContractStageError
	if !errors.As(err, &classified) || classified == nil {
		return ""
	}
	switch classified.stage {
	case preparedContractStageIdentity:
		return "identity"
	case preparedContractStageVerifiedInput:
		return "verified_input"
	case preparedContractStageNativeGeneration:
		return "native_generation"
	case preparedContractStageNativeFixed:
		return "native_fixed"
	case preparedContractStageDescriptor:
		return "descriptor"
	case preparedContractStageIngress:
		return "ingress"
	case preparedContractStageUnsupportedInput:
		return "unsupported_input"
	case preparedContractStageDriver:
		return "driver"
	case preparedContractStagePreflight:
		return "preflight"
	case preparedContractStageIngressShape:
		return "ingress_shape"
	case preparedContractStageIngressManifest:
		return "ingress_manifest"
	case preparedContractStageIngressRecords:
		return "ingress_records"
	case preparedContractStageIngressTrigger:
		return "ingress_trigger"
	case preparedContractStageIngressSource:
		return "ingress_source"
	case preparedContractStageIngressHistory:
		return "ingress_history"
	case preparedContractStageIngressUploads:
		return "ingress_uploads"
	default:
		return ""
	}
}
