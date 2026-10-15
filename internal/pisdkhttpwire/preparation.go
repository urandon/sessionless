package pisdkhttpwire

import (
	"reflect"
	"runtime"
)

// SequencePreparation describes logical Go object storage only, not allocator,
// scheduler, native, or full-profile fit. Reserve it BEFORE NewSequence.
// The hash allowance covers the fixed pointer-free Go SHA256 digest inspected
// in the pinned toolchain; an alternate implementation is not qualified by it.
type SequencePreparation struct{ LiveBytes, Nodes uint64 }

const sequenceHashAllowance = 512

// SequencePreparationSupported is a pre-constructor exclusion gate, not build
// attestation. Funding assumes the source-audited official Go1.26.8 tuple; a
// qualified installed artifact still needs its own immutable build provenance.
// Never invoke an unaudited alternate crypto constructor then measure it.
func SequencePreparationSupported() bool {
	return sequenceGoHashBackend && runtime.Version() == "go1.26.8" &&
		(runtime.GOOS == "darwin" || runtime.GOOS == "linux") &&
		(runtime.GOARCH == "arm64" || runtime.GOARCH == "amd64")
}

func MeasureSequencePreparation() SequencePreparation {
	return SequencePreparation{
		LiveBytes: uint64(reflect.TypeOf(Sequence{}).Size()) + uint64(reflect.TypeOf(sequenceState{}).Size()) + sequenceHashAllowance,
		Nodes:     3, // wrapper, private state, fixed SHA digest
	}
}

// PreparedLayoutFits checks the funded concrete hash object without allocating
// another digest. It is not native qualification. The coordinator refuses an
// unexpected implementation, including opaque external crypto state.
func (s *Sequence) PreparedLayoutFits() bool {
	if s == nil || s.state == nil || s.state.digestState == nil {
		return false
	}
	t := reflect.TypeOf(s.state.digestState)
	if t.Kind() != reflect.Pointer {
		return false
	}
	t = t.Elem()
	// Fixed source-audited layout, no recursive reflection or Marshal/Clone.
	return t.PkgPath() == "crypto/internal/fips140/sha256" && t.Name() == "Digest" &&
		t.Size() == 120 && t.Size() <= sequenceHashAllowance &&
		s.state.digestState.Size() == 32 && s.state.digestState.BlockSize() == 64
}
