package localoperator

import (
	"errors"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
)

// FitContext executes the closed estimator using only this original Source's
// retained calibration body and the actual immutable frozen request. It reads
// no files, model, network, caller coefficients or approval callback. The result
// is DATA, not qualification, current Store authority or permission to send.
// Missing calibration does not change the existing one-shot intake contract.
func (s *Source) FitContext(frozen *configuredinference.FrozenRequestV1, ceilings configuredinference.ContextFitCeilingsV1) (configuredinference.ContextFitV1, error) {
	deny := func() (configuredinference.ContextFitV1, error) {
		return configuredinference.ContextFitV1{}, ErrDenied
	}
	if s == nil {
		return deny()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil || !s.aliveLocked() || len(s.bodies) == 0 || len(s.bodies) > 16 {
		return deny()
	}
	index := -1
	for i := range s.bodies {
		if s.bodies[i].Reference.Digest != s.config.CalibrationDigest {
			continue
		}
		if index != -1 {
			return deny()
		}
		index = i
	}
	if index == -1 {
		return deny()
	}
	body := s.bodies[index]
	if body.Reference.Validate() != nil || body.Reference.Bytes == 0 || body.Reference.Bytes > configuredinference.MaxContextCalibrationBytesV1 || body.Reference.Bytes != uint64(len(body.Body)) || hash(body.Body) != s.config.CalibrationDigest {
		return deny()
	}
	calibration, err := configuredinference.DecodeContextCalibrationV1(s.config, body.Body)
	if err != nil {
		return deny()
	}
	result, fitErr := calibration.FitRequest(frozen, ceilings)
	// Close is serialized on this mutex; parent cancellation/expiry can still
	// occur during pure calculation. Never return even a no-fit result as if
	// its original Source were still alive after that revocation.
	if !s.aliveLocked() {
		return deny()
	}
	if errors.Is(fitErr, configuredinference.ErrContextNoFit) {
		return configuredinference.ContextFitV1{}, configuredinference.ErrContextNoFit
	}
	if fitErr != nil {
		return deny()
	}
	return result, nil
}
