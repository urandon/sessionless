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
	calibration, err := s.contextCalibrationLocked()
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

// BoundCompactionSummary bounds the COMPLETE encoded summary string using the
// original retained calibration body. It does not validate summary semantics,
// citations or publication authority; the result is declaration-based DATA,
// not measured tokens, model qualification or permission. No reported usage or
// max_tokens value can replace this check of actual output bytes.
func (s *Source) BoundCompactionSummary(text string) (uint64, error) {
	if s == nil {
		return 0, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	calibration, err := s.contextCalibrationLocked()
	if err != nil {
		return 0, ErrDenied
	}
	bound, boundErr := calibration.BoundSummaryText(text)
	if !s.aliveLocked() {
		return 0, ErrDenied
	}
	if errors.Is(boundErr, configuredinference.ErrContextNoFit) {
		return 0, configuredinference.ErrContextNoFit
	}
	if boundErr != nil {
		return 0, ErrDenied
	}
	return bound, nil
}

// Caller holds s.mu. Exactly one retained BODY must match the installed digest;
// both public calculations share this original-self/lifetime/bytes validation.
func (s *Source) contextCalibrationLocked() (*configuredinference.ContextCalibrationV1, error) {
	if s.ctx == nil || !s.aliveLocked() || len(s.bodies) == 0 || len(s.bodies) > 16 {
		return nil, ErrDenied
	}
	index := -1
	for i := range s.bodies {
		if s.bodies[i].Reference.Digest != s.config.CalibrationDigest {
			continue
		}
		if index != -1 {
			return nil, ErrDenied
		}
		index = i
	}
	if index == -1 {
		return nil, ErrDenied
	}
	body := s.bodies[index]
	if body.Reference.Validate() != nil || body.Reference.Bytes == 0 || body.Reference.Bytes > configuredinference.MaxContextCalibrationBytesV1 || body.Reference.Bytes != uint64(len(body.Body)) || hash(body.Body) != s.config.CalibrationDigest {
		return nil, ErrDenied
	}
	calibration, err := configuredinference.DecodeContextCalibrationV1(s.config, body.Body)
	if err != nil {
		return nil, ErrDenied
	}
	return calibration, nil
}
