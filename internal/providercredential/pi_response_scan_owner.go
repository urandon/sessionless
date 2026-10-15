package providercredential

// No caller limits/arena/ledger replacement: use the original sealed P and
// immutable file maximum before Issue. This prepares parsed DATA only; serving
// eligibility and the remaining closed policy/semantic guard are unchanged.
func (p *PreparedPiFileUseV1) prepareResponseScan() error {
	if p == nil || p.state == nil {
		return ErrInvalid
	}
	s := p.state
	if !s.mu.TryLock() {
		s.abort()
		return ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.scanPrepared || s.claimed.Load() || s.callback != 0 || s.auth == nil || s.http == nil || s.file == nil || s.file.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	s.scanPrepared = true
	f := s.file
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	if f.service.preparedPiFiles[f.scope] != s {
		s.abort()
		return ErrConsumed
	}
	scan, err := newPiResponseScan(s.ledger, s.auth.responseLimits, uint64(f.maxBytes))
	if err != nil {
		s.abort()
		return err
	}
	s.responseScan = scan
	s.live += scan.live
	if s.stopped.Load() || f.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	return nil
}
