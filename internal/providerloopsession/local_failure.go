package providerloopsession

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This route never receives an error DTO, caller stop code or replacement
// frontier. The original entry independently authenticates its retained holder.
func (s *localState) claimKnownMCPFailure() {
	if s.entry == nil || s.credentialed || s.entryRoute != localEntryWorker {
		return
	}
	h, err := s.entry.ClaimKnownMCPFailure()
	if err != nil {
		return
	}
	d, err := h.Deadline()
	if err != nil {
		return
	}
	p := &PreparedLocalFailureV1{owner: s, entry: h, deadline: d}
	p.self = p
	s.mu.Lock()
	s.knownFailure = p
	s.terminalRequested = true
	s.terminalDeadline = d
	s.mu.Unlock()
}

type PreparedLocalFailureV1 struct {
	self     *PreparedLocalFailureV1
	owner    *localState
	entry    *ydbstore.LocalFailureV1
	deadline time.Time
}

func (*PreparedLocalFailureV1) MarshalJSON() ([]byte, error) { return nil, ErrSession }
func (*PreparedLocalFailureV1) String() string               { return "PreparedLocalFailureV1{private:[redacted]}" }
func (v *LocalV1) KnownMCPFailure() (*PreparedLocalFailureV1, error) {
	if v == nil || v.state == nil {
		return nil, ErrSession
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.knownFailure
	if p == nil || p.self != p || p.owner != s || !s.runtimeFailed || s.closed {
		return nil, ErrSession
	}
	return p, nil
}
func (p *PreparedLocalFailureV1) Deadline() (time.Time, error) {
	if p == nil || p.self != p || p.owner == nil || p.entry == nil {
		return time.Time{}, ErrSession
	}
	return p.deadline, nil
}

// Each explicit phase is one independently bounded logical operation, matching
// the existing success-terminal API. None renews the original cleanup end.
func (p *PreparedLocalFailureV1) beginPhase(ctx context.Context) (context.Context, func(), error) {
	if p == nil || p.self != p || p.owner == nil || p.entry == nil || ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrSession
	}
	s := p.owner
	select {
	case s.gate <- struct{}{}:
	default:
		return nil, nil, ErrBusy
	}
	s.mu.Lock()
	valid := !s.closed && s.knownFailure == p && s.terminalRequested && s.runtimeFailed
	s.mu.Unlock()
	if !valid {
		<-s.gate
		return nil, nil, ErrSession
	}
	call, cancel := context.WithDeadline(ctx, p.deadline)
	return call, func() { cancel(); <-s.gate }, nil
}
func (p *PreparedLocalFailureV1) PrepareOutput(ctx context.Context) error {
	call, finish, err := p.beginPhase(ctx)
	if err != nil {
		return err
	}
	defer finish()
	return p.entry.Prepare(call)
}
func (p *PreparedLocalFailureV1) PublishOutput(ctx context.Context) error {
	call, finish, err := p.beginPhase(ctx)
	if err != nil {
		return err
	}
	defer finish()
	return p.entry.Publish(call)
}
func (p *PreparedLocalFailureV1) FinishCleanup(ctx context.Context) error {
	call, finish, err := p.beginPhase(ctx)
	if err != nil {
		return err
	}
	defer finish()
	return p.entry.FinishCleanup(call)
}
func (p *PreparedLocalFailureV1) Complete(ctx context.Context) error {
	call, finish, err := p.beginPhase(ctx)
	if err != nil {
		return err
	}
	defer finish()
	return p.entry.Complete(call)
}
