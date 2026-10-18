package providerloopsession

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// PreparedLocalTerminalV1 is the original session's private frozen-frontier
// owner. It contains no completed execution evidence. It cannot dispatch or be
// reconstructed from JSON. Capsule publication, retirement, erasure and the
// original terminal transaction still have to run before canonical success.
type PreparedLocalTerminalV1 struct {
	self     *PreparedLocalTerminalV1
	owner    *localState
	entry    *ydbstore.LocalTerminalV1
	source   *ydbstore.LocalCompactionSourceV1
	deadline time.Time
	output   *ydbstore.LocalPreparedOutputV1
}

// PrepareTerminal freezes the SAME session/entry and seals its original idle
// Keeper, then captures the complete known frontier BEFORE bridge close. A
// failure burns terminal entry and cannot be retried into success. Neither
// idle sealing nor this captured DATA claims transport cleanup or output PUT.
func (v *LocalV1) PrepareTerminal(ctx context.Context) (_ *PreparedLocalTerminalV1, err error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if s.entry == nil || !s.runtimeStarted || s.runtimeFailed {
		return nil, ErrSession
	}
	s.mu.Lock()
	s.terminalRequested = true
	s.mu.Unlock()
	defer func() {
		if err != nil {
			s.runtimeFailed = true
			s.cancel()
		}
	}()
	if ctx.Err() != nil {
		return nil, ErrSession
	}
	entry, claimErr := s.entry.ClaimTerminal(call)
	if entry == nil {
		return nil, ErrSession
	}
	deadline, e := entry.Deadline()
	if e != nil {
		return nil, ErrSession
	}
	s.mu.Lock()
	s.terminalDeadline = deadline
	s.mu.Unlock()
	// Even a busy/failed entry must close transport admission irreversibly.
	// An active Keeper seal remains failure-only, even after it later drains.
	sealErr := s.keeper.SealTerminal(deadline)
	if claimErr != nil || sealErr != nil || call.Err() != nil || ctx.Err() != nil {
		return nil, ErrSession
	}
	source, e := entry.Capture(call)
	if e != nil || source == nil {
		return nil, ErrSession
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || call.Err() != nil || ctx.Err() != nil || s.terminalOutput != nil {
		source.Clear()
		return nil, ErrSession
	}
	h := &PreparedLocalTerminalV1{owner: s, entry: entry, source: source, deadline: deadline}
	h.self = h
	s.terminalOutput = h
	return h, nil
}

// Deadline remains historical DATA after close; it is never renewed or an
// authorization/evidence accessor. A copied wrapper cannot become its owner.
func (h *PreparedLocalTerminalV1) Deadline() (time.Time, error) {
	if h == nil || h.self != h || h.owner == nil || h.entry == nil {
		return time.Time{}, ErrSession
	}
	h.owner.mu.Lock()
	defer h.owner.mu.Unlock()
	if h.owner.terminalOutput != h {
		return time.Time{}, ErrSession
	}
	return h.deadline, nil
}

// PrepareOutput consumes only this original frozen session. It cannot re-open
// begin/effect admission or accept a caller's summary/frontier. Planned DATA is
// not publication or completed evidence. The original session owns buffer join.
func (h *PreparedLocalTerminalV1) PrepareOutput(ctx context.Context) (*ydbstore.LocalPreparedOutputV1, error) {
	if h == nil || h.self != h || h.owner == nil || h.entry == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrSession
	}
	s := h.owner
	select {
	case s.gate <- struct{}{}:
	default:
		return nil, ErrBusy
	}
	defer func() { <-s.gate }()
	s.mu.Lock()
	valid := s.terminalOutput == h && !s.closed && s.terminalRequested && !s.runtimeFailed && h.output == nil &&
		s.caller.Err() == nil && s.ctx.Err() == nil && s.entry != nil && h.source != nil
	base := s.ctx
	if s.execution != nil {
		base = s.execution
	}
	s.mu.Unlock()
	if !valid || base.Err() != nil || !time.Now().Before(h.deadline) {
		return nil, ErrSession
	}
	call, cancel := context.WithDeadline(base, h.deadline)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if ctx.Err() != nil {
		cancel()
	}
	output, err := h.entry.PrepareOutput(call)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil || output == nil || s.closed || s.caller.Err() != nil || ctx.Err() != nil || call.Err() != nil {
		if output != nil {
			output.Clear()
		}
		s.runtimeFailed = true
		s.cancel()
		return nil, ErrSession
	}
	h.output = output
	return output, nil
}

// PublishOutput transfers only the original composer's prepared buffers. The
// original Store publisher registers and independently verifies the complete
// Run postimage before any object write. This is custody, not canonical success
// or proof of allocation retirement and body erasure.
func (h *PreparedLocalTerminalV1) PublishOutput(ctx context.Context) error {
	if h == nil || h.self != h || h.owner == nil || h.entry == nil || ctx == nil || ctx.Err() != nil {
		return ErrSession
	}
	s := h.owner
	select {
	case s.gate <- struct{}{}:
	default:
		return ErrBusy
	}
	defer func() { <-s.gate }()
	s.mu.Lock()
	valid := s.terminalOutput == h && !s.closed && s.terminalRequested && !s.runtimeFailed && h.output != nil &&
		s.caller.Err() == nil && s.ctx.Err() == nil && s.entry != nil && h.source != nil
	base, output := s.ctx, h.output
	if s.execution != nil {
		base = s.execution
	}
	s.mu.Unlock()
	if !valid || base.Err() != nil || !time.Now().Before(h.deadline) {
		return ErrSession
	}
	call, cancel := context.WithDeadline(base, h.deadline)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if ctx.Err() != nil {
		cancel()
	}
	err := output.Publish(call)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil || s.closed || ctx.Err() != nil || call.Err() != nil || s.caller.Err() != nil {
		s.runtimeFailed = true
		s.cancel()
		return ErrSession
	}
	return nil
}

func (*PreparedLocalTerminalV1) String() string     { return "PreparedLocalTerminalV1{private:[redacted]}" }
func (h *PreparedLocalTerminalV1) GoString() string { return h.String() }
func (*PreparedLocalTerminalV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("prepared local terminal is transient ownership")
}
