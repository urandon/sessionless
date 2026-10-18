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

func (*PreparedLocalTerminalV1) String() string     { return "PreparedLocalTerminalV1{private:[redacted]}" }
func (h *PreparedLocalTerminalV1) GoString() string { return h.String() }
func (*PreparedLocalTerminalV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("prepared local terminal is transient ownership")
}
