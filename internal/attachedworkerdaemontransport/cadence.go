package attachedworkerdaemontransport

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

// CadencedSource is the feature-disabled seam between the daemon's serial
// Source.Next loop and the outbound transport poller. The daemon may call Next
// after its local idle backoff, but only Poller.Step schedules a network
// exchange; there is no second timer with network authority.
type CadencedSource struct {
	poller *attachedworkertransport.Poller
	cycle  *cadenceCycle
	gate   chan struct{}
}

type cadenceCycle struct {
	source     attachedworkerdaemon.Source
	invocation attachedworkerdaemon.Invocation
	available  bool
}

func (cycle *cadenceCycle) Exchange(ctx context.Context) error {
	cycle.invocation = attachedworkerdaemon.Invocation{}
	cycle.available = false
	invocation, available, err := cycle.source.Next(ctx)
	if err != nil {
		return err
	}
	cycle.invocation = invocation
	cycle.available = available
	return nil
}

func NewCadencedSource(source attachedworkerdaemon.Source, config attachedworkertransport.Config) (*CadencedSource, error) {
	if source == nil {
		return nil, ErrInvalidConfiguration
	}
	prepared, err := attachedworkertransport.PrepareConfig(config)
	if err != nil {
		return nil, err
	}
	return NewPreparedCadencedSource(source, prepared)
}

// NewPreparedCadencedSource cannot re-read fallible local config or entropy
// after a caller has reconciled the durable connection head.
func NewPreparedCadencedSource(source attachedworkerdaemon.Source, config attachedworkertransport.PreparedConfig) (*CadencedSource, error) {
	if source == nil {
		return nil, ErrInvalidConfiguration
	}
	cycle := &cadenceCycle{source: source}
	poller, err := attachedworkertransport.NewPreparedPoller(config, cycle)
	if err != nil {
		return nil, err
	}
	sourceGate := make(chan struct{}, 1)
	sourceGate <- struct{}{}
	return &CadencedSource{poller: poller, cycle: cycle, gate: sourceGate}, nil
}

func (source *CadencedSource) Next(ctx context.Context) (attachedworkerdaemon.Invocation, bool, error) {
	if source == nil || source.poller == nil || source.cycle == nil || source.gate == nil || ctx == nil {
		return attachedworkerdaemon.Invocation{}, false, ErrInvalidConfiguration
	}
	select {
	case <-ctx.Done():
		return attachedworkerdaemon.Invocation{}, false, ctx.Err()
	case <-source.gate:
	}
	defer func() { source.gate <- struct{}{} }()
	if err := source.poller.Step(ctx); err != nil {
		return attachedworkerdaemon.Invocation{}, false, err
	}
	// Move the result to the daemon and clear this intermediate slot while the
	// gate is held. A shallow-copy return must not retain stdin/credential
	// references in the cadence source for the lifetime of the next idle poll.
	invocation, available := source.cycle.invocation, source.cycle.available
	source.cycle.invocation = attachedworkerdaemon.Invocation{}
	source.cycle.available = false
	return invocation, available, nil
}

// Wake is only a coalesced local hint; it cannot override a connection or
// attempt authority, an in-flight exchange, or the minimum cadence.
func (source *CadencedSource) Wake() error {
	if source == nil || source.poller == nil {
		return ErrInvalidConfiguration
	}
	return source.poller.Wake()
}

var _ attachedworkerdaemon.Source = (*CadencedSource)(nil)
