package attachedworkerdaemontransport

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

const reconnectCadenceCleanupTimeout = 5 * time.Second

type recoveredSession interface {
	SessionPort
	ReconnectRecovery() (attachedworkersession.ReconnectRecoveryV1, error)
	Close(context.Context) error
}

type reconnectPort interface {
	Reconnect(context.Context, attachedworkersession.ReconnectInputV1) (recoveredSession, error)
}

type concreteReconnectPort struct {
	connector *attachedworkersession.Connector
}

func (port concreteReconnectPort) Reconnect(ctx context.Context, input attachedworkersession.ReconnectInputV1) (recoveredSession, error) {
	return port.connector.Reconnect(ctx, input)
}

// IdleRecoveredCadence owns a server-reconciled session and one serial daemon
// source. It does not run a daemon or enable the product foreground.
type IdleRecoveredCadence struct {
	Source  *CadencedSource
	Adapter *Adapter
	session recoveredSession
}

// ReconnectIdleCadence can construct a polling source only after the existing
// Connector has checked the local checkpoint against the exact durable server
// head. Active attempt and terminal recovery intent remain a separate gate;
// this helper never infers that a process effect may be repeated.
func ReconnectIdleCadence(
	ctx context.Context,
	connector *attachedworkersession.Connector,
	input attachedworkersession.ReconnectInputV1,
	materializer Materializer,
	adapterConfig Config,
	pollConfig attachedworkertransport.Config,
) (*IdleRecoveredCadence, error) {
	if connector == nil {
		return nil, ErrInvalidConfiguration
	}
	return reconnectIdleCadence(ctx, concreteReconnectPort{connector: connector}, input, materializer, adapterConfig, pollConfig)
}

func reconnectIdleCadence(
	ctx context.Context,
	port reconnectPort,
	input attachedworkersession.ReconnectInputV1,
	materializer Materializer,
	adapterConfig Config,
	pollConfig attachedworkertransport.Config,
) (*IdleRecoveredCadence, error) {
	if ctx == nil || port == nil || materializer == nil {
		return nil, ErrInvalidConfiguration
	}
	if !pollConfig.Enabled {
		return nil, attachedworkertransport.ErrPollingDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Persistent local mistakes must not consume a connection generation,
	// challenge, activation, or Manifest on every new process. Recheck the
	// adapter after reconnect as well, in case its filesystem root changes.
	if _, err := prepareAdapterConfig(adapterConfig); err != nil {
		return nil, err
	}
	pollConfig.StartWithCooldown = true
	preparedPollConfig, err := attachedworkertransport.PrepareConfig(pollConfig)
	if err != nil {
		return nil, err
	}
	session, err := port.Reconnect(ctx, input)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, ErrReconciliationRequired
	}
	cleanup := func(prior error) (*IdleRecoveredCadence, error) {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconnectCadenceCleanupTimeout)
		defer cancel()
		return nil, errors.Join(prior, session.Close(closeCtx))
	}
	recovery, err := session.ReconnectRecovery()
	if err != nil {
		return cleanup(errors.Join(ErrReconciliationRequired, err))
	}
	if recovery.ConnectionState != attachedworkerprotocol.ConnectionReady ||
		recovery.AttemptState != attachedworkerprotocol.AttemptIdle ||
		recovery.TerminalDecision != attachedworkerprotocol.ReconnectTerminalNone || recovery.Terminal != nil {
		return cleanup(ErrReconciliationRequired)
	}
	adapter, err := New(session, materializer, adapterConfig)
	if err != nil {
		// A root/profile that changed after the pre-network check is no longer
		// a harmless local validation failure: reconnect already had an effect.
		return cleanup(ErrReconciliationRequired)
	}
	if _, err := adapter.readySnapshot(); err != nil {
		return cleanup(err)
	}
	// Reconnect already accepted and checkpointed a Manifest. The first idle
	// Heartbeat must not be immediate. This cooldown is conservative local
	// scheduling; it does not substitute for server-side durable authority.
	source, err := NewPreparedCadencedSource(adapter, preparedPollConfig)
	if err != nil {
		return cleanup(err)
	}
	return &IdleRecoveredCadence{Source: source, Adapter: adapter, session: session}, nil
}

func (cadence *IdleRecoveredCadence) Close(ctx context.Context) error {
	if cadence == nil || cadence.session == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	return cadence.session.Close(ctx)
}
