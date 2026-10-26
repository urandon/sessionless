package attachedworkerdaemontransport

import (
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
)

// NewPreparedAdapter joins the original authenticated Session and the exact
// manifest-pinned prepared Stack. Initial and idle-reconnect runtime assembly
// use this same post-connect path. It never constructs a Session or a runner,
// advances a connection generation, or performs a model/transport operation.
// Lease-held Stack preflight must still precede Connector network effects.
//
// The caller retains cleanup ownership of Session and Stack on every return.
// Configuration is checked before the original one-shot bind. A failed bind
// is not permission to replace either original owner or infer replay safety.
func NewPreparedAdapter(session *attachedworkersession.Session, materializer Materializer, config Config, stack *attachedworkerstack.Stack) (*Adapter, error) {
	if session == nil || stack == nil || materializer == nil || config.CapabilityManifest == nil ||
		config.ReceiptCandidate != nil || config.preparedReceipt != nil || config.preparedInlineV2 {
		return nil, ErrInvalidConfiguration
	}
	inline, err := stack.PreparedInlineOutputV2(config.Profile.Executable, config.Profile.ExecutableDigest, config.Profile.Arguments, config.Profile.Environment, *config.CapabilityManifest)
	if err != nil {
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	var original attachedworkerdaemon.Runner = stack
	prepared := &preflightRunner{current: &original, inlineOutputV2: inline}
	if err := installPreparedReceipt(&config, *config.CapabilityManifest, prepared); err != nil {
		return nil, err
	}
	adapter, err := newAdapter(session, materializer, config)
	if err != nil {
		return nil, err
	}
	if _, err := adapter.readySnapshot(); err != nil {
		return nil, err
	}
	if err := stack.BindPreparedConnection(session); err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	return adapter, nil
}
