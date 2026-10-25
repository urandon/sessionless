package providerloopsession

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// Run uses the same finite MODEL/COMPACTION/MCP engine as CredentialedV1, with
// zero credential lifecycle calls. It owns one original start through terminal
// freeze under the same session gate. No caller engine, operation, permission
// callback, secret materializer or alternative budget enters this route.
// Publication/cleanup/completion remain explicit on the returned original owner.
func (v *LocalV1) Run(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (ModelStepResultV1, *PreparedLocalTerminalV1, error) {
	result, terminal, claimed, err := v.runNoauth(ctx, request)
	if claimed && err != nil {
		_ = v.Close()
	}
	return result, terminal, err
}

func (v *LocalV1) runNoauth(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (result ModelStepResultV1, terminal *PreparedLocalTerminalV1, claimed bool, err error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return result, nil, false, err
	}
	defer finish()
	if s.credentialed || s.started || s.runtimeStarted || s.attempted || s.entryRoute != localEntryUnselected {
		return result, nil, false, ErrSession
	}
	claimed = true
	defer func() {
		if err != nil {
			s.runtimeFailed = true
			s.cancel()
		}
	}()
	if _, err = s.startLocal(ctx, call, request); err != nil {
		return result, nil, claimed, ErrSession
	}
	result, err = s.runBoundedEngine(ctx, call)
	if err != nil {
		return result, nil, claimed, ErrIncomplete
	}
	terminal, err = s.prepareTerminal(ctx, call)
	if err != nil {
		return result, nil, claimed, ErrIncomplete
	}
	return result, terminal, claimed, nil
}
