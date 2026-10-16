package providercredential

import (
	"context"
	"crypto/sha256"
	"reflect"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

// Private expectation derived ONLY from the sealed original body before Issue.
// Full grammar/decoded-key uniqueness is checked, including input. Input string
// values are validated but not retained; keys/nodes remain for duplicate checks.
// No expected snapshot, credential, source grant or approval is caller-supplied.
type piResponseExpected struct {
	graph *piResponseScan
	root  int32
	live  uint64
}

var piOriginalRequestFields = [...]string{"model", "input", "stream", "store", "max_output_tokens", "temperature", "tool_choice", "reasoning", "include", "prompt_cache_options", "truncation", "text"}

func newPiResponseExpected(ctx context.Context, ledger attachedworkerdaemon.PreparedAPILedgerV1, l domain.AttachedPiSDKNativeLimitsV1, body []byte, digest [32]byte) (_ *piResponseExpected, err error) {
	if ctx == nil || ctx.Err() != nil || nilPiLedger(ledger) || len(body) == 0 || uint64(len(body)) > l.NativeRequestBytes || uint64(len(body)) > pisdkphasewire.TransportMaxFrameBytes || l.JSONDepth == 0 || l.JSONDepth > 32 || l.JSONNodes == 0 {
		return nil, ErrInvalid
	}
	r := uint64(len(body))
	n := min(l.JSONNodes, 2*r+1)
	base := uint64(reflect.TypeOf(piResponseExpected{}).Size()) + 8192
	arena := uint64(reflect.TypeOf(piResponseScan{}).Size()) + (n+1)*uint64(reflect.TypeOf(piResponseNode{}).Size()) + 2*r
	live := base + arena + 2*r + 96*n + 8192
	// Initialization and exactly one retirement, UTF8/hash passes, and bounded
	// parser/number metadata are funded before allocation. This is logical Go
	// accounting, not a native/SDK/allocator fit attestation.
	if live > l.PreSendAllocationBytes || 2*arena+2*r > l.ResponseCopyWorkBytes || ledger.Reserve(l.ResponseCopyWorkBytes, live, 4+3*n) != nil {
		return nil, ErrInvalid
	}
	x := &piResponseExpected{live: live}
	g := &piResponseScan{limits: l, nodes: make([]piResponseNode, int(n+1)), text: make([]uint16, int(r)), ctx: ctx, maxNodes: n, work: 2*arena + 2*r}
	x.graph = g
	// Return owned cleared state on funded failure. The original file owner
	// retains its debit until safe retirement; no failed refund is hidden.
	defer func() {
		if err != nil {
			g.clear()
		}
	}()
	if !utf8.Valid(body) || sha256.Sum256(body) != digest || ctx.Err() != nil {
		return x, ErrInvalid
	}
	p := piResponseJSON{q: g, raw: body, trustedExpected: true}
	root, e := p.parse()
	if e != nil || !g.expectedContract(root) || g.denied || ctx.Err() != nil {
		return x, ErrInvalid
	}
	x.root = root
	return x, nil
}
