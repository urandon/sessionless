package providercredential

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// PrepareHTTPIOV1 fixes the concrete original-file driver BEFORE Issue. There
// is no caller driver, endpoint, response, root bundle or approval override.
// The private installed HTTPS policy must already be prepared; no production
// source factory currently selects it, so this does not clear MissingProducer.
// The returned opaque plan remains mechanics DATA, never #200 custody.
func (p *PreparedPiFileUseV1) PrepareHTTPIOV1(supervisor *attachedworkerdaemon.Supervisor, request attachedworkerdaemon.PreparedAPIRequestV1) (*attachedworkerdaemon.PreparedPiHTTPStdioV1, error) {
	if p == nil || p.state == nil {
		return nil, ErrInvalid
	}
	s := p.state
	if !s.mu.TryLock() {
		s.abort()
		return nil, ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.ioPrepared || s.claimed.Load() || s.callback != 0 || s.auth == nil || s.http == nil || s.responseScan == nil || s.expected == nil || s.network.Load() == nil || s.file == nil || s.file.ctx.Err() != nil {
		s.abort()
		return nil, ErrConsumed
	}
	s.ioPrepared = true
	f := s.file
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	if f.service.preparedPiFiles[f.scope] != s ||
		([4]int{len(request.Body), len(request.Model), len(request.Schema), len(request.Headers)}) != s.auth.sourceLengths {
		s.abort()
		return nil, ErrInvalid
	}
	l := s.auth.responseLimits
	work, chunk, err := measurePiHTTPReplay(l, s.auth.sourceLengths)
	if err != nil || s.ledger.Reserve(work, 256, 1) != nil {
		s.abort()
		return nil, ErrInvalid
	}
	s.live += 256
	hashes := [4][32]byte{sha256.Sum256(request.Body), sha256.Sum256(request.Model), sha256.Sum256(request.Schema), sha256.Sum256(request.Headers)}
	if request.Profile != s.auth.sourceProfile || !bytes.Equal(request.Body, s.body) || sha256.Sum256(s.body) != s.bodyHash || hashes != s.auth.sourceHashes {
		s.abort()
		return nil, ErrInvalid
	}
	id := attachedworkerdaemon.InvocationIdentity{TenantID: f.scope.TenantID, OwnerUserID: f.scope.OwnerUserID,
		WorkerID: domain.AttachedWorkerID(f.scope.WorkerID), RunID: f.scope.RunID, AttemptID: f.scope.AttemptID, LeaseID: f.scope.LeaseID, FenceToken: f.scope.LeaseFence}
	plan, err := supervisor.PreparePiHTTPStdioV1(id, request, s.ledger, s.nonce, s.auth.binding, s.runHTTPIO)
	if err != nil {
		s.abort()
		return nil, err
	}
	s.ioPlan = plan
	s.ioChunk = chunk // same canonical SPU3 C; plan validated all remaining bounds
	if s.stopped.Load() || f.ctx.Err() != nil {
		s.abort()
		return nil, ErrConsumed
	}
	return plan, nil
}

// Independent original-ledger partition for THIS driver's work, not the wire
// encoder/coordinator's separately funded work or a native heap certificate.
// Only fixed integer metadata is read before admission. Even C=1 pays for
// every Frame construction, argument/interface copy and loop/control step.
func measurePiHTTPReplay(l domain.AttachedPiSDKNativeLimitsV1, lengths [4]int) (work, chunk uint64, err error) {
	if l.ProcessFrameBytes < 114 || l.ProcessFrameBytes > 1<<20 ||
		l.ResponseBodyBytes == 0 || l.ResponseBodyBytes > 4<<20 ||
		l.ResponseHeaderBytes < 2 || l.ResponseHeaderBytes > 1<<20 ||
		l.ResponseChunkBytes == 0 || l.ResponseChunkBytes > l.ResponseBodyBytes ||
		l.ProcessEvents == 0 || l.ProcessEvents > 4096 {
		return 0, 0, ErrInvalid
	}
	chunk = min(l.ResponseChunkBytes, l.ProcessFrameBytes-21)
	n := 1 + (l.ResponseBodyBytes-1)/chunk
	if 6+2*n > l.ProcessEvents { // canonical SPU3 maximum, including pulls
		return 0, 0, ErrInvalid
	}
	// Four value appearances plus 256 scalar/control work units per emitted
	// Start/Part/End. Eight payload passes cover comparisons and hashes; profile
	// equality can scan only original-length strings (unequal lengths are O(1)).
	frame := uint64(reflect.TypeOf(pisdkhttpwire.Frame{}).Size())
	profile := uint64(reflect.TypeOf(domain.AttachedPiSDKNativeProfileV1{}).Size())
	work = (n+2)*(4*frame+256) + 8*l.ResponseBodyBytes + 8*l.ResponseHeaderBytes +
		8*uint64(domain.AttachedPiSDKCanonicalMaxBytesV1) + 4*profile + 16384
	for _, length := range lengths {
		if length < 0 || uint64(length) > (^uint64(0)-work)/8 {
			return 0, 0, ErrInvalid
		}
		work += 8 * uint64(length)
	}
	return work, chunk, nil
}

// This method value is fixed by preparation, never supplied by a caller. It
// neither launches another runner nor issues/finalizes/releases credentials.
func (s *preparedPiFileUse) runHTTPIO(ctx context.Context, lease *attachedworkerdaemon.PiHTTPIOLeaseV1) (err error) {
	defer func() {
		if err != nil {
			s.abort()
			lease.Abort()
		}
	}()
	if ctx == nil || ctx.Err() != nil || lease == nil {
		return ErrConsumed
	}
	frame, _, err := lease.Request()
	if err != nil {
		return err
	}
	borrow, err := s.beginHTTPS(frame) // Source/F, actual HTTP EOF and ALL quarantine checks
	if err != nil {
		return err
	}
	defer borrow.close() // failed/partial writes abort; completed End consumed this latch
	if err = borrow.replay(ctx, lease, s.ioChunk); err != nil {
		return err
	}
	return nil
}

// Only this PRIVATE helper has an interface seam for byte/lifetime fixtures.
// Production runHTTPIO passes solely the daemon's concrete opaque lease.
type piHTTPReplaySink interface {
	Write(pisdkhttpwire.Frame) error
}

func (b *piHTTPSResponseBorrow) replay(ctx context.Context, sink piHTTPReplaySink, chunk uint64) error {
	if b == nil || b.closed.Load() || ctx == nil || ctx.Err() != nil || sink == nil || chunk == 0 ||
		len(b.view.entity) == 0 || uint64(len(b.view.entity)) > b.s.auth.responseBody || uint64(len(b.view.headers)) > b.s.auth.responseHeaders ||
		chunk > b.s.auth.responseLimits.ResponseChunkBytes || chunk > b.s.auth.responseLimits.ProcessFrameBytes-21 {
		return ErrInvalid
	}
	n := uint64(len(b.view.entity))
	count := uint32(1 + (n-1)/chunk)
	digest := sha256.Sum256(b.view.entity)
	if err := sink.Write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseStart, Count: count, Bytes: n, Digest: digest, Headers: b.view.headers}); err != nil {
		return err
	}
	if b.s.afterReplayWrite != nil {
		b.s.afterReplayWrite(pisdkhttpwire.ResponseStart, b.ctx)
	}
	for i, offset := uint32(0), uint64(0); offset < n; i++ {
		end := min(n, offset+chunk)
		if ctx.Err() != nil {
			return ErrConsumed
		}
		if err := sink.Write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponsePart, Index: i, Body: b.view.entity[int(offset):int(end)]}); err != nil {
			return err
		}
		if b.s.afterReplayWrite != nil {
			b.s.afterReplayWrite(pisdkhttpwire.ResponsePart, b.ctx)
		}
		offset = end
	}
	if ctx.Err() != nil {
		return ErrConsumed
	}
	if err := sink.Write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseEnd, Count: count, Bytes: n, Digest: digest}); err != nil {
		return err
	}
	if b.s.afterReplayWrite != nil {
		b.s.afterReplayWrite(pisdkhttpwire.ResponseEnd, b.ctx)
	}
	return b.finishReplay() // no Source-live test after committed End
}
