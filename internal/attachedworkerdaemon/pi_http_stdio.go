package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// piHTTPStdio is a daemon-private mechanics arm, not a source-selected provider
// driver or a prepared producer. It can attach only through the SAME Supervisor
// Run. Even successful actual pipe observations never fill result.piStdio.
// Real original tuple/Issue/Materialize/file/HTTP quarantine qualification is
// still required before the source owner can use this arm or stamp a result.
type piHTTPStdio struct {
	owner  *Supervisor
	send   *preparedAPIPhysicalSend
	ledger PreparedAPILedgerV1
	core   *piHTTPCoordinator
	ctx    context.Context
	cancel context.CancelFunc
	driver func(context.Context, *piHTTPIO) error // trusted Go only; no public registration
	lease  piHTTPIO
	live   uint64
	state  atomic.Uint32 // prepared0, selected by Run1, Run returned2, retired3, retiring4

	ioMu                                      sync.Mutex
	leaseMu                                   sync.Mutex // closes admission before joining active writes
	stopped                                   bool
	started                                   bool
	stdin                                     *os.File
	stdoutFile                                *os.File
	stdout                                    io.Reader
	observed                                  *boundedBuffer
	violation                                 chan<- string
	doneOnce                                  sync.Once
	done, readerDone, watchDone, requestReady chan struct{}
	creditWake, endWake, writeWake            chan struct{} // one waiter per signal
	writing, leaseActive                      atomic.Bool
	failure                                   atomic.Bool
	lastParent                                atomic.Uint32
	complete                                  bool // published only by closing done
	initTicket                                piHTTPTicket
	initWire                                  []byte // owned by core, never caller memory
	header                                    [17]byte
	pullMetadata                              [4]byte
	requestDigest, stdoutDigest               [sha256.Size]byte
	digest                                    hash.Hash
	childBytes                                uint64 // sole reader owns count/hash/scratch
	clearErr                                  error
	retireMu                                  sync.Mutex
	afterWrite                                func(pisdkhttpwire.Kind) // test-only completed syscall barrier; never an approval
	afterRequest                              func()                   // test-only admission barrier; held through borrowed-frame validation
}

type piHTTPIO struct{ owner *piHTTPStdio }

func (b *piHTTPStdio) selectForRun(parent context.Context) error {
	if !sameOriginalAPIContext(parent, b.ctx) || !b.state.CompareAndSwap(0, 1) {
		return ErrSupervisorConfig
	}
	return nil
}

func (*piHTTPStdio) String() string     { return "piHTTPStdio{data:[redacted]}" }
func (b *piHTTPStdio) GoString() string { return b.String() }
func (piHTTPIO) String() string         { return "piHTTPIO{private:[redacted]}" }
func (v piHTTPIO) GoString() string     { return v.String() }

func measurePiHTTPStdio(l pisdkhttpwire.Limits) (piHTTPCoordinatorBudget, error) {
	a, err := pisdkhttpwire.Measure(l)
	if err != nil {
		return piHTTPCoordinatorBudget{}, err
	}
	// Seven fixed signal records, one fixed Go SHA state and context/parent-edge
	// allowance. This is logical Go storage, NOT scheduler/allocator/native fit.
	m := uint64(reflect.TypeOf(piHTTPStdio{}).Size()) + 7*256 + 512 + 2048
	return piHTTPCoordinatorBudget{Live: m, Work: 8*a.ChildBytes + 2*a.ParentBytes + 2*m, Nodes: 16}, nil
}

func preparePiHTTPStdio(s *Supervisor, l pisdkhttpwire.Limits, ledger PreparedAPILedgerV1,
	send *preparedAPIPhysicalSend, init pisdkhttpwire.Frame, driver func(context.Context, *piHTTPIO) error) (*piHTTPStdio, error) {
	p, err := preparePiHTTPStdioPlan(s, l, ledger, driver)
	if err != nil {
		return nil, err
	}
	if err := p.bind(send, init); err != nil {
		return nil, errors.Join(err, p.clear())
	}
	return p, nil
}

// Reserve every payload arena and fixed signal before Issue. The genuine send,
// its IO context and Materialize's exact Init do not exist yet at this point.
func preparePiHTTPStdioPlan(s *Supervisor, l pisdkhttpwire.Limits, ledger PreparedAPILedgerV1,
	driver func(context.Context, *piHTTPIO) error) (*piHTTPStdio, error) {
	b, err := measurePiHTTPStdio(l)
	if err != nil || s == nil || driver == nil || nilPreparedAPIDependency(ledger) || !pisdkhttpwire.SequencePreparationSupported() {
		return nil, ErrSupervisorConfig
	}
	if ledger.Reserve(b.Work, b.Live, b.Nodes) != nil {
		return nil, ErrSupervisorConfig
	}
	core, err := preparePiHTTPCoordinator(l, ledger)
	if err != nil {
		return nil, errors.Join(err, ledger.EndLive(b.Live))
	}
	p := &piHTTPStdio{owner: s, ledger: ledger, core: core,
		driver: driver, live: b.Live, digest: sha256.New(),
		done: make(chan struct{}), readerDone: make(chan struct{}), watchDone: make(chan struct{}), requestReady: make(chan struct{}),
		creditWake: make(chan struct{}, 1), endWake: make(chan struct{}, 1), writeWake: make(chan struct{}, 1)}
	p.lease.owner = p
	return p, nil
}

// bind has one caller under the owning plan's once-only Run admission. It
// snapshots the now-materialized Init into the already-funded outbound arena.
// Only fixed context bookkeeping is created here, never a new payload pool or
// Source deadline. The original send still owns the pre-Issue deadline monitor.
func (b *piHTTPStdio) bind(send *preparedAPIPhysicalSend, init pisdkhttpwire.Frame) error {
	if b == nil || b.send != nil || b.ctx != nil || send == nil || !samePiLedger(b.ledger, send.ledger) ||
		send.ownerCtx == nil || send.ownerCtx.Err() != nil || init.Kind != pisdkhttpwire.Initialize {
		return ErrSupervisorConfig
	}
	ioParent := send.deadline.ioContext()
	if ioParent == nil || ioParent.Err() != nil {
		return ErrSupervisorConfig
	}
	ticket, raw, err := b.core.beginWrite(init)
	if err != nil {
		return err
	}
	b.initTicket, b.initWire, b.send = ticket, raw, send
	b.ctx, b.cancel = context.WithCancel(ioParent)
	_ = b.ctx.Done()
	return nil
}

func piHTTPWake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// stopIO marks withdrawal under the same lock used for descriptor publication.
// No sync.Once is consumed on nil descriptors: any later attach closes its
// actual descriptors immediately. Never hold this lock during IO or a join.
func (b *piHTTPStdio) stopIO() {
	if b.cancel != nil {
		b.cancel()
	}
	b.core.abort()
	b.ioMu.Lock()
	b.stopped = true
	in, out := b.stdin, b.stdoutFile
	b.ioMu.Unlock()
	if in != nil {
		_ = in.Close()
	}
	if out != nil {
		_ = out.Close()
	}
}

func (b *piHTTPStdio) fail() {
	b.failure.Store(true)
	if b.send != nil {
		b.send.invalid.Store(true)
		b.send.revoke()
	}
	b.stopIO()
	b.ioMu.Lock()
	v := b.violation
	b.ioMu.Unlock()
	select {
	case v <- "pi_http_stdio_invalid":
	default:
	}
}

func (b *piHTTPStdio) start(source io.Reader, sourceFile, sink *os.File, observed *boundedBuffer, violation chan<- string) <-chan struct{} {
	b.ioMu.Lock()
	if b.started || b.stopped || b.ctx.Err() != nil {
		b.ioMu.Unlock()
		_ = sourceFile.Close()
		_ = sink.Close()
		b.failure.Store(true)
		b.doneOnce.Do(func() { close(b.done) })
		return b.done
	}
	b.stdout, b.stdoutFile, b.stdin, b.observed, b.violation = source, sourceFile, sink, observed, violation
	b.started = true
	b.ioMu.Unlock()
	go func() {
		defer close(b.watchDone)
		select {
		case <-b.ctx.Done():
			b.stopIO()
		case <-b.done:
		}
	}()
	go b.pump()
	return b.done // MUST NOT wait for Initialize Write here
}

func (b *piHTTPStdio) authorizedForNewWrite() bool {
	if b.ctx.Err() != nil || !b.send.mu.TryLock() {
		return false
	}
	defer b.send.mu.Unlock()
	return b.send.authorized && !b.send.failed && !b.send.invalid.Load() && !b.send.retired.Load() &&
		b.send.ctx != nil && b.send.ctx.Err() == nil && b.send.cutoff != b.send.cutoff.Round(0) && time.Now().Before(b.send.cutoff)
}

func (b *piHTTPStdio) finishWrite(t piHTTPTicket, raw []byte, kind pisdkhttpwire.Kind) error {
	n, err := 0, error(nil)
	if b.ctx.Err() != nil {
		err = b.ctx.Err()
	} else {
		n, err = b.stdin.Write(raw)
	}
	if b.afterWrite != nil {
		b.afterWrite(kind)
	}
	if b.ctx.Err() != nil && err == nil {
		err = b.ctx.Err()
	}
	commitErr := b.core.completeWrite(t, n, err)
	if commitErr != nil {
		b.fail()
		return commitErr
	}
	b.lastParent.Store(uint32(kind))
	piHTTPWake(b.creditWake) // may have committed metadata-first pending Pull
	if kind == pisdkhttpwire.ResponseEnd {
		piHTTPWake(b.endWake)
	}
	return nil
}

func (b *piHTTPStdio) pump() {
	defer b.doneOnce.Do(func() { close(b.done) })
	if b.finishWrite(b.initTicket, b.initWire, pisdkhttpwire.Initialize) != nil {
		return
	}
	b.initWire = nil
	if b.core.startReader() != nil {
		b.fail()
		return
	}
	go func() {
		defer close(b.readerDone)
		if b.readStream() != nil {
			b.fail()
		}
		if b.core.endReader() != nil {
			b.fail()
		}
	}()
	b.leaseActive.Store(true)
	driverErr := b.driver(b.ctx, &b.lease)
	b.leaseMu.Lock()
	b.leaseActive.Store(false)
	b.leaseMu.Unlock()
	if driverErr != nil {
		b.fail()
	}
	// A returning driver cannot abandon a writer still borrowing the arena.
	for b.writing.Load() {
		<-b.writeWake
	}
	if b.core.endDriver(driverErr) != nil || b.lastParent.Load() != uint32(pisdkhttpwire.ResponseEnd) {
		b.fail()
	}
	<-b.readerDone // nil driver return does not cancel Final/EOF drain
	if !b.failure.Load() && b.ctx.Err() == nil {
		b.digest.Sum(b.stdoutDigest[:0])
		b.complete = true
	}
}

func (v *piHTTPIO) request() (pisdkhttpwire.Frame, [32]byte, error) {
	return v.requestChecked(nil)
}

func (v *piHTTPIO) requestChecked(expected *preparedPiHTTPState) (pisdkhttpwire.Frame, [32]byte, error) {
	if v == nil || v.owner == nil {
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	b := v.owner
	select {
	case <-b.requestReady:
	case <-b.ctx.Done():
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	b.leaseMu.Lock()
	defer b.leaseMu.Unlock()
	if !b.leaseActive.Load() || b.state.Load() != 1 || b.ctx.Err() != nil {
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	f, err := b.core.borrowRequest()
	if err != nil {
		b.fail()
		return pisdkhttpwire.Frame{}, [32]byte{}, err
	}
	if b.afterRequest != nil {
		b.afterRequest()
	}
	if b.ctx.Err() != nil {
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	// Keep admission until every wrapper-owned payload read finishes. Pump's
	// deactivation must join this check before completion permits retirement.
	if expected != nil && (f.Profile != expected.profileSHA || len(f.Body) != expected.requestLen[0] || sha256.Sum256(f.Body) != expected.requestSHA[0]) {
		b.fail()
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	return f, b.requestDigest, err
}

func (v *piHTTPIO) write(f pisdkhttpwire.Frame) error {
	if v == nil || v.owner == nil {
		return ErrSupervisorConfig
	}
	b := v.owner
	b.leaseMu.Lock()
	if !b.leaseActive.Load() || b.state.Load() != 1 || b.ctx.Err() != nil {
		b.leaseMu.Unlock()
		return ErrSupervisorConfig
	}
	claimed := b.writing.CompareAndSwap(false, true)
	if !claimed {
		// The rejected caller still borrows failure/cancellation fields. Keep
		// it inside admission ownership until fail returns, so pump deactivation
		// cannot publish aggregate completion underneath this losing writer.
		b.fail()
		b.leaseMu.Unlock()
		return ErrSupervisorConfig
	}
	b.leaseMu.Unlock()
	defer func() { b.writing.Store(false); piHTTPWake(b.writeWake) }()
	if !b.leaseActive.Load() || f.Kind != pisdkhttpwire.ResponseStart && f.Kind != pisdkhttpwire.ResponsePart && f.Kind != pisdkhttpwire.ResponseEnd {
		b.fail()
		return ErrSupervisorConfig
	}
	if f.Kind == pisdkhttpwire.ResponsePart {
		for {
			b.core.mu.Lock()
			ready := b.core.credit
			failed := b.core.failed.Load()
			b.core.mu.Unlock()
			if failed || b.ctx.Err() != nil {
				b.fail()
				return ErrSupervisorConfig
			}
			if ready {
				break
			}
			select {
			case <-b.creditWake:
			case <-b.ctx.Done():
				b.fail()
				return ErrSupervisorConfig
			}
		}
	}
	if !b.authorizedForNewWrite() {
		b.fail()
		return ErrCredentialUnavailable
	}
	t, raw, err := b.core.beginWrite(f)
	if err != nil {
		b.fail()
		return err
	}
	return b.finishWrite(t, raw, f.Kind)
}

// readExact records every actual positive byte, including Read(n>0,err). It
// reads no byte beyond its caller's already-admitted fixed region. Only a
// first-byte probe may carry EOF; region truncation never becomes wire EOF.
func (b *piHTTPStdio) readExact(dst []byte) (int, error) {
	read := 0
	for read < len(dst) {
		if b.ctx.Err() != nil {
			return read, ErrSupervisorConfig
		}
		n, err := b.stdout.Read(dst[read:])
		if n < 0 || n > len(dst)-read {
			return read, ErrSupervisorConfig
		}
		if n > 0 {
			piece := dst[read : read+n]
			b.childBytes += uint64(n)
			_, _ = b.digest.Write(piece)
			_, observedErr := b.observed.Write(piece)
			read += n
			if observedErr != nil || b.childBytes > b.core.limits.ChildBytes {
				return read, ErrSupervisorConfig
			}
		}
		if err != nil {
			if read == len(dst) && errors.Is(err, io.EOF) {
				return read, nil
			}
			return read, err
		}
		if n == 0 {
			return read, ErrSupervisorConfig
		}
	}
	return read, nil
}

func (b *piHTTPStdio) readStream() error {
	for {
		n, err := b.readExact(b.header[:1])
		if err != nil {
			if n == 0 && errors.Is(err, io.EOF) {
				return b.core.observeEOF()
			}
			return ErrSupervisorConfig
		}
		ticket, err := b.core.observePrefix(b.header[0])
		if err != nil {
			return err
		}
		if _, err = b.readExact(b.header[1:]); err != nil {
			return err
		}
		if err = b.core.acceptHeader(ticket, b.header[1:]); err != nil {
			return err
		}
		h, err := pisdkhttpwire.InspectHeader(b.header[:], pisdkhttpwire.ChildToParent, b.core.limits)
		if err != nil {
			return err
		}
		var metadata []byte
		switch h.Kind {
		case pisdkhttpwire.FetchRequest:
			metadata = b.core.request[17 : 17+int(h.MetadataBytes)]
		case pisdkhttpwire.Pull:
			metadata = b.pullMetadata[:]
		}
		if _, err = b.readExact(metadata); err != nil {
			return err
		}
		if err = b.core.acceptMetadata(ticket, metadata); err != nil {
			return err
		}
		piHTTPWake(b.creditWake)
		if h.Kind == pisdkhttpwire.Pull {
			continue
		} // keep probing, never wait on credit consumption
		for {
			_, ready, e := b.core.bodyReady(ticket)
			if e != nil {
				return e
			}
			if ready {
				break
			}
			select {
			case <-b.endWake:
			case <-b.ctx.Done():
				return ErrSupervisorConfig
			}
		}
		var body []byte
		if h.Kind == pisdkhttpwire.FetchRequest {
			offset := 17 + int(h.MetadataBytes)
			body = b.core.request[offset : offset+int(h.BodyBytes)]
		} else {
			body = b.core.final[:int(h.BodyBytes)]
		}
		if _, err = b.readExact(body); err != nil {
			return err
		}
		if err = b.core.acceptBody(ticket, body); err != nil {
			return err
		}
		if h.Kind == pisdkhttpwire.FetchRequest {
			b.requestDigest = sha256.Sum256(b.core.request[:int(h.TotalBytes)])
			close(b.requestReady) // publish frozen actual-read digest before borrowed views
		}
	}
}

// stopAndJoin is bounded cleanup DATA. An uncooperative trusted Go driver may
// prevent join: preserve the plan/debit and report failure, NEVER clear beneath
// it or turn a cleanup timeout into evidence that the borrower has returned.
func (b *piHTTPStdio) stopAndJoin(grace time.Duration) bool {
	b.stopIO()
	b.ioMu.Lock()
	started := b.started
	b.ioMu.Unlock()
	if !started {
		if b.initTicket.owner != nil {
			_ = b.core.completeWrite(b.initTicket, 0, ErrSupervisorConfig)
		}
		b.initWire = nil
		return true
	}
	joined, _ := waitProcessSignal(b.done, grace)
	watchJoined, _ := waitProcessSignal(b.watchDone, grace)
	return joined && watchJoined
}

func waitProcessSignal(done <-chan struct{}, grace time.Duration) (bool, error) {
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return true, nil
	case <-timer.C:
		select {
		case <-done:
			return true, nil
		default:
			return false, ErrSupervisorConfig
		}
	}
}

func (b *piHTTPStdio) clear() error {
	if !b.retireMu.TryLock() {
		return ErrPiHTTPRetirementPending
	}
	defer b.retireMu.Unlock()
	for {
		state := b.state.Load()
		if state == 1 {
			b.stopIO()
			return ErrPiHTTPRetirementPending
		}
		if state == 3 {
			return b.clearErr
		}
		if state != 0 && state != 2 {
			return ErrPiHTTPRetirementPending
		}
		if b.state.CompareAndSwap(state, 4) {
			break
		} // excludes Run BEFORE dropping any alias
	}
	if !b.stopAndJoin(b.owner.grace) {
		b.state.Store(2) // Run already returned, but actual borrowers still own live state
		return ErrPiHTTPRetirementPending
	}
	if err := b.core.clear(); err != nil {
		b.core.mu.Lock()
		retired := b.core.cleared
		b.core.mu.Unlock()
		if retired {
			b.clearErr = err
			b.state.Store(3) // terminal coordinator accounting error
			return err
		}
		b.state.Store(2) // still borrowed; no accounting attempted
		return errors.Join(ErrPiHTTPRetirementPending, err)
	}
	b.driver, b.digest, b.send = nil, nil, nil
	// owner is immutable: retained *lease/copies reject on atomic lifecycle,
	// without racing an unsynchronized pointer clear after return.
	b.ioMu.Lock()
	b.stdin, b.stdoutFile, b.stdout, b.observed = nil, nil, nil, nil
	b.ioMu.Unlock()
	b.clearErr = b.ledger.EndLive(b.live)
	if b.clearErr == nil {
		b.ledger, b.live = nil, 0
	}
	b.state.Store(3)
	return b.clearErr
}
