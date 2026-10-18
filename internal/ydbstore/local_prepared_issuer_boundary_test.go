package ydbstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The real original issuer uses the unchanged authoritative fixture clock.
// Its pre-construction clock hook only synchronizes a blocked mint/consume;
// it neither returns approval nor replaces issuer authentication or Store joins.
func TestLocalPreparedIssuerReturnBoundary(t *testing.T) {
	for _, kind := range []string{"model", "compaction"} {
		for _, phase := range []string{"mint", "consume"} {
			for _, stop := range []string{"live", "caller", "source", "invocation"} {
				t.Run(kind+"/"+phase+"/"+stop, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						var countdown atomic.Int32
						entered, release := make(chan struct{}), make(chan struct{})
						releaseBarrier := sync.OnceFunc(func() { close(release) })
						barrier := func() {
							if countdown.Load() > 0 && countdown.Add(-1) == 0 {
								close(entered)
								<-release
							}
						}
						f, engine := localPreparedModelFixture(t, 0, barrier)
						var op domain.AttemptOperationV1
						var m *localPreparedModelV1
						var g *localCompactionGenerationV1
						var err error
						if kind == "model" {
							m, err = f.entry.PrepareModel(t.Context())
							if err == nil {
								op = m.operation
							}
						} else {
							g, err = f.entry.PrepareCompaction(t.Context())
							if err == nil {
								op = g.operation
							}
						}
						if err != nil {
							t.Fatal("original preparation", err)
						}
						installPreparedFutureProxy(t, f, op)
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						var reserved ports.ReserveAttemptOperationResultV1
						if phase == "consume" {
							reserved, err = f.store.ReserveAttemptOperation(ctx, ports.ReserveAttemptOperationRequestV1{Operation: op})
							if err != nil || reserved.Capability == nil {
								t.Fatal("original reserve", err)
							}
							countdown.Store(1)
						} else {
							// operationResult mints the read-only grant first, then
							// blocks at the real send-capability mint clock.
							countdown.Store(2)
						}
						done := make(chan struct{})
						// Register release/join before launch so every fatal path,
						// including a failed Source.Close, cleans up the blocked
						// original issuer instead of stranding it inside its mutex.
						t.Cleanup(func() {
							cancel()
							releaseBarrier()
							synctest.Wait()
							select {
							case <-done:
							default:
								t.Error("original issuer operation did not finish during cleanup")
							}
						})
						go func() {
							defer close(done)
							if phase == "mint" {
								reserved, err = f.store.ReserveAttemptOperation(ctx, ports.ReserveAttemptOperationRequestV1{Operation: op})
							} else {
								err = f.store.AuthorizeAttemptOperationBeforeSend(ctx, reserved.Capability.Clone())
							}
						}()
						synctest.Wait()
						select {
						case <-entered:
						default:
							t.Fatal("original issuer did not reach the mint/consume clock barrier")
						}
						if stop == "caller" {
							cancel()
						}
						if stop == "invocation" {
							f.cancelInvocation()
						}
						if stop == "source" {
							if closeErr := f.actual.source.Close(); closeErr != nil {
								t.Fatal(closeErr)
							}
						}
						releaseBarrier()
						synctest.Wait()
						select {
						case <-done:
						default:
							t.Fatal("original issuer operation did not finish after barrier release")
						}
						if stop == "live" {
							if err != nil || reserved.Capability == nil {
								t.Fatal("live original issuer boundary denied", err)
							}
						} else if !errors.Is(err, domain.ErrMembershipDenied) || (phase == "mint" && reserved.Capability != nil) {
							t.Fatalf("%s returned fresh authority after %s: err=%v capability=%t", phase, stop, err, reserved.Capability != nil)
						}
						attempted := m != nil && m.reservationAttempted || g != nil && g.reservationAttempted
						if !attempted || engine.writes != 1 {
							t.Fatal("boundary refunded/repeated reservation")
						}
						if stop != "live" && phase == "mint" {
							if (m != nil && m.reservation.Version != 0) || (g != nil && g.reservation.Version != 0) {
								t.Fatal("canceled mint installed ready reservation")
							}
						}
						if phase == "consume" {
							if err := f.bootstrap.operate.ConsumeAttemptOperationCapability(reserved.Capability.Clone()); err == nil {
								t.Fatal("consumed original capability was refunded")
							}
						}
					})
				})
			}
		}
	}
}
