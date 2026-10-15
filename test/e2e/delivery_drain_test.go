//go:build e2elocal

package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type deliveryDrainRead func(context.Context, runRef) (domain.TelegramDeliveryOutbox, uint64, error)

// Terminal Run state commits before asynchronous frontend materialization.
// Observe the same Run's sent delivery AND removal of its projection records;
// neither a missing delivery nor an empty queue alone establishes completion.
func awaitTelegramDeliveryDrain(ctx context.Context, interval time.Duration, runs []runRef, read deliveryDrainRead) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		complete := true
		observations := make([]string, 0, len(runs))
		for _, run := range runs {
			delivery, pending, err := read(ctx, run)
			if errors.Is(err, sql.ErrNoRows) {
				observations = append(observations, fmt.Sprintf("run=%s delivery=missing", run.RunID))
				complete = false
				continue
			}
			if err != nil {
				return fmt.Errorf("run %s delivery drain: %w", run.RunID, err)
			}
			if delivery.TenantID != run.TenantID || delivery.RunID != run.RunID ||
				delivery.Projection == nil || delivery.Projection.SessionID != run.SessionID {
				return fmt.Errorf("run %s delivery drain: foreign or noncanonical delivery", run.RunID)
			}
			observations = append(observations, fmt.Sprintf("run=%s delivery=%s pending=%d", run.RunID, delivery.Status, pending))
			if delivery.Status != domain.DeliverySent || pending != 0 {
				complete = false
			}
		}
		if complete {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("delivery drain (%s): %w", strings.Join(observations, "; "), err)
		}
		select {
		case <-ctx.Done():
			// Re-observe completion before reporting a readiness timeout. The
			// reader retains this same canceled context; no new query grace.
		case <-ticker.C:
		}
	}
}

func drainFixture(run runRef, status domain.DeliveryStatus) domain.TelegramDeliveryOutbox {
	return domain.TelegramDeliveryOutbox{
		TenantID: run.TenantID, RunID: run.RunID, Status: status,
		Projection: &domain.TelegramProjectionRef{SessionID: run.SessionID},
	}
}

func TestAwaitTelegramDeliveryDrainStages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		var phase atomic.Int32
		done := make(chan error, 1)
		go func() {
			done <- awaitTelegramDeliveryDrain(ctx, time.Second, []runRef{{RunID: "run-a"}},
				func(_ context.Context, run runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
					switch phase.Load() {
					case 0:
						return domain.TelegramDeliveryOutbox{}, 0, sql.ErrNoRows
					case 1:
						return drainFixture(run, domain.DeliveryPending), 0, nil
					case 2:
						return drainFixture(run, domain.DeliverySent), 1, nil
					default:
						return drainFixture(run, domain.DeliverySent), 0, nil
					}
				})
		}()
		for next := int32(1); next <= 3; next++ {
			synctest.Wait()
			select {
			case err := <-done:
				t.Fatalf("phase %d completed before sent delivery and empty projection queue: %v", next-1, err)
			default:
			}
			phase.Store(next)
			time.Sleep(time.Second)
		}
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestAwaitTelegramDeliveryDrainAllRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		var ready atomic.Bool
		done := make(chan error, 1)
		go func() {
			done <- awaitTelegramDeliveryDrain(ctx, time.Second, []runRef{{RunID: "run-a"}, {RunID: "run-b"}},
				func(_ context.Context, run runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
					if run.RunID == "run-b" && !ready.Load() {
						return domain.TelegramDeliveryOutbox{}, 0, sql.ErrNoRows
					}
					return drainFixture(run, domain.DeliverySent), 0, nil
				})
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("one ready Run incorrectly completed both Runs: %v", err)
		default:
		}
		ready.Store(true)
		time.Sleep(time.Second)
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestAwaitTelegramDeliveryDrainPreservesReadFailure(t *testing.T) {
	readErr := errors.New("synthetic malformed delivery/query failure")
	reads := 0
	err := awaitTelegramDeliveryDrain(context.Background(), time.Second, []runRef{{RunID: "run-a"}},
		func(context.Context, runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
			reads++
			return domain.TelegramDeliveryOutbox{}, 0, readErr
		})
	if !errors.Is(err, readErr) || reads != 1 || !strings.Contains(err.Error(), "run-a") {
		t.Fatalf("got err=%v reads=%d, want original causal error/run and no retry", err, reads)
	}
}

func TestAwaitTelegramDeliveryDrainDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		t.Cleanup(cancel)
		err := awaitTelegramDeliveryDrain(ctx, time.Second, []runRef{{RunID: "run-a"}},
			func(_ context.Context, run runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
				return drainFixture(run, domain.DeliverySent), 1, nil
			})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "pending=1") || !strings.Contains(err.Error(), "run-a") {
			t.Fatalf("got %v, want bounded timeout with observed Run/projection state", err)
		}
	})
}

func TestAwaitTelegramDeliveryDrainRejectsForeignProjection(t *testing.T) {
	run := runRef{TenantID: "tenant-a", RunID: "run-a", SessionID: "session-a"}
	for _, name := range []string{"tenant", "run", "session", "missing-projection"} {
		t.Run(name, func(t *testing.T) {
			delivery := drainFixture(run, domain.DeliverySent)
			switch name {
			case "tenant":
				delivery.TenantID = "tenant-b"
			case "run":
				delivery.RunID = "run-b"
			case "session":
				delivery.Projection.SessionID = "session-b"
			case "missing-projection":
				delivery.Projection = nil
			}
			err := awaitTelegramDeliveryDrain(context.Background(), time.Second, []runRef{run},
				func(context.Context, runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
					return delivery, 0, nil
				})
			if err == nil || !strings.Contains(err.Error(), "foreign or noncanonical") {
				t.Fatalf("got %v, want denial of %s delivery association", err, name)
			}
		})
	}
}

func TestAwaitTelegramDeliveryDrainCanceledBeforeRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := awaitTelegramDeliveryDrain(ctx, time.Second, []runRef{{RunID: "run-a"}},
		func(context.Context, runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
			t.Fatal("canceled wait performed a read")
			return domain.TelegramDeliveryOutbox{}, 0, nil
		})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
