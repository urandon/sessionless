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

type deliveryDrainKind uint8

const (
	canonicalDeliveryDrain deliveryDrainKind = iota
	inlineCommandDeliveryDrain
)

// Terminal Run state commits before asynchronous frontend materialization.
// Observe the same Run's sent delivery AND removal of its projection records;
// neither a missing delivery nor an empty queue alone establishes completion.
func awaitTelegramDeliveryDrain(ctx context.Context, interval time.Duration, runs []runRef, read deliveryDrainRead) error {
	return awaitTelegramDeliveryDrainKind(ctx, interval, runs, read, canonicalDeliveryDrain)
}

func awaitTelegramDeliveryDrainKind(ctx context.Context, interval time.Duration, runs []runRef, read deliveryDrainRead, kind deliveryDrainKind) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if kind != canonicalDeliveryDrain && kind != inlineCommandDeliveryDrain {
		return fmt.Errorf("unknown delivery drain kind %d", kind)
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
			if delivery.TenantID != run.TenantID || delivery.RunID != run.RunID {
				return fmt.Errorf("run %s delivery drain: foreign or noncanonical delivery", run.RunID)
			}
			if kind == canonicalDeliveryDrain {
				if delivery.Projection == nil || delivery.Projection.SessionID != run.SessionID {
					return fmt.Errorf("run %s delivery drain: foreign or noncanonical delivery", run.RunID)
				}
			} else if delivery.Projection != nil || strings.TrimSpace(delivery.Text) == "" {
				return fmt.Errorf("run %s delivery drain: expected inline command text without a projection", run.RunID)
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

func TestAwaitTelegramDeliveryDrainInlineCommand(t *testing.T) {
	run := runRef{TenantID: "tenant-a", RunID: "new-command", SessionID: "new-session"}
	delivery := domain.TelegramDeliveryOutbox{
		TenantID: run.TenantID, RunID: run.RunID, Status: domain.DeliverySent,
		Text: "A new session was created and this Telegram chat now points to it.",
	}
	err := awaitTelegramDeliveryDrainKind(context.Background(), time.Second, []runRef{run},
		func(context.Context, runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
			return delivery, 0, nil
		}, inlineCommandDeliveryDrain)
	if err != nil {
		t.Fatalf("valid inline /new delivery did not drain: %v", err)
	}
}

func TestAwaitTelegramDeliveryDrainRejectsWrongKind(t *testing.T) {
	run := runRef{TenantID: "tenant-a", RunID: "run-a", SessionID: "session-a"}
	for _, tc := range []struct {
		name     string
		kind     deliveryDrainKind
		delivery domain.TelegramDeliveryOutbox
	}{
		{name: "inline cannot satisfy canonical", kind: canonicalDeliveryDrain, delivery: domain.TelegramDeliveryOutbox{TenantID: run.TenantID, RunID: run.RunID, Status: domain.DeliverySent, Text: "command reply"}},
		{name: "canonical cannot satisfy inline", kind: inlineCommandDeliveryDrain, delivery: drainFixture(run, domain.DeliverySent)},
		{name: "empty inline payload", kind: inlineCommandDeliveryDrain, delivery: domain.TelegramDeliveryOutbox{TenantID: run.TenantID, RunID: run.RunID, Status: domain.DeliverySent}},
		{name: "foreign inline run", kind: inlineCommandDeliveryDrain, delivery: domain.TelegramDeliveryOutbox{TenantID: run.TenantID, RunID: "other", Status: domain.DeliverySent, Text: "reply"}},
		{name: "foreign inline tenant", kind: inlineCommandDeliveryDrain, delivery: domain.TelegramDeliveryOutbox{TenantID: "other", RunID: run.RunID, Status: domain.DeliverySent, Text: "reply"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := awaitTelegramDeliveryDrainKind(context.Background(), time.Second, []runRef{run},
				func(context.Context, runRef) (domain.TelegramDeliveryOutbox, uint64, error) {
					return tc.delivery, 0, nil
				}, tc.kind)
			if err == nil {
				t.Fatal("wrong delivery kind or association unexpectedly completed")
			}
		})
	}
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
