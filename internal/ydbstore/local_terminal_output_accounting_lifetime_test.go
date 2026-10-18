package ydbstore

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"testing/synctest"
)

// A valid original caller whose standard AfterFunc delivery is held until
// stopped. Done/Err are synchronous. WithoutCancel masks the underlying
// cancelCtx identity, so children cannot accidentally borrow that identity.
// This controls callback delivery only, not any production authority/data.
type heldTerminalPlanningCaller struct {
	context.Context
	mu        sync.Mutex
	done      chan struct{}
	err       error
	callbacks int
}

func (c *heldTerminalPlanningCaller) Done() <-chan struct{} { return c.done }
func (c *heldTerminalPlanningCaller) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
func (c *heldTerminalPlanningCaller) AfterFunc(func()) func() bool {
	c.mu.Lock()
	c.callbacks++
	c.mu.Unlock()
	return func() bool { return true }
}
func (c *heldTerminalPlanningCaller) cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = context.Canceled
		close(c.done)
	}
}

func TestLocalTerminalOutputAccountingFinalCallerLifetime(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		name := "live_original_positive"
		if cancelCaller {
			name = "cancel_after_accounting_verifier_delivery_held"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, engine, h := terminalOutputFixture(t)
				caller := &heldTerminalPlanningCaller{Context: context.WithoutCancel(t.Context()), done: make(chan struct{})}
				t.Cleanup(caller.cancel)
				connector := &terminalOutputCloseSQL{base: compactionPublicationConnector{engine}, targetStatement: localCompactionOperationsSQL, targetRead: 3}
				if cancelCaller {
					connector.close = func() error { caller.cancel(); return nil }
				}
				db := sql.OpenDB(connector)
				db.SetMaxOpenConns(1)
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				f.store.db = db
				before := len(f.canonical.objects)
				p, err := h.PrepareOutput(caller)
				if p != nil {
					t.Cleanup(p.Clear)
				}
				connector.mu.Lock()
				fired, cancelled := connector.fired, connector.cancelled
				connector.mu.Unlock()
				caller.mu.Lock()
				callbacks := caller.callbacks
				caller.mu.Unlock()
				if !fired || cancelled || callbacks == 0 {
					t.Fatal("original final accounting verifier/delayed delivery not established", fired, cancelled, callbacks)
				}
				if cancelCaller {
					if err == nil || p != nil {
						t.Error("cancelled original caller received a new prepared terminal output")
					}
				} else if err != nil || p == nil {
					t.Fatal("live original control", err)
				}
				if len(f.canonical.objects) != before {
					t.Fatal("planner cancellation test performed storage writes")
				}
			})
		})
	}
}
