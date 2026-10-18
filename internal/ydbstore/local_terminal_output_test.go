package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Test-only transport synchronization AFTER the actual source verifier in the
// final current join. It changes no authority rows/owner/context; it closes the
// real borrowed Source and samples the actual SQL context synchronously.
type terminalOutputCloseSQL struct {
	base             driver.Connector
	mu               sync.Mutex
	reads            int
	targetStatement  string
	targetRead       int
	close            func() error
	fired, cancelled bool
	closeErr         error
}
type terminalOutputCloseConn struct {
	driver.Conn
	owner *terminalOutputCloseSQL
}

func (c *terminalOutputCloseSQL) Driver() driver.Driver { return c.base.Driver() }
func (c *terminalOutputCloseSQL) Connect(ctx context.Context) (driver.Conn, error) {
	base, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &terminalOutputCloseConn{Conn: base, owner: c}, nil
}
func (c *terminalOutputCloseConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c *terminalOutputCloseConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, statement, args)
}
func (c *terminalOutputCloseConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, statement, args)
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	target, targetRead := c.owner.targetStatement, c.owner.targetRead
	if target == "" {
		target = localCompactionOperationsSQL
	}
	if targetRead == 0 {
		targetRead = 2
	}
	if err == nil && statement == target {
		c.owner.reads++
		if c.owner.reads == targetRead {
			c.owner.fired = true
			if c.owner.close != nil {
				c.owner.closeErr = c.owner.close()
			}
			c.owner.cancelled = ctx.Err() != nil
		}
	}
	return rows, err
}

func TestLocalTerminalOutputFinalJoinSourceLifetime(t *testing.T) {
	for _, closeSource := range []bool{false, true} {
		name := "live_original_positive"
		if closeSource {
			name = "source_closed_after_final_verifier"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, engine, h := terminalOutputFixture(t)
				connector := &terminalOutputCloseSQL{base: compactionPublicationConnector{engine}}
				if closeSource {
					connector.close = f.actual.source.Close
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
				p, err := h.PrepareOutput(t.Context())
				connector.mu.Lock()
				fired, cancelled, closed := connector.fired, connector.cancelled, connector.closeErr
				connector.mu.Unlock()
				if !fired || closed != nil {
					t.Fatal("actual final post-verifier point not reached", fired, closed)
				}
				if closeSource {
					if !cancelled {
						t.Error("final SQL call did not synchronously inherit original Source cancellation")
					}
					if p != nil {
						t.Cleanup(p.Clear)
					}
					if p != nil || err == nil {
						t.Error("Source closed after final verifier still returned prepared success")
					}
				} else {
					if cancelled || err != nil || p == nil {
						t.Fatal("live original positive failed", cancelled, err)
					}
					t.Cleanup(p.Clear)
				}
				if len(f.canonical.objects) != before {
					t.Fatal("lifetime test performed output mutation")
				}
			})
		})
	}
}

// Actual original entry, Store, issuer, body adapter and normalized decoder;
// SQL/funding/proxy/model DATA remain fixtures. No output PUT or cleanup claim.
func terminalOutputFixture(t *testing.T) (*compactionSourceFixture, *compactionPublicationSQL, *LocalTerminalV1) {
	t.Helper()
	f, engine := localPreparedModelFixture(t, 0)
	m, err := f.entry.PrepareModel(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	recordPreparedLocalResponse(t, f, m.operation, m.frozen, "  original <answer> & final  ")
	h, err := f.entry.ClaimTerminal(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	s, err := h.Capture(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Clear)
	return f, engine, h
}

func TestLocalTerminalOutputOriginalProducerAndExactBounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, engine, h := terminalOutputFixture(t)
		objects, writes := len(f.canonical.objects), engine.writes
		p, err := h.PrepareOutput(t.Context())
		if err != nil || p == nil {
			t.Fatal("original captured output planning", err)
		}
		t.Cleanup(p.Clear)
		plan, err := p.Plan()
		if err != nil || plan.Validate() != nil || plan.Capsule.Size != int64(len(p.capsule)) {
			t.Fatal("bounded private plan", err)
		}
		if len(f.canonical.objects) != objects || engine.writes != writes {
			t.Fatal("planning performed PUT or SQL mutation")
		}
		var capsule map[string]json.RawMessage
		if json.Unmarshal(p.capsule, &capsule) != nil || len(capsule) != 9 {
			t.Fatal("closed nine-field capsule")
		}
		for _, name := range []string{"schema", "scope", "profile_digest", "codec_version", "frontier_digest", "operations", "derived_context", "events", "public_manifest"} {
			if capsule[name] == nil {
				t.Fatalf("missing capsule field %s", name)
			}
		}
		if !bytes.Equal(capsule["derived_context"], []byte("null")) || bytes.Contains(p.capsule, []byte(`"messages":[`)) {
			t.Fatal("capsule retained raw request or substituted no-compaction metadata")
		}
		var operations []struct {
			Normalized json.RawMessage         `json:"normalized_body"`
			Projection string                  `json:"projection"`
			Events     []domain.SessionEventID `json:"event_ids"`
		}
		if json.Unmarshal(capsule["operations"], &operations) != nil || len(operations) != 1 || operations[0].Projection != "assistant" || !bytes.Equal(operations[0].Normalized, h.source.state.operations[0].Response) {
			t.Fatal("original normalized bytes/disposition changed")
		}
		events := p.canonical.Events()
		draft, err := canonicaloutput.PlanEventDraftV1(h.source.state.snapshot.Job.Run, events[0].ID, events[0].Kind, events[0].Payload, p.manifest.CreatedAt)
		draft.DisplayText = events[0].DisplayText
		if err != nil || len(p.drafts) != 1 || !reflect.DeepEqual(draft, p.drafts[0]) || draft.DisplayText != "original <answer> & final" || operations[0].Events[0] != draft.ID {
			t.Fatal("original canonical wire or final anchor changed", err)
		}
		if p.manifest.ID != canonicaloutput.ManifestIDV1(plan.Scope.RunID) || len(p.manifest.Artifacts) != 0 {
			t.Fatal("private capsule leaked into public artifacts")
		}
		var identities []localCapsuleEventV1
		if json.Unmarshal(capsule["events"], &identities) != nil {
			t.Fatal("capsule event identities")
		}
		ops := h.source.state.operations
		pd, err := f.actual.config.Digest()
		if err != nil {
			t.Fatal(err)
		}
		exact, err := encodeLocalTerminalCapsule(plan.Scope, pd, h.source.state.digest, ops, nil, identities, p.manifest, uint64(len(p.capsule)))
		if err != nil || !bytes.Equal(exact, p.capsule) {
			t.Fatal("exact encoded boundary rejected", err)
		}
		clear(exact)
		if over, err := encodeLocalTerminalCapsule(plan.Scope, pd, h.source.state.digest, ops, nil, identities, p.manifest, uint64(len(p.capsule)-1)); err == nil || over != nil {
			t.Fatal("first encoded byte over accepted")
		}
		views, _ := p.Drafts()
		views[0].DisplayText = "caller substitution"
		views[0].Payload.Key = "caller path"
		if got, _ := p.Drafts(); got[0] != draft {
			t.Fatal("accessor retargeted original drafts")
		}
		if _, err := json.Marshal(p); err == nil || strings.Contains(p.String(), "answer") {
			t.Fatal("private output diagnostics serialized content")
		}
		copyOutput := *p
		if strings.Contains(fmt.Sprintf("%+v %#v %+v %#v", p, p, copyOutput, copyOutput), "original <answer>") {
			t.Fatal("copied output formatting leaked private content")
		}
		copyOutput.Clear()
		if _, err := copyOutput.Plan(); err == nil {
			t.Fatal("copied owner became original")
		}
		if _, err := h.PrepareOutput(t.Context()); err == nil {
			t.Fatal("planning allowance renewed")
		}
		alias := p.capsule
		p.Clear()
		if _, err := p.Plan(); err == nil || !bytes.Equal(alias, make([]byte, len(alias))) {
			t.Fatal("cleared owner retained material or usable plan")
		}
	})
}

func TestLocalTerminalOutputRejectsChangedOriginalMaterial(t *testing.T) {
	for _, name := range []string{"request", "response", "observation", "frontier", "cleared", "expired", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, engine, h := terminalOutputFixture(t)
				ctx := t.Context()
				switch name {
				case "request":
					h.source.state.operations[0].Request[0] ^= 1
				case "response":
					h.source.state.operations[0].Response[0] ^= 1
				case "observation":
					h.source.state.operations[0].Observation.ResponseDigest = strings.Repeat("f", 64)
				case "frontier":
					h.source.state.operations = nil
				case "cleared":
					h.source.Clear()
				case "expired":
					time.Sleep(time.Until(h.deadline)) // virtual expiry, not synchronization
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				objects, writes := len(f.canonical.objects), engine.writes
				if p, err := h.PrepareOutput(ctx); p != nil || err == nil {
					t.Fatal("changed/cancelled frontier planned output")
				}
				if len(f.canonical.objects) != objects || engine.writes != writes {
					t.Fatal("denied output performed physical mutation")
				}
			})
		})
	}
}
