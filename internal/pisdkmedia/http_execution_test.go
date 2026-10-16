//go:build darwin || linux

package pisdkmedia

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// No prepared private HTTP transport exists at this public seam. Even complete
// request/Init DATA must deny before Issue/launch; a test may not fake a grant.
// The concrete factory's own tests separately isolate the missing-root case
// after genuine auth/receive/scan preparation.
func TestPreparedPiHTTPExecutionMissingPolicyAndForeignDataBeforeIssue(t *testing.T) {
	for _, mode := range []string{"missing transport", "foreign model", "foreign schema", "foreign headers", "foreign body", "foreign argv", "stdin", "work one short"} {
		t.Run(mode, func(t *testing.T) {
			cfg := initFixtureConfig(t)
			sum := sha256.Sum256(cfg.FinalSchema)
			cfg.FinalSchemaSourceDigest = hex.EncodeToString(sum[:])
			cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
			projection, ledger := initFixtureProjection(t, cfg, false)
			credentials := initFixtureCredentials(t, projection)
			init, err := PrepareHTTPInitV1(t.Context(), projection, []byte(initFixtureModel), 1000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := init.Close(); e != nil {
					t.Error(e)
				}
			})
			fileUse, err := credentials.source.PreparePiFileUseV1(ledger, projection.Body(), projection.binding.Backend.BackendProfileDigest)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if e := fileUse.Close(); e != nil {
					t.Error(e)
				}
			})
			args := init.EntryArguments()
			process := attachedworkerdaemon.AttemptSpec{Executable: "/synthetic-never-executed", Arguments: append([]string{"fixed-http-entry"}, args[:]...)}
			headers := wireWriter{out: make([]byte, 0, 4096), cap: 4096, maxDepth: 1, maxNodes: 4096}
			writeExecutionHeaders(&headers, projection.config.headers)
			request := attachedworkerdaemon.PreparedAPIRequestV1{Profile: *cfg.Profile, Body: projection.Body(), Model: []byte(initFixtureModel), Schema: cfg.FinalSchema, Headers: headers.out}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			launcher := &executionNoLaunch{}
			supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{ScratchRoot: root, Launcher: launcher, Timeout: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "foreign model":
				request.Model = []byte(`{}`)
			case "foreign schema":
				request.Schema = []byte(`{}`)
			case "foreign headers":
				request.Headers = []byte(`{}`)
			case "foreign body":
				request.Body = bytes.Clone(request.Body)
				request.Body[0] ^= 1
			case "foreign argv":
				process.Arguments[len(process.Arguments)-1] = "1"
			case "stdin":
				process.Stdin = []byte("unfunded")
			case "work one short":
				left := ledger.Remaining().CopyBytes
				if err := ledger.Reserve(left-1, 0, 0); err != nil {
					t.Fatal(err)
				}
			}
			before := ledger.Accounting()
			p, err := PreparePiHTTPExecutionV1(supervisor, projection, init, fileUse, request, process)
			if err == nil || p != nil || ledger.Accounting().LiveBytes != before.LiveBytes || init.state.emitted.Load() || init.state.executionBorrowed {
				t.Fatal("unqualified/foreign HTTP preparation accepted, emitted, or leaked", mode, err, before, ledger.Accounting())
			}
			copyInit := *init
			if p, err := PreparePiHTTPExecutionV1(supervisor, projection, &copyInit, fileUse, request, process); err == nil || p != nil {
				t.Fatal("failed constructor replayed through copied Init")
			}
			if launcher.calls != 0 || credentials.store.loads != 0 || credentials.store.reads != 0 {
				t.Fatal("preIssue denial touched backend or launched")
			}
		})
	}
}

func TestPreparedPiHTTPExecutionSharedRunAndRetirementGuards(t *testing.T) {
	// Pure shared-owner state-machine tests, deliberately no stdio/file/native
	// plan. They prove denial, not a successful constructor or actual process.
	var zero PreparedPiHTTPExecutionV1
	if _, err := zero.RunPrepared(t.Context(), attachedworkerdaemon.PreparedInvocationV1{}); err == nil || zero.Close() == nil {
		t.Fatal("zero executor usable")
	}
	s := &piHTTPExecutionState{stage: 1}
	p := PreparedPiHTTPExecutionV1{state: s}
	copyPlan := p
	if p.Close() == nil {
		t.Fatal("Close retired a running owner")
	}
	if _, err := copyPlan.RunPrepared(t.Context(), attachedworkerdaemon.PreparedInvocationV1{}); err == nil {
		t.Fatal("copied running owner reentered")
	}
	s.stage = 3
	s.closeErr = ErrProjection
	if copyPlan.Close() != ErrProjection || p.Close() != ErrProjection {
		t.Fatal("copies lost shared terminal retirement error")
	}
}

func TestPreparedPiHTTPExecutionPendingRetirementPreservesInitAndCanFinish(t *testing.T) {
	// Pure transition counterpart to the daemon's actual held-driver test.
	// No constructor/process/private transport success is claimed here.
	cfg := initFixtureConfig(t)
	request, ledger := initFixtureProjection(t, cfg, false)
	credentials := initFixtureCredentials(t, request)
	init, err := PrepareHTTPInitV1(t.Context(), request, []byte(initFixtureModel), 1000, credentials.source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := init.Close(); err != nil {
			t.Error(err)
		}
	})
	before := ledger.Accounting()
	if err := ledger.Reserve(0, 100, 0); err != nil {
		t.Fatal(err)
	}
	init.state.executionBorrowed = true
	s := &piHTTPExecutionState{stage: 3, init: init, metadata: &preparedExecutionSnapshot{request: request, live: 100}}
	p := PreparedPiHTTPExecutionV1{state: s}
	copyPlan := p
	if err := s.finishRetirement(attachedworkerdaemon.ErrPiHTTPRetirementPending); err == nil || s.stage != 2 || !init.state.executionBorrowed || ledger.Accounting().LiveBytes != before.LiveBytes+100 {
		t.Fatal("pending retirement lost retry/borrow/debit", err, s.stage, ledger.Accounting())
	}
	if init.Close() == nil {
		t.Fatal("Init cleared underneath pending execution")
	}
	// Zero stdio below is pure joined-state input, not a genuine run/plan.
	if err := copyPlan.Close(); err != nil || s.stage != 3 || init.state.executionBorrowed || ledger.Accounting().LiveBytes != before.LiveBytes {
		t.Fatal("postjoin retirement could not finish once", err, s.stage, ledger.Accounting())
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}
