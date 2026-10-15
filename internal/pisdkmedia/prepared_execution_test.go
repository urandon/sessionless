//go:build darwin || linux

package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// Ordinary tests import no SDK and launch no process. The genuine positive
// original-owner join is the separate bounded opt-in functional test.
type executionNoLaunch struct{ calls int }

func (*executionNoLaunch) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{Name: "SYNTHETIC-never-launched", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true}
}
func (l *executionNoLaunch) Prepare(context.Context, attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	l.calls++
	return nil, errors.New("unexpected launch")
}

func TestPreparedPiExecutionBeforeIssueOwnershipAndDenials(t *testing.T) {
	cfg := initFixtureConfig(t)
	sum := sha256.Sum256(cfg.FinalSchema)
	cfg.FinalSchemaSourceDigest = hex.EncodeToString(sum[:])
	cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
	projection, ledger := initFixtureProjection(t, cfg, false)
	credentials := initFixtureCredentials(t, projection)
	init, err := PrepareInitV1(t.Context(), projection, []byte(initFixtureModel), 1000, credentials.source)
	if err != nil {
		t.Fatal(err)
	}
	defer init.Clear()
	values := init.EntryArguments()
	args := append([]string{"fixed-source-entry"}, values[:]...)
	process := attachedworkerdaemon.AttemptSpec{Executable: "/synthetic-never-executed", Arguments: args}
	header := wireWriter{out: make([]byte, 0, 4096), cap: 4096, maxDepth: 1, maxNodes: 4096}
	writeExecutionHeaders(&header, projection.config.headers)
	request := attachedworkerdaemon.PreparedAPIRequestV1{Profile: *cfg.Profile, Body: projection.Body(), Model: []byte(initFixtureModel), Schema: cfg.FinalSchema, Headers: header.out}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	launcher := &executionNoLaunch{}
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{ScratchRoot: root, Launcher: launcher, Timeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"foreign-model", "foreign-schema", "foreign-headers", "foreign-body", "missing-args", "wrong-args", "stdin"} {
		t.Run(mode, func(t *testing.T) {
			r, proc := request, process
			switch mode {
			case "foreign-model":
				r.Model = []byte(`{}`)
			case "foreign-schema":
				r.Schema = []byte(`{}`)
			case "foreign-headers":
				r.Headers = []byte(`{}`)
			case "foreign-body":
				r.Body = bytes.Clone(r.Body)
				r.Body[0] = '['
			case "missing-args":
				proc.Arguments = nil
			case "wrong-args":
				proc.Arguments = append([]string(nil), args...)
				proc.Arguments[len(proc.Arguments)-1] = "1"
			case "stdin":
				proc.Stdin = []byte("not prefunded Init")
			}
			before := ledger.Accounting()
			if p, err := PreparePiExecutionV1(supervisor, projection, init, r, proc); err == nil || p != nil {
				t.Fatal("bad association prepared")
			}
			if ledger.Accounting().LiveBytes != before.LiveBytes || ledger.Accounting().CopyBytes < before.CopyBytes {
				t.Fatal("failed construction leaked live/refunded work")
			}
		})
	}
	before := ledger.Accounting()
	execution, err := PreparePiExecutionV1(supervisor, projection, init, request, process)
	if err != nil {
		t.Fatal(err)
	}
	args[0] = "caller mutation"
	if execution.process.Arguments[0] != "fixed-source-entry" {
		t.Fatal("retained process arguments alias caller")
	}
	// Bad post-Issue DATA must deny before even spending the actual file slot.
	bad := request
	bad.Model = []byte(`{}`)
	if _, err = execution.RunPrepared(t.Context(), attachedworkerdaemon.PreparedInvocationV1{APIRequest: &bad, Ledger: ledger,
		VerifiedInput: &attachedworkerdaemon.VerifiedAttachedInputV1{AcceptedAttempt: projection.head}, Process: process}); err == nil || init.spent {
		t.Fatal("foreign metadata reached materialization")
	}
	if _, err = execution.RunPrepared(t.Context(), attachedworkerdaemon.PreparedInvocationV1{}); err == nil {
		t.Fatal("spent executor reentered")
	}
	if err = execution.Close(); err != nil {
		t.Fatal(err)
	}
	if err = execution.Close(); err != nil {
		t.Fatal(err)
	}
	if ledger.Accounting().LiveBytes != before.LiveBytes || ledger.Accounting().CopyBytes <= before.CopyBytes {
		t.Fatal("Close did not retire own live exactly once/preserve cumulative work")
	}
	if launcher.calls != 0 || credentials.store.loads != 0 || credentials.store.reads != 0 {
		t.Fatal("preflight/denial performed Issue/backend read/launch")
	}
	left := ledger.Remaining().CopyBytes
	if err := ledger.Reserve(left-1, 0, 0); err != nil {
		t.Fatal(err)
	}
	exhausted := ledger.Accounting()
	if p, err := PreparePiExecutionV1(supervisor, projection, init, request, process); err == nil || p != nil || ledger.Accounting() != exhausted {
		t.Fatal("combined first-over accepted/reset budget/allocated partial process state")
	}
}
