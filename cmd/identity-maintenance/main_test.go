package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func maintenanceArgsTest(mode string) []string {
	return []string{mode, "--environment-digest", strings.Repeat("a", 64), "--manifest-digest", strings.Repeat("b", 64), "--source-commit", strings.Repeat("c", 40)}
}
func TestIdentityMaintenanceCLIClosedModesAndFlags(t *testing.T) {
	for _, mode := range []string{"inventory", "prepare", "apply", "reconcile"} {
		t.Run(mode, func(t *testing.T) {
			args := maintenanceArgsTest(mode)
			switch mode {
			case "prepare":
				args = append(args, "--kind", "identity", "--selector", "private-owner", "--candidate", filepath.Join(t.TempDir(), "candidate.json"))
			case "apply", "reconcile":
				args = append(args, "--candidate", filepath.Join(t.TempDir(), "candidate.json"))
			}
			if _, err := parseMaintenance(args); err != nil {
				t.Fatal("valid mode baseline", err)
			}
			for _, bad := range [][]string{append(append([]string{}, args...), "--manifest-digest", strings.Repeat("b", 64)), append(append([]string{}, args...), "--freeze-verified", "true"), append([]string{"unknown"}, args[1:]...)} {
				if _, err := parseMaintenance(bad); err == nil {
					t.Fatal("invalid flag or mode accepted")
				}
			}
		})
	}
}
func TestIdentityMaintenanceCLIPrivateArtifactOwnershipAndNoOverwrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	name := filepath.Join(dir, "private.json")
	raw := []byte(`{"private":"selector"}`)
	if err := writeMaintenanceArtifact(ctx, name, raw); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(name)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("artifact not private")
	}
	got, err := readMaintenanceArtifact(ctx, name)
	if err != nil || !bytes.Equal(raw, got) {
		t.Fatal("valid artifact read", err)
	}
	if writeMaintenanceArtifact(ctx, name, []byte("replacement")) == nil {
		t.Fatal("existing candidate overwritten")
	}
	link := filepath.Join(dir, "symlink")
	if err := os.Symlink(name, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readMaintenanceArtifact(ctx, link); err == nil {
		t.Fatal("symlink followed")
	}
	if writeMaintenanceArtifact(ctx, link, raw) == nil {
		t.Fatal("symlink overwrite")
	}
	if err := os.Chmod(name, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readMaintenanceArtifact(ctx, name); err == nil {
		t.Fatal("public permissions accepted")
	}
	if err := os.Chmod(name, 0600); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(dir, "hardlink")
	if err := os.Link(name, hard); err != nil {
		t.Fatal(err)
	}
	if _, err := readMaintenanceArtifact(ctx, name); err == nil {
		t.Fatal("aliased candidate accepted")
	}
}

type maintenanceShortWriterTest struct{}

func (maintenanceShortWriterTest) Write(raw []byte) (int, error) {
	return len(raw) - 1, io.ErrShortWrite
}
func TestIdentityMaintenanceCLIUnknownOutcomesAndRedaction(t *testing.T) {
	for _, name := range []string{"success", "driver_error", "close_error", "short_output", "incomplete"} {
		t.Run(name, func(t *testing.T) {
			calls, closes := 0, 0
			private := "raw-private-driver-cookie-subject"
			opener := func(ctx context.Context, dsn string) (maintenanceConnection, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing original deadline")
				}
				return maintenanceConnection{run: func(ctx context.Context, r ydbstore.IdentityMaintenanceRequestV1) (ydbstore.IdentityMaintenanceResultV1, error) {
					calls++
					if r.Mode != "inventory" {
						t.Fatal("wrong mode")
					}
					result := ydbstore.IdentityMaintenanceResultV1{Report: []byte("{\"outcome\":\"inventoryComplete\"}\n")}
					if name == "driver_error" {
						return ydbstore.IdentityMaintenanceResultV1{}, errors.New(private)
					}
					if name == "incomplete" {
						return result, ydbstore.ErrIdentityMaintenanceIncompleteV1
					}
					return result, nil
				}, close: func(context.Context) error {
					closes++
					if name == "close_error" {
						return errors.New(private)
					}
					return nil
				}}, nil
			}
			var output bytes.Buffer
			var writer io.Writer = &output
			if name == "short_output" {
				writer = maintenanceShortWriterTest{}
			}
			err := runMaintenance(context.Background(), maintenanceArgsTest("inventory"), func(string) string { return "private-dsn" }, opener, writer)
			if calls != 1 || closes != 1 {
				t.Fatalf("calls=%d closes=%d", calls, closes)
			}
			if name == "success" && err != nil || name != "success" && err == nil {
				t.Fatal("wrong outcome", err)
			}
			if strings.Contains(output.String(), private) || err != nil && strings.Contains(err.Error(), private) {
				t.Fatal("raw private error exposed")
			}
		})
	}
}
func TestIdentityMaintenanceCLIPreparePersistsCandidateNotStdout(t *testing.T) {
	name := filepath.Join(t.TempDir(), "candidate")
	args := append(maintenanceArgsTest("prepare"), "--kind", "identity", "--selector", "private-owner", "--candidate", name)
	raw := []byte(`{"private_selector":"private-owner"}`)
	var out bytes.Buffer
	open := func(context.Context, string) (maintenanceConnection, error) {
		return maintenanceConnection{run: func(context.Context, ydbstore.IdentityMaintenanceRequestV1) (ydbstore.IdentityMaintenanceResultV1, error) {
			return ydbstore.IdentityMaintenanceResultV1{Candidate: raw, Report: []byte("{\"outcome\":\"prepared\"}\n")}, nil
		}, close: func(context.Context) error { return nil }}, nil
	}
	if err := runMaintenance(context.Background(), args, func(string) string { return "test-dsn" }, open, &out); err != nil {
		t.Fatal(err)
	}
	got, e := readMaintenanceArtifact(context.Background(), name)
	if e != nil || !bytes.Equal(got, raw) || strings.Contains(out.String(), "private-owner") {
		t.Fatal("candidate leaked or not persisted", e)
	}
	if err := runMaintenance(context.Background(), args, func(string) string { return "test-dsn" }, open, &out); !errors.Is(err, errArtifact) {
		t.Fatal("existing file overwritten", err)
	}
}
func TestIdentityMaintenanceCLIApplyLostOutputNoReplay(t *testing.T) {
	name := filepath.Join(t.TempDir(), "candidate")
	if err := writeMaintenanceArtifact(context.Background(), name, []byte(`{"contract":"private-test-candidate"}`)); err != nil {
		t.Fatal(err)
	}
	args := append(maintenanceArgsTest("apply"), "--candidate", name)
	calls := 0
	open := func(context.Context, string) (maintenanceConnection, error) {
		return maintenanceConnection{run: func(_ context.Context, r ydbstore.IdentityMaintenanceRequestV1) (ydbstore.IdentityMaintenanceResultV1, error) {
			calls++
			if len(r.Candidate) == 0 {
				t.Fatal("missing candidate")
			}
			return ydbstore.IdentityMaintenanceResultV1{Report: []byte("{\"outcome\":\"applied\"}\n")}, nil
		}, close: func(context.Context) error { return nil }}, nil
	}
	err := runMaintenance(context.Background(), args, func(string) string { return "test-dsn" }, open, maintenanceShortWriterTest{})
	if !errors.Is(err, ydbstore.ErrIdentityMaintenancePossiblyCommittedV1) || calls != 1 {
		t.Fatal("lost output automatically retried", err)
	}
}

func TestIdentityMaintenanceCLIFIFORejectedWithoutBlocking(t *testing.T) {
	const helper = "SESSIONLESS_MAINTENANCE_FIFO_TEST_PATH"
	if name := os.Getenv(helper); name != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := readMaintenanceArtifact(ctx, name); !errors.Is(err, errArtifact) {
			t.Fatal("nonregular candidate was not refused", err)
		}
		return
	}
	name := filepath.Join(t.TempDir(), "candidate.fifo")
	if err := syscall.Mkfifo(name, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestIdentityMaintenanceCLIFIFORejectedWithoutBlocking$", "-test.count=1")
	command.Env = append(os.Environ(), helper+"="+name)
	command.WaitDelay = time.Second
	if err := command.Run(); err != nil {
		t.Fatalf("FIFO rejection did not complete in the owned subprocess: %v (context: %v)", err, ctx.Err())
	}
}

func TestIdentityMaintenanceCLIWrapperKeepsPrivateArgumentsOffStdout(t *testing.T) {
	dir := t.TempDir()
	source, err := os.ReadFile(filepath.Join("..", "..", "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "Makefile"), source, 0600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "inert-argv")
	// This replaces only the Go executable, not the actual Make target. It
	// cannot compile, open a database or invoke the maintenance command.
	stub := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$MAINTENANCE_TEST_ARGV\"\nprintf '%s\\n' '{\"outcome\":\"inert-wrapper-test\"}'\n"
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	selector := "synthetic-private-owner"
	candidate := filepath.Join(dir, "private-candidate")
	args := append(maintenanceArgsTest("prepare"), "--kind", "identity", "--selector", selector, "--candidate", candidate)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "make", "--no-print-directory", "identity-maintenance", "SESSIONLESS_GO_CACHE_ROOT="+filepath.Join(dir, "cache"), "ARGS="+strings.Join(args, " "))
	command.Dir = dir
	command.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "MAINTENANCE_TEST_ARGV="+marker)
	command.WaitDelay = time.Second
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatal("inert Make wrapper failed", err)
	}
	actual, err := os.ReadFile(marker)
	want := strings.Join(append([]string{"run", "./cmd/identity-maintenance"}, args...), "\n") + "\n"
	if err != nil || string(actual) != want {
		t.Fatal("wrapper did not invoke exactly one intended inert command", err)
	}
	if string(output) != "{\"outcome\":\"inert-wrapper-test\"}\n" || bytes.Contains(output, []byte(selector)) || bytes.Contains(output, []byte(candidate)) {
		t.Fatal("Make wrapper exposed private arguments or extra output")
	}
}

type maintenanceFaultArtifactTest struct {
	file                  *os.File
	fault                 string
	writes, syncs, closes int
}

func (f *maintenanceFaultArtifactTest) Write(raw []byte) (int, error) {
	f.writes++
	if f.fault == "short" || f.fault == "write" {
		n, err := f.file.Write(raw[:1])
		if err == nil && f.fault == "write" {
			err = errors.New("synthetic private write failure")
		}
		return n, err
	}
	return f.file.Write(raw)
}
func (f *maintenanceFaultArtifactTest) Sync() error {
	f.syncs++
	if err := f.file.Sync(); err != nil {
		return err
	}
	if f.fault == "sync" {
		return errors.New("synthetic private sync failure")
	}
	return nil
}
func (f *maintenanceFaultArtifactTest) Close() error {
	f.closes++
	if err := f.file.Close(); err != nil {
		return err
	}
	if f.fault == "close" {
		return errors.New("synthetic private close failure")
	}
	return nil
}

func TestIdentityMaintenanceCLIArtifactPartialSyncCloseFailures(t *testing.T) {
	for _, fault := range []string{"success", "short", "write", "sync", "close"} {
		t.Run(fault, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "private-candidate")
			file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = file.Close() })
			sink := &maintenanceFaultArtifactTest{file: file, fault: fault}
			raw := []byte(`{"private_selector":"synthetic-only"}`)
			err = finishMaintenanceArtifact(context.Background(), sink, raw)
			if fault == "success" && err != nil || fault != "success" && !errors.Is(err, errArtifact) {
				t.Fatal("artifact persistence reported a false outcome", err)
			}
			wantSync := 1
			wantBytes := raw
			if fault == "short" || fault == "write" {
				wantSync = 0
				wantBytes = raw[:1]
			}
			if sink.writes != 1 || sink.syncs != wantSync || sink.closes != 1 {
				t.Fatalf("unexpected persistence/retry counts: write=%d sync=%d close=%d", sink.writes, sink.syncs, sink.closes)
			}
			persisted, readErr := os.ReadFile(name)
			info, statErr := os.Stat(name)
			if readErr != nil || statErr != nil || !bytes.Equal(persisted, wantBytes) || info.Mode().Perm() != 0600 {
				t.Fatal("failed persistence deleted, replaced, renewed or exposed its private artifact", readErr, statErr)
			}
		})
	}
}
