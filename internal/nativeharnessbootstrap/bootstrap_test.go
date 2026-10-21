//go:build darwin || linux

package nativeharnessbootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

func contract() (attachedworkerdaemon.LaunchSpec, attachedworkerdaemon.NativeLaunchV2) {
	d := attachedworkerdaemon.ExecutableDigest{1}
	a := attachedworkerdaemon.NativeArtifactV2{Executable: "/owned/helper", Digest: d}
	return attachedworkerdaemon.LaunchSpec{Executable: "/owned/native", ExecutableDigest: d, Arguments: []string{"literal argument", "$not-a-shell"}}, attachedworkerdaemon.NativeLaunchV2{
		Version: 2, Bootstrap: a, Relay: a, Companion: a, SocketDestination: "/owned/session.sock", ProviderHosts: []string{"api.openai.com"},
		Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 128, OutboundBytes: 129, Active: 3 * time.Second, Cleanup: time.Second, CompanionMemoryBytes: 1, CompanionGoroutines: 1},
	}
}

func explicitEnvironment(deadline time.Time) []string {
	values := []string{}
	for _, name := range nativeNames {
		v := "/owned/empty"
		if name == "PATH" {
			v = ""
		}
		values = append(values, name+"="+v)
	}
	return append(values, deadlineName+"="+strconv.FormatInt(deadline.UnixNano(), 10), "FAKE_SECRET=never-inherit", "ALL_PROXY=http://forbidden", "NO_PROXY=*", "HTTP_PROXY=http://forbidden")
}

func TestCanonicalArgumentsAndBounds(t *testing.T) {
	w, b := contract()
	boot, relay, err := Arguments(w, b)
	if err != nil {
		t.Fatal(err)
	}
	b.BootstrapArguments, b.RelayArguments = boot, relay
	if err := ValidateArguments(w, b); err != nil {
		t.Fatal(err)
	}
	p, err := parseBootstrap(boot)
	if err != nil || !slices.Equal(p.native.args, w.Arguments) {
		t.Fatalf("literal native argv lost: %+v %v", p, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func([]string) []string
	}{
		{"trailing", func(a []string) []string { return append(a, "extra") }},
		{"wrong-mode", func(a []string) []string { a[0] = "other"; return a }},
		{"noncanonical-number", func(a []string) []string { a[2] = "03000000000"; return a }},
		{"nul", func(a []string) []string { a[len(a)-1] = "bad\x00arg"; return a }},
		{"oversize", func(a []string) []string { a[len(a)-1] = strings.Repeat("x", 4097); return a }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseBootstrap(tc.mutate(slices.Clone(boot))); !errors.Is(err, ErrConfig) {
				t.Fatalf("malformed invocation accepted: %v", err)
			}
		})
	}
	altered := b
	altered.BootstrapArguments = append(slices.Clone(boot), "extra")
	if ValidateArguments(w, altered) == nil {
		t.Fatal("unsealed bootstrap accepted")
	}
	altered = b
	altered.RelayArguments = slices.Clone(relay)
	altered.RelayArguments[len(relay)-1] = "4"
	if ValidateArguments(w, altered) == nil {
		t.Fatal("wrong readiness FD accepted")
	}
	w.Arguments = make([]string, 128)
	if _, _, err := Arguments(w, b); err == nil {
		t.Fatal("compiled vector over 128 arguments accepted")
	}
	if _, err := parseRelay(append(slices.Clone(relay), "extra")); err == nil {
		t.Fatal("relay trailing argument accepted")
	}
}

func TestEnvironmentIsReplacementNotInheritance(t *testing.T) {
	env := explicitEnvironment(time.Unix(100, 0))
	base, _, err := environment(env)
	if err != nil {
		t.Fatal(err)
	}
	if len(base) != len(nativeNames) {
		t.Fatalf("replacement length %d", len(base))
	}
	for _, v := range base {
		if strings.Contains(v, "PROXY") || strings.Contains(v, "SECRET") || strings.HasPrefix(v, deadlineName+"=") {
			t.Fatalf("ambient/control variable leaked: %q", v)
		}
	}
	if _, _, err := environment(append(env, "CODEX_HOME=/duplicate")); err == nil {
		t.Fatal("duplicate selected variable accepted")
	}
	if _, _, err := environment(env[1:]); err == nil {
		t.Fatal("missing baseline variable accepted")
	}
}

func TestBootstrapEnvironmentKeepsCapturedDeadlineAndCallerBase(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deadline := time.Now().Add(3 * time.Second)
		base, _, err := environment(explicitEnvironment(deadline))
		if err != nil {
			t.Fatal(err)
		}
		original := slices.Clone(base)
		time.Sleep(time.Second) // Logical preparation cost, not synchronization.
		built, err := BootstrapEnvironment(base, deadline)
		if err != nil {
			t.Fatal(err)
		}
		_, value, err := environment(built)
		if err != nil || value != strconv.FormatInt(deadline.UnixNano(), 10) || !slices.Equal(base, original) {
			t.Fatalf("captured deadline/base changed: %q %v", value, err)
		}
		if _, err := BootstrapEnvironment(built, deadline); err == nil {
			t.Fatal("caller-authored duplicate control variable accepted")
		}
		if _, err := BootstrapEnvironment(base, time.Time{}); err == nil {
			t.Fatal("missing deadline accepted")
		}
	})
}

func TestOriginalDeadlineNeverRenews(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		original := time.Now().Add(3 * time.Second)
		value := strconv.FormatInt(original.UnixNano(), 10)
		time.Sleep(time.Second) // Logical startup time, not readiness synchronization.
		ctx, cancel, err := activeContext(t.Context(), value, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()
		got, _ := ctx.Deadline()
		if !got.Equal(original) {
			t.Fatalf("deadline renewed: %v != %v", got, original)
		}
		parent, stop := context.WithDeadline(t.Context(), time.Now().Add(time.Second))
		defer stop()
		bounded, finish, err := activeContext(parent, value, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer finish()
		d, _ := bounded.Deadline()
		pd, _ := parent.Deadline()
		if !d.Equal(pd) {
			t.Fatal("shorter original parent deadline lost")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if live(ctx) {
			t.Fatal("original deadline still live")
		}
		if _, _, err := activeContext(t.Context(), value, 3*time.Second); err == nil {
			t.Fatal("expired original deadline accepted")
		}
	})
}

// The only launched fixtures are two owned copies of this test process,
// reached through pinned scripts. Neither fixture opens a network socket.
func TestOwnedBootstrapProcess(t *testing.T) {
	index := slices.Index(os.Args, "--")
	if index < 0 {
		return
	}
	a := os.Args[index+1:]
	if len(a) < 5 {
		return
	}
	role, mode, marker, other, gate := a[0], a[1], a[2], a[3], a[4]
	fail := func() { os.Exit(91) }
	if os.Getenv("FAKE_SECRET") != "" || os.Getenv("ALL_PROXY") != "" || os.Getenv("NO_PROXY") != "" {
		fail()
	}
	if role == "native" {
		proxy := "http://127.0.0.1:31217"
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
			if os.Getenv(key) != proxy {
				fail()
			}
		}
		if os.Getenv("CODEX_HOME") == "" || os.Getenv(deadlineName) != "" {
			fail()
		}
		// A Go runtime may reuse FD3, but the original pipe capability must not leak.
		var stat syscall.Stat_t
		if syscall.Fstat(3, &stat) == nil && stat.Mode&syscall.S_IFMT == syscall.S_IFIFO {
			fail()
		}
	} else {
		for _, key := range []string{"CODEX_HOME", "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
			if os.Getenv(key) != "" {
				fail()
			}
		}
		if os.Getenv(deadlineName) == "" {
			fail()
		}
	}
	if mode == "ignore-term" || mode == "late-ready" {
		signal.Ignore(syscall.SIGTERM)
	}
	if err := os.WriteFile(marker+".tmp", []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		fail()
	}
	if os.Rename(marker+".tmp", marker) != nil {
		fail()
	}
	if role == "native" && mode == "echo" {
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			fail()
		}
		os.Exit(0)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if mode == "ignore-term" || mode == "late-ready" {
		stop()
		signal.Ignore(syscall.SIGTERM)
		ctx = context.Background()
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	waitFile := func(path string) bool {
		for {
			if _, err := os.Stat(path); err == nil {
				return true
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return false
			case <-deadline.C:
				return false
			}
		}
	}
	if role == "relay" {
		if mode == "late-ready" && !waitFile(gate) {
			os.Exit(0)
		}
		fd := os.NewFile(3, "fixture-readiness")
		if fd == nil {
			fail()
		}
		ack := "127.0.0.1:31217\n"
		if mode == "trailing" {
			ack += "x"
		}
		if mode == "oversize" {
			ack = strings.Repeat("x", 33)
		}
		if mode == "bad-address" {
			ack = "127.0.0.2:31217\n"
		}
		if _, err := io.WriteString(fd, ack); err != nil {
			os.Exit(0)
		}
		if mode != "no-eof" {
			if fd.Close() != nil {
				fail()
			}
		}
		if os.WriteFile(marker+".ack", []byte("written"), 0600) != nil {
			fail()
		}
		if mode == "die-after-native" {
			if waitFile(other) {
				os.Exit(17)
			}
			os.Exit(0)
		}
	}
	select {
	case <-ctx.Done():
		if role == "relay" && mode == "teardown-exit23" {
			if os.WriteFile(marker+".teardown", []byte("TERM received"), 0600) != nil {
				fail()
			}
			// Immediate fixture exit: race-runtime exit sleep must not replace
			// the observed nonzero status with the parent's later SIGKILL.
			syscall.Exit(23)
		}
	case <-deadline.C:
	}
	os.Exit(0)
}

type fixture struct {
	args, env           []string
	in, out, errout     *os.File
	relay, native, gate string
	result              chan error
	cancel              context.CancelFunc
	done                bool
	deadline            time.Time
}

func newFixture(t *testing.T, relayMode, nativeMode string) *fixture {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{relay: filepath.Join(dir, "relay.pid"), native: filepath.Join(dir, "native.pid"), gate: filepath.Join(dir, "gate"), result: make(chan error, 1)}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	script := func(role, mode, marker, other string) string {
		path := filepath.Join(dir, role)
		body := "#!/bin/sh\nexec " + quote(exe) + " -test.run='^TestOwnedBootstrapProcess$' -- " + quote(role) + " " + quote(mode) + " " + quote(marker) + " " + quote(other) + " " + quote(f.gate) + " \"$@\"\n"
		if err := os.WriteFile(path, []byte(body), 0700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	relay := script("relay", relayMode, f.relay, f.native)
	native := script("native", nativeMode, f.native, f.relay)
	w, b := contract()
	w.Executable = native
	w.ExecutableDigest, err = attachedworkerdaemon.DigestExecutable(native)
	if err != nil {
		t.Fatal(err)
	}
	rd, err := attachedworkerdaemon.DigestExecutable(relay)
	if err != nil {
		t.Fatal(err)
	}
	b.Relay = attachedworkerdaemon.NativeArtifactV2{Executable: relay, Digest: rd}
	b.Bootstrap = b.Relay
	b.Companion = b.Relay
	if nativeMode == "start-failure" {
		if err := os.WriteFile(native, []byte("not executable format\n"), 0700); err != nil {
			t.Fatal(err)
		}
		w.ExecutableDigest, err = attachedworkerdaemon.DigestExecutable(native)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.args, b.RelayArguments, err = Arguments(w, b)
	if err != nil {
		t.Fatal(err)
	}
	b.BootstrapArguments = f.args
	if ValidateArguments(w, b) != nil {
		t.Fatal("fixture vectors not canonical")
	}
	open := func(name string) *os.File {
		p, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Errorf("fixture file close: %v", err)
			}
		})
		return p
	}
	f.in, f.out, f.errout = open("stdin"), open("stdout"), open("stderr")
	if _, err := f.in.WriteString("canonical stdin\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.in.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	f.cancel = cancel
	t.Cleanup(func() {
		cancel()
		if !f.done {
			f.await(t)
		}
		f.assertReaped(t)
	})
	f.deadline = time.Now().Add(3 * time.Second)
	f.env = explicitEnvironment(f.deadline)
	go func() { f.result <- Run(ctx, f.args, f.env, f.in, f.out, f.errout) }()
	return f
}

func (f *fixture) await(t *testing.T) error {
	t.Helper()
	// Observer bound covers original active (3s) plus the one cleanup budget
	// (1s), without granting either resource a renewed lifetime.
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var err error
	select {
	case err = <-f.result:
	case <-timer.C:
		select {
		case err = <-f.result:
		default:
			t.Fatal("bootstrap did not join within bounded cleanup")
		}
	}
	f.done = true
	if errors.Is(err, ErrNotJoined) {
		t.Fatalf("cleanup not joined: %v", err)
	}
	return err
}

func (f *fixture) marker(t *testing.T, path string) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-tick.C:
		case err := <-f.result:
			f.done = true
			t.Fatalf("bootstrap exited before %s: %v", filepath.Base(path), err)
		case <-timer.C:
			if _, err := os.Stat(path); err == nil {
				return
			}
			select {
			case err := <-f.result:
				f.done = true
				t.Fatalf("bootstrap exited before marker: %v", err)
			default:
				t.Fatalf("fixture marker %s never became ready", filepath.Base(path))
			}
		}
	}
}

func (f *fixture) assertReaped(t *testing.T) {
	t.Helper()
	for _, path := range []string{f.relay, f.native} {
		raw, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Error(err)
			continue
		}
		pid, err := strconv.Atoi(string(raw))
		if err != nil || pid <= 0 {
			t.Errorf("invalid fixture PID %q", raw)
			continue
		}
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("owned helper PID %d remains: %v", pid, err)
		}
		if err := syscall.Kill(-pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("owned helper group %d remains: %v", pid, err)
		}
	}
}

func TestBootstrapNativeExitJoinsPairAndPreservesStdio(t *testing.T) {
	f := newFixture(t, "ready", "echo")
	if err := f.await(t); err != nil {
		t.Fatal(err)
	}
	f.assertReaped(t)
	raw, err := os.ReadFile(f.out.Name())
	if err != nil || string(raw) != "canonical stdin\n" {
		t.Fatalf("native stdio = %q, %v", raw, err)
	}
}

func TestRelayExitStatusExceptionIsExactAndOwned(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requested bool
		status    syscall.WaitStatus
		want      bool
	}{
		{"owned-kill", true, syscall.WaitStatus(syscall.SIGKILL), true},
		{"unowned-kill", false, syscall.WaitStatus(syscall.SIGKILL), false},
		{"ordinary-exit", true, syscall.WaitStatus(23 << 8), false},
		{"term-not-kill", true, syscall.WaitStatus(syscall.SIGTERM), false},
		{"clean", true, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := expectedRelayKill(tc.requested, tc.status); got != tc.want {
				t.Fatalf("owned=%v status=%v exception=%v want%v", tc.requested, tc.status, got, tc.want)
			}
		})
	}
}

func TestLeaderKillProvenanceRequiresPendingLeader(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pending bool
		sig     syscall.Signal
		rawErr  error
		want    bool
	}{
		{"pending-owned-kill", true, syscall.SIGKILL, nil, true},
		{"joined-leader-residual-group-kill", false, syscall.SIGKILL, nil, false},
		{"no-such-group", true, syscall.SIGKILL, syscall.ESRCH, false},
		{"signal-failure", true, syscall.SIGKILL, syscall.EPERM, false},
		{"term-only", true, syscall.SIGTERM, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ownsPendingLeaderKill(tc.pending, tc.sig, tc.rawErr); got != tc.want {
				t.Fatalf("pending=%v signal=%v rawErr=%v ownership=%v want%v", tc.pending, tc.sig, tc.rawErr, got, tc.want)
			}
		})
	}
}

func TestBootstrapRelayTeardownFailureSurvivesNativeSuccess(t *testing.T) {
	f := newFixture(t, "teardown-exit23", "echo")
	err := f.await(t)
	var exit *exec.ExitError
	if !errors.Is(err, ErrLifecycle) || !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("relay teardown exit23 discarded after native0: %v", err)
	}
	if _, err := os.Stat(f.relay + ".teardown"); err != nil {
		t.Fatalf("relay did not observe requested teardown: %v", err)
	}
	raw, err := os.ReadFile(f.out.Name())
	if err != nil || string(raw) != "canonical stdin\n" {
		t.Fatalf("native did not complete successfully: %q, %v", raw, err)
	}
	f.assertReaped(t)
}

func TestBootstrapRelayOwnedKillEscalationIsJoined(t *testing.T) {
	f := newFixture(t, "ignore-term", "echo")
	if err := f.await(t); err != nil {
		t.Fatalf("owned relay kill escalation: %v", err)
	}
	f.assertReaped(t)
}

func TestBootstrapRejectsUnfinishedOrMalformedReadiness(t *testing.T) {
	for _, mode := range []string{"trailing", "oversize", "bad-address", "no-eof", "late-ready"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t, mode, "echo")
			if mode == "no-eof" {
				f.marker(t, f.relay+".ack")
			}
			if mode == "late-ready" {
				f.marker(t, f.relay)
				f.cancel()
				if err := os.WriteFile(f.gate, []byte("late"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.await(t); err == nil {
				t.Fatal("malformed/cancelled readiness accepted")
			}
			if _, err := os.Stat(f.native); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("native started after invalid readiness: %v", err)
			}
			f.assertReaped(t)
		})
	}
}

func TestBootstrapRelayExitStopsAndJoinsNative(t *testing.T) {
	f := newFixture(t, "die-after-native", "ignore-term")
	if err := f.await(t); err == nil {
		t.Fatal("unexpected relay exit accepted")
	}
	if _, err := os.Stat(f.native); err != nil {
		t.Fatalf("native never started: %v", err)
	}
	f.assertReaped(t)
}

func TestBootstrapNativeStartFailureJoinsRelay(t *testing.T) {
	f := newFixture(t, "ready", "start-failure")
	if err := f.await(t); err == nil {
		t.Fatal("invalid executable succeeded")
	}
	f.assertReaped(t)
}

func TestCancelledStartCannotCreateChild(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	if !live(ctx) {
		cancel()
		t.Fatal("cancelled-start fixture was not initially live with a future deadline")
	}
	cancel()
	c, err := startChild(ctx, program{path: executable, digest: digest, args: []string{"-test.run=^$"}}, nil, nil, nil, nil, nil)
	if c != nil {
		t.Cleanup(func() {
			if err := stopChildren(time.Now().Add(time.Second), c); err != nil {
				t.Errorf("unexpected cancelled-start child cleanup: %v", err)
			}
		})
	}
	if c != nil || err == nil {
		t.Fatalf("cancelled start = %v, %v", c, err)
	}
}

func ExampleArguments() {
	w, b := contract()
	boot, relay, err := Arguments(w, b)
	fmt.Println(boot[0], relay[0], err) /* Output: bootstrap relay <nil> */
}
