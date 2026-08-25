package codexsurface

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const (
	execFixtureModeEnvironment = "SESSIONLESS_EXEC_FAILURE_FIXTURE"
	execFixturePIDFile         = "SESSIONLESS_EXEC_FAILURE_PID_FILE"
)

func TestExecFailureClassifications(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	tests := []struct {
		name            string
		mode            string
		wantClass       string
		wantAccepted    bool
		wantTerminal    bool
		wantFinal       bool
		wantFailureCode string
	}{
		{name: "clean completion", mode: "success", wantClass: ExecClassCompleted, wantAccepted: true, wantTerminal: true, wantFinal: true},
		{name: "loss before acceptance", mode: "pre-acceptance-loss", wantClass: ExecClassPreAcceptance},
		{name: "turn without thread", mode: "turn-without-thread", wantClass: ExecClassPreAcceptance, wantFailureCode: "turn_started_before_thread"},
		{name: "loss after acceptance", mode: "ambiguous-loss", wantClass: ExecClassAmbiguous, wantAccepted: true},
		{name: "loss after terminal", mode: "post-terminal-loss", wantClass: ExecClassCompletedWithTeardownFailure, wantAccepted: true, wantTerminal: true, wantFinal: true},
		{name: "terminal followed by event", mode: "post-terminal-event", wantClass: ExecClassTerminalProtocolDrift, wantAccepted: true, wantTerminal: true, wantFinal: true, wantFailureCode: "event_after_terminal"},
		{name: "terminal without final", mode: "terminal-without-final", wantClass: ExecClassTerminalProtocolDrift, wantAccepted: true, wantFailureCode: "terminal_without_final_agent_item"},
		{name: "duplicate terminal", mode: "duplicate-terminal", wantClass: ExecClassTerminalProtocolDrift, wantAccepted: true, wantTerminal: true, wantFinal: true, wantFailureCode: "event_after_terminal"},
		{name: "buffered duplicate terminal", mode: "buffered-duplicate-terminal", wantClass: ExecClassTerminalProtocolDrift, wantAccepted: true, wantTerminal: true, wantFinal: true, wantFailureCode: "event_after_terminal"},
		{name: "buffered post-terminal event", mode: "buffered-post-terminal-event", wantClass: ExecClassTerminalProtocolDrift, wantAccepted: true, wantTerminal: true, wantFinal: true, wantFailureCode: "event_after_terminal"},
		{name: "stdout overflow after terminal", mode: "terminal-stdout-bomb", wantClass: ExecClassTerminalProtocolDrift, wantAccepted: true, wantTerminal: true, wantFinal: true, wantFailureCode: "jsonl_line_limit_exceeded"},
		{name: "stderr overflow after terminal", mode: "terminal-stderr-bomb", wantClass: ExecClassCompletedWithTeardownFailure, wantAccepted: true, wantTerminal: true, wantFinal: true, wantFailureCode: "stderr_limit_exceeded"},
		{name: "unexpected effect", mode: "unexpected-effect", wantClass: ExecClassAmbiguous, wantAccepted: true, wantFailureCode: "unexpected_effect_item"},
		{name: "unterminated frame", mode: "unterminated", wantClass: ExecClassAmbiguous, wantAccepted: true, wantFailureCode: "jsonl_unterminated_frame"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := RunExecFailureFixture(context.Background(), execFixtureConfig(t, testCase.mode))
			if err != nil {
				t.Fatal(err)
			}
			if result.Classification != testCase.wantClass || result.Accepted != testCase.wantAccepted ||
				result.Terminal != testCase.wantTerminal || result.FinalAgentItem != testCase.wantFinal ||
				result.FailureCode != testCase.wantFailureCode || !result.DescendantsReaped {
				t.Fatalf("result = %+v", result)
			}
			if result.StdoutBytes > (32<<10) || result.StderrBytes > (4<<10) {
				t.Fatalf("unbounded result counters = %+v", result)
			}
		})
	}
}

func TestExecFailureRejectsUnsafeOrUnboundedConfigurationBeforeSpawn(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	tests := []struct {
		name      string
		configure func(*ExecFailureConfig)
	}{
		{name: "line cap", configure: func(config *ExecFailureConfig) {
			config.MaxLineBytes = maxExecLineBytes + 1
			config.MaxStdoutBytes = config.MaxLineBytes
		}},
		{name: "stdout cap", configure: func(config *ExecFailureConfig) { config.MaxStdoutBytes = maxExecStdoutBytes + 1 }},
		{name: "stderr cap", configure: func(config *ExecFailureConfig) { config.MaxStderrBytes = maxExecStderrBytes + 1 }},
		{name: "event cap", configure: func(config *ExecFailureConfig) { config.MaxEvents = maxExecEvents + 1 }},
		{name: "final cap", configure: func(config *ExecFailureConfig) {
			config.MaxFinalBytes = maxExecFinalBytes + 1
			config.MaxLineBytes = config.MaxFinalBytes
			config.MaxStdoutBytes = config.MaxLineBytes
		}},
		{name: "api key", configure: func(config *ExecFailureConfig) {
			config.Environment = append(config.Environment, "OPENAI_API_KEY=forbidden")
		}},
		{name: "duplicate environment", configure: func(config *ExecFailureConfig) { config.Environment = append(config.Environment, "PATH=/bin") }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			config := execFixtureConfig(t, "success")
			testCase.configure(&config)
			if _, err := RunExecFailureFixture(context.Background(), config); err == nil {
				t.Fatal("unsafe exec failure configuration was accepted")
			}
		})
	}
	config := execFixtureConfig(t, "success")
	if _, err := RunExecFailureFixture(nil, config); err == nil {
		t.Fatal("nil parent context was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunExecFailureFixture(ctx, config); err == nil {
		t.Fatal("already-cancelled parent context was accepted")
	}
}

func TestExecFailureDeadlineEscalatesAndReapsWholeGroup(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	config := execFixtureConfig(t, "ignore-term")
	config.Environment = append(config.Environment, execFixturePIDFile+"="+pidFile)
	config.Timeout = 100 * time.Millisecond
	config.TerminationGrace = 50 * time.Millisecond
	result, err := RunExecFailureFixture(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Classification != ExecClassCancelledAmbiguous || !result.Accepted ||
		!result.Deadline || !result.TermSent || !result.KillSent || !result.DescendantsReaped ||
		result.DirectChildPeakRSSBytes <= 0 {
		t.Fatalf("deadline result = %+v", result)
	}
	if runtime.GOOS == "linux" {
		if !result.GroupUsageAvailable || result.GroupUsageFailureCode != "" ||
			result.GroupPeakRSSBytes <= 0 || result.MaxDescendants < 1 {
			t.Fatalf("Linux group usage result = %+v", result)
		}
	} else if result.GroupUsageAvailable ||
		result.GroupUsageFailureCode != "process_group_usage_unsupported" ||
		result.GroupPeakRSSBytes != 0 || result.MaxDescendants != 0 {
		t.Fatalf("Darwin group usage must be explicitly unavailable: %+v", result)
	}
	pid := readFixturePID(t, pidFile)
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d survived: %v", pid, err)
	}
}

func TestExecFailureUsageDenialPreservesLifecycleClassification(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	config := execFixtureConfig(t, "success")
	config.testSampleGroup = func(int) (processGroupUsage, error) {
		return processGroupUsage{}, errProcessGroupUsagePermission
	}
	result, err := RunExecFailureFixture(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Classification != ExecClassCompleted || result.FailureCode != "" ||
		!result.Terminal || !result.FinalAgentItem || result.GroupUsageAvailable ||
		result.GroupUsageFailureCode != "process_group_usage_permission_denied" ||
		result.GroupPeakRSSBytes != 0 || result.MaxDescendants != 0 ||
		result.DirectChildPeakRSSBytes <= 0 {
		t.Fatalf("denied group sampler discarded or polluted lifecycle result: %+v", result)
	}
}

func TestExecFailureBlockingUsageSamplerCannotHangRunner(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	release := make(chan struct{})
	config := execFixtureConfig(t, "success")
	config.testSampleGroup = func(int) (processGroupUsage, error) {
		<-release
		return processGroupUsage{}, nil
	}
	type outcome struct {
		result ExecFailureResult
		err    error
	}
	finished := make(chan outcome, 1)
	go func() {
		result, err := RunExecFailureFixture(context.Background(), config)
		finished <- outcome{result: result, err: err}
	}()
	var got outcome
	select {
	case got = <-finished:
		close(release)
	case <-time.After(2 * time.Second):
		close(release)
		select {
		case <-finished:
		case <-time.After(time.Second):
		}
		t.Fatal("blocking process-usage sample hung the lifecycle runner")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.result.Classification != ExecClassCompleted || got.result.FailureCode != "" ||
		got.result.GroupUsageAvailable ||
		got.result.GroupUsageFailureCode != "process_group_usage_timeout" ||
		got.result.DirectChildPeakRSSBytes <= 0 {
		t.Fatalf("blocking sampler result = %+v", got.result)
	}
}

func TestExecFailureBufferedTerminalDriftIsNeverLost(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	for _, mode := range []string{"buffered-duplicate-terminal", "buffered-post-terminal-event"} {
		t.Run(mode, func(t *testing.T) {
			for range 25 {
				result, err := RunExecFailureFixture(context.Background(), execFixtureConfig(t, mode))
				if err != nil {
					t.Fatal(err)
				}
				if result.Classification != ExecClassTerminalProtocolDrift ||
					result.FailureCode != "event_after_terminal" || !result.Terminal {
					t.Fatalf("buffered terminal drift was lost: %+v", result)
				}
			}
		})
	}
}

func TestExecFailureFailedKillSignalHasBoundedRetry(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	config := execFixtureConfig(t, "ignore-term")
	config.Timeout = 30 * time.Millisecond
	config.TerminationGrace = 40 * time.Millisecond
	var processGroupID atomic.Int64
	var suppressedKill atomic.Int32
	config.testSignalGroup = func(groupID int, signal probeProcessGroupSignal) bool {
		processGroupID.Store(int64(groupID))
		if signal == probeProcessGroupKill && suppressedKill.CompareAndSwap(0, 1) {
			return false
		}
		return signalProcessGroupIfAlive(groupID, signal)
	}
	type outcome struct {
		result ExecFailureResult
		err    error
	}
	finished := make(chan outcome, 1)
	go func() {
		result, err := RunExecFailureFixture(context.Background(), config)
		finished <- outcome{result: result, err: err}
	}()
	var got outcome
	select {
	case got = <-finished:
	case <-time.After(2 * time.Second):
		// Cleanup also makes the regression case (the old unbounded wait)
		// terminate, so the test cannot leak its helper process or goroutine.
		if groupID := int(processGroupID.Load()); groupID > 0 {
			_ = signalProbeProcessGroup(groupID, probeProcessGroupKill)
		}
		select {
		case <-finished:
		case <-time.After(time.Second):
		}
		t.Fatal("runner blocked after a failed guarded KILL signal")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if suppressedKill.Load() != 1 || !got.result.KillSent || !got.result.Deadline ||
		!got.result.DescendantsReaped {
		t.Fatalf("bounded retry result = %+v, suppressed kills = %d", got.result, suppressedKill.Load())
	}
}

func TestExecFailureNaturalLeaderExitStillReapsDescendant(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	pidFile := filepath.Join(t.TempDir(), "descendant.pid")
	config := execFixtureConfig(t, "leader-exits")
	config.Environment = append(config.Environment, execFixturePIDFile+"="+pidFile)
	result, err := RunExecFailureFixture(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Classification != ExecClassCompletedWithTeardownFailure ||
		!result.TermSent || !result.DescendantsReaped {
		t.Fatalf("natural-exit result = %+v", result)
	}
	pid := readFixturePID(t, pidFile)
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d survived natural leader exit: %v", pid, err)
	}
}

func TestExecFailureBoundsStopOutputAndProcess(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	tests := []struct {
		name      string
		mode      string
		configure func(*ExecFailureConfig)
		wantCode  string
		wantClass string
	}{
		{name: "line", mode: "stdout-bomb", configure: func(config *ExecFailureConfig) {
			config.MaxLineBytes = 128
			config.MaxStdoutBytes = 512
			config.MaxFinalBytes = 64
		}, wantCode: "jsonl_line_limit_exceeded", wantClass: ExecClassAmbiguous},
		{name: "stderr", mode: "stderr-bomb", configure: func(config *ExecFailureConfig) { config.MaxStderrBytes = 64 }, wantCode: "stderr_limit_exceeded", wantClass: ExecClassPreAcceptance},
		{name: "events", mode: "event-bomb", configure: func(config *ExecFailureConfig) { config.MaxEvents = 3 }, wantCode: "event_count_exceeded", wantClass: ExecClassAmbiguous},
		{name: "final", mode: "large-final", configure: func(config *ExecFailureConfig) { config.MaxFinalBytes = 16 }, wantCode: "invalid_final_agent_item", wantClass: ExecClassAmbiguous},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			config := execFixtureConfig(t, testCase.mode)
			testCase.configure(&config)
			result, err := RunExecFailureFixture(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			if result.Classification != testCase.wantClass || result.FailureCode != testCase.wantCode ||
				!result.DescendantsReaped {
				t.Fatalf("bounded result = %+v", result)
			}
		})
	}
}

func TestExecFailureCancellationAfterAcceptanceIsAmbiguous(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	readyFile := filepath.Join(t.TempDir(), "accepted")
	config := execFixtureConfig(t, "wait-for-cancel")
	config.Environment = append(config.Environment, execFixturePIDFile+"="+readyFile)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		waitForFixtureFile(readyFile, 2*time.Second)
		cancel()
	}()
	result, err := RunExecFailureFixture(ctx, config)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if result.Classification != ExecClassCancelledAmbiguous || !result.Cancelled ||
		!result.Accepted || !result.TermSent || !result.DescendantsReaped {
		t.Fatalf("cancel result = %+v", result)
	}
}

func execFixtureConfig(t *testing.T, mode string) ExecFailureConfig {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		t.Fatal(err)
	}
	return ExecFailureConfig{
		Executable: executable,
		Arguments:  []string{"-test.run=^TestExecFailureHelperProcess$"},
		Directory:  t.TempDir(),
		Environment: []string{
			execFixtureModeEnvironment + "=" + mode,
			"PATH=/usr/local/bin:/usr/bin:/bin",
		},
		Timeout: 2 * time.Second, TerminationGrace: 100 * time.Millisecond,
		MaxLineBytes: 4 << 10, MaxStdoutBytes: 32 << 10,
		MaxStderrBytes: 4 << 10, MaxEvents: 32, MaxFinalBytes: 1 << 10,
	}
}

func readFixturePID(t *testing.T, path string) int {
	t.Helper()
	data := waitForFixtureFile(path, 2*time.Second)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		t.Fatalf("fixture pid = %q, error = %v", data, err)
	}
	return pid
}

func waitForFixtureFile(path string, bound time.Duration) []byte {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			return data
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

func TestExecFailureHelperProcess(t *testing.T) {
	mode := os.Getenv(execFixtureModeEnvironment)
	if mode == "" {
		return
	}
	emit := func(value string) { _, _ = fmt.Fprintln(os.Stdout, value) }
	accepted := func() {
		emit(`{"type":"thread.started","thread_id":"fixture"}`)
		emit(`{"type":"turn.started"}`)
	}
	terminal := func() {
		emit(`{"type":"item.completed","item":{"type":"agent_message","text":"bounded"}}`)
		emit(`{"type":"turn.completed"}`)
	}
	switch mode {
	case "success":
		accepted()
		terminal()
	case "pre-acceptance-loss":
		os.Exit(17)
	case "turn-without-thread":
		emit(`{"type":"turn.started"}`)
	case "ambiguous-loss":
		accepted()
		os.Exit(17)
	case "post-terminal-loss":
		accepted()
		terminal()
		os.Exit(17)
	case "post-terminal-event":
		accepted()
		terminal()
		emit(`{"type":"item.started"}`)
	case "terminal-without-final":
		accepted()
		emit(`{"type":"turn.completed"}`)
	case "duplicate-terminal":
		accepted()
		terminal()
		emit(`{"type":"turn.completed"}`)
	case "buffered-duplicate-terminal":
		_, _ = fmt.Fprint(os.Stdout, strings.Join([]string{
			`{"type":"thread.started","thread_id":"fixture"}`,
			`{"type":"turn.started"}`,
			`{"type":"item.completed","item":{"type":"agent_message","text":"bounded"}}`,
			`{"type":"turn.completed"}`,
			`{"type":"turn.completed"}`,
		}, "\n")+"\n")
	case "buffered-post-terminal-event":
		_, _ = fmt.Fprint(os.Stdout, strings.Join([]string{
			`{"type":"thread.started","thread_id":"fixture"}`,
			`{"type":"turn.started"}`,
			`{"type":"item.completed","item":{"type":"agent_message","text":"bounded"}}`,
			`{"type":"turn.completed"}`,
			`{"type":"item.started"}`,
		}, "\n")+"\n")
	case "terminal-stdout-bomb":
		accepted()
		terminal()
		_, _ = fmt.Fprintln(os.Stdout, strings.Repeat("x", 8<<10))
		waitExecFixtureForever()
	case "terminal-stderr-bomb":
		accepted()
		terminal()
		_, _ = fmt.Fprint(os.Stderr, strings.Repeat("x", 8<<10))
		waitExecFixtureForever()
	case "unexpected-effect":
		accepted()
		emit(`{"type":"item.completed","item":{"type":"command_execution"}}`)
	case "unterminated":
		accepted()
		_, _ = fmt.Fprint(os.Stdout, `{"type":"item.started"}`)
	case "stdout-bomb":
		accepted()
		_, _ = fmt.Fprintln(os.Stdout, strings.Repeat("x", 1024))
		waitExecFixtureForever()
	case "stderr-bomb":
		_, _ = fmt.Fprint(os.Stderr, strings.Repeat("x", 1024))
		waitExecFixtureForever()
	case "event-bomb":
		accepted()
		for range 64 {
			emit(`{"type":"item.started"}`)
		}
		waitExecFixtureForever()
	case "large-final":
		accepted()
		emit(`{"type":"item.completed","item":{"type":"agent_message","text":"this-is-larger-than-the-bound"}}`)
		waitExecFixtureForever()
	case "wait-for-cancel":
		accepted()
		_ = os.WriteFile(os.Getenv(execFixturePIDFile), []byte("ready"), 0o600)
		waitExecFixtureForever()
	case "ignore-term":
		signalIgnoreTERM()
		accepted()
		startExecFixtureDescendant(t, "ignore-term-descendant")
		waitExecFixtureForever()
	case "leader-exits":
		accepted()
		terminal()
		startExecFixtureDescendant(t, "idle-descendant")
	case "ignore-term-descendant":
		signalIgnoreTERM()
		waitExecFixtureForever()
	case "idle-descendant":
		waitExecFixtureForever()
	default:
		os.Exit(91)
	}
	os.Exit(0)
}

func startExecFixtureDescendant(t *testing.T, mode string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestExecFailureHelperProcess$")
	command.Env = []string{
		execFixtureModeEnvironment + "=" + mode,
		"PATH=/usr/local/bin:/usr/bin:/bin",
	}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv(execFixturePIDFile); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(command.Process.Pid)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func signalIgnoreTERM() {
	// os/signal is kept in the helper only so the runner itself never owns or
	// mutates process-global signal handlers.
	signal.Ignore(syscall.SIGTERM)
}

func waitExecFixtureForever() {
	wake := make(chan os.Signal, 1)
	signal.Notify(wake, syscall.SIGUSR1)
	<-wake
}
