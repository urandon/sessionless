package codexsurface

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	ExecClassCompleted                    = "completed"
	ExecClassPreAcceptance                = "pre_acceptance"
	ExecClassAmbiguous                    = "ambiguous"
	ExecClassCompletedWithTeardownFailure = "completed_with_teardown_failure"
	ExecClassCancelledAmbiguous           = "cancelled_ambiguous"
	ExecClassTerminalProtocolDrift        = "terminal_protocol_drift"

	maxExecLineBytes   = 1 << 20
	maxExecStdoutBytes = 16 << 20
	maxExecStderrBytes = 1 << 20
	maxExecEvents      = 4096
	maxExecFinalBytes  = 1 << 20
)

var errExecOutputLimit = errors.New("exec probe output limit exceeded")

// ExecFailureConfig is a credential-free failure-fixture seam. It deliberately
// accepts an exact executable and argument vector, but never a shell command.
// Production selection and digest pinning remain the responsibility of the
// surface probe and the future worker adapter.
type ExecFailureConfig struct {
	Executable       string
	Arguments        []string
	Directory        string
	Environment      []string
	Timeout          time.Duration
	TerminationGrace time.Duration
	MaxLineBytes     int
	MaxStdoutBytes   int
	MaxStderrBytes   int
	MaxEvents        int
	MaxFinalBytes    int
	testSignalGroup  func(int, probeProcessGroupSignal) bool
	testSampleGroup  processGroupUsageSampleFunc
}

// ExecFailureResult contains classifications and bounded counters only. Model
// text, JSONL frames, paths, credentials, and provider errors are intentionally
// not representable.
type ExecFailureResult struct {
	Classification    string
	Accepted          bool
	Terminal          bool
	FinalAgentItem    bool
	Cancelled         bool
	Deadline          bool
	TermSent          bool
	KillSent          bool
	DescendantsReaped bool
	ExitCode          int
	EventCount        int
	StdoutBytes       int
	StderrBytes       int
	// DirectChildPeakRSSBytes is only the direct child's rusage lower bound;
	// it is never presented as aggregate process-group usage.
	DirectChildPeakRSSBytes int64
	// GroupPeakRSSBytes and MaxDescendants are valid only when
	// GroupUsageAvailable is true. Otherwise they are zero and the stable
	// GroupUsageFailureCode explains why instrumentation was unavailable.
	GroupPeakRSSBytes     int64
	MaxDescendants        int
	GroupUsageAvailable   bool
	GroupUsageFailureCode string
	Duration              time.Duration
	FailureCode           string
}

func withExecFailureDefaults(config ExecFailureConfig) ExecFailureConfig {
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Second
	}
	if config.TerminationGrace <= 0 {
		config.TerminationGrace = 250 * time.Millisecond
	}
	if config.MaxLineBytes <= 0 {
		config.MaxLineBytes = 256 << 10
	}
	if config.MaxStdoutBytes <= 0 {
		config.MaxStdoutBytes = 4 << 20
	}
	if config.MaxStderrBytes <= 0 {
		config.MaxStderrBytes = 64 << 10
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = 1024
	}
	if config.MaxFinalBytes <= 0 {
		config.MaxFinalBytes = 256 << 10
	}
	return config
}

func validateExecFailureConfig(config ExecFailureConfig) error {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return errors.New("exec failure process groups are unsupported")
	}
	if !filepath.IsAbs(config.Executable) || !filepath.IsAbs(config.Directory) ||
		filepath.Clean(config.Executable) != config.Executable ||
		filepath.Clean(config.Directory) != config.Directory {
		return errors.New("exec failure paths must be normalized and absolute")
	}
	if len(config.Environment) == 0 || config.Timeout <= 0 || config.TerminationGrace <= 0 ||
		config.MaxLineBytes < 1 || config.MaxStdoutBytes < config.MaxLineBytes ||
		config.MaxStderrBytes < 1 || config.MaxEvents < 1 || config.MaxFinalBytes < 1 ||
		config.MaxFinalBytes > config.MaxLineBytes || config.MaxLineBytes > maxExecLineBytes ||
		config.MaxStdoutBytes > maxExecStdoutBytes || config.MaxStderrBytes > maxExecStderrBytes ||
		config.MaxEvents > maxExecEvents || config.MaxFinalBytes > maxExecFinalBytes {
		return errors.New("invalid exec failure bounds")
	}
	seenEnvironment := make(map[string]struct{}, len(config.Environment))
	for _, value := range config.Environment {
		separator := strings.IndexByte(value, '=')
		if separator < 1 || strings.IndexByte(value, 0) >= 0 {
			return errors.New("invalid exec failure environment")
		}
		name := value[:separator]
		if name == "OPENAI_API_KEY" {
			return errors.New("exec failure environment contains an API key route")
		}
		if _, exists := seenEnvironment[name]; exists {
			return errors.New("duplicate exec failure environment variable")
		}
		seenEnvironment[name] = struct{}{}
	}
	return nil
}

// RunExecFailureFixture exercises the OS and JSONL failure contract without a
// provider call. The returned error is reserved for a broken local runner;
// child/protocol failures are expressed by Result.Classification.
func RunExecFailureFixture(parent context.Context, config ExecFailureConfig) (ExecFailureResult, error) {
	if parent == nil {
		return ExecFailureResult{}, errors.New("exec failure parent context is nil")
	}
	if err := parent.Err(); err != nil {
		return ExecFailureResult{}, errors.New("exec failure parent context is already done")
	}
	config = withExecFailureDefaults(config)
	if err := validateExecFailureConfig(config); err != nil {
		return ExecFailureResult{}, err
	}
	startedAt := time.Now()
	command := exec.Command(config.Executable, config.Arguments...)
	command.Dir = config.Directory
	command.Env = append([]string(nil), config.Environment...)
	configureProbeProcessGroup(command)
	stdout, stdoutWriter, err := os.Pipe()
	if err != nil {
		return ExecFailureResult{}, errors.New("create exec failure stdout")
	}
	stderr, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return ExecFailureResult{}, errors.New("create exec failure stderr")
	}
	command.Stdout = stdoutWriter
	command.Stderr = stderrWriter
	if err := command.Start(); err != nil {
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()
		return ExecFailureResult{}, errors.New("start exec failure fixture")
	}
	// The parent owns these pipes. Closing only its writer copies after Start
	// preserves all child/descendant output until process-group teardown and
	// avoids exec.Cmd.Wait closing a StdoutPipe/StderrPipe under active readers.
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	defer stdout.Close()
	defer stderr.Close()
	processGroupID := command.Process.Pid
	usageSampler := startProcessGroupUsageSampler(processGroupID, config.testSampleGroup)
	usageFinished := false
	defer func() {
		if !usageFinished {
			_, _ = usageSampler.finish(processUsageFinishBound)
		}
	}()
	state := &execJSONLState{maxFinalBytes: config.MaxFinalBytes}
	violation := make(chan string, 1)
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})
	go func() {
		defer close(stdoutDone)
		if code := readExecJSONL(stdout, config, state); code != "" {
			notifyExecViolation(violation, code)
		}
	}()
	stderrCounter := &boundedCounter{limit: config.MaxStderrBytes}
	go func() {
		defer close(stderrDone)
		if _, copyErr := io.Copy(stderrCounter, stderr); errors.Is(copyErr, errExecOutputLimit) {
			notifyExecViolation(violation, "stderr_limit_exceeded")
		}
	}()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()

	timer := time.NewTimer(config.Timeout)
	defer timer.Stop()
	var waitErr error
	var failureCode string
	var cancelled, deadline, termSent, killSent bool
	processWaited := false
	select {
	case waitErr = <-waited:
		processWaited = true
	case failureCode = <-violation:
	case <-parent.Done():
		cancelled = true
	case <-timer.C:
		deadline = true
	}

	if !processWaited {
		termSent = config.signalProcessGroupIfAlive(processGroupID, probeProcessGroupTerminate)
		grace := time.NewTimer(config.TerminationGrace)
		select {
		case waitErr = <-waited:
			processWaited = true
		case code := <-violation:
			if failureCode == "" {
				failureCode = code
			}
			select {
			case waitErr = <-waited:
				processWaited = true
			case <-grace.C:
			}
		case <-grace.C:
		}
		if !grace.Stop() {
			select {
			case <-grace.C:
			default:
			}
		}
		if !processWaited {
			killSent = config.signalProcessGroupIfAlive(processGroupID, probeProcessGroupKill)
			waitErr, processWaited = waitExecProcess(waited, config.TerminationGrace)
			if !processWaited {
				// The guarded signal can fail because inspection raced or was
				// denied. Retry KILL without the inspection step, then retain a
				// second hard wait bound so the local runner itself cannot hang.
				if signalProbeProcessGroup(processGroupID, probeProcessGroupKill) == nil {
					killSent = true
				}
				waitErr, processWaited = waitExecProcess(waited, config.TerminationGrace)
				if !processWaited {
					_ = signalProbeProcessGroup(processGroupID, probeProcessGroupKill)
					return ExecFailureResult{}, errors.New("exec failure process did not exit after kill")
				}
			}
		}
	}

	// A leader can exit normally while a tool or another descendant keeps the
	// process group alive. Always drain that group, including the natural-exit
	// path, before accepting completion.
	groupAlive, groupErr := probeProcessGroupAlive(processGroupID)
	if groupErr != nil {
		return ExecFailureResult{}, errors.New("inspect exec failure process group")
	}
	if groupAlive {
		if config.signalProcessGroupIfAlive(processGroupID, probeProcessGroupTerminate) {
			termSent = true
		}
		gone, waitGroupErr := waitProbeProcessGroupGone(processGroupID, config.TerminationGrace)
		if waitGroupErr != nil {
			return ExecFailureResult{}, waitGroupErr
		}
		if !gone {
			if config.signalProcessGroupIfAlive(processGroupID, probeProcessGroupKill) {
				killSent = true
			}
			gone, waitGroupErr = waitProbeProcessGroupGone(processGroupID, config.TerminationGrace)
			if waitGroupErr != nil {
				return ExecFailureResult{}, waitGroupErr
			}
			if !gone {
				return ExecFailureResult{}, errors.New("exec failure descendants survived teardown")
			}
		}
	}

	if err := waitExecReader(stdoutDone, config.TerminationGrace); err != nil {
		return ExecFailureResult{}, err
	}
	if err := waitExecReader(stderrDone, config.TerminationGrace); err != nil {
		return ExecFailureResult{}, err
	}
	usage, usageErr := usageSampler.finish(processUsageFinishBound)
	usageFinished = true
	if usageErr != nil {
		// Partial samples are not a trustworthy peak/count measurement.
		usage = processGroupUsage{}
	}
	directPeak := processStatePeakRSS(command.ProcessState)
	select {
	case code := <-violation:
		if failureCode == "" {
			failureCode = code
		}
	default:
	}
	snapshot := state.snapshot()
	if snapshot.failureCode != "" && failureCode == "" {
		failureCode = snapshot.failureCode
	}
	exitCode := -1
	if command.ProcessState != nil {
		exitCode = command.ProcessState.ExitCode()
	}
	teardownFailure := waitErr != nil || termSent || killSent || failureCode == "stderr_limit_exceeded"
	classification := classifyExecFailure(snapshot, failureCode, cancelled, deadline, teardownFailure)
	return ExecFailureResult{
		Classification: classification, Accepted: snapshot.accepted,
		Terminal: snapshot.terminal, FinalAgentItem: snapshot.finalAgentItem,
		Cancelled: cancelled, Deadline: deadline, TermSent: termSent, KillSent: killSent,
		DescendantsReaped: true, ExitCode: exitCode, EventCount: snapshot.eventCount,
		StdoutBytes: snapshot.stdoutBytes, StderrBytes: stderrCounter.countValue(),
		DirectChildPeakRSSBytes: directPeak,
		GroupPeakRSSBytes:       usage.PeakRSSBytes,
		MaxDescendants:          usage.MaxDescendants,
		GroupUsageAvailable:     usageErr == nil,
		GroupUsageFailureCode:   processGroupUsageFailureCode(usageErr),
		Duration:                time.Since(startedAt), FailureCode: failureCode,
	}, nil
}

func classifyExecFailure(
	state execJSONLSnapshot,
	failureCode string,
	cancelled bool,
	deadline bool,
	teardownFailure bool,
) string {
	if state.terminalProtocolDrift {
		return ExecClassTerminalProtocolDrift
	}
	if state.terminal {
		if teardownFailure || cancelled || deadline {
			return ExecClassCompletedWithTeardownFailure
		}
		return ExecClassCompleted
	}
	if state.accepted {
		if cancelled || deadline {
			return ExecClassCancelledAmbiguous
		}
		return ExecClassAmbiguous
	}
	return ExecClassPreAcceptance
}

func signalProcessGroupIfAlive(processGroupID int, signal probeProcessGroupSignal) bool {
	alive, err := probeProcessGroupAlive(processGroupID)
	if err != nil || !alive {
		return false
	}
	return signalProbeProcessGroup(processGroupID, signal) == nil
}

func (config ExecFailureConfig) signalProcessGroupIfAlive(
	processGroupID int,
	signal probeProcessGroupSignal,
) bool {
	if config.testSignalGroup != nil {
		return config.testSignalGroup(processGroupID, signal)
	}
	return signalProcessGroupIfAlive(processGroupID, signal)
}

func waitExecProcess(waited <-chan error, bound time.Duration) (error, bool) {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case err := <-waited:
		return err, true
	case <-timer.C:
		return nil, false
	}
}

func waitProbeProcessGroupGone(processGroupID int, bound time.Duration) (bool, error) {
	deadline := time.NewTimer(bound)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		alive, err := probeProcessGroupAlive(processGroupID)
		if err != nil {
			return false, errors.New("inspect exec failure process group")
		}
		if !alive {
			return true, nil
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return false, nil
		}
	}
}

func waitExecReader(done <-chan struct{}, bound time.Duration) error {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("exec failure output reader did not stop")
	}
}

func notifyExecViolation(channel chan<- string, code string) {
	select {
	case channel <- code:
	default:
	}
}

type boundedCounter struct {
	mu    sync.Mutex
	limit int
	count int
}

func (counter *boundedCounter) Write(value []byte) (int, error) {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	remaining := counter.limit - counter.count
	if len(value) > remaining {
		if remaining > 0 {
			counter.count += remaining
		}
		return 0, errExecOutputLimit
	}
	counter.count += len(value)
	return len(value), nil
}

func (counter *boundedCounter) countValue() int {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	return counter.count
}

type execJSONLState struct {
	mu                    sync.Mutex
	maxFinalBytes         int
	threadStarted         bool
	accepted              bool
	terminal              bool
	finalAgentItem        bool
	terminalProtocolDrift bool
	eventCount            int
	stdoutBytes           int
	failureCode           string
}

type execJSONLSnapshot struct {
	accepted              bool
	terminal              bool
	finalAgentItem        bool
	terminalProtocolDrift bool
	eventCount            int
	stdoutBytes           int
	failureCode           string
}

func (state *execJSONLState) snapshot() execJSONLSnapshot {
	state.mu.Lock()
	defer state.mu.Unlock()
	return execJSONLSnapshot{
		accepted: state.accepted, terminal: state.terminal,
		finalAgentItem:        state.finalAgentItem,
		terminalProtocolDrift: state.terminalProtocolDrift,
		eventCount:            state.eventCount, stdoutBytes: state.stdoutBytes,
		failureCode: state.failureCode,
	}
}

func readExecJSONL(reader io.Reader, config ExecFailureConfig, state *execJSONLState) string {
	buffered := bufio.NewReaderSize(reader, config.MaxLineBytes)
	for {
		line, err := buffered.ReadSlice('\n')
		if state.addStdoutBytes(len(line), config.MaxStdoutBytes) {
			return state.recordReadFailure("stdout_limit_exceeded")
		}
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > config.MaxLineBytes {
			return state.recordReadFailure("jsonl_line_limit_exceeded")
		}
		if len(line) > 0 {
			if err != nil {
				return state.recordReadFailure("jsonl_unterminated_frame")
			}
			frame := bytes.TrimSuffix(line, []byte{'\n'})
			frame = bytes.TrimSuffix(frame, []byte{'\r'})
			if len(frame) == 0 {
				return state.recordReadFailure("jsonl_empty_frame")
			}
			if code := state.consume(frame, config.MaxEvents); code != "" {
				return code
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return ""
			}
			return state.recordReadFailure("jsonl_read_failed")
		}
	}
}

type execEventEnvelope struct {
	Type string          `json:"type"`
	Item json.RawMessage `json:"item"`
}

type execItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (state *execJSONLState) consume(frame []byte, maxEvents int) string {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.eventCount++
	if state.eventCount > maxEvents {
		return state.fail("event_count_exceeded", state.terminal)
	}
	if state.terminal {
		return state.fail("event_after_terminal", true)
	}
	var envelope execEventEnvelope
	if json.Unmarshal(frame, &envelope) != nil || envelope.Type == "" {
		return state.fail("invalid_jsonl_event", false)
	}
	switch envelope.Type {
	case "thread.started":
		if state.threadStarted || state.accepted {
			return state.fail("thread_started_out_of_order", false)
		}
		state.threadStarted = true
	case "turn.started":
		if !state.threadStarted {
			return state.fail("turn_started_before_thread", false)
		}
		if state.accepted {
			return state.fail("duplicate_turn_started", false)
		}
		state.accepted = true
	case "item.started":
		if !state.accepted {
			return state.fail("item_before_acceptance", false)
		}
		if len(envelope.Item) != 0 {
			var item execItem
			if json.Unmarshal(envelope.Item, &item) != nil ||
				(item.Type != "agent_message" && item.Type != "reasoning") {
				return state.fail("unexpected_effect_item", false)
			}
		}
	case "item.completed":
		if !state.accepted {
			return state.fail("item_before_acceptance", false)
		}
		var item execItem
		if len(envelope.Item) == 0 || json.Unmarshal(envelope.Item, &item) != nil || item.Type == "" {
			return state.fail("invalid_completed_item", false)
		}
		switch item.Type {
		case "agent_message":
			if state.finalAgentItem || item.Text == "" || len(item.Text) > state.maxFinalBytes {
				return state.fail("invalid_final_agent_item", false)
			}
			state.finalAgentItem = true
		case "reasoning":
			// Reasoning completion is non-terminal and its content is not retained.
		default:
			return state.fail("unexpected_effect_item", false)
		}
	case "turn.completed":
		if !state.accepted || !state.finalAgentItem {
			return state.fail("terminal_without_final_agent_item", true)
		}
		if state.failureCode != "" {
			return state.fail("terminal_after_error", true)
		}
		state.terminal = true
	case "error":
		if state.failureCode == "" {
			state.failureCode = "codex_error_event"
		}
	default:
		return state.fail("unknown_jsonl_event", false)
	}
	return ""
}

func (state *execJSONLState) fail(code string, terminalDrift bool) string {
	if state.failureCode == "" {
		state.failureCode = code
	}
	if terminalDrift || state.terminal {
		state.terminalProtocolDrift = true
	}
	return code
}

func (state *execJSONLState) addStdoutBytes(count, limit int) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if count > limit-state.stdoutBytes {
		state.stdoutBytes = limit
		return true
	}
	state.stdoutBytes += count
	return false
}

func (state *execJSONLState) recordReadFailure(code string) string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.fail(code, state.terminal)
}
