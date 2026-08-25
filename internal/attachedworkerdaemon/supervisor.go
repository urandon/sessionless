// Package attachedworkerdaemon implements the local, owner-managed worker
// daemon boundary. It deliberately requires an external isolation launcher;
// a replacement environment and a private directory are not treated as a
// filesystem, network, or same-UID isolation boundary.
package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxSupervisorStdoutBytes = 16 << 20
	maxSupervisorStderrBytes = 1 << 20
	maxSupervisorGrace       = 30 * time.Second
)

var (
	ErrIsolationUnsupported = errors.New("attached worker isolation is unsupported")
	ErrSupervisorConfig     = errors.New("attached worker supervisor configuration is invalid")
	ErrExecutableChanged    = errors.New("attached worker harness executable digest changed")
	environmentNamePattern  = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
)

// IsolationProfile is trusted only after its concrete Launcher implementation
// has been independently reviewed. Every property is mandatory for a
// production attempt. The daemon never upgrades a missing property to a pass.
type IsolationProfile struct {
	Name                    string
	FilesystemReadBoundary  bool
	FilesystemWriteBoundary bool
	NetworkDenied           bool
	ProcessBoundary         bool
	DiskBytesBounded        bool
}

func (profile IsolationProfile) Validate() error {
	if profile.Name == "" || !profile.FilesystemReadBoundary ||
		!profile.FilesystemWriteBoundary || !profile.NetworkDenied ||
		!profile.ProcessBoundary || !profile.DiskBytesBounded {
		return ErrIsolationUnsupported
	}
	return nil
}

// LaunchSpec is the complete replacement process contract presented to an
// isolation launcher. It contains no inherited environment or shell command.
type LaunchSpec struct {
	Executable  string
	Arguments   []string
	Directory   string
	Environment []string
	ReadFiles   []string
	ReadRoots   []string
	WriteFiles  []string
	WriteRoots  []string
}

// IsolationBoundary is the complete lifecycle of one prepared OS isolation
// boundary. Command starts the local client/launcher process; the remaining
// methods control and verify the isolated workload itself. This distinction is
// required for runtimes whose workload can outlive their local CLI process.
// Implementations must honor context cancellation and must make Release
// idempotent.
type IsolationBoundary interface {
	Command() *exec.Cmd
	GracefulStop(context.Context) error
	ForceStop(context.Context) error
	Alive(context.Context) (bool, error)
	Release(context.Context) error
}

// IsolationLauncher prepares the reviewed OS boundary and exact harness
// command. Prepare must either return a fully releasable boundary or clean up
// every resource it created before returning an error.
type IsolationLauncher interface {
	Profile() IsolationProfile
	Prepare(context.Context, LaunchSpec) (IsolationBoundary, error)
}

type SupervisorConfig struct {
	ScratchRoot             string
	Launcher                IsolationLauncher
	Timeout                 time.Duration
	TerminationGrace        time.Duration
	MaxStdoutBytes          int
	MaxStderrBytes          int
	AllowedEnvironmentNames []string
	AllowedReadRoots        []string
}

type ExecutableDigest [sha256.Size]byte

func DigestExecutable(path string) (ExecutableDigest, error) {
	canonical, err := validateRegularPath(path)
	if err != nil {
		return ExecutableDigest{}, err
	}
	file, err := os.Open(canonical)
	if err != nil {
		return ExecutableDigest{}, ErrSupervisorConfig
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return ExecutableDigest{}, ErrSupervisorConfig
	}
	var result ExecutableDigest
	copy(result[:], digest.Sum(nil))
	return result, nil
}

type EnvironmentVariable struct {
	Name  string
	Value string
}

type AttemptSpec struct {
	Executable          string
	ExecutableDigest    ExecutableDigest
	Arguments           []string
	Environment         []EnvironmentVariable
	AdditionalReadRoots []string
	credentialWriteFile string
}

type AttemptResult struct {
	ExitCode          int
	Cancelled         bool
	Deadline          bool
	TermSent          bool
	KillSent          bool
	DescendantsReaped bool
	Stdout            []byte
	StdoutBytes       int
	StderrBytes       int
	FailureCode       string
	Duration          time.Duration
	IsolationProfile  string
	BoundaryReleased  bool
	CleanupSucceeded  bool
}

type Supervisor struct {
	root             string
	launcher         IsolationLauncher
	profileName      string
	timeout          time.Duration
	grace            time.Duration
	maxStdout        int
	maxStderr        int
	allowedEnv       map[string]struct{}
	allowedReadRoots []string
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return nil, ErrIsolationUnsupported
	}
	if config.Launcher == nil {
		return nil, ErrIsolationUnsupported
	}
	profile := config.Launcher.Profile()
	if profile.Validate() != nil {
		return nil, ErrIsolationUnsupported
	}
	root, err := validateDirectoryPath(config.ScratchRoot)
	if err != nil {
		return nil, ErrSupervisorConfig
	}
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Minute
	}
	if config.TerminationGrace <= 0 {
		config.TerminationGrace = 5 * time.Second
	}
	if config.MaxStdoutBytes <= 0 {
		config.MaxStdoutBytes = 4 << 20
	}
	if config.MaxStderrBytes <= 0 {
		config.MaxStderrBytes = 64 << 10
	}
	if config.TerminationGrace > maxSupervisorGrace ||
		config.MaxStdoutBytes < 1 || config.MaxStdoutBytes > maxSupervisorStdoutBytes ||
		config.MaxStderrBytes < 1 || config.MaxStderrBytes > maxSupervisorStderrBytes {
		return nil, ErrSupervisorConfig
	}
	allowed := make(map[string]struct{}, len(config.AllowedEnvironmentNames))
	for _, name := range config.AllowedEnvironmentNames {
		if !environmentNamePattern.MatchString(name) || reservedEnvironmentName(name) {
			return nil, ErrSupervisorConfig
		}
		if _, exists := allowed[name]; exists {
			return nil, ErrSupervisorConfig
		}
		allowed[name] = struct{}{}
	}
	allowedReadRoots := make([]string, 0, len(config.AllowedReadRoots))
	seenReadRoots := make(map[string]struct{}, len(config.AllowedReadRoots))
	for _, value := range config.AllowedReadRoots {
		canonical, err := validateDirectoryPath(value)
		if err != nil || canonical != value {
			return nil, ErrSupervisorConfig
		}
		if _, exists := seenReadRoots[canonical]; exists {
			return nil, ErrSupervisorConfig
		}
		seenReadRoots[canonical] = struct{}{}
		allowedReadRoots = append(allowedReadRoots, canonical)
	}
	sort.Strings(allowedReadRoots)
	return &Supervisor{
		root: root, launcher: config.Launcher, profileName: profile.Name, timeout: config.Timeout,
		grace: config.TerminationGrace, maxStdout: config.MaxStdoutBytes,
		maxStderr: config.MaxStderrBytes, allowedEnv: allowed, allowedReadRoots: allowedReadRoots,
	}, nil
}

func (supervisor *Supervisor) Run(parent context.Context, spec AttemptSpec) (result AttemptResult, err error) {
	if parent == nil || parent.Err() != nil {
		return AttemptResult{}, ErrSupervisorConfig
	}
	if err := supervisor.validateAttemptSpec(spec); err != nil {
		return AttemptResult{}, err
	}
	actualDigest, err := DigestExecutable(spec.Executable)
	if err != nil || actualDigest != spec.ExecutableDigest {
		return AttemptResult{}, ErrExecutableChanged
	}
	attemptRoot, err := supervisor.createAttemptRoot()
	if err != nil {
		return AttemptResult{}, err
	}
	result.CleanupSucceeded = false
	rootCleaned := false
	boundaryReleased := true
	boundaryTeardownAttempted := false
	boundaryTeardownSucceeded := true
	defer func() {
		cleanupErr := cleanupAttemptRoot(supervisor.root, attemptRoot)
		rootCleaned = cleanupErr == nil
		result.CleanupSucceeded = rootCleaned && boundaryTeardownSucceeded && boundaryReleased
		if err == nil && cleanupErr != nil {
			err = cleanupErr
		}
	}()
	environment, err := supervisor.replacementEnvironment(attemptRoot, spec.Environment)
	if err != nil {
		return result, err
	}
	readRoots := []string{attemptRoot}
	readRoots = append(readRoots, spec.AdditionalReadRoots...)
	launch := LaunchSpec{
		Executable: spec.Executable, Arguments: append([]string(nil), spec.Arguments...),
		Directory: filepath.Join(attemptRoot, "work"), Environment: environment,
		ReadFiles: []string{spec.Executable}, ReadRoots: canonicalRootSet(readRoots),
		WriteRoots: []string{attemptRoot},
	}
	if spec.credentialWriteFile != "" {
		launch.WriteFiles = []string{spec.credentialWriteFile}
	}
	prepareCtx, cancelPrepare := context.WithTimeout(parent, supervisor.timeout)
	boundary, err := supervisor.launcher.Prepare(prepareCtx, launch)
	cancelPrepare()
	if err != nil || boundary == nil {
		return result, ErrSupervisorConfig
	}
	boundaryReleased = false
	boundaryTeardownSucceeded = false
	defer func() {
		if !boundaryTeardownAttempted {
			boundaryTeardownAttempted = true
			boundaryTeardownSucceeded = teardownIsolationBoundary(boundary, supervisor.grace) == nil
		}
		releaseErr := callBoundary(supervisor.grace, boundary.Release)
		boundaryReleased = releaseErr == nil
		result.BoundaryReleased = boundaryReleased
		result.CleanupSucceeded = rootCleaned && boundaryTeardownSucceeded && boundaryReleased
		if err == nil && !boundaryTeardownSucceeded {
			err = errors.New("teardown attached worker isolation boundary")
		}
		if err == nil && releaseErr != nil {
			err = errors.New("release attached worker isolation boundary")
		}
	}()
	command := boundary.Command()
	if command == nil || command.Dir != launch.Directory ||
		!equalStrings(command.Env, launch.Environment) || command.Stdin != nil || len(command.ExtraFiles) != 0 {
		return result, ErrSupervisorConfig
	}
	configureProcessGroup(command)
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return result, errors.New("create attached worker stdout")
	}
	defer stdoutReader.Close()
	defer stdoutWriter.Close()
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		return result, errors.New("create attached worker stderr")
	}
	defer stderrReader.Close()
	defer stderrWriter.Close()
	command.Stdout = stdoutWriter
	command.Stderr = stderrWriter
	startedAt := time.Now()
	if err := command.Start(); err != nil {
		return result, errors.New("start attached worker harness")
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	processGroupID := command.Process.Pid
	stdout := &boundedBuffer{limit: supervisor.maxStdout}
	stderr := &boundedBuffer{limit: supervisor.maxStderr, discard: true}
	violation := make(chan string, 2)
	stdoutDone := copyBounded(stdoutReader, stdout, violation, "stdout_limit_exceeded")
	stderrDone := copyBounded(stderrReader, stderr, violation, "stderr_limit_exceeded")
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	timer := time.NewTimer(supervisor.timeout)
	defer timer.Stop()
	var waitErr error
	processWaited := false
	select {
	case waitErr = <-waited:
		processWaited = true
	case result.FailureCode = <-violation:
	case <-parent.Done():
		result.Cancelled = true
	case <-timer.C:
		result.Deadline = true
	}
	if !processWaited {
		result.TermSent = signalProcessGroup(processGroupID, processTerminate) == nil
		waitErr, processWaited = waitProcess(waited, supervisor.grace)
		if !processWaited {
			result.KillSent = signalProcessGroup(processGroupID, processKill) == nil
			waitErr, processWaited = waitProcess(waited, supervisor.grace)
			if !processWaited {
				_ = signalProcessGroup(processGroupID, processKill)
				return result, errors.New("attached worker harness survived kill")
			}
		}
	}
	alive, inspectErr := processGroupAlive(processGroupID)
	if inspectErr != nil {
		return result, errors.New("inspect attached worker process group")
	}
	if alive {
		if signalProcessGroup(processGroupID, processTerminate) == nil {
			result.TermSent = true
		}
		gone, waitErr := waitProcessGroupGone(processGroupID, supervisor.grace)
		if waitErr != nil {
			return result, waitErr
		}
		if !gone {
			if signalProcessGroup(processGroupID, processKill) == nil {
				result.KillSent = true
			}
			gone, waitErr = waitProcessGroupGone(processGroupID, supervisor.grace)
			if waitErr != nil || !gone {
				return result, errors.New("attached worker descendants survived teardown")
			}
		}
	}
	boundaryTeardownAttempted = true
	if err := teardownIsolationBoundary(boundary, supervisor.grace); err != nil {
		return result, err
	}
	boundaryTeardownSucceeded = true
	if !waitChannel(stdoutDone, supervisor.grace) || !waitChannel(stderrDone, supervisor.grace) {
		return result, errors.New("attached worker output reader survived teardown")
	}
	select {
	case code := <-violation:
		if result.FailureCode == "" {
			result.FailureCode = code
		}
	default:
	}
	result.ExitCode = -1
	if command.ProcessState != nil {
		result.ExitCode = command.ProcessState.ExitCode()
	}
	if waitErr != nil && result.FailureCode == "" {
		result.FailureCode = "process_exit_failed"
	}
	result.DescendantsReaped = true
	result.StdoutBytes = stdout.countValue()
	result.StderrBytes = stderr.countValue()
	if result.FailureCode == "" {
		result.Stdout = stdout.bytesValue()
	}
	result.Duration = time.Since(startedAt)
	result.IsolationProfile = supervisor.profileName
	return result, nil
}

func teardownIsolationBoundary(boundary IsolationBoundary, timeout time.Duration) error {
	alive, err := inspectBoundary(boundary, timeout)
	if err != nil {
		forceErr := callBoundary(timeout, boundary.ForceStop)
		stillAlive, verifyErr := inspectBoundary(boundary, timeout)
		if forceErr != nil || verifyErr != nil || stillAlive {
			return errors.New("attached worker isolation boundary cleanup failed")
		}
		return errors.New("inspect attached worker isolation boundary")
	}
	if !alive {
		return nil
	}
	gracefulErr := callBoundary(timeout, boundary.GracefulStop)
	alive, err = inspectBoundary(boundary, timeout)
	if err != nil {
		forceErr := callBoundary(timeout, boundary.ForceStop)
		stillAlive, verifyErr := inspectBoundary(boundary, timeout)
		if forceErr != nil || verifyErr != nil || stillAlive {
			return errors.New("attached worker isolation boundary cleanup failed")
		}
		return errors.New("inspect attached worker isolation boundary")
	}
	if !alive {
		if gracefulErr != nil {
			return errors.New("stop attached worker isolation boundary")
		}
		return nil
	}
	forceErr := callBoundary(timeout, boundary.ForceStop)
	alive, err = inspectBoundary(boundary, timeout)
	if err != nil {
		return errors.New("inspect attached worker isolation boundary")
	}
	if alive {
		return errors.New("attached worker isolation boundary survived teardown")
	}
	if gracefulErr != nil || forceErr != nil {
		return errors.New("stop attached worker isolation boundary")
	}
	return nil
}

func callBoundary(timeout time.Duration, operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- operation(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func inspectBoundary(boundary IsolationBoundary, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	type inspection struct {
		alive bool
		err   error
	}
	done := make(chan inspection, 1)
	go func() {
		alive, err := boundary.Alive(ctx)
		done <- inspection{alive: alive, err: err}
	}()
	select {
	case result := <-done:
		return result.alive, result.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (supervisor *Supervisor) validateAttemptSpec(spec AttemptSpec) error {
	canonical, err := validateRegularPath(spec.Executable)
	if err != nil || canonical != spec.Executable || spec.ExecutableDigest == (ExecutableDigest{}) {
		return ErrSupervisorConfig
	}
	for _, argument := range spec.Arguments {
		if strings.IndexByte(argument, 0) >= 0 {
			return ErrSupervisorConfig
		}
	}
	seen := make(map[string]struct{}, len(spec.Environment))
	for _, variable := range spec.Environment {
		if !environmentNamePattern.MatchString(variable.Name) || reservedEnvironmentName(variable.Name) ||
			strings.IndexByte(variable.Value, 0) >= 0 {
			return ErrSupervisorConfig
		}
		if _, allowed := supervisor.allowedEnv[variable.Name]; !allowed {
			return ErrSupervisorConfig
		}
		if _, exists := seen[variable.Name]; exists {
			return ErrSupervisorConfig
		}
		seen[variable.Name] = struct{}{}
	}
	seenRoots := make(map[string]struct{}, len(spec.AdditionalReadRoots))
	for _, root := range spec.AdditionalReadRoots {
		canonical, err := validateDirectoryPath(root)
		if err != nil || canonical != root || !supervisor.readRootAllowed(root) {
			return ErrSupervisorConfig
		}
		if _, exists := seenRoots[root]; exists {
			return ErrSupervisorConfig
		}
		seenRoots[root] = struct{}{}
	}
	if spec.credentialWriteFile != "" {
		canonical, err := validateDataFilePath(spec.credentialWriteFile)
		if err != nil || canonical != spec.credentialWriteFile ||
			filepath.Base(canonical) != "auth.json" {
			return ErrSupervisorConfig
		}
		if _, exists := seenRoots[filepath.Dir(canonical)]; !exists {
			return ErrSupervisorConfig
		}
	}
	return nil
}

func (supervisor *Supervisor) readRootAllowed(candidate string) bool {
	for _, allowed := range supervisor.allowedReadRoots {
		relative, err := filepath.Rel(allowed, candidate)
		if err == nil && relative != "." && relative != ".." &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func (supervisor *Supervisor) createAttemptRoot() (string, error) {
	root, err := os.MkdirTemp(supervisor.root, "attempt-")
	if err != nil || os.Chmod(root, 0o700) != nil {
		return "", errors.New("create attached worker attempt root")
	}
	for _, relative := range []string{"home", "tmp", "work", "xdg/config", "xdg/cache", "xdg/data"} {
		if err := os.MkdirAll(filepath.Join(root, relative), 0o700); err != nil {
			_ = cleanupAttemptRoot(supervisor.root, root)
			return "", errors.New("create attached worker attempt directory")
		}
	}
	return root, nil
}

func (supervisor *Supervisor) replacementEnvironment(root string, extra []EnvironmentVariable) ([]string, error) {
	values := []string{
		"HOME=" + filepath.Join(root, "home"),
		"TMPDIR=" + filepath.Join(root, "tmp"),
		"XDG_CONFIG_HOME=" + filepath.Join(root, "xdg/config"),
		"XDG_CACHE_HOME=" + filepath.Join(root, "xdg/cache"),
		"XDG_DATA_HOME=" + filepath.Join(root, "xdg/data"),
		"PATH=",
		"LANG=C.UTF-8",
		"LC_ALL=C.UTF-8",
		"NO_COLOR=1",
	}
	ordered := append([]EnvironmentVariable(nil), extra...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	for _, variable := range ordered {
		values = append(values, variable.Name+"="+variable.Value)
	}
	return values, nil
}

func validateDirectoryPath(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", ErrSupervisorConfig
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrSupervisorConfig
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return "", ErrSupervisorConfig
	}
	return canonical, nil
}

func validateRegularPath(path string) (string, error) {
	canonical, info, err := validateDataFile(path)
	if err != nil || info.Mode().Perm()&0o111 == 0 {
		return "", ErrSupervisorConfig
	}
	return canonical, nil
}

func validateDataFilePath(path string) (string, error) {
	canonical, _, err := validateDataFile(path)
	return canonical, err
}

func validateDataFile(path string) (string, os.FileInfo, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", nil, ErrSupervisorConfig
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", nil, ErrSupervisorConfig
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return "", nil, ErrSupervisorConfig
	}
	return canonical, info, nil
}

func cleanupAttemptRoot(supervisorRoot, attemptRoot string) error {
	if filepath.Dir(attemptRoot) != supervisorRoot || !strings.HasPrefix(filepath.Base(attemptRoot), "attempt-") {
		return errors.New("refuse unsafe attached worker cleanup")
	}
	info, err := os.Lstat(attemptRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refuse changed attached worker attempt root")
	}
	if err := os.RemoveAll(attemptRoot); err != nil {
		return errors.New("remove attached worker attempt root")
	}
	return nil
}

func reservedEnvironmentName(name string) bool {
	switch name {
	case "HOME", "TMPDIR", "TMP", "TEMP", "PATH", "LANG", "LC_ALL", "NO_COLOR",
		"XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "OPENAI_API_KEY",
		"ANTHROPIC_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY":
		return true
	}
	return strings.HasSuffix(name, "_API_KEY") || strings.HasSuffix(name, "_ACCESS_TOKEN") ||
		strings.HasSuffix(name, "_SECRET")
}

func canonicalRootSet(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var errBoundedOutput = errors.New("attached worker output limit exceeded")

type boundedBuffer struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	limit   int
	count   int
	discard bool
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	remaining := buffer.limit - buffer.count
	if len(value) > remaining {
		if remaining > 0 {
			if !buffer.discard {
				_, _ = buffer.buffer.Write(value[:remaining])
			}
			buffer.count += remaining
		}
		return 0, errBoundedOutput
	}
	buffer.count += len(value)
	if !buffer.discard {
		_, _ = buffer.buffer.Write(value)
	}
	return len(value), nil
}

func (buffer *boundedBuffer) countValue() int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.count
}

func (buffer *boundedBuffer) bytesValue() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return append([]byte(nil), buffer.buffer.Bytes()...)
}

func copyBounded(reader io.Reader, writer io.Writer, violation chan<- string, code string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := io.Copy(writer, reader); errors.Is(err, errBoundedOutput) {
			select {
			case violation <- code:
			default:
			}
		}
	}()
	return done
}

func waitProcess(waited <-chan error, bound time.Duration) (error, bool) {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case err := <-waited:
		return err, true
	case <-timer.C:
		return nil, false
	}
}

func waitProcessGroupGone(processGroupID int, bound time.Duration) (bool, error) {
	timer := time.NewTimer(bound)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		alive, err := processGroupAlive(processGroupID)
		if err != nil {
			return false, errors.New("inspect attached worker process group")
		}
		if !alive {
			return true, nil
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			return false, nil
		}
	}
}

func waitChannel(done <-chan struct{}, bound time.Duration) bool {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
