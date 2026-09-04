package codexapp

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
	"strings"
	"sync"
	"time"
)

const (
	defaultRequestTimeout  = 15 * time.Second
	defaultTurnTimeout     = 10 * time.Minute
	defaultShutdownTimeout = 3 * time.Second
	defaultMaxFrameBytes   = 4 << 20
	defaultMaxStderrBytes  = 64 << 10
	defaultMaxAuthBytes    = 1 << 20
)

type rpcResponse struct {
	result json.RawMessage
	err    error
}

type wireError struct {
	Code int64 `json:"code"`
}

type wireEnvelope struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *wireError      `json:"error"`
}

type Client struct {
	config Config
	paths  Paths
	cmd    *exec.Cmd
	stdin  io.WriteCloser

	writeMu sync.Mutex
	mu      sync.Mutex
	nextID  uint64
	pending map[string]chan rpcResponse
	fatal   error
	closing bool

	initialized bool
	authMode    string
	authValid   bool
	threadID    string
	threads     map[string]struct{}
	turns       map[string]string
	turnDone    map[string]TurnResult
	turnWait    map[string][]chan TurnResult
	turnErrors  map[string]string
	loginDone   map[string]LoginResult
	loginWait   map[string][]chan LoginResult
	rateLimits  RateLimits
	rateReadMu  sync.Mutex
	rateReading bool
	rateUpdates []RateLimitSnapshot
	tokenUsage  TokenUsage

	done        chan struct{}
	processDone chan struct{}
	ioDone      sync.WaitGroup
	failOnce    sync.Once
	cleanupOnce sync.Once
	closeOnce   sync.Once
	closeDone   chan struct{}
	stderr      *boundedBuffer
	ownedRoot   string
	prepared    *preparedGuard
}

// Start launches one invocation-scoped app-server. ctx owns the process
// lifetime: cancellation kills and reaps the entire process group.
func Start(ctx context.Context, config Config) (*Client, error) {
	config = withDefaults(config)
	paths, err := createPaths(config.ScratchRoot)
	if err != nil {
		return nil, ErrProcessUnavailable
	}
	return startClient(ctx, config, paths, paths.Root, nil)
}

// StartPrepared borrows an invocation CODEX_HOME and Manager work directory.
// Only the fresh auxiliary Root/Home/TempDir returned in Paths is owned and
// removed by the client.
func StartPrepared(ctx context.Context, config Config, prepared PreparedPaths) (*Client, error) {
	config = withDefaults(config)
	guard, err := openPreparedPaths(prepared, config.MaxAuthBytes)
	if err != nil {
		return nil, ErrProcessUnavailable
	}
	paths, err := createPreparedPaths(config.ScratchRoot, prepared)
	if err != nil {
		guard.close()
		return nil, ErrProcessUnavailable
	}
	return startClient(ctx, config, paths, paths.Root, guard)
}

func startClient(
	ctx context.Context,
	config Config,
	paths Paths,
	ownedRoot string,
	guard *preparedGuard,
) (*Client, error) {
	executable := config.Executable
	if executable == "" {
		executable = "codex"
	}
	arguments := append([]string(nil), config.testArguments...)
	if len(arguments) == 0 {
		arguments = []string{"app-server", "--stdio"}
	}

	command := exec.Command(executable, arguments...)
	configureProcessGroup(command)
	command.Dir = paths.WorkDir
	command.Env = childEnvironment(paths)
	stdin, err := command.StdinPipe()
	if err != nil {
		cleanupStartFailure(ownedRoot, guard)
		return nil, ErrProcessUnavailable
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		cleanupStartFailure(ownedRoot, guard)
		return nil, ErrProcessUnavailable
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		cleanupStartFailure(ownedRoot, guard)
		return nil, ErrProcessUnavailable
	}
	client := &Client{
		config: config, paths: paths, cmd: command, stdin: stdin,
		pending: make(map[string]chan rpcResponse),
		threads: make(map[string]struct{}), turns: make(map[string]string),
		turnDone: make(map[string]TurnResult), turnWait: make(map[string][]chan TurnResult),
		turnErrors: make(map[string]string),
		loginDone:  make(map[string]LoginResult), loginWait: make(map[string][]chan LoginResult),
		rateLimits: RateLimits{ByLimitID: make(map[string]RateLimitSnapshot)},
		done:       make(chan struct{}), processDone: make(chan struct{}), closeDone: make(chan struct{}),
		stderr: &boundedBuffer{limit: config.MaxStderrBytes}, ownedRoot: ownedRoot, prepared: guard,
	}
	if guard != nil && guard.recheck(true) != nil {
		_ = stdin.Close()
		cleanupStartFailure(ownedRoot, guard)
		return nil, ErrProcessUnavailable
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		cleanupStartFailure(ownedRoot, guard)
		return nil, ErrProcessUnavailable
	}
	if guard != nil && guard.recheck(true) != nil {
		killProcessGroup(command)
		_ = command.Wait()
		_ = stdin.Close()
		cleanupStartFailure(ownedRoot, guard)
		return nil, ErrProcessUnavailable
	}
	client.ioDone.Add(2)
	go client.readLoop(stdout)
	go func() {
		defer client.ioDone.Done()
		_, _ = io.Copy(client.stderr, stderr)
	}()
	go client.waitProcess()
	go func() {
		select {
		case <-ctx.Done():
			client.fail(ErrDeadline)
		case <-client.done:
		}
	}()
	return client, nil
}

func withDefaults(config Config) Config {
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.TurnTimeout <= 0 {
		config.TurnTimeout = defaultTurnTimeout
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}
	if config.MaxFrameBytes <= 0 {
		config.MaxFrameBytes = defaultMaxFrameBytes
	}
	if config.MaxStderrBytes <= 0 {
		config.MaxStderrBytes = defaultMaxStderrBytes
	}
	if config.MaxAuthBytes <= 0 {
		config.MaxAuthBytes = defaultMaxAuthBytes
	}
	return config
}

func createPreparedPaths(parent string, prepared PreparedPaths) (Paths, error) {
	if err := validatePrivateDirectory(parent); err != nil {
		return Paths{}, err
	}
	root, err := os.MkdirTemp(parent, "sessionless-codexapp-")
	if err != nil {
		return Paths{}, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return Paths{}, err
	}
	paths := Paths{
		Root: root, Home: filepath.Join(root, "home"), TempDir: filepath.Join(root, "tmp"),
		CodexHome: prepared.CodexHome, WorkDir: prepared.WorkDir,
	}
	for _, path := range []string{paths.Home, paths.TempDir} {
		if err := os.Mkdir(path, 0o700); err != nil {
			_ = os.RemoveAll(root)
			return Paths{}, err
		}
	}
	if !disjointRoots(paths.Root, paths.CodexHome) || !disjointRoots(paths.Root, paths.WorkDir) ||
		!disjointRoots(paths.CodexHome, paths.WorkDir) {
		_ = os.RemoveAll(root)
		return Paths{}, errors.New("prepared roots overlap")
	}
	return paths, nil
}

func validatePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("directory path is not normalized")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("directory path is not canonical")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		return errors.New("directory is not private")
	}
	return nil
}

func disjointRoots(left, right string) bool {
	for _, pair := range [][2]string{{left, right}, {right, left}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err != nil || relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
			return false
		}
	}
	return true
}

func cleanupStartFailure(ownedRoot string, guard *preparedGuard) {
	if guard != nil {
		guard.close()
	}
	if ownedRoot != "" {
		_ = os.RemoveAll(ownedRoot)
	}
}

func createPaths(parent string) (Paths, error) {
	root, err := os.MkdirTemp(parent, "sessionless-codexapp-")
	if err != nil {
		return Paths{}, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return Paths{}, err
	}
	paths := Paths{
		Root: root, Home: filepath.Join(root, "home"), CodexHome: filepath.Join(root, "codex-home"),
		WorkDir: filepath.Join(root, "workspace"), TempDir: filepath.Join(root, "tmp"),
	}
	for _, path := range []string{paths.Home, paths.CodexHome, paths.WorkDir, paths.TempDir} {
		if err := os.Mkdir(path, 0o700); err != nil {
			_ = os.RemoveAll(root)
			return Paths{}, err
		}
	}
	return paths, nil
}

func childEnvironment(paths Paths) []string {
	return []string{
		"HOME=" + paths.Home,
		"CODEX_HOME=" + paths.CodexHome,
		"TMPDIR=" + paths.TempDir,
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"LANG=C.UTF-8",
		"LC_ALL=C.UTF-8",
		"NO_COLOR=1",
	}
}

func (client *Client) Paths() Paths { return client.paths }

func (client *Client) readLoop(stdout io.Reader) {
	defer client.ioDone.Done()
	reader := bufio.NewReaderSize(stdout, 64<<10)
	for {
		frame, err := readBoundedFrame(reader, client.config.MaxFrameBytes)
		if err != nil {
			if errors.Is(err, ErrFrameTooLarge) {
				client.fail(ErrFrameTooLarge)
			} else {
				client.mu.Lock()
				closing := client.closing
				client.mu.Unlock()
				if !closing {
					client.fail(ErrProcessExited)
				}
			}
			return
		}
		if err := client.handleFrame(frame); err != nil {
			client.fail(err)
			return
		}
	}
}

func readBoundedFrame(reader *bufio.Reader, limit int) ([]byte, error) {
	var frame []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(frame)+len(fragment) > limit {
			return nil, ErrFrameTooLarge
		}
		frame = append(frame, fragment...)
		if err == nil {
			frame = bytes.TrimSuffix(frame, []byte{'\n'})
			frame = bytes.TrimSuffix(frame, []byte{'\r'})
			if len(frame) == 0 {
				return nil, ErrProtocol
			}
			return frame, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}

func (client *Client) handleFrame(frame []byte) error {
	var envelope wireEnvelope
	if err := json.Unmarshal(frame, &envelope); err != nil {
		return ErrProtocol
	}
	if envelope.Method != "" {
		if len(envelope.ID) != 0 {
			return ErrUnexpectedServerRequest
		}
		return client.handleNotification(envelope.Method, envelope.Params)
	}
	if len(envelope.ID) == 0 || (envelope.Error == nil) == (len(envelope.Result) == 0) {
		return ErrProtocol
	}
	var id string
	if err := json.Unmarshal(envelope.ID, &id); err != nil || id == "" {
		return ErrProtocol
	}
	client.mu.Lock()
	waiter, found := client.pending[id]
	if found {
		delete(client.pending, id)
	}
	client.mu.Unlock()
	if !found {
		return ErrProtocol
	}
	response := rpcResponse{result: envelope.Result}
	if envelope.Error != nil {
		response.err = &RemoteError{Code: envelope.Error.Code}
	}
	waiter <- response
	return nil
}

func (client *Client) fail(err error) {
	client.failOnce.Do(func() {
		client.mu.Lock()
		client.fatal = err
		pending := client.pending
		client.pending = make(map[string]chan rpcResponse)
		client.mu.Unlock()
		close(client.done)
		for _, waiter := range pending {
			waiter <- rpcResponse{err: err}
		}
		killProcessGroup(client.cmd)
	})
}

func (client *Client) waitProcess() {
	_ = client.cmd.Wait()
	if client.prepared != nil {
		if client.prepared.recheck(false) != nil {
			client.fail(ErrProcessUnavailable)
		}
		client.prepared.close()
	}
	client.mu.Lock()
	closing := client.closing
	client.mu.Unlock()
	if !closing {
		client.fail(ErrProcessExited)
	}
	client.ioDone.Wait()
	client.cleanup()
	close(client.processDone)
}

func (client *Client) cleanup() {
	client.cleanupOnce.Do(func() {
		if client.ownedRoot != "" {
			_ = os.RemoveAll(client.ownedRoot)
		}
	})
}

func (client *Client) Close() error {
	client.closeOnce.Do(func() {
		client.mu.Lock()
		client.closing = true
		client.mu.Unlock()
		_ = client.stdin.Close()
		killProcessGroup(client.cmd)
		<-client.processDone
		client.fail(ErrClosed)
		close(client.closeDone)
	})
	<-client.closeDone
	return nil
}

type boundedBuffer struct {
	mu        sync.Mutex
	limit     int
	data      []byte
	truncated bool
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	written := len(data)
	remaining := buffer.limit - len(buffer.data)
	if remaining > 0 {
		if remaining > len(data) {
			remaining = len(data)
		}
		buffer.data = append(buffer.data, data[:remaining]...)
	}
	if remaining < len(data) {
		buffer.truncated = true
	}
	return written, nil
}

type RemoteError struct{ Code int64 }

func (err *RemoteError) Error() string { return "codex app-server request rejected" }
