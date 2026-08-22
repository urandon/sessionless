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
	"sync"
	"time"
)

const (
	defaultRequestTimeout  = 15 * time.Second
	defaultTurnTimeout     = 10 * time.Minute
	defaultShutdownTimeout = 3 * time.Second
	defaultMaxFrameBytes   = 4 << 20
	defaultMaxStderrBytes  = 64 << 10
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

	done        chan struct{}
	processDone chan struct{}
	ioDone      sync.WaitGroup
	failOnce    sync.Once
	cleanupOnce sync.Once
	stderr      *boundedBuffer
}

// Start launches one invocation-scoped app-server. ctx owns the process
// lifetime: cancellation kills and reaps the entire process group.
func Start(ctx context.Context, config Config) (*Client, error) {
	config = withDefaults(config)
	paths, err := createPaths(config.ScratchRoot)
	if err != nil {
		return nil, ErrProcessUnavailable
	}
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
		_ = os.RemoveAll(paths.Root)
		return nil, ErrProcessUnavailable
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		_ = os.RemoveAll(paths.Root)
		return nil, ErrProcessUnavailable
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = os.RemoveAll(paths.Root)
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
		done:       make(chan struct{}), processDone: make(chan struct{}),
		stderr: &boundedBuffer{limit: config.MaxStderrBytes},
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = os.RemoveAll(paths.Root)
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
	return config
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
	close(client.processDone)
	client.mu.Lock()
	closing := client.closing
	client.mu.Unlock()
	if !closing {
		client.fail(ErrProcessExited)
	}
	client.cleanup()
}

func (client *Client) cleanup() {
	client.cleanupOnce.Do(func() { _ = os.RemoveAll(client.paths.Root) })
}

func (client *Client) Close() error {
	client.mu.Lock()
	if client.closing {
		client.mu.Unlock()
		<-client.processDone
		return nil
	}
	client.closing = true
	client.mu.Unlock()
	_ = client.stdin.Close()
	timer := time.NewTimer(client.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-client.processDone:
	case <-timer.C:
		client.fail(ErrClosed)
		<-client.processDone
	}
	client.fail(ErrClosed)
	client.ioDone.Wait()
	client.cleanup()
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
