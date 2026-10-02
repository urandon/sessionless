// Package attachedworkerservice provides permission-bound, content-free local
// control of the single attached-worker foreground owner. It is not a second
// daemon, transport, attempt, or provider authority.
package attachedworkerservice

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerforeground"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

const (
	VersionV1      = uint32(1)
	SocketFileName = "control.sock"
	maxFrameBytes  = 4096
	operationBound = 10 * time.Second
)

var (
	ErrInvalid   = errors.New("attached worker local control configuration is invalid")
	ErrBusy      = errors.New("attached worker local control owner is already present")
	ErrIO        = errors.New("attached worker local control failed")
	ErrAmbiguous = errors.New("attached worker local control mutation outcome is ambiguous")
)

type Action string

const (
	ActionStatus   Action = "status"
	ActionDoctor   Action = "doctor"
	ActionDrain    Action = "drain"
	ActionShutdown Action = "shutdown"
)

type RequestV1 struct {
	Version                  uint32 `json:"version"`
	Action                   Action `json:"action"`
	ExpectedManifestRevision uint64 `json:"expected_manifest_revision,omitempty"`
}

// ResponseV1 never carries a path, token, prompt, provider payload, raw
// error, or host environment. Its live bit means only that the local owner
// answered this request; it does not imply server or provider health.
type ResponseV1 struct {
	Version    uint32                              `json:"version"`
	Code       string                              `json:"code"`
	Live       bool                                `json:"live"`
	Foreground *attachedworkerforeground.ResultV1  `json:"foreground,omitempty"`
	Doctor     *attachedworkerlocal.DoctorResultV1 `json:"doctor,omitempty"`
}

type Owner interface {
	Result() attachedworkerforeground.ResultV1
	Drain(context.Context) error
	Shutdown(context.Context) error
}

type Doctor func(context.Context) (attachedworkerlocal.DoctorResultV1, error)

// Directory is a sibling of the strict installation state root. The state
// store rejects unrecognized files, so the socket must never be put inside it.
func Directory(stateRoot string) (string, error) {
	if stateRoot == "" || !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot ||
		filepath.Dir(stateRoot) == stateRoot {
		return "", ErrInvalid
	}
	return stateRoot + ".control", nil
}

// Serve holds the one foreground owner until an explicit shutdown or signal.
// A response to shutdown is sent only after its bounded cleanup has finished.
func Serve(ctx context.Context, directory string, owner Owner, doctor Doctor) (resultErr error) {
	if ctx == nil || owner == nil || doctor == nil ||
		(runtime.GOOS != "darwin" && runtime.GOOS != "linux") {
		return ErrInvalid
	}
	path, err := socketPath(directory, true)
	if err != nil {
		return err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return errors.Join(ErrIO, err)
	}
	unixListener := listener.(*net.UnixListener)
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		return errors.Join(ErrIO, err)
	}
	defer func() {
		_ = listener.Close()
		// Only the process holding the runtime lease may reach here. The private
		// sibling directory prevents another UID from replacing this socket.
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, ErrIO)
		}
	}()
	for {
		if err := unixListener.SetDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			return errors.Join(ErrIO, err)
		}
		connection, acceptErr := unixListener.AcceptUnix()
		if acceptErr != nil {
			if ctx.Err() != nil {
				return stopOwner(owner)
			}
			if timeout, ok := acceptErr.(net.Error); ok && timeout.Timeout() {
				continue
			}
			return errors.Join(ErrIO, acceptErr, stopOwner(owner))
		}
		stop, terminalErr := handle(ctx, connection, owner, doctor)
		_ = connection.Close()
		if stop {
			return terminalErr
		}
	}
}

func handle(ctx context.Context, connection *net.UnixConn, owner Owner, doctor Doctor) (bool, error) {
	_ = connection.SetDeadline(time.Now().Add(operationBound))
	var request RequestV1
	if decode(connection, &request) != nil || request.Version != VersionV1 {
		_ = json.NewEncoder(connection).Encode(ResponseV1{Version: VersionV1, Code: "invalid_request", Live: true})
		return false, nil
	}
	response := ResponseV1{Version: VersionV1, Code: "ok", Live: true}
	stop := false
	var terminalErr error
	switch request.Action {
	case ActionStatus:
		result := owner.Result()
		response.Foreground = &result
	case ActionDoctor:
		opCtx, cancel := context.WithTimeout(ctx, operationBound)
		result, err := doctor(opCtx)
		cancel()
		response.Doctor = &result
		if err != nil {
			response.Code = "doctor_unknown"
		}
	case ActionDrain:
		if request.ExpectedManifestRevision == 0 || request.ExpectedManifestRevision != owner.Result().ManifestRevision {
			response.Code = "revision_conflict"
			break
		}
		opCtx, cancel := context.WithTimeout(ctx, operationBound)
		if err := owner.Drain(opCtx); err != nil {
			response.Code = "drain_unknown"
		}
		cancel()
		result := owner.Result()
		response.Foreground = &result
	case ActionShutdown:
		if request.ExpectedManifestRevision == 0 || request.ExpectedManifestRevision != owner.Result().ManifestRevision {
			response.Code = "revision_conflict"
			break
		}
		if err := stopOwner(owner); err != nil {
			response.Code = "shutdown_unknown"
			terminalErr = err
		}
		// Shutdown may have released the runtime lease even when Drain or
		// final cleanup failed. Never keep an endpoint alive after that.
		stop = true
		result := owner.Result()
		response.Foreground = &result
	default:
		response.Code = "invalid_request"
	}
	_ = json.NewEncoder(connection).Encode(response)
	return stop, terminalErr
}

func stopOwner(owner Owner) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationBound)
	defer cancel()
	// Drain first closes admission; Shutdown then cancels and reaps the
	// active attempt. Neither operation invents a successful remote effect.
	drainErr := owner.Drain(ctx)
	shutdownErr := owner.Shutdown(ctx)
	return errors.Join(drainErr, shutdownErr)
}

// Call performs exactly one local request. A lost response is ambiguous and
// callers must not retry a mutation automatically.
func Call(ctx context.Context, directory string, action Action, expectedManifestRevision uint64) (ResponseV1, error) {
	if ctx == nil || ctx.Err() != nil ||
		(action != ActionStatus && action != ActionDoctor && action != ActionDrain && action != ActionShutdown) ||
		((action == ActionDrain || action == ActionShutdown) != (expectedManifestRevision != 0)) {
		return ResponseV1{}, ErrInvalid
	}
	path, err := socketPath(directory, false)
	if err != nil {
		return ResponseV1{}, err
	}
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return ResponseV1{}, errors.Join(ErrIO, err)
	}
	defer connection.Close()
	deadline := time.Now().Add(operationBound)
	if until, ok := ctx.Deadline(); ok && until.Before(deadline) {
		deadline = until
	}
	_ = connection.SetDeadline(deadline)
	mutation := action == ActionDrain || action == ActionShutdown
	if err := json.NewEncoder(connection).Encode(RequestV1{Version: VersionV1, Action: action, ExpectedManifestRevision: expectedManifestRevision}); err != nil {
		if mutation {
			return ResponseV1{}, ErrAmbiguous
		}
		return ResponseV1{}, errors.Join(ErrIO, err)
	}
	var response ResponseV1
	if err := decode(connection, &response); err != nil || response.Version != VersionV1 || !response.Live {
		if mutation {
			return ResponseV1{}, ErrAmbiguous
		}
		return ResponseV1{}, ErrIO
	}
	return response, nil
}

func socketPath(directory string, create bool) (string, error) {
	if directory == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory ||
		filepath.Dir(directory) == directory {
		return "", ErrInvalid
	}
	path := filepath.Join(directory, SocketFileName)
	if len(path) >= 100 { // Darwin's sockaddr_un path is shorter than Linux's.
		return "", ErrInvalid
	}
	if create {
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", errors.Join(ErrIO, err)
		}
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return "", ErrInvalid
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil || canonical != directory {
		return "", ErrInvalid
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return "", ErrIO
	}
	if create {
		if len(entries) > 1 || (len(entries) == 1 && entries[0].Name() != SocketFileName) {
			return "", ErrInvalid
		}
		if len(entries) == 1 {
			// The caller already holds the installation runtime lease. A live
			// listener cannot also own that lease; only a stale socket is removed.
			if conn, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond); dialErr == nil {
				_ = conn.Close()
				return "", ErrBusy
			}
			if socket, statErr := os.Lstat(path); statErr != nil || socket.Mode()&os.ModeSocket == 0 ||
				socket.Mode().Perm() != 0o600 || !ownedByCurrentUser(socket) {
				return "", ErrInvalid
			}
			if err := os.Remove(path); err != nil {
				return "", ErrIO
			}
		}
	} else if len(entries) != 1 || entries[0].Name() != SocketFileName {
		return "", ErrInvalid
	}
	if !create {
		socket, statErr := os.Lstat(path)
		if statErr != nil || socket.Mode()&os.ModeSocket == 0 ||
			socket.Mode().Perm() != 0o600 || !ownedByCurrentUser(socket) {
			return "", ErrInvalid
		}
	}
	return path, nil
}

func decode(reader io.Reader, target any) error {
	frame, err := bufio.NewReader(io.LimitReader(reader, maxFrameBytes+1)).ReadBytes('\n')
	if err != nil || len(frame) > maxFrameBytes {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(frame))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	return nil
}

// Keep errors and response codes stable and content-free. In particular,
// never report the absolute socket or state path through the wire protocol.
func (response ResponseV1) Valid() bool {
	return response.Version == VersionV1 && response.Live && response.Code != "" &&
		!strings.ContainsAny(response.Code, "/\\\n\r")
}
