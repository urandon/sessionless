//go:build darwin || linux

package nativeharnessbootstrap

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessrelay"
)

// Run implements only the two contained helper modes. Environment is the
// launcher's explicit replacement environment, not host credential discovery.
// The host supervisor remains the workload/credential/finalization owner.
func Run(ctx context.Context, args, env []string, stdin, stdout, stderr *os.File) error {
	if ctx == nil || ctx.Err() != nil || !argsValid(args) {
		return ErrConfig
	}
	switch args[0] {
	case "bootstrap":
		return runBootstrap(ctx, args, env, stdin, stdout, stderr)
	case "relay":
		return runRelay(ctx, args, env)
	default:
		return ErrConfig
	}
}

type child struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	// Written only by the containing cleanup, never by the Wait goroutine.
	killRequested bool
}

func startChild(ctx context.Context, p program, env []string, stdin, stdout, stderr *os.File, extra []*os.File) (*child, error) {
	actual, err := attachedworkerdaemon.DigestExecutable(p.path)
	if err != nil || actual != p.digest || !live(ctx) {
		return nil, ErrConfig
	}
	command := exec.CommandContext(ctx, p.path, p.args...)
	command.Env = append([]string(nil), env...)
	command.Stdin, command.Stdout, command.Stderr = stdin, stdout, stderr
	command.ExtraFiles = extra
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// CommandContext checks cancellation at Start; only the containing bootstrap
	// cleanup sends signals, with one independent budget for both original groups.
	command.Cancel = func() error { return nil }
	if err := command.Start(); err != nil {
		return nil, errors.Join(ErrLifecycle, err)
	}
	c := &child{cmd: command, done: make(chan struct{})}
	go func() { c.err = command.Wait(); close(c.done) }()
	if !live(ctx) {
		return c, errors.Join(ErrLifecycle, ctx.Err())
	}
	return c, nil
}

func live(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	deadline, ok := ctx.Deadline()
	return ok && time.Now().Before(deadline)
}

func signalChild(c *child, sig syscall.Signal) error {
	if c == nil || c.cmd.Process == nil || c.cmd.Process.Pid <= 0 {
		return nil
	}
	leaderPending := !joined(c)
	err := syscall.Kill(-c.cmd.Process.Pid, sig)
	// ESRCH is an idempotent cleanup result, not evidence that this cleanup
	// sent a signal. Only successful delivery may exempt an exact KILL exit.
	if ownsPendingLeaderKill(leaderPending, sig, err) {
		c.killRequested = true
	}
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func ownsPendingLeaderKill(leaderPending bool, sig syscall.Signal, rawErr error) bool {
	return leaderPending && sig == syscall.SIGKILL && rawErr == nil
}

func joined(c *child) bool {
	if c == nil {
		return true
	}
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
func groupGone(c *child) (bool, error) {
	if c == nil {
		return true, nil
	}
	err := syscall.Kill(-c.cmd.Process.Pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	if err == nil || errors.Is(err, syscall.EPERM) {
		return false, nil
	}
	return false, err
}

// The relay reports its own cleanup errors through its exit status. Joining
// alone must not erase them. Only this cleanup's exact SIGKILL escalation is
// expected; an ordinary nonzero exit (including after TERM) remains tainted.
func relayExitError(c *child) error {
	if c == nil {
		return nil
	}
	if !joined(c) {
		return ErrNotJoined
	}
	if c.err == nil {
		return nil
	}
	var exit *exec.ExitError
	if errors.As(c.err, &exit) {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && expectedRelayKill(c.killRequested, status) {
			return nil
		}
	}
	return errors.Join(ErrLifecycle, c.err)
}

func expectedRelayKill(requested bool, status syscall.WaitStatus) bool {
	return requested && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// stopChildren owns only groups actually started above. It does not equate
// leader exit with container/descendant quiescence or perform host-wide cleanup.
func stopChildren(deadline time.Time, children ...*child) error {
	started := time.Now()
	termEnd := started.Add(max(0, deadline.Sub(started)) / 2)
	var result error
	for _, c := range children {
		result = errors.Join(result, signalChild(c, syscall.SIGTERM))
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	killed := false
	for {
		all := true
		for _, c := range children {
			gone, err := groupGone(c)
			if err != nil {
				result = errors.Join(result, err)
			}
			all = all && joined(c) && gone
		}
		if all {
			return result
		}
		now := time.Now()
		if !killed && !now.Before(termEnd) {
			killed = true
			for _, c := range children {
				err := signalChild(c, syscall.SIGKILL)
				result = errors.Join(result, err)
			}
		}
		if !now.Before(deadline) {
			// Recheck completion at the failure bound; it never replenishes it.
			all = true
			for _, c := range children {
				gone, err := groupGone(c)
				result = errors.Join(result, err)
				all = all && joined(c) && gone
			}
			if all {
				return result
			}
			return errors.Join(ErrNotJoined, result, context.DeadlineExceeded)
		}
		select {
		case <-ticker.C:
		}
	}
}

func runBootstrap(ctx context.Context, args, env []string, stdin, stdout, stderr *os.File) (result error) {
	if stdin == nil || stdout == nil || stderr == nil {
		return ErrConfig
	}
	p, err := parseBootstrap(args)
	if err != nil {
		return err
	}
	base, value, err := environment(env)
	if err != nil {
		return err
	}
	active, cancel, err := activeContext(ctx, value, p.network.limits.Active)
	if err != nil {
		return err
	}
	defer cancel()
	deadline, _ := active.Deadline()
	reader, writer, err := os.Pipe()
	if err != nil {
		return errors.Join(ErrLifecycle, err)
	}
	defer reader.Close()
	defer writer.Close()
	readyDeadline := minTime(deadline, time.Now().Add(2*time.Second))
	if err := reader.SetReadDeadline(readyDeadline); err != nil {
		return errors.Join(ErrLifecycle, err)
	}
	relayEnv := []string{deadlineName + "=" + strconv.FormatInt(deadline.UnixNano(), 10), "PATH=", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "NO_COLOR=1"}
	relay, err := startChild(active, p.relay, relayEnv, nil, nil, stderr, []*os.File{writer})
	var native *child
	var cleanupEnd time.Time
	cleanupDeadline := func() time.Time {
		if cleanupEnd.IsZero() {
			cleanupEnd = time.Now().Add(p.network.cleanup)
		}
		return cleanupEnd
	}
	if relay != nil {
		defer func() {
			stopErr := stopChildren(cleanupDeadline(), native, relay)
			result = errors.Join(result, stopErr, relayExitError(relay))
		}()
	}
	if err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return errors.Join(ErrLifecycle, err)
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(active, func() { defer close(callbackDone); _ = reader.Close() })
	// The sole reader is this goroutine, not a detached readiness reader.
	raw, readErr := io.ReadAll(io.LimitReader(reader, 33))
	if !stop() {
		joined, finish := context.WithDeadline(context.Background(), cleanupDeadline())
		select {
		case <-callbackDone:
		case <-joined.Done():
			select {
			case <-callbackDone:
			default:
				finish()
				return ErrNotJoined
			}
		}
		finish()
	}
	if readErr != nil || len(raw) == 0 || len(raw) > 32 || raw[len(raw)-1] != '\n' || !live(active) {
		return errors.Join(ErrLifecycle, readErr, active.Err())
	}
	address, err := netip.ParseAddrPort(string(raw[:len(raw)-1]))
	if err != nil || address.Addr() != netip.MustParseAddr("127.0.0.1") || address.Port() == 0 || address.String()+"\n" != string(raw) {
		return ErrConfig
	}
	if joined(relay) {
		return errors.Join(ErrLifecycle, relay.err)
	}
	proxy := "http://" + address.String()
	nativeEnv := append(base, "HTTP_PROXY="+proxy, "HTTPS_PROXY="+proxy, "http_proxy="+proxy, "https_proxy="+proxy)
	if !live(active) {
		return errors.Join(ErrLifecycle, active.Err())
	}
	native, err = startChild(active, p.native, nativeEnv, stdin, stdout, stderr, nil)
	if err != nil {
		return err
	}
	select {
	case <-native.done:
	case <-relay.done:
	case <-active.Done():
	}
	if !live(active) {
		return errors.Join(ErrLifecycle, active.Err())
	}
	if joined(relay) {
		return errors.Join(ErrLifecycle, relay.err)
	}
	if !joined(native) {
		return ErrLifecycle
	}
	return native.err
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func runRelay(ctx context.Context, args, env []string) (result error) {
	p, err := parseRelay(args)
	if err != nil {
		return err
	}
	if len(env) > 64 {
		return ErrConfig
	}
	value := ""
	found := false
	for _, v := range env {
		if len(v) > 4096 || strings.ContainsRune(v, 0) {
			return ErrConfig
		}
		name, x, ok := strings.Cut(v, "=")
		if !ok {
			return ErrConfig
		}
		if name == deadlineName {
			if found {
				return ErrConfig
			}
			found = true
			value = x
		}
	}
	active, cancel, err := activeContext(ctx, value, p.limits.Active)
	if err != nil {
		return err
	}
	defer cancel()
	ready := os.NewFile(3, "native-relay-readiness")
	if ready == nil {
		return ErrConfig
	}
	defer ready.Close()
	info, err := ready.Stat()
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		return ErrConfig
	}
	deadline, _ := active.Deadline()
	if ready.SetWriteDeadline(minTime(deadline, time.Now().Add(2*time.Second))) != nil {
		return ErrConfig
	}
	r, err := nativeharnessrelay.Start(active, p.socket, p.limits)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), p.cleanup)
		defer stop()
		closeErr := r.Close(cleanup)
		// Only a joined, otherwise clean explicit shutdown is success. In
		// particular, an unknown I/O/cleanup failure must survive cancellation.
		if result == context.Canceled && closeErr == context.Canceled && ctx.Err() == context.Canceled {
			result = nil
			closeErr = nil
		}
		result = errors.Join(result, closeErr)
	}()
	ack := r.Address().String() + "\n"
	if !live(active) {
		return errors.Join(ErrLifecycle, active.Err())
	}
	if n, err := io.WriteString(ready, ack); err != nil || n != len(ack) {
		return errors.Join(ErrLifecycle, err)
	}
	if err := ready.Close(); err != nil {
		return errors.Join(ErrLifecycle, err)
	}
	waitErr := r.Wait(active)
	// Wait's active-bound failure says nothing about the later independent
	// cleanup join. Preserve its cause, then let Close establish actual join.
	if errors.Is(waitErr, nativeharnessrelay.ErrNotJoined) && active.Err() != nil {
		return active.Err()
	}
	return waitErr
}
