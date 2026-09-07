// The attached-worker OCI probe is cross-compiled as a static Linux binary by
// the opt-in real-engine test. It reports only a stable pass/fail marker.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "allowed-write":
		err = allowedWrite()
	case "forbidden-read":
		err = forbiddenRead(argument(2))
	case "forbidden-write":
		err = forbiddenWrite(argument(2))
	case "network-denied":
		err = networkDenied()
	case "detached-child":
		err = detachedChild()
	case "child":
		child()
	case "disk-bound":
		err = diskBound()
	default:
		err = errors.New("unknown probe")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "probe_failed")
		os.Exit(1)
	}
	fmt.Println("probe_ok")
}

func argument(index int) string {
	if len(os.Args) <= index {
		return ""
	}
	return os.Args[index]
}

func allowedWrite() error {
	home := os.Getenv("HOME")
	if home == "" {
		return errors.New("missing home")
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(home, "allowed"), []byte("ok"), 0o600)
}

func forbiddenRead(path string) error {
	if path == "" {
		return errors.New("missing path")
	}
	if _, err := os.ReadFile(path); err == nil {
		return errors.New("forbidden read succeeded")
	}
	return nil
}

func forbiddenWrite(path string) error {
	if path == "" {
		return errors.New("missing path")
	}
	if err := os.WriteFile(path, []byte("escape"), 0o600); err == nil {
		return errors.New("forbidden write succeeded")
	}
	return nil
}

func networkDenied() error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	loopback := false
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagLoopback != 0 {
			loopback = true
			continue
		}
		if networkInterface.Flags&net.FlagUp != 0 {
			return errors.New("non-loopback interface is up")
		}
	}
	if !loopback {
		return errors.New("loopback interface missing")
	}
	routes, err := os.Open("/proc/net/route")
	if err != nil {
		return err
	}
	defer routes.Close()
	scanner := bufio.NewScanner(routes)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) > 1 && fields[0] != "Iface" && fields[0] != "lo" {
			return errors.New("non-loopback route exists")
		}
	}
	return scanner.Err()
}

func detachedChild() error {
	command := exec.Command(os.Args[0], "child")
	command.Stdout = nil
	command.Stderr = nil
	return command.Start()
}

func child() {
	for {
		time.Sleep(time.Hour)
	}
}

func diskBound() error {
	root := os.Getenv("HOME")
	if root == "" {
		return errors.New("missing home")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	block := make([]byte, 512<<10)
	for index := 0; index < 256; index++ {
		path := filepath.Join(root, "block-"+strconv.Itoa(index))
		err := os.WriteFile(path, block, 0o600)
		if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EFBIG) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return errors.New("disk bound was not reached")
}
