//go:build darwin || linux

package nativeharnessinput

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// VerifyMounted checks only the actual workload-visible bytes and held file
// identities. It cannot prove original host custody, inspected mappings or
// admission. The root must represent the workload filesystem, not /sessionless.
// Synchronous regular-file operations cannot be interrupted mid-system-call;
// cancellation is checked between every bounded read and traversal operation.
func VerifyMounted(ctx context.Context, root *os.Root, d NativeExecutionDescriptorV3) error {
	if ctx == nil || ctx.Err() != nil || root == nil {
		return ErrMounted
	}
	slots, err := Slots(d)
	if err != nil {
		return err
	}
	base, err := root.Open(".")
	if err != nil {
		return errors.Join(ErrMounted, err)
	}
	defer base.Close()
	// Hold every directory through verification: Openat + O_NOFOLLOW denies
	// even transient intermediate symlinks, rather than checking names alone.
	session, err := openDirectory(ctx, base, "sessionless")
	if err != nil {
		return errors.Join(ErrMounted, err)
	}
	defer session.Close()
	profile, err := openDirectory(ctx, session, "profile")
	if err != nil {
		return errors.Join(ErrMounted, err)
	}
	defer profile.Close()
	if err = exactEntries(ctx, profile, []string{"models.json", "output.schema.json"}); err != nil {
		return errors.Join(ErrMounted, err)
	}
	for _, slot := range slots[:2] {
		if err = verifyFile(ctx, profile, slot); err != nil {
			return errors.Join(ErrMounted, err)
		}
	}
	input, err := openDirectory(ctx, session, "input")
	if err != nil {
		if len(slots) == 2 && errors.Is(err, os.ErrNotExist) {
			if err := exactEntries(ctx, session, []string{"profile"}); err != nil {
				return errors.Join(ErrMounted, err)
			}
			return verifyDirectoryLinks(root, base, session, profile, nil)
		}
		return errors.Join(ErrMounted, err)
	}
	defer input.Close()
	if err = exactEntries(ctx, session, []string{"profile", "input"}); err != nil {
		return errors.Join(ErrMounted, err)
	}
	names := make([]string, 0, len(slots)-2)
	for _, slot := range slots[2:] {
		names = append(names, basename(slot.RelativePath))
	}
	if err = exactEntries(ctx, input, names); err != nil {
		return errors.Join(ErrMounted, err)
	}
	for _, slot := range slots[2:] {
		if err = verifyFile(ctx, input, slot); err != nil {
			return errors.Join(ErrMounted, err)
		}
	}
	if ctx.Err() != nil {
		return errors.Join(ErrMounted, ctx.Err())
	}
	return verifyDirectoryLinks(root, base, session, profile, input)
}

func basename(path string) string { return path[strings.LastIndexByte(path, '/')+1:] }

func openDirectory(ctx context.Context, parent *os.File, name string) (*os.File, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func exactEntries(ctx context.Context, dir *os.File, names []string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	entries, err := dir.ReadDir(len(names) + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) != len(names) {
		return ErrMounted
	}
	for _, entry := range entries {
		found := false
		for _, name := range names {
			if entry.Name() == name {
				found = true
				break
			}
		}
		if !found {
			return ErrMounted
		}
	}
	return nil
}

func fileIdentity(info *unix.Stat_t, size uint64) bool {
	return info.Mode&unix.S_IFMT == unix.S_IFREG && info.Mode&0o222 == 0 && info.Uid == uint32(os.Geteuid()) && info.Nlink == 1 && info.Size >= 0 && uint64(info.Size) == size
}

func verifyFile(ctx context.Context, parent *os.File, slot SlotV3) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	name := basename(slot.RelativePath)
	var before unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if !fileIdentity(&before, slot.Size) {
		return ErrMounted
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var held unix.Stat_t
	if unix.Fstat(fd, &held) != nil || !sameFile(&before, &held) || !fileIdentity(&held, slot.Size) {
		return ErrMounted
	}
	h := sha256.New()
	var used uint64
	var buffer [8192]byte
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, readErr := file.Read(buffer[:min(uint64(len(buffer)), slot.Size+1-used)])
		used += uint64(n)
		if used > slot.Size {
			return ErrMounted
		}
		_, _ = h.Write(buffer[:n])
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	var after, path unix.Stat_t
	if unix.Fstat(fd, &after) != nil || unix.Fstatat(int(parent.Fd()), name, &path, unix.AT_SYMLINK_NOFOLLOW) != nil ||
		!sameFile(&before, &after) || !sameFile(&before, &path) || !fileIdentity(&after, slot.Size) || !fileIdentity(&path, slot.Size) ||
		used != slot.Size || hex.EncodeToString(h.Sum(nil)) != slot.SHA256 {
		return ErrMounted
	}
	return nil
}

func sameFile(a, b *unix.Stat_t) bool { return a.Dev == b.Dev && a.Ino == b.Ino }

func verifyDirectoryLinks(root *os.Root, base, session, profile, input *os.File) error {
	for _, entry := range []struct {
		path string
		held *os.File
	}{
		{".", base}, {"sessionless", session}, {"sessionless/profile", profile}, {"sessionless/input", input},
	} {
		if entry.held == nil {
			continue
		}
		actual, err := root.Lstat(entry.path)
		held, heldErr := entry.held.Stat()
		if err != nil || heldErr != nil || !actual.IsDir() || actual.Mode()&os.ModeSymlink != 0 || !os.SameFile(actual, held) {
			return ErrMounted
		}
	}
	return nil
}
