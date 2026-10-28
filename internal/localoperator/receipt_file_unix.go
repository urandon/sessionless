//go:build darwin || linux

package localoperator

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"golang.org/x/sys/unix"
)

// ProfileReceiptFile owns a single exclusive output slot. It is used only by
// the trusted first-install command, serially, and is not an authority issuer.
// Close NEVER removes the slot: even an empty/partial file prevents a new
// installation at this path after a failed or ambiguous publication.
type ProfileReceiptFile struct {
	file, directory *os.File
	path            string
	identity        unix.Stat_t
	ops             receiptFileOps
	finished        bool
}

type receiptFileOps struct {
	sync  func(*os.File) error
	write func(*os.File, []byte) (int, error)
}

// ReserveProfileReceipt durably reserves an unused path BEFORE any fresh
// Source or DB effect. The caller must already have connected to its database.
// This fences one pathname, not all installations made from a manifest.
func ReserveProfileReceipt(ctx context.Context, path string) (*ProfileReceiptFile, error) {
	return reserveProfileReceipt(ctx, path, receiptFileOps{
		sync: (*os.File).Sync, write: (*os.File).Write,
	})
}

// ReadProfileReceipt supplies historical DATA for installation roundtrip and
// future normal startup. It never creates or repairs any operator file.
func ReadProfileReceipt(ctx context.Context, path string) (ports.OwnedProfilePublicationReceiptV1, error) {
	deny := func() (ports.OwnedProfilePublicationReceiptV1, error) {
		return ports.OwnedProfilePublicationReceiptV1{}, ErrDenied
	}
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return deny()
	}
	fd, err := openCanonicalDir(filepath.Dir(path))
	if err != nil {
		return deny()
	}
	defer unix.Close(fd)
	var root, current unix.Stat_t
	if unix.Fstat(fd, &root) != nil || !privateOperatorRoot(root) {
		return deny()
	}
	raw, err := readPrivateOperatorFile(ctx, fd, filepath.Base(path), domain.OwnedAgentCurrentMaxRecordBytesV1, 0)
	defer clear(raw)
	if err != nil {
		return deny()
	}
	check, err := openCanonicalDir(filepath.Dir(path))
	if err != nil {
		return deny()
	}
	statErr := unix.Fstat(check, &current)
	closeErr := unix.Close(check)
	if ctx.Err() != nil || statErr != nil || closeErr != nil || !privateOperatorRoot(current) || root.Dev != current.Dev || root.Ino != current.Ino {
		return deny()
	}
	return DecodeProfileReceipt(raw)
}

func reserveProfileReceipt(ctx context.Context, path string, ops receiptFileOps) (*ProfileReceiptFile, error) {
	if ctx == nil || ctx.Err() != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || ops.sync == nil || ops.write == nil {
		return nil, ErrDenied
	}
	parent, err := openCanonicalDir(filepath.Dir(path))
	if err != nil {
		return nil, ErrDenied
	}
	dir := os.NewFile(uintptr(parent), filepath.Dir(path))
	var root unix.Stat_t
	if unix.Fstat(parent, &root) != nil || !privateOperatorRoot(root) {
		_ = dir.Close()
		return nil, ErrDenied
	}
	fd, err := unix.Openat(parent, filepath.Base(path), unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0o600)
	if err != nil {
		_ = dir.Close()
		return nil, ErrDenied
	}
	slot := &ProfileReceiptFile{file: os.NewFile(uintptr(fd), path), directory: dir, path: path, ops: ops}
	if unix.Fstat(fd, &slot.identity) != nil || slot.verify(ctx, 0) != nil || ops.sync(slot.file) != nil || ops.sync(slot.directory) != nil || slot.verify(ctx, 0) != nil {
		_ = slot.Close()
		return nil, ErrDenied
	}
	return slot, nil
}

func (f *ProfileReceiptFile) verify(ctx context.Context, size int64) error {
	if f == nil || f.file == nil || f.directory == nil || ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	var current, entry, root, namedRoot unix.Stat_t
	if unix.Fstat(int(f.file.Fd()), &current) != nil || current.Dev != f.identity.Dev || current.Ino != f.identity.Ino ||
		current.Mode&unix.S_IFMT != unix.S_IFREG || current.Mode&0o7777 != 0o600 || current.Uid != uint32(os.Geteuid()) || current.Nlink != 1 || current.Size != size ||
		unix.Fstatat(int(f.directory.Fd()), filepath.Base(f.path), &entry, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(current, entry) ||
		unix.Fstat(int(f.directory.Fd()), &root) != nil || !privateOperatorRoot(root) {
		return ErrDenied
	}
	check, err := openCanonicalDir(filepath.Dir(f.path))
	if err != nil {
		return ErrDenied
	}
	statErr := unix.Fstat(check, &namedRoot)
	closeErr := unix.Close(check)
	if statErr != nil || closeErr != nil || !privateOperatorRoot(namedRoot) || root.Dev != namedRoot.Dev || root.Ino != namedRoot.Ino {
		return ErrDenied
	}
	return nil
}

// WriteReceipt persists only the actual accepted Store result. It never
// serializes a Source, key or publication grant. Every failure leaves the
// original slot for explicit operator reconciliation, without a retry.
func (f *ProfileReceiptFile) WriteReceipt(ctx context.Context, receipt ports.OwnedProfilePublicationReceiptV1) error {
	if f == nil || f.finished || f.verify(ctx, 0) != nil || receipt.Profile.ValidateInitial() != nil {
		return ErrDenied
	}
	f.finished = true
	digest, err := receipt.Profile.Digest()
	if err != nil || digest != receipt.Digest {
		return ErrDenied
	}
	raw, err := json.Marshal(receipt)
	defer clear(raw)
	if err != nil || len(raw) == 0 || len(raw) > domain.OwnedAgentCurrentMaxRecordBytesV1 {
		return ErrDenied
	}
	n, writeErr := f.ops.write(f.file, raw)
	if writeErr != nil || n != len(raw) || n == 0 || f.verify(ctx, int64(len(raw))) != nil || f.ops.sync(f.file) != nil || f.ops.sync(f.directory) != nil || f.verify(ctx, int64(len(raw))) != nil {
		return ErrDenied
	}
	var before, after unix.Stat_t
	if unix.Fstat(int(f.file.Fd()), &before) != nil {
		return ErrDenied
	}
	closeErr := f.file.Close()
	f.file = nil
	if closeErr != nil || ctx.Err() != nil || unix.Fstatat(int(f.directory.Fd()), filepath.Base(f.path), &after, unix.AT_SYMLINK_NOFOLLOW) != nil || !stableOperatorStat(before, after) {
		return ErrDenied
	}
	return f.Close()
}

func (f *ProfileReceiptFile) Close() error {
	if f == nil {
		return nil
	}
	var result error
	for _, file := range []*os.File{f.file, f.directory} {
		if file != nil {
			if err := file.Close(); err != nil {
				result = io.ErrClosedPipe
			}
		}
	}
	f.file, f.directory = nil, nil
	if result != nil {
		return ErrDenied
	}
	return nil
}
