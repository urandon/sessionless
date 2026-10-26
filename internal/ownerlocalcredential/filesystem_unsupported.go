//go:build !linux && !darwin

package ownerlocalcredential

import "context"

type filesystem struct {
	device uint64
	inode  uint64
}

func openFilesystem(string) (*filesystem, error) { return nil, ErrDenied }
func (*filesystem) check() error                 { return ErrDenied }
func (*filesystem) read(context.Context, string, int) ([]byte, bool, error) {
	return nil, false, ErrDenied
}
func (*filesystem) onlyLock() error                              { return ErrDenied }
func (*filesystem) writeAtomic(context.Context, []byte) error    { return ErrDenied }
func (*filesystem) syncDirectory() error                         { return ErrDenied }
func (*filesystem) create(context.Context, string, []byte) error { return ErrDenied }
func (*filesystem) entries() (map[string]int64, error)           { return nil, ErrDenied }
func (*filesystem) remove(string) error                          { return ErrDenied }
func (*filesystem) close() error                                 { return ErrDenied }
func (*filesystem) space(uint64) error                           { return ErrDenied }
