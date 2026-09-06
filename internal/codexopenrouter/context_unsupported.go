//go:build !darwin && !linux

package codexopenrouter

import "os"

// The provider adapter fails closed where descriptor-pinned context reads are
// unavailable. No production registration is enabled on any platform.
func openContextHistory(string, int64) (*os.File, error) {
	return nil, ErrContract
}
