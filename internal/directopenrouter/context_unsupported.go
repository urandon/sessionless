//go:build !darwin && !linux

package directopenrouter

import "os"

func openContextHistory(string, int64) (*os.File, error) {
	return nil, ErrContract
}
