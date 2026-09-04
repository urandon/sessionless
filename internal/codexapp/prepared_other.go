//go:build !darwin && !linux

package codexapp

type preparedGuard struct{}

func openPreparedPaths(PreparedPaths, int64) (*preparedGuard, error) {
	return nil, ErrProcessUnavailable
}

func (*preparedGuard) recheck(bool) error { return ErrProcessUnavailable }
func (*preparedGuard) close()             {}
