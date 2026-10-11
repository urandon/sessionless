//go:build !darwin && !linux

package codexexec

type preparedHistory struct{ path string }

func stagePreparedHistory(string, []byte) (*preparedHistory, error) { return nil, ErrContract }
func (*preparedHistory) remove() error                              { return ErrContract }
