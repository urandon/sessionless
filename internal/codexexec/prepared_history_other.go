//go:build !darwin && !linux

package codexexec

type preparedHistory struct {
	self     *preparedHistory
	path     string
	nativeV2 *nativeInputStageV2
}

func stagePreparedHistory(string, []byte) (*preparedHistory, error) { return nil, ErrContract }
func (*preparedHistory) remove() error                              { return ErrContract }
