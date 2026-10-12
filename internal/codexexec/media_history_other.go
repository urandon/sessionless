//go:build !darwin && !linux

package codexexec

func (*preparedHistory) stageImages(*mediaInput) error  { return ErrContract }
func (*preparedHistory) verifyImages(*mediaInput) error { return ErrContract }
func (*preparedHistory) imagePaths() []string           { return nil }
