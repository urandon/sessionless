//go:build !darwin && !linux

package localoperator

import "context"

func readOperatorFiles(context.Context, string) ([]byte, [32]byte, error) {
	return nil, [32]byte{}, ErrDenied
}
func inspectExecutable(context.Context, string) (string, error) { return "", ErrDenied }
