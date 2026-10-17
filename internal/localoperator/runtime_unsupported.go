//go:build !darwin && !linux

package localoperator

import "context"

func inspectNetworkIdentity(context.Context, string) (string, error) { return "", ErrDenied }
func inspectBootIdentity(context.Context) (string, error)            { return "", ErrDenied }
