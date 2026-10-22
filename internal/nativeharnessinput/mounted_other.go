//go:build !darwin && !linux

package nativeharnessinput

import (
	"context"
	"os"
)

// Unsupported platforms must not certify no-follow descriptor traversal.
func VerifyMounted(context.Context, *os.Root, NativeExecutionDescriptorV3) error { return ErrMounted }
