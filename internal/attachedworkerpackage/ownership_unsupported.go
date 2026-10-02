//go:build !darwin && !linux

package attachedworkerpackage

import "os"

func ownedByCurrentUser(os.FileInfo) bool { return false }
