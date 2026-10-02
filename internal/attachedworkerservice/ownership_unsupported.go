//go:build !darwin && !linux

package attachedworkerservice

import "os"

func ownedByCurrentUser(os.FileInfo) bool { return false }
