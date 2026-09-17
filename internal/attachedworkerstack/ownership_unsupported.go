//go:build !darwin && !linux

package attachedworkerstack

import "io/fs"

func ownedByCurrentUser(fs.FileInfo) bool { return false }
