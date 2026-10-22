//go:build !darwin && !linux

package attachedworkerdaemon

import "os"

func nativeInputIdentityV3(os.FileInfo) (uint64, uint64, bool) { return 0, 0, false }
