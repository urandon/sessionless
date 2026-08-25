//go:build darwin || linux

package attachedworkerdaemon

import (
	"os"
	"os/signal"
)

func signalIgnore(value os.Signal) {
	signal.Ignore(value)
}
