//go:build !windows
// +build !windows

package tableflip

import (
	"os"
	"os/exec"
	"syscall"
)

// exitSignal reports whether the process was terminated by a
// signal, and if so, which one.
func exitSignal(err *exec.ExitError) (os.Signal, bool) {
	if ws, ok := err.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return ws.Signal(), true
	}
	return nil, false
}
