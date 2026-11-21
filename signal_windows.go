//go:build windows
// +build windows

package tableflip

import (
	"os"
	"os/exec"
)

// exitSignal always reports false on Windows, which has no
// Unix-style wait status.
func exitSignal(err *exec.ExitError) (os.Signal, bool) {
	return nil, false
}
