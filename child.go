package tableflip

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
)

// maxStderrTail bounds how much of the child's stderr output is
// retained for diagnostics. Only the tail is kept.
const maxStderrTail = 4 * 1024

type child struct {
	*env
	proc           process
	readyR, readyW *os.File
	namesW         *os.File
	stderrR        *os.File
	stderr         *tailBuffer
	stderrDone     <-chan struct{}
	ready          <-chan *os.File
	readyDone      <-chan struct{}
	result         <-chan error
	exited         <-chan struct{}
}

func startChild(env *env, passedFiles map[fileName]*file) (*child, error) {
	// These pipes are used for communication between parent and child
	// readyW is passed to the child, readyR stays with the parent
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pipe failed: %s", err)
	}

	namesR, namesW, err := os.Pipe()
	if err != nil {
		readyR.Close()
		readyW.Close()
		return nil, fmt.Errorf("pipe failed: %s", err)
	}

	// stderrR/stderrW capture the child's stderr for diagnostics.
	// Output is forwarded to the parent's stderr and the tail is
	// retained to diagnose a child that fails to become ready.
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		readyR.Close()
		readyW.Close()
		namesR.Close()
		namesW.Close()
		return nil, fmt.Errorf("pipe failed: %s", err)
	}

	// Copy passed fds and append the notification pipe
	fds := []*os.File{os.Stdin, os.Stdout, stderrW, readyW, namesR}
	var fdNames [][]string
	for name, file := range passedFiles {
		nameSlice := make([]string, len(name))
		copy(nameSlice, name[:])
		fdNames = append(fdNames, nameSlice)
		fds = append(fds, file.File)
	}

	// Copy environment and append the notification env vars
	sentinel := fmt.Sprintf("%s=yes", sentinelEnvVar)
	var environ []string
	for _, val := range env.environ() {
		if val != sentinel {
			environ = append(environ, val)
		}
	}
	environ = append(environ, sentinel)

	proc, err := env.newProc(os.Args[0], os.Args[1:], fds, environ)
	if err != nil {
		readyR.Close()
		readyW.Close()
		namesR.Close()
		namesW.Close()
		stderrR.Close()
		stderrW.Close()
		return nil, fmt.Errorf("can't start process %s: %s", os.Args[0], err)
	}

	// The child holds its own copy of the write end now. Closing
	// ours ensures the drainer sees EOF once the child exits.
	stderrW.Close()

	exited := make(chan struct{})
	result := make(chan error, 1)
	ready := make(chan *os.File, 1)
	readyDone := make(chan struct{})
	stderrDone := make(chan struct{})

	stderr := &tailBuffer{}

	c := &child{
		env:        env,
		proc:       proc,
		readyR:     readyR,
		readyW:     readyW,
		namesW:     namesW,
		stderrR:    stderrR,
		stderr:     stderr,
		stderrDone: stderrDone,
		ready:      ready,
		readyDone:  readyDone,
		result:     result,
		exited:     exited,
	}
	go c.writeNames(fdNames)
	go c.waitExit(result, exited)
	go c.waitReady(ready, readyDone)
	go c.drainStderr(stderrR, stderr, stderrDone)
	return c, nil
}

func (c *child) String() string {
	return c.proc.String()
}

func (c *child) Kill() {
	c.proc.Signal(os.Kill)
}

// cleanup releases all resources tied to a failed child: it kills
// the process if it is still running (the waitExit goroutine reaps
// it) and closes the pipes so that the waitReady, writeNames and
// stderr drainer goroutines can terminate.
func (c *child) cleanup() {
	select {
	case <-c.exited:
		// Already reaped, mustn't signal it again.
	default:
		c.Kill()
	}
	// Unblock waitReady if the child is somehow keeping the ready
	// pipe open, e.g. via a grandchild that inherited the fd.
	c.readyR.Close()
	c.stderrR.Close()
	<-c.stderrDone
	<-c.readyDone
}

// exitError builds the diagnostic error for a child that exited
// before signalling readiness. It distinguishes children that were
// killed by a signal from plain exits and appends the retained
// stderr tail. The child's environment is deliberately not
// included, since it may contain sensitive values.
func (c *child) exitError(waitErr error) error {
	var stderr string
	if tail := c.stderr.String(); tail != "" {
		stderr = "\nlast stderr output:\n" + tail
	}

	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if sig, ok := exitSignal(exitErr); ok {
			return fmt.Errorf("child %s terminated by signal %s: %w%s",
				c, sig, ErrChildSignaled, stderr)
		}
		return fmt.Errorf("child %s exited with status %d: %w%s",
			c, exitErr.ExitCode(), ErrChildExited, stderr)
	}

	if waitErr != nil {
		return fmt.Errorf("child %s: %s: %w%s",
			c, waitErr, ErrChildExited, stderr)
	}

	return fmt.Errorf("child %s exited without signalling readiness: %w%s",
		c, ErrChildExited, stderr)
}

func (c *child) waitExit(result chan<- error, exited chan<- struct{}) {
	err := c.proc.Wait()
	// The child's end of the ready pipe was closed by the OS when
	// it exited. Closing our write end as well lets waitReady
	// observe EOF after it has drained a readiness notification
	// that is still buffered in the pipe. This ensures that a
	// child which signalled readiness and exited at the same time
	// is still treated as ready.
	c.readyW.Close()
	// Unblock writeNames.
	c.namesW.Close()
	close(exited)
	result <- err
}

func (c *child) waitReady(ready chan<- *os.File, done chan<- struct{}) {
	defer close(done)
	defer c.readyR.Close()

	var b [1]byte
	if n, _ := c.readyR.Read(b[:]); n > 0 && b[0] == notifyReady {
		// We know that writeNames has exited by this point.
		// Closing the FD now signals to the child that the parent
		// has exited.
		ready <- c.namesW
	}
}

func (c *child) writeNames(names [][]string) {
	enc := gob.NewEncoder(c.namesW)
	if names == nil {
		// Gob panics on nil
		_ = enc.Encode([][]string{})
		return
	}
	_ = enc.Encode(names)
}

// drainStderr forwards the child's stderr to the parent's stderr
// while retaining a bounded tail for diagnostics. It terminates
// when the child's stderr reaches EOF or the pipe is closed by
// cleanup.
func (c *child) drainStderr(r *os.File, tail *tailBuffer, done chan<- struct{}) {
	defer close(done)
	// Tee, don't just buffer: the child's output must appear on the
	// parent's stderr exactly once, whether the upgrade succeeds or
	// fails.
	_, _ = io.Copy(io.MultiWriter(os.Stderr, tail), r)
}

// tailBuffer retains the last maxStderrTail bytes written to it.
// It is safe for concurrent use.
type tailBuffer struct {
	mu        sync.Mutex
	buf       []byte
	discarded int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(p) >= maxStderrTail {
		t.discarded += len(t.buf) + len(p) - maxStderrTail
		t.buf = append(t.buf[:0], p[len(p)-maxStderrTail:]...)
		return len(p), nil
	}

	t.buf = append(t.buf, p...)
	if excess := len(t.buf) - maxStderrTail; excess > 0 {
		t.discarded += excess
		t.buf = append(t.buf[:0], t.buf[excess:]...)
	}
	return len(p), nil
}

// String returns the retained output. If output was discarded the
// result is prefixed with an explicit truncation marker.
func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.buf) == 0 {
		return ""
	}
	if t.discarded > 0 {
		return fmt.Sprintf("[... stderr truncated, %d bytes discarded, last %d bytes follow ...]\n%s",
			t.discarded, len(t.buf), t.buf)
	}
	return string(t.buf)
}
