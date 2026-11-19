package tableflip

import (
	"encoding/gob"
	"fmt"
	"os"
)

type child struct {
	*env
	proc           process
	readyR, namesW *os.File
	ready          <-chan *os.File
	prepared       <-chan struct{}
	aborted        <-chan struct{}
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

	// Copy passed fds and append the notification pipe
	fds := []*os.File{os.Stdin, os.Stdout, os.Stderr, readyW, namesR}
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
		return nil, fmt.Errorf("can't start process %s: %s", os.Args[0], err)
	}

	// The child now owns its copies of readyW and namesR. Close the
	// parent's copies, so that the parent observes EOF on readyR once
	// the child closes readyW (or dies) without committing, and so
	// that the descriptors don't leak.
	readyW.Close()
	namesR.Close()

	exited := make(chan struct{})
	result := make(chan error, 1)
	ready := make(chan *os.File, 1)
	prepared := make(chan struct{}, 1)
	aborted := make(chan struct{})

	c := &child{
		env,
		proc,
		readyR,
		namesW,
		ready,
		prepared,
		aborted,
		result,
		exited,
	}
	go c.writeNames(fdNames)
	go c.waitExit(result, exited)
	go c.waitReady(ready, prepared, aborted)
	return c, nil
}

func (c *child) String() string {
	return c.proc.String()
}

func (c *child) Kill() {
	c.proc.Signal(os.Kill)
}

func (c *child) waitExit(result chan<- error, exited chan<- struct{}) {
	result <- c.proc.Wait()
	close(exited)
	// Unblock waitReady and writeNames
	c.readyR.Close()
	c.namesW.Close()
}

func (c *child) waitReady(ready chan<- *os.File, prepared chan<- struct{}, aborted chan<- struct{}) {
	defer c.readyR.Close()

	var b [1]byte
	for {
		n, _ := c.readyR.Read(b[:])
		if n == 0 {
			// The child closed the pipe (or died) without
			// committing: the upgrade is rolled back.
			close(aborted)
			return
		}

		switch b[0] {
		case notifyPrepared:
			// The child took over its listeners, but isn't
			// ready to commit yet. Don't block if the parent
			// hasn't consumed a previous notification.
			select {
			case prepared <- struct{}{}:
			default:
			}

		case notifyReady:
			// We know that writeNames has exited by this point.
			// Closing the FD now signals to the child that the parent
			// has exited.
			ready <- c.namesW
			return
		}
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
