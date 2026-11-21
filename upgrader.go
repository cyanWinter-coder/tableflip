package tableflip

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// DefaultUpgradeTimeout is the duration before the Upgrader kills the new process if no
// readiness notification was received.
const DefaultUpgradeTimeout time.Duration = time.Minute

// Options control the behaviour of the Upgrader.
type Options struct {
	// Time after which an upgrade is considered failed. Defaults to
	// DefaultUpgradeTimeout.
	UpgradeTimeout time.Duration
	// The PID of a ready process is written to this file.
	PIDFile string
	// ListenConfig is a custom ListenConfig. Defaults to an empty ListenConfig
	ListenConfig *net.ListenConfig
}

// Upgrader handles zero downtime upgrades and passing files between processes.
type Upgrader struct {
	*Fds

	*env
	opts      Options
	parent    *parent
	parentErr chan error
	readyOnce sync.Once
	readyC    chan struct{}
	stopOnce  sync.Once
	stopC     chan struct{}
	upgradeC  chan chan<- error
	exitC     chan struct{}
	exitFd    chan neverCloseThisFile
}

var (
	stdEnvMu       sync.Mutex
	stdEnvUpgrader *Upgrader
)

var ErrNotSupported = errors.New("tableflip: platform does not support graceful restart")

// New creates a new Upgrader. Files are passed from the parent and may be empty.
//
// Only the first call to this function will succeed. May return ErrNotSupported.
func New(opts Options) (upg *Upgrader, err error) {
	stdEnvMu.Lock()
	defer stdEnvMu.Unlock()

	if !isSupportedOS() {
		return nil, fmt.Errorf("%w", ErrNotSupported)
	}

	if stdEnvUpgrader != nil {
		return nil, errors.New("tableflip: only a single Upgrader allowed")
	}

	upg, err = newUpgrader(stdEnv, opts)
	// Store a reference to upg in a private global variable, to prevent
	// it from being GC'ed and exitFd being closed prematurely.
	stdEnvUpgrader = upg
	return
}

func newUpgrader(env *env, opts Options) (*Upgrader, error) {
	if initialWD == "" {
		return nil, errors.New("couldn't determine initial working directory")
	}

	parent, files, err := newParent(env)
	if err != nil {
		return nil, err
	}

	if opts.UpgradeTimeout <= 0 {
		opts.UpgradeTimeout = DefaultUpgradeTimeout
	}

	u := &Upgrader{
		env:       env,
		opts:      opts,
		parent:    parent,
		parentErr: make(chan error, 1),
		readyC:    make(chan struct{}),
		stopC:     make(chan struct{}),
		upgradeC:  make(chan chan<- error),
		exitC:     make(chan struct{}),
		exitFd:    make(chan neverCloseThisFile, 1),
		Fds:       newFds(files, opts.ListenConfig),
	}

	go u.run()

	return u, nil
}

// Ready signals that the current process is ready to accept connections.
// It must be called to finish the upgrade.
//
// All fds which were inherited but not used are closed after the call to Ready.
func (u *Upgrader) Ready() error {
	u.readyOnce.Do(func() {
		u.Fds.closeInherited()
		close(u.readyC)
	})

	if u.opts.PIDFile != "" {
		if err := writePIDFile(u.opts.PIDFile); err != nil {
			return fmt.Errorf("tableflip: can't write PID file: %s", err)
		}
	}

	if u.parent == nil {
		return nil
	}
	return u.parent.sendReady()
}

// Exit returns a channel which is closed when the process should
// exit.
func (u *Upgrader) Exit() <-chan struct{} {
	return u.exitC
}

// Stop prevents any more upgrades from happening, and closes
// the exit channel.
//
// If this function is called before a call to Upgrade() has
// succeeded, it is assumed that the process is being shut down
// completely. All Unix sockets known to Upgrader.Fds are then
// unlinked from the filesystem.
func (u *Upgrader) Stop() {
	u.stopOnce.Do(func() {
		// Interrupt any running Upgrade(), and
		// prevent new upgrade from happening.
		close(u.stopC)
	})
}

// WaitForParent blocks until the parent has exited.
//
// Returns an error if the parent misbehaved during shutdown.
func (u *Upgrader) WaitForParent(ctx context.Context) error {
	if u.parent == nil {
		return nil
	}

	var err error
	select {
	case err = <-u.parent.result:
	case err = <-u.parentErr:
	case <-ctx.Done():
		return ctx.Err()
	}

	// This is a bit cheeky, since it means that multiple
	// calls to WaitForParent resolve in sequence, but that
	// probably doesn't matter.
	u.parentErr <- err
	return err
}

// HasParent checks if the current process is an upgrade or the first invocation.
func (u *Upgrader) HasParent() bool {
	return u.parent != nil
}

// Upgrade triggers an upgrade.
//
// At most one upgrade is ever in progress at a given time. Calls made while
// an upgrade is being prepared are coalesced into a single pending request:
// they do not spawn another child and do not get an early reply. When the
// running upgrade fails, one coalesced request drives exactly one retry and
// every caller observes the result of the attempt it was attached to. A
// successful upgrade moves the process into the exit phase, after which all
// calls return an error.
func (u *Upgrader) Upgrade() error {
	// The response channel is buffered so that the coordinating goroutine
	// never blocks on a reply, even if the caller stops waiting for it.
	response := make(chan error, 1)
	select {
	case <-u.stopC:
		return errors.New("terminating")
	case <-u.exitC:
		return errors.New("already upgraded")
	case u.upgradeC <- response:
	}

	select {
	case err := <-response:
		return err
	case <-u.stopC:
		// The caller gave up waiting. Abandoning the response does not
		// cancel anything: the coordinating goroutine keeps owning the
		// transaction and will never kill a child that took over.
		return errors.New("terminating")
	case <-u.exitC:
		// A concurrent upgrade committed while we were waiting: the
		// upgrade the caller asked for has effectively happened.
		return nil
	}
}

var errNotReady = errors.New("process is not ready yet")

// upgradeOutcome reports how a single upgrade transaction ended.
type upgradeOutcome int

const (
	// upgradeCommitted means the child signalled readiness and the old
	// process has moved into the exit phase.
	upgradeCommitted upgradeOutcome = iota
	// upgradeFailed means the child could not take over, leaving the
	// current process in charge and able to accept new requests.
	upgradeFailed
	// upgradeStopping means Stop was invoked before the child took over.
	upgradeStopping
)

func (u *Upgrader) run() {
	defer close(u.exitC)

	var (
		parentExited <-chan struct{}
		processReady = u.readyC
	)

	if u.parent != nil {
		parentExited = u.parent.exited
	}

	for {
		select {
		case <-parentExited:
			parentExited = nil

		case <-processReady:
			processReady = nil

		case <-u.stopC:
			u.Fds.closeAndRemoveUsed()
			return

		case request := <-u.upgradeC:
			outcome := u.runUpgradeTransaction(request, processReady != nil, parentExited != nil)
			switch outcome {
			case upgradeCommitted:
				// The process is exiting and must never start another
				// child. Drain any request that raced the commit and
				// report success: the upgraded generation the caller
				// asked for is now running. This matches the exitC
				// branch of Upgrade so the result is deterministic no
				// matter which select case the caller wakes up on.
				u.drainAndReplyUpgradeRequests(nil)
				return
			case upgradeStopping:
				u.Fds.closeAndRemoveUsed()
				return
			case upgradeFailed:
				// The failed transaction is cleaned up and the loop
				// accepts a fresh, explicit upgrade request.
			}
		}
	}
}

// runUpgradeTransaction runs a single upgrade and the at-most-one retry
// coalesced from requests arriving while a child is being prepared.
//
// The initial request is the first caller; every further request received
// while an attempt is running is queued. A failed attempt consumes at most
// one queued request to drive a retry, a successful attempt ends the
// transaction for every queued caller.
func (u *Upgrader) runUpgradeTransaction(initial chan<- error, notReady, parentAlive bool) upgradeOutcome {
	waiters := []chan<- error{initial}

	for {
		if notReady {
			replyAll(waiters, errNotReady)
			return upgradeFailed
		}
		if parentAlive {
			replyAll(waiters, errors.New("parent hasn't exited"))
			return upgradeFailed
		}

		file, err, outcome := u.attemptUpgrade()
		switch outcome {
		case upgradeCommitted:
			// Save file in exitFd, so that it's only closed when the
			// process exits. This signals to the new process that the
			// old process has exited.
			u.exitFd <- neverCloseThisFile{file}
			u.Fds.closeUsed()
			replyAll(waiters, nil)
			return upgradeCommitted
		case upgradeStopping:
			replyAll(waiters, err)
			return upgradeStopping
		case upgradeFailed:
			// The attempt failed. Coalesce every request that arrived
			// while the child was preparing into one intentional retry,
			// and clear the failed transaction so a new attempt can
			// start cleanly.
			replyAll(waiters, err)
			waiters = u.drainUpgradeRequests()
			if len(waiters) == 0 {
				return upgradeFailed
			}
		}
	}
}

// attemptUpgrade starts a child and waits for it to take over.
//
// Upgrade requests arriving while the child is preparing are left queued on
// upgradeC for the transaction loop: this function never starts a second
// child and never answers upgrade requests itself. A child that has
// signalled readiness is never killed, even if Stop fires concurrently.
func (u *Upgrader) attemptUpgrade() (*os.File, error, upgradeOutcome) {
	child, err := startChild(u.env, u.Fds.copy())
	if err != nil {
		return nil, fmt.Errorf("can't start child: %s", err), upgradeFailed
	}

	readyTimeout := time.After(u.opts.UpgradeTimeout)
	for {
		select {
		case err := <-child.result:
			if err == nil {
				return nil, fmt.Errorf("child %s exited", child), upgradeFailed
			}
			return nil, fmt.Errorf("child %s exited: %s", child, err), upgradeFailed

		case <-u.stopC:
			child.Kill()
			return nil, errors.New("terminating"), upgradeStopping

		case <-readyTimeout:
			child.Kill()
			return nil, fmt.Errorf("new child %s timed out", child), upgradeFailed

		case file := <-child.ready:
			// The child owns the inherited resources now. Do not
			// select on stopC anymore: a shutdown must not kill a
			// process that has successfully taken over.
			return file, nil, upgradeCommitted
		}
	}
}

// drainUpgradeRequests collects every upgrade request currently waiting.
// The coordinating goroutine is the only receiver of upgradeC, so no
// requests can be missed.
func (u *Upgrader) drainUpgradeRequests() []chan<- error {
	var requests []chan<- error
	for {
		select {
		case request := <-u.upgradeC:
			requests = append(requests, request)
		default:
			return requests
		}
	}
}

// drainAndReplyUpgradeRequests answers any request that raced the end of the
// coordinating loop. Channels are buffered, so a caller that abandoned its
// wait never blocks the coordinating goroutine and no goroutine is leaked.
func (u *Upgrader) drainAndReplyUpgradeRequests(err error) {
	for _, request := range u.drainUpgradeRequests() {
		request <- err
	}
}

// replyAll delivers err to every waiting caller.
func replyAll(requests []chan<- error, err error) {
	for _, request := range requests {
		request <- err
	}
}

// This file must never be closed by the Go runtime, since its used by the
// child to determine when the parent has died. It must only be closed
// by the OS.
// Hence we make sure that this file can't be garbage collected by referencing
// it from an Upgrader.
type neverCloseThisFile struct {
	file *os.File
}

func writePIDFile(path string) error {
	dir, file := filepath.Split(path)

	// if dir is empty, the user probably specified just the name
	// of the pid file expecting it to be created in the current work directory
	if dir == "" {
		dir = initialWD
	}

	if dir == "" {
		return errors.New("empty initial working directory")
	}

	fh, err := ioutil.TempFile(dir, file)
	if err != nil {
		return err
	}
	defer fh.Close()
	// Remove temporary PID file if something fails
	defer os.Remove(fh.Name())

	_, err = fh.WriteString(strconv.Itoa(os.Getpid()))
	if err != nil {
		return err
	}

	return os.Rename(fh.Name(), path)
}

// Check if this is a supported OS.
// That is currently all Unix-like OS's.
// At the moment, we assume that is everything except Windows.
func isSupportedOS() bool {
	return runtime.GOOS != "windows"
}
