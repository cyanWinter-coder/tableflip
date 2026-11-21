package tableflip

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

type testUpgrader struct {
	*Upgrader
	procs chan *testProcess
}

func newTestUpgrader(opts Options) *testUpgrader {
	env, procs := testEnv()
	u, err := newUpgrader(env, opts)
	if err != nil {
		panic(err)
	}
	err = u.Ready()
	if err != nil {
		panic(err)
	}

	return &testUpgrader{
		Upgrader: u,
		procs:    procs,
	}
}

func (tu *testUpgrader) upgradeProc(t *testing.T) (*testProcess, <-chan error) {
	t.Helper()

	ch := make(chan error, 1)
	go func() {
		for {
			err := tu.Upgrade()
			if err != errNotReady {
				ch <- err
				return
			}
		}
	}()

	select {
	case err := <-ch:
		t.Fatal("Upgrade failed:", err)
		return nil, nil

	case proc := <-tu.procs:
		return proc, ch
	}
}

// queuedUpgrades returns the number of child processes that have been
// started but not yet consumed by the test.
func (tu *testUpgrader) queuedUpgrades() int {
	return len(tu.procs)
}

// launchUpgrade runs Upgrade in its own goroutine, forwarding its result.
func launchUpgrade(u *testUpgrader, errC chan<- error) {
	go func() { errC <- u.Upgrade() }()
}

// launchReadyUpgrade starts an Upgrade that retries until the Upgrader has
// processed Ready. newTestUpgrader calls Ready just before returning, so the
// very first Upgrade can otherwise race the coordinator and observe
// errNotReady. This mirrors the existing upgradeProc helper.
func launchReadyUpgrade(u *testUpgrader, errC chan<- error) {
	go func() {
		for {
			err := u.Upgrade()
			if err == errNotReady {
				continue
			}
			errC <- err
			return
		}
	}()
}

// collectErrors reads exactly n values from errC, failing on timeout.
func collectErrors(t *testing.T, errC <-chan error, n int) []error {
	t.Helper()
	errs := make([]error, 0, n)
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case err := <-errC:
			errs = append(errs, err)
		case <-deadline:
			t.Fatalf("timed out collecting error %d/%d, got %d", i+1, n, len(errs))
		}
	}
	return errs
}

// assertNoImmediateError fails if errC is already carrying a reply.
func assertNoImmediateError(t *testing.T, errC <-chan error) {
	t.Helper()
	select {
	case err := <-errC:
		t.Fatal("Upgrade returned before the transaction resolved:", err)
	case <-time.After(20 * time.Millisecond):
	}
}

var names = []string{"zaphod", "beeblebrox"}

func TestMain(m *testing.M) {
	upg, err := New(Options{})
	if errors.Is(err, ErrNotSupported) {
		fmt.Fprintln(os.Stderr, "Skipping tests, OS is not supported")
		os.Exit(0)
	}
	if err != nil {
		panic(err)
	}

	if upg.parent == nil {
		// Execute test suite if there is no parent.
		os.Exit(m.Run())
	}

	if err := childProcess(upg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

type childState struct {
	PID int
}

// Used by Benchmark and TestUpgraderOnOS
func childProcess(upg *Upgrader) error {
	if !upg.HasParent() {
		return errors.New("Upgrader doesn't recognize parent")
	}

	wState, err := upg.Fds.File("wState")
	if err != nil {
		return err
	}
	if wState != nil {
		state := &childState{
			PID: os.Getpid(),
		}
		if err := gob.NewEncoder(wState).Encode(state); err != nil {
			return err
		}
		wState.Close()
	}

	for _, name := range names {
		file, err := upg.Fds.File(name)
		if err != nil {
			return fmt.Errorf("can't get file %s: %s", name, err)
		}
		if file == nil {
			continue
		}
		if _, err := io.WriteString(file, name); err != nil {
			return fmt.Errorf("can't write to %s: %s", name, err)
		}
		file.Close()
	}

	rExit, err := upg.Fds.File("rExit")
	if err != nil {
		return err
	}

	// Ready closes all inherited but unused files.
	if err := upg.Ready(); err != nil {
		return fmt.Errorf("can't signal ready: %s", err)
	}

	// Block until the parent is done with us. Returning an
	// error here won't make the parent fail, so don't bother.
	if rExit != nil {
		var b [1]byte
		rExit.Read(b[:])
	}

	return nil
}

func TestUpgraderOnOS(t *testing.T) {
	u, err := newUpgrader(stdEnv, Options{})
	if err != nil {
		t.Fatal("Can't create Upgrader:", err)
	}
	defer u.Stop()

	pipe := func() (r, w *os.File) {
		t.Helper()

		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		return r, w
	}

	addPipe := func(name string, file *os.File) {
		t.Helper()

		if err := u.Fds.AddFile(name, file); err != nil {
			t.Fatal(err)
		}
		file.Close()
	}

	rState, wState := pipe()
	defer rState.Close()

	addPipe("wState", wState)

	rExit, wExit := pipe()
	defer wExit.Close()

	addPipe("rExit", rExit)

	var readers []*os.File
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	for _, name := range names {
		r, w := pipe()
		addPipe(name, w)
		readers = append(readers, r)
	}

	if err := u.Upgrade(); err == nil {
		t.Error("Upgrade before Ready should return an error")
	}

	if err := u.Ready(); err != nil {
		t.Fatal("Ready failed:", err)
	}

	for {
		if err := u.Upgrade(); err == nil {
			break
		} else if err != errNotReady {
			t.Fatal("Upgrade failed:", err)
		}
	}

	// Tell child it's OK to exit now.
	wExit.Close()

	// Close copies of write pipes, so that
	// reads below return EOF.
	u.Stop()

	var state childState
	if err := gob.NewDecoder(rState).Decode(&state); err != nil {
		t.Fatal("Can't decode state from child:", err)
	}

	if state.PID == os.Getpid() {
		t.Error("Child did not execute in new process")
	}

	for i, name := range names {
		nameBytes, err := ioutil.ReadAll(readers[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(nameBytes, []byte(name)) {
			t.Fatalf("File %s has name %s in child", name, string(nameBytes))
		}
	}
}

func TestUpgraderCleanExit(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	proc, errs := u.upgradeProc(t)

	proc.exit(nil)
	if err := <-errs; err == nil {
		t.Error("Expected Upgrade to return error when new child exits clean")
	}
}

func TestUpgraderUncleanExit(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	proc, errs := u.upgradeProc(t)

	proc.exit(errors.New("some error"))
	if err := <-errs; err == nil {
		t.Error("Expected Upgrade to return error when new child exits unclean")
	}
}

func TestUpgraderTimeout(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{
		UpgradeTimeout: 10 * time.Millisecond,
	})
	defer u.Stop()

	new, errs := u.upgradeProc(t)

	if sig := new.recvSignal(nil); sig != os.Kill {
		t.Error("Expected os.Kill, got", sig)
	}

	if err := <-errs; err == nil {
		t.Error("Expected Upgrade to return error when new child times out")
	}
}

func TestUpgraderListenConfig(t *testing.T) {
	t.Parallel()

	var listenConfigUsed bool
	u := newTestUpgrader(Options{
		ListenConfig: &net.ListenConfig{
			Control: func(network, address string, c syscall.RawConn) error {
				listenConfigUsed = true
				return nil
			},
		},
	})
	defer u.Stop()

	new, _ := u.upgradeProc(t)

	go new.recvSignal(nil)

	_, err := u.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("Unexpected error from listen: %v", err)
	}

	if !listenConfigUsed {
		t.Error("Expected ListenConfig to be called during Listen")
	}

	new.exit(nil)
}

func TestUpgraderConcurrentUpgrade(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	new, _ := u.upgradeProc(t)

	// A second request while the child is preparing must not start another
	// child and must not be answered early: it is coalesced into the
	// pending transaction.
	secondErr := make(chan error, 1)
	go func() { secondErr <- u.Upgrade() }()

	// Give the coordinator time to receive the request. Starting a second
	// child would at this point have to appear on u.procs.
	time.Sleep(50 * time.Millisecond)
	assertNoImmediateError(t, secondErr)

	if got := u.queuedUpgrades(); got != 0 {
		t.Fatalf("expected no additional child, got %d", got)
	}

	go new.recvSignal(nil)
	new.exit(nil)

	// The first attempt fails; instead of getting an early reply, the
	// coalesced request becomes the single intentional retry.
	retry := <-u.procs

	// No third child may exist: there is at most one pending retry.
	if got := u.queuedUpgrades(); got != 0 {
		t.Fatalf("coalescing spawned %d retry children, want 1", got)
	}

	// The retry commits and the coalesced caller observes success.
	if _, _, err := retry.notify(); err != nil {
		t.Fatal(err)
	}
	if err := <-secondErr; err != nil {
		t.Error("Coalesced Upgrade should report the committed retry, got:", err)
	}

	select {
	case <-u.Exit():
	case <-time.After(5 * time.Second):
		t.Fatal("Exit() not closed after coalesced retry committed")
	}
}

// TestUpgraderSignalStormRetry fires many upgrade signals while a child is
// being prepared. They must be coalesced into at most one pending retry,
// never start additional children, and all resolve once the transaction
// settles without leaking goroutines.
func TestUpgraderSignalStormRetry(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	const storm = 10

	// First request starts the initial child.
	firstErr := make(chan error, 1)
	launchReadyUpgrade(u, firstErr)
	first := <-u.procs

	// A burst of signals arrives while the first child is preparing. The
	// coordinator is busy with the first attempt and does not read from
	// upgradeC, so these callers queue behind it.
	stormErrs := make(chan error, storm)
	for i := 0; i < storm; i++ {
		launchUpgrade(u, stormErrs)
	}
	// Give the burst goroutines time to reach the coordinator and queue.
	// No child can be started while the first attempt is still running,
	// which is asserted explicitly below.
	time.Sleep(50 * time.Millisecond)
	assertNoImmediateError(t, stormErrs)

	// Only the first child exists while it is being prepared.
	if got := u.queuedUpgrades(); got != 0 {
		t.Fatalf("signal storm started %d additional children", got)
	}

	// The first attempt fails. Only the caller that started it is attached
	// to that attempt; the storm signals were coalesced as retry intents.
	go first.recvSignal(nil)
	first.exit(errors.New("first failed"))
	if err := <-firstErr; err == nil {
		t.Error("first attempt should have reported its failure")
	}

	// All storm requests were coalesced into exactly one retry child.
	select {
	case second := <-u.procs:
		if got := u.queuedUpgrades(); got != 0 {
			t.Fatalf("coalescing spawned %d retry children, want 1", got)
		}

		// The retry commits: the old process enters the exit phase and
		// every coalesced caller observes the successful generation.
		if _, _, err := second.notify(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no retry child started from coalesced requests")
	}

	for _, err := range collectErrors(t, stormErrs, storm) {
		if err != nil {
			t.Error("coalesced caller should observe committed retry, got:", err)
		}
	}

	select {
	case <-u.Exit():
	case <-time.After(5 * time.Second):
		t.Fatal("Exit() not closed after coalesced retry committed")
	}

	// A further signal after the commit can never start another child.
	// The caller is either drained by the committing transaction (nil) or
	// turned away once the exit channel is closed ("already upgraded").
	postErr := make(chan error, 1)
	launchUpgrade(u, postErr)
	if err := <-postErr; err != nil && err.Error() != "already upgraded" {
		t.Fatal("post-commit caller got unexpected error:", err)
	}
	if got := u.queuedUpgrades(); got != 0 {
		t.Fatalf("post-commit signal spawned %d children", got)
	}
}

// TestUpgraderCommitRace ensures that once an upgrade has committed and the
// process is exiting, no racing signal can start another child and every
// caller resolves.
func TestUpgraderCommitRace(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	const racers = 8

	ready := make(chan error, 1)
	launchReadyUpgrade(u, ready)
	proc := <-u.procs

	// Fire signals that are pending while the commit happens. The
	// coordinator is busy waiting for the child and cannot start another
	// one or answer these callers yet.
	errs := make(chan error, racers)
	for i := 0; i < racers; i++ {
		launchUpgrade(u, errs)
	}
	// Let the racing callers queue behind the committing transaction.
	time.Sleep(50 * time.Millisecond)

	if _, _, err := proc.notify(); err != nil {
		t.Fatal(err)
	}
	if err := <-ready; err != nil {
		t.Fatal("Upgrade failed:", err)
	}

	// Every racing caller resolves and observes the committed generation.
	for _, err := range collectErrors(t, errs, racers) {
		if err != nil {
			t.Error("racing caller should observe the committed upgrade, got", err)
		}
	}

	select {
	case <-u.Exit():
	case <-time.After(5 * time.Second):
		t.Fatal("Exit() not closed after commit")
	}
	if got := u.queuedUpgrades(); got != 0 {
		t.Fatalf("commit race spawned %d children", got)
	}
}

// TestUpgraderFailureRetry verifies that a failed preparation clears the
// transaction completely, a new explicit upgrade works, and a retry only
// happens when another request is actually pending.
func TestUpgraderFailureRetry(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	firstErr := make(chan error, 1)
	launchReadyUpgrade(u, firstErr)
	first := <-u.procs

	go first.recvSignal(nil)
	first.exit(errors.New("preparation failed"))
	if err := <-firstErr; err == nil {
		t.Fatal("expected failure from first attempt")
	}

	// With no coalesced request the process stays in charge and no retry
	// child appears. The coordinator never starts a child on its own, so a
	// non-blocking check is sufficient here.
	select {
	case proc := <-u.procs:
		t.Fatal("unexpected retry child without a pending request:", proc)
	default:
	}
	select {
	case <-u.Exit():
		t.Fatal("Exit() must not close after a failed attempt")
	default:
	}

	// A fresh, explicit upgrade is accepted and can commit.
	secondErr := make(chan error, 1)
	launchUpgrade(u, secondErr)
	var second *testProcess
	select {
	case second = <-u.procs:
	case <-time.After(5 * time.Second):
		t.Fatal("new explicit upgrade did not start a child")
	}
	if _, _, err := second.notify(); err != nil {
		t.Fatal(err)
	}
	if err := <-secondErr; err != nil {
		t.Fatal("retry upgrade failed:", err)
	}
	select {
	case <-u.Exit():
	case <-time.After(5 * time.Second):
		t.Fatal("Exit() not closed after successful retry")
	}
}

// TestUpgraderRepeatedFailureThenSuccess drives two failed attempts back to
// back through the coalescing path, proving each failed transaction is fully
// cleared before the next, and that a later explicit upgrade still commits.
func TestUpgraderRepeatedFailureThenSuccess(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	firstErr := make(chan error, 1)
	launchReadyUpgrade(u, firstErr)
	first := <-u.procs

	// This request is queued while the first child prepares and becomes the
	// single retry after the first failure.
	retryErr := make(chan error, 1)
	launchUpgrade(u, retryErr)
	time.Sleep(50 * time.Millisecond)

	go first.recvSignal(nil)
	first.exit(errors.New("first failed"))
	if err := <-firstErr; err == nil {
		t.Fatal("first attempt should fail")
	}

	var second *testProcess
	select {
	case second = <-u.procs:
	case <-time.After(5 * time.Second):
		t.Fatal("coalesced retry did not start a child")
	}
	if got := u.queuedUpgrades(); got != 0 {
		t.Fatalf("expected exactly one retry child, got %d more", got)
	}

	// The coalesced retry also fails. Its caller observes the error and the
	// transaction ends with no further queued request.
	go second.recvSignal(nil)
	second.exit(errors.New("retry failed"))
	if err := <-retryErr; err == nil {
		t.Fatal("retry attempt should fail")
	}
	select {
	case proc := <-u.procs:
		t.Fatal("unexpected third child without a pending request:", proc)
	default:
	}
	select {
	case <-u.Exit():
		t.Fatal("Exit() must stay open while all attempts failed")
	default:
	}

	// A new explicit upgrade after two failures works and commits.
	finalErr := make(chan error, 1)
	launchUpgrade(u, finalErr)
	var final *testProcess
	select {
	case final = <-u.procs:
	case <-time.After(5 * time.Second):
		t.Fatal("final explicit upgrade did not start a child")
	}
	if _, _, err := final.notify(); err != nil {
		t.Fatal(err)
	}
	if err := <-finalErr; err != nil {
		t.Fatal("final upgrade failed:", err)
	}
	select {
	case <-u.Exit():
	case <-time.After(5 * time.Second):
		t.Fatal("Exit() not closed after final commit")
	}
}

// TestUpgraderStopAfterCommitKeepsChild makes sure that stopping waiters, or
// calling Stop, after the child has taken over neither kills the new process
// nor leaves coordinating goroutines behind.
func TestUpgraderStopAfterCommitKeepsChild(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	errs := make(chan error, 1)
	launchReadyUpgrade(u, errs)
	proc := <-u.procs

	if _, _, err := proc.notify(); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal("Upgrade failed:", err)
	}

	// Stop during the exit phase must not signal the child that took over.
	u.Stop()
	select {
	case sig := <-proc.signals:
		t.Fatalf("committed child received signal %v after Stop", sig)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHasParent(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	if u.HasParent() {
		t.Fatal("First process cannot have a parent")
	}
}

func TestUpgraderWaitForParent(t *testing.T) {
	t.Parallel()

	env, procs := testEnv()
	child, err := startChild(env, nil)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	u, err := newUpgrader(&proc.env, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Stop()

	if err := u.Ready(); err != nil {
		t.Fatal(err)
	}

	exited := make(chan error, 1)
	go func() {
		exited <- u.WaitForParent(context.Background())
	}()

	select {
	case <-exited:
		t.Fatal("Returned before parent exited")
	case <-time.After(time.Second):
	}

	readyFile := <-child.ready
	if err := readyFile.Close(); err != nil {
		t.Fatal(err)
	}

	if err := <-exited; err != nil {
		t.Fatal("Unexpected error:", err)
	}
}

func TestUpgraderReady(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	new, errs := u.upgradeProc(t)

	_, exited, err := new.notify()
	if err != nil {
		t.Fatal("Can't notify Upgrader:", err)
	}

	if err := <-errs; err != nil {
		t.Fatal("Expected Upgrade to return nil when child is ready")
	}

	select {
	case <-u.Exit():
	default:
		t.Error("Expected Exit() to be closed when upgrade is done")
	}

	// Simulate the process exiting
	file := <-u.exitFd
	file.file.Close()

	select {
	case err := <-exited:
		if err != nil {
			t.Error("exit error", err)
		}
	case <-time.After(time.Second):
		t.Error("Child wasn't notified of parent exiting")
	}
}

func TestUpgraderShutdownCancelsUpgrade(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	new, errs := u.upgradeProc(t)

	go new.recvSignal(nil)

	u.Stop()
	if err := <-errs; err == nil {
		t.Error("Upgrade doesn't return an error when Stopp()ed")
	}

	if err := u.Upgrade(); err == nil {
		t.Error("Upgrade doesn't return an error after Stop()")
	}
}

func TestReadyWritesPIDFile(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "tableflip")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	file := dir + "/pid"
	u := newTestUpgrader(Options{
		PIDFile: file,
	})
	defer u.Stop()

	if err := u.Ready(); err != nil {
		t.Fatal("Ready returned error:", err)
	}

	fh, err := os.Open(file)
	if err != nil {
		t.Fatal("PID file doesn't exist:", err)
	}
	defer fh.Close()

	var pid int
	if _, err := fmt.Fscan(fh, &pid); err != nil {
		t.Fatal("Can't read PID:", err)
	}

	if pid != os.Getpid() {
		t.Error("PID doesn't match")
	}
}

func TestWritePidFileWithoutPath(t *testing.T) {
	pidFile := "tableflip-test.pid"

	err := writePIDFile(pidFile)
	if err != nil {
		t.Fatal("Could not write pidfile:", err)
	}
	defer os.Remove(pidFile)

	// lets see if we are able to read the file back
	fh, err := os.Open(pidFile)
	if err != nil {
		t.Fatal("PID file doesn't exist:", err)
	}
	defer fh.Close()

	// just to be sure: check the pid for correctness
	// if something failed at a previous run we could be reading
	// a bogus pidfile
	var pid int
	if _, err := fmt.Fscan(fh, &pid); err != nil {
		t.Fatal("Can't read PID:", err)
	}

	if pid != os.Getpid() {
		t.Error("PID doesn't match")
	}
}

func BenchmarkUpgrade(b *testing.B) {
	for _, n := range []int{4, 400, 4000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			fds := newFds(nil, nil)
			for i := 0; i < n; i += 2 {
				r, w, err := os.Pipe()
				if err != nil {
					b.Fatal(err)
				}

				err = fds.AddFile(strconv.Itoa(n), r)
				if err != nil {
					b.Fatal(err)
				}
				r.Close()

				err = fds.AddFile(strconv.Itoa(n), w)
				if err != nil {
					b.Fatal(err)
				}
				w.Close()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				u, err := newUpgrader(stdEnv, Options{})
				if err != nil {
					b.Fatal("Can't create Upgrader:", err)
				}
				if err := u.Ready(); err != nil {
					b.Fatal("Can't call Ready:", err)
				}

				u.Fds = fds
				if err := u.Upgrade(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			for _, f := range fds.used {
				f.Close()
			}
		})
	}
}
