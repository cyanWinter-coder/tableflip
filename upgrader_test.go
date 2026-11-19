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

	go new.recvSignal(nil)

	if err := u.Upgrade(); err == nil {
		t.Error("Expected Upgrade to refuse concurrent upgrade")
	}

	new.exit(nil)
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

// newChildUpgrader creates an Upgrader which has a parent process,
// along with the parent-side handle of the child.
func newChildUpgrader(t *testing.T, opts Options, passedFiles map[fileName]*file) (*Upgrader, *child) {
	t.Helper()

	env, procs := testEnv()
	child, err := startChild(env, passedFiles)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	u, err := newUpgrader(&proc.env, opts)
	if err != nil {
		t.Fatal(err)
	}

	return u, child
}

func TestUpgraderPrepareDoesntCommit(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{})
	defer u.Stop()

	proc, errs := u.upgradeProc(t)

	parent, _, err := newParent(&proc.env)
	if err != nil {
		t.Fatal(err)
	}

	// The child took over its listeners, but isn't ready yet.
	if err := parent.sendPrepared(); err != nil {
		t.Fatal(err)
	}

	// The old process must not exit before the child commits.
	select {
	case err := <-errs:
		t.Fatal("Upgrade returned after prepare:", err)
	case <-time.After(200 * time.Millisecond):
	}

	select {
	case <-u.Exit():
		t.Error("Exit() closed before child committed")
	default:
	}

	// Commit the upgrade.
	if err := parent.sendReady(); err != nil {
		t.Fatal(err)
	}

	if err := <-errs; err != nil {
		t.Fatal("Expected Upgrade to return nil when child is ready")
	}
}

func TestUpgraderPrepareResetsTimeout(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{
		UpgradeTimeout: 500 * time.Millisecond,
	})
	defer u.Stop()

	proc, errs := u.upgradeProc(t)

	parent, _, err := newParent(&proc.env)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare halfway through the timeout.
	time.Sleep(250 * time.Millisecond)
	if err := parent.sendPrepared(); err != nil {
		t.Fatal(err)
	}

	// Without the reset the child would be killed ~250ms from now.
	start := time.Now()
	if sig := proc.recvSignal(nil); sig != os.Kill {
		t.Error("Expected os.Kill, got", sig)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Error("Timeout was not reset by prepare, killed after", elapsed)
	}

	if err := <-errs; err == nil {
		t.Error("Expected Upgrade to return error when child times out")
	}
}

func TestUpgraderAbortKeepsOldProcess(t *testing.T) {
	t.Parallel()

	u := newTestUpgrader(Options{
		UpgradeTimeout: time.Minute,
	})
	defer u.Stop()

	proc, errs := u.upgradeProc(t)

	parent, _, err := newParent(&proc.env)
	if err != nil {
		t.Fatal(err)
	}

	// Roll back: close the notification pipe without sending ready.
	parent.abort()

	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("Expected Upgrade to fail when child aborts")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Upgrade didn't return after child aborted")
	}

	// The old process must still accept connections and upgrades.
	if _, err := u.Listen("tcp", "localhost:0"); err != nil {
		t.Fatal("Old process lost its listeners:", err)
	}

	proc2, errs2 := u.upgradeProc(t)
	if _, _, err := proc2.notify(); err != nil {
		t.Fatal("Can't notify Upgrader:", err)
	}
	if err := <-errs2; err != nil {
		t.Fatal("Second upgrade failed after rollback:", err)
	}
}

func TestChildPrepareThenReady(t *testing.T) {
	t.Parallel()

	u, child := newChildUpgrader(t, Options{}, nil)
	defer u.Stop()

	if err := u.Prepare(); err != nil {
		t.Fatal("Prepare:", err)
	}
	if err := u.Prepare(); err != nil {
		t.Fatal("Prepare is not idempotent:", err)
	}

	select {
	case <-child.prepared:
	case <-time.After(time.Second):
		t.Fatal("Parent not notified of prepare")
	}

	if err := u.Ready(); err != nil {
		t.Fatal("Ready after Prepare:", err)
	}

	select {
	case <-child.ready:
	case <-time.After(time.Second):
		t.Fatal("Parent not notified of ready")
	}
}

func TestChildRollbackThenReadyFails(t *testing.T) {
	t.Parallel()

	u, child := newChildUpgrader(t, Options{}, nil)
	defer u.Stop()

	if err := u.Rollback(); err != nil {
		t.Fatal("Rollback:", err)
	}

	if err := u.Ready(); err != ErrRolledBack {
		t.Error("Ready after Rollback should return ErrRolledBack, got", err)
	}
	if err := u.Prepare(); err != ErrRolledBack {
		t.Error("Prepare after Rollback should return ErrRolledBack, got", err)
	}
	if err := u.Rollback(); err != ErrRolledBack {
		t.Error("Second Rollback should return ErrRolledBack, got", err)
	}

	// The parent observes the abort.
	select {
	case <-child.aborted:
	case <-time.After(time.Second):
		t.Fatal("Parent not notified of abort")
	}
}

func TestChildReadyThenRollbackFails(t *testing.T) {
	t.Parallel()

	u, child := newChildUpgrader(t, Options{}, nil)
	defer u.Stop()

	if err := u.Ready(); err != nil {
		t.Fatal("Ready:", err)
	}

	if err := u.Rollback(); err != ErrCommitted {
		t.Error("Rollback after Ready should return ErrCommitted, got", err)
	}
	if err := u.Prepare(); err != ErrCommitted {
		t.Error("Prepare after Ready should return ErrCommitted, got", err)
	}

	select {
	case <-child.ready:
	case <-time.After(time.Second):
		t.Fatal("Parent not notified of ready")
	}
}

func TestChildRollbackClosesInheritedFds(t *testing.T) {
	t.Parallel()

	u, _ := newChildUpgrader(t, Options{}, nil)
	defer u.Stop()

	r1, w1, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	defer w1.Close()

	r2, w2, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	defer w2.Close()

	// Add a file to the used set.
	if err := u.Fds.AddFile("used", w1); err != nil {
		t.Fatal(err)
	}

	// Inject a file into the inherited set.
	inheritedKey := fileName{fdKind, "inherited", ""}
	inheritedDup, err := dupFd(r2.Fd(), inheritedKey)
	if err != nil {
		t.Fatal(err)
	}
	u.Fds.inherited[inheritedKey] = inheritedDup

	usedFile := u.Fds.used[fileName{fdKind, "used", ""}]
	if usedFile == nil {
		t.Fatal("Missing file in used set")
	}

	if err := u.Rollback(); err != nil {
		t.Fatal("Rollback:", err)
	}

	if len(u.Fds.inherited) != 0 {
		t.Error("Rollback didn't close inherited fds")
	}
	if len(u.Fds.used) != 0 {
		t.Error("Rollback didn't close used fds")
	}

	// The child's own descriptors must be closed.
	if err := usedFile.Close(); !errors.Is(err, os.ErrClosed) {
		t.Error("Used fd is still open after Rollback")
	}
	if err := inheritedDup.Close(); !errors.Is(err, os.ErrClosed) {
		t.Error("Inherited fd is still open after Rollback")
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
