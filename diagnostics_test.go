package tableflip

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestTailBuffer(t *testing.T) {
	t.Parallel()

	var tb tailBuffer
	if got := tb.String(); got != "" {
		t.Error("Empty buffer should produce empty string, got:", got)
	}

	if _, err := tb.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := tb.String(); got != "hello" {
		t.Error("Expected contents without truncation marker, got:", got)
	}
}

func TestTailBufferTruncation(t *testing.T) {
	t.Parallel()

	var tb tailBuffer

	// One large write: the first 10 bytes must be discarded.
	chunk := strings.Repeat("x", 10) + strings.Repeat("a", maxStderrTail)
	if _, err := tb.Write([]byte(chunk)); err != nil {
		t.Fatal(err)
	}
	// Several small writes: 5 more bytes discarded.
	if _, err := tb.Write([]byte("bcdef")); err != nil {
		t.Fatal(err)
	}

	got := tb.String()
	if !strings.Contains(got, "truncated") {
		t.Error("Expected explicit truncation marker, got:", got)
	}
	if !strings.Contains(got, "15 bytes discarded") {
		t.Error("Expected discarded byte count in marker, got:", got)
	}
	if strings.Contains(got, "x") {
		t.Error("Retained output contains discarded prefix")
	}
	if !strings.HasSuffix(got, "bcdef") {
		t.Error("Retained output lost its tail")
	}
	if tail := got[strings.LastIndex(got, "\n")+1:]; len(tail) != maxStderrTail {
		t.Errorf("Retained tail is %d bytes, want %d", len(tail), maxStderrTail)
	}
}

// crashEnv returns an env whose child process writes stderrMsg to
// its stderr while "starting up".
func crashEnv(stderrMsg string, environ []string) (*env, chan *testProcess) {
	procs := make(chan *testProcess, 1)
	return &env{
		newProc: func(_ string, _ []string, files []*os.File, envstr []string) (process, error) {
			p, err := newTestProcess(files, envstr)
			if err != nil {
				return nil, err
			}
			if _, err := files[2].WriteString(stderrMsg); err != nil {
				return nil, err
			}
			procs <- p
			return p, nil
		},
		environ:     func() []string { return environ },
		getenv:      func(string) string { return "" },
		closeOnExec: func(int) {},
	}, procs
}

// startUpgrade calls Upgrade in the background and returns the
// started child process along with the upgrade result channel.
func startUpgrade(t *testing.T, u *Upgrader, procs chan *testProcess) (*testProcess, <-chan error) {
	t.Helper()

	errs := make(chan error, 1)
	go func() {
		for {
			err := u.Upgrade()
			if err != errNotReady {
				errs <- err
				return
			}
		}
	}()

	select {
	case proc := <-procs:
		return proc, errs
	case err := <-errs:
		t.Fatal("Upgrade failed before child started:", err)
		return nil, nil
	case <-time.After(5 * time.Second):
		t.Fatal("No child started")
		return nil, nil
	}
}

func awaitUpgrade(t *testing.T, errs <-chan error) error {
	t.Helper()

	select {
	case err := <-errs:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Upgrade did not return after child exited")
		return nil
	}
}

func TestUpgradeChildCrashDiagnostics(t *testing.T) {
	t.Parallel()

	const secret = "tableflip-test-secret"
	env, procs := crashEnv("fatal: cannot allocate widget\n", []string{"SECRET=" + secret})

	u, err := newUpgrader(env, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Stop()
	if err := u.Ready(); err != nil {
		t.Fatal(err)
	}

	proc, errs := startUpgrade(t, u, procs)

	// Crash the child with exit status 3.
	proc.exit(exec.Command("sh", "-c", "exit 3").Run())

	err = awaitUpgrade(t, errs)
	if err == nil {
		t.Fatal("Expected Upgrade to fail when child crashes")
	}
	if !errors.Is(err, ErrChildExited) {
		t.Error("Expected ErrChildExited, got:", err)
	}
	if errors.Is(err, ErrChildSignaled) || errors.Is(err, ErrUpgradeTimeout) {
		t.Error("Error must not be categorised as signal or timeout:", err)
	}
	if !strings.Contains(err.Error(), "status 3") {
		t.Error("Expected real exit status in error, got:", err)
	}
	if !strings.Contains(err.Error(), "cannot allocate widget") {
		t.Error("Expected child stderr tail in error, got:", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Error("Error leaks inherited environment value:", err)
	}
}

func TestUpgradeChildSignaled(t *testing.T) {
	t.Parallel()

	env, procs := crashEnv("going down\n", nil)

	u, err := newUpgrader(env, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Stop()
	if err := u.Ready(); err != nil {
		t.Fatal(err)
	}

	proc, errs := startUpgrade(t, u, procs)

	// Kill the child with SIGTERM.
	proc.exit(exec.Command("sh", "-c", "kill -TERM $$").Run())

	err = awaitUpgrade(t, errs)
	if err == nil {
		t.Fatal("Expected Upgrade to fail when child is signaled")
	}
	if !errors.Is(err, ErrChildSignaled) {
		t.Error("Expected ErrChildSignaled, got:", err)
	}
	if errors.Is(err, ErrUpgradeTimeout) {
		t.Error("Error must not be categorised as timeout:", err)
	}
	if !strings.Contains(err.Error(), "terminated") {
		t.Error("Expected signal in error, got:", err)
	}
	if !strings.Contains(err.Error(), "going down") {
		t.Error("Expected child stderr tail in error, got:", err)
	}
}

func TestUpgradeReadyExitRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		env, procs := testEnv()

		u, err := newUpgrader(env, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := u.Ready(); err != nil {
			t.Fatal(err)
		}

		proc, errs := startUpgrade(t, u, procs)

		// The child signals readiness and exits at the same
		// time. Since readiness was signalled, the upgrade is
		// committed and must succeed.
		if _, _, err := proc.notify(); err != nil {
			t.Fatal("Can't notify:", err)
		}
		proc.exit(errors.New("child exited during handoff"))

		if err := awaitUpgrade(t, errs); err != nil {
			t.Fatalf("iteration %d: committed upgrade must succeed, got: %s", i, err)
		}

		u.Stop()
	}
}

func TestChildCleanupReleasesResources(t *testing.T) {
	t.Parallel()

	env, procs := testEnv()

	child, err := startChild(env, nil)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	proc.exit(nil)
	<-child.result

	done := make(chan struct{})
	go func() {
		child.cleanup()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not return")
	}

	select {
	case <-child.readyDone:
	default:
		t.Error("waitReady goroutine still running after cleanup")
	}
	select {
	case <-child.stderrDone:
	default:
		t.Error("stderr drainer still running after cleanup")
	}
}
