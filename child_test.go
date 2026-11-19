package tableflip

import (
	"os"
	"testing"
)

func TestChildExit(t *testing.T) {
	env, procs := testEnv()

	child, err := startChild(env, nil)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	proc.exit(nil)
	if err := <-child.result; err != nil {
		t.Error("Wait returns non-nil error:", err)
	}
}

func TestChildKill(t *testing.T) {
	env, procs := testEnv()

	child, err := startChild(env, nil)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs

	go child.Kill()
	if sig := proc.recvSignal(nil); sig != os.Kill {
		t.Errorf("Received %v instead of os.Kill", sig)
	}

	proc.exit(nil)
}

func TestChildNotReady(t *testing.T) {
	env, procs := testEnv()

	child, err := startChild(env, nil)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	proc.exit(nil)
	<-child.result
	<-child.exited

	select {
	case <-child.ready:
		t.Error("Child signals readiness without pipe being closed")
	default:
	}
}

func TestChildReady(t *testing.T) {
	env, procs := testEnv()

	child, err := startChild(env, nil)
	if err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	if _, _, err := proc.notify(); err != nil {
		t.Fatal("Can't notify:", err)
	}
	<-child.ready
	proc.exit(nil)
}

func TestChildPassedFds(t *testing.T) {
	env, procs := testEnv()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	// Reuse the *os.File objects instead of wrapping the same
	// descriptors twice: two *os.File objects for a single fd
	// race in their finalizers and can close an unrelated,
	// recycled descriptor.
	in := map[fileName]*file{
		{"r"}: {File: r, fd: r.Fd()},
		{"w"}: {File: w, fd: w.Fd()},
	}

	if _, err := startChild(env, in); err != nil {
		t.Fatal(err)
	}

	proc := <-procs
	out, _, err := proc.notify()
	if err != nil {
		t.Fatal("Notify failed:", err)
	}

	if len(out) != len(in) {
		t.Errorf("Expected %d files, got %d", len(in), len(out))
	}

	for name := range in {
		if _, ok := out[name]; !ok {
			t.Error(name, "is missing")
		}
	}

	// The child receives its own copies of the files, referring to
	// the same underlying pipe. Check both directions.
	if _, err := w.Write([]byte{1}); err != nil {
		t.Fatal("Can't write to w:", err)
	}
	if _, err := out[fileName{"r"}].Read(make([]byte, 1)); err != nil {
		t.Error("Can't read from child's copy of r:", err)
	}

	if _, err := out[fileName{"w"}].Write([]byte{1}); err != nil {
		t.Fatal("Can't write to child's copy of w:", err)
	}
	if _, err := r.Read(make([]byte, 1)); err != nil {
		t.Error("Can't read from r:", err)
	}

	proc.exit(nil)
}
