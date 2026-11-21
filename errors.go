package tableflip

import "errors"

// Error categories returned by Upgrade and WaitForParent.
//
// Use errors.Is to distinguish between them: a child that never
// became ready in time (ErrUpgradeTimeout), a child that exited
// on its own (ErrChildExited), a child that was terminated by a
// signal (ErrChildSignaled) and garbled communication between
// parent and child (ErrProtocol) all indicate different
// operational problems.
var (
	// ErrUpgradeTimeout indicates that the child process did not
	// signal readiness within Options.UpgradeTimeout and was killed.
	ErrUpgradeTimeout = errors.New("tableflip: child did not become ready in time")

	// ErrChildExited indicates that the child process exited before
	// it signalled readiness.
	ErrChildExited = errors.New("tableflip: child exited before becoming ready")

	// ErrChildSignaled indicates that the child process was
	// terminated by a signal before it signalled readiness.
	ErrChildSignaled = errors.New("tableflip: child terminated by signal")

	// ErrProtocol indicates a protocol error in the communication
	// between parent and child process.
	ErrProtocol = errors.New("tableflip: protocol error")
)
