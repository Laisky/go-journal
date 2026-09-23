package journal

import "fmt"

var (
	// ErrNotStarted means Start has not initialized a usable journal.
	ErrNotStarted = fmt.Errorf("journal not started")
	// ErrDuringRotate rotate error
	ErrDuringRotate = fmt.Errorf("during rotating")
)
